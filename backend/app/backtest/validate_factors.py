"""
validate_factors.py — Sanity-check factor_reconstruction against live scanner.

Usage:
    python -m app.backtest.validate_factors

Compares reconstructed historical factors (from price_history + iv_history)
against the most recent live scan_results row for the same tickers.
"""
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

_BACKEND_DIR = Path(__file__).resolve().parent.parent.parent
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.backtest.factor_reconstruction import reconstruct_factors

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:uIcMzUUNlqmhekvgoKBcQxRIQOoajQyu@nozomi.proxy.rlwy.net:46336/railway",
)

TEST_DATE = date(2026, 5, 20)

TICKERS = ["AAPL", "MSFT", "SMTC", "AEHR", "BE", "MOD", "ALAB", "CRDO", "POWL", "VICR"]

# Map: (display_name, recon_key, live_key, is_numeric)
FACTOR_ROWS = [
    ("IV Rank",         "iv_rank",          "iv_rank",          True),
    ("RV20",            "rv20",             "realized_vol_20d", True),
    ("VRP Spread",      "vrp_spread",       "vrp_spread",       True),
    ("VRP State",       "vrp_state",        "vrp_state",        False),
    ("EMA20",           "ema20",            "ema_20",           True),
    ("EMA50",           "ema50",            "ema_50",           True),
    ("Extension Ratio", "extension_ratio",  "extension_ratio",  True),
    ("Trend",           "trend",            None,               False),
    ("Dist Days",       "distribution_days",None,               False),
]


def pct_diff(a, b) -> float | None:
    """Percent difference relative to b."""
    if a is None or b is None:
        return None
    if b == 0:
        return None if a == 0 else float("inf")
    return abs(a - b) / abs(b) * 100.0


def match_label(recon, live, is_numeric: bool) -> str:
    if live is None:
        return "NO LIVE"
    if recon is None:
        return "NO RECON"
    if not is_numeric:
        return "EXACT" if str(recon).lower() == str(live).lower() else "MISMATCH"
    pd = pct_diff(recon, live)
    if pd is None:
        return "N/A"
    if pd < 0.1:
        return "EXACT"
    if pd < 5.0:
        return "CLOSE"
    return "MISMATCH"


def fmt_val(v, is_numeric: bool) -> str:
    if v is None:
        return "N/A"
    if is_numeric:
        return f"{float(v):.4f}"
    return str(v)


def fmt_diff(recon, live, is_numeric: bool) -> str:
    if live is None or recon is None or not is_numeric:
        return "—"
    try:
        d = float(recon) - float(live)
        return f"{d:+.4f}"
    except Exception:
        return "—"


def run():
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    Session = sessionmaker(bind=engine)
    session = Session()

    print(f"\n{'='*70}")
    print(f"FACTOR RECONSTRUCTION VALIDATION — test_date={TEST_DATE}")
    print(f"{'='*70}\n")

    # ── Fetch live scanner results ────────────────────────────────────────────
    tickers_sql = "','".join(TICKERS)
    live_rows = session.execute(text(f"""
        SELECT ticker, iv_rank, vrp_spread, ema_20, ema_50, extension_ratio,
               realized_vol_20d, risk_grade, vrp_state
        FROM scan_results
        WHERE run_id = (SELECT id FROM scan_runs ORDER BY id DESC LIMIT 1)
          AND ticker IN ('{tickers_sql}')
    """)).fetchall()

    live_map: dict[str, dict] = {}
    for r in live_rows:
        live_map[r[0]] = {
            "iv_rank":          r[1],
            "vrp_spread":       r[2],
            "ema_20":           r[3],
            "ema_50":           r[4],
            "extension_ratio":  r[5],
            "realized_vol_20d": r[6],
            "risk_grade":       r[7],
            "vrp_state":        r[8],
        }

    # ── Per-ticker comparison ─────────────────────────────────────────────────
    total_exact = total_close = total_mismatch = total_no_live = total_no_recon = 0
    mismatches: list[tuple[str, str]] = []

    col_w = [16, 14, 14, 10, 9]  # widths: Factor, Recon, Live, Diff, Match

    header = (
        f"{'Factor':<{col_w[0]}} | "
        f"{'Reconstructed':>{col_w[1]}} | "
        f"{'Live Scanner':>{col_w[2]}} | "
        f"{'Diff':>{col_w[3]}} | "
        f"{'Match?':<{col_w[4]}}"
    )
    divider = "-" * len(header)

    for ticker in TICKERS:
        print(f"TICKER: {ticker}")
        live = live_map.get(ticker, {})
        if not live:
            print("  (no live scanner row found for this ticker)\n")

        recon = reconstruct_factors(ticker, TEST_DATE, session)

        print(header)
        print(divider)

        for display, recon_key, live_key, is_numeric in FACTOR_ROWS:
            r_val = recon.get(recon_key)
            l_val = live.get(live_key) if live_key and live else None

            label = match_label(r_val, l_val, is_numeric)
            diff_str = fmt_diff(r_val, l_val, is_numeric)

            r_str = fmt_val(r_val, is_numeric)
            l_str = fmt_val(l_val, is_numeric) if live_key else "N/A"

            flag = " <<<" if label == "MISMATCH" else ""
            print(
                f"{display:<{col_w[0]}} | "
                f"{r_str:>{col_w[1]}} | "
                f"{l_str:>{col_w[2]}} | "
                f"{diff_str:>{col_w[3]}} | "
                f"{label:<{col_w[4]}}{flag}"
            )

            if label == "EXACT":        total_exact    += 1
            elif label == "CLOSE":      total_close    += 1
            elif label == "MISMATCH":   total_mismatch += 1; mismatches.append((ticker, display))
            elif label == "NO LIVE":    total_no_live  += 1
            elif label == "NO RECON":   total_no_recon += 1

        print(
            f"\n  available_factors={recon['available_factors']}/5  "
            f"missing={recon['missing_factors']}  "
            f"grade_basis={recon['grade_basis']}\n"
        )

    # ── Summary ───────────────────────────────────────────────────────────────
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"  EXACT    : {total_exact}")
    print(f"  CLOSE    : {total_close}")
    print(f"  MISMATCH : {total_mismatch}")
    print(f"  NO LIVE  : {total_no_live}  (factor exists in recon, not in live scanner)")
    print(f"  NO RECON : {total_no_recon}  (factor in live scanner, missing in recon)")

    if mismatches:
        print(f"\n{'!'*70}")
        print("  MISMATCHES DETECTED:")
        for ticker, factor in mismatches:
            print(f"    {ticker} — {factor}")
        print(f"{'!'*70}")
    else:
        print("\n  No mismatches. All comparable factors within tolerance.")

    session.close()


if __name__ == "__main__":
    run()
