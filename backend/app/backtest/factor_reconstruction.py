"""
factor_reconstruction.py — Phase 4 backtesting layer.

Reconstructs all available scanner factors for a given ticker on a given
historical date using ONLY data on or before that date (no look-ahead).

Primary entry point:
    reconstruct_factors(ticker, test_date, db_session) -> dict

Uses raw SQL via db_session.execute() — no pandas, stays lightweight.
"""
from __future__ import annotations

import math
from datetime import date
from typing import Optional

from sqlalchemy import text


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def reconstruct_factors(ticker: str, test_date: date, db_session) -> dict:
    """
    Reconstruct all available factors for `ticker` as of `test_date`.

    All queries are bounded to data <= test_date — no look-ahead.
    Returns a flat dict ready for analysis or DB storage.
    """
    # ── 1. IV Rank ────────────────────────────────────────────────────────────
    current_iv, iv_rank = _compute_iv_rank(ticker, test_date, db_session)

    # ── 2. Realized Volatility 20-day ─────────────────────────────────────────
    rv20 = _compute_rv20(ticker, test_date, db_session)

    # ── 3. VRP Spread ─────────────────────────────────────────────────────────
    vrp_spread, vrp_state = _compute_vrp(current_iv, rv20)

    # ── 4. EMA 20 / EMA 50 ────────────────────────────────────────────────────
    current_close, ema20, ema50, trend = _compute_emas(ticker, test_date, db_session)

    # ── 5. Extension Ratio ────────────────────────────────────────────────────
    extension_ratio, extension_label = _compute_extension(current_close, ema50)

    # ── 6. Distribution Days ──────────────────────────────────────────────────
    distribution_days = _compute_distribution_days(ticker, test_date, db_session)

    # ── Availability accounting ───────────────────────────────────────────────
    factor_map = {
        "iv_rank":           iv_rank,
        "vrp":               vrp_spread,
        "trend":             trend,
        "extension":         extension_ratio,
        "distribution_days": distribution_days,
    }
    missing = [k for k, v in factor_map.items() if v is None]
    available = len(factor_map) - len(missing)

    return {
        "ticker":            ticker,
        "test_date":         test_date,
        "current_iv":        current_iv,
        "iv_rank":           iv_rank,
        "rv20":              rv20,
        "vrp_spread":        vrp_spread,
        "vrp_state":         vrp_state,
        "ema20":             ema20,
        "ema50":             ema50,
        "trend":             trend,
        "extension_ratio":   extension_ratio,
        "extension_label":   extension_label,
        "distribution_days": distribution_days,
        "current_close":     current_close,
        "available_factors": available,
        "missing_factors":   missing,
        "grade_basis":       "partial_v1",
    }


def reconstruct_batch(tickers: list, test_date: date, db_session) -> list:
    """Reconstruct factors for a list of tickers on the same test_date."""
    return [reconstruct_factors(t, test_date, db_session) for t in tickers]


def sanity_report(results: list) -> dict:
    """
    Summarise a list of reconstruct_factors() results.

    Returns counts per factor: how many tickers have it vs. are missing it,
    plus a list of tickers with zero available factors.
    """
    if not results:
        return {"total": 0}

    factor_names = ["iv_rank", "vrp", "trend", "extension", "distribution_days"]
    available_counts = {f: 0 for f in factor_names}
    missing_counts   = {f: 0 for f in factor_names}
    zero_factor_tickers = []

    for r in results:
        missing_set = set(r.get("missing_factors", []))
        for f in factor_names:
            if f in missing_set:
                missing_counts[f] += 1
            else:
                available_counts[f] += 1
        if r.get("available_factors", 0) == 0:
            zero_factor_tickers.append(r["ticker"])

    return {
        "total":               len(results),
        "available_counts":    available_counts,
        "missing_counts":      missing_counts,
        "zero_factor_tickers": zero_factor_tickers,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Private helpers
# ─────────────────────────────────────────────────────────────────────────────

def _compute_iv_rank(
    ticker: str, test_date: date, db_session
) -> tuple[Optional[float], Optional[float]]:
    """
    Return (current_iv, iv_rank).

    Fetches up to 253 rows from iv_history (<= test_date).
    current_iv = most recent row.
    iv_rank uses the prior 252 rows as the 52-week window.
    Returns (None, None) if fewer than 60 rows available.
    """
    rows = db_session.execute(
        text(
            "SELECT iv FROM iv_history "
            "WHERE ticker = :ticker AND recorded_date <= :dt "
            "ORDER BY recorded_date DESC "
            "LIMIT 253"
        ),
        {"ticker": ticker, "dt": test_date},
    ).fetchall()

    if not rows:
        return None, None

    current_iv = float(rows[0][0])

    if len(rows) < 60:
        return current_iv, None

    window = [float(r[0]) for r in rows[1:]]  # prior 252 rows
    min_252 = min(window)
    max_252 = max(window)

    if max_252 == min_252:
        iv_rank = 50.0
    else:
        iv_rank = (current_iv - min_252) / (max_252 - min_252) * 100.0

    return current_iv, round(iv_rank, 2)


def _compute_rv20(
    ticker: str, test_date: date, db_session
) -> Optional[float]:
    """
    20-day annualised realised volatility as a percentage.

    Needs 21 closing prices (to produce 20 log-returns).
    Returns None if insufficient data.
    """
    rows = db_session.execute(
        text(
            "SELECT close FROM price_history "
            "WHERE ticker = :ticker AND date <= :dt AND close IS NOT NULL "
            "ORDER BY date DESC "
            "LIMIT 21"
        ),
        {"ticker": ticker, "dt": test_date},
    ).fetchall()

    if len(rows) < 21:
        return None

    closes = [float(r[0]) for r in rows]  # newest first

    log_returns = []
    for i in range(20):
        if closes[i + 1] <= 0 or closes[i] <= 0:
            continue
        log_returns.append(math.log(closes[i] / closes[i + 1]))

    if len(log_returns) < 20:
        return None

    mean = sum(log_returns) / len(log_returns)
    variance = sum((x - mean) ** 2 for x in log_returns) / (len(log_returns) - 1)
    rv20 = math.sqrt(variance) * math.sqrt(252) * 100.0
    return round(rv20, 4)


def _compute_vrp(
    current_iv: Optional[float], rv20: Optional[float]
) -> tuple[Optional[float], Optional[str]]:
    """
    VRP spread = current_iv - rv20.
    Classification: Rich >15, Moderate 5-15, Weak 0-5, Negative <0.
    """
    if current_iv is None or rv20 is None:
        return None, None

    spread = round(current_iv - rv20, 4)

    if spread > 15:
        state = "Rich"
    elif spread >= 5:
        state = "Moderate"
    elif spread >= 0:
        state = "Weak"
    else:
        state = "Negative"

    return spread, state


def _compute_emas(
    ticker: str, test_date: date, db_session
) -> tuple[Optional[float], Optional[float], Optional[float], Optional[str]]:
    """
    Return (current_close, ema20, ema50, trend).

    Fetches all price_history rows <= test_date in ascending order.
    EMA50 requires at least 50 rows; EMA20 requires at least 20.
    trend = 'bullish' if ema20 > ema50, else 'bearish'.
    """
    rows = db_session.execute(
        text(
            "SELECT close FROM price_history "
            "WHERE ticker = :ticker AND date <= :dt AND close IS NOT NULL "
            "ORDER BY date ASC"
        ),
        {"ticker": ticker, "dt": test_date},
    ).fetchall()

    if not rows:
        return None, None, None, None

    closes = [float(r[0]) for r in rows]
    n = len(closes)
    current_close = closes[-1]

    ema20 = _ema_series(closes, 20) if n >= 20 else None
    ema50 = _ema_series(closes, 50) if n >= 50 else None

    if ema20 is not None and ema50 is not None:
        trend = "bullish" if ema20 > ema50 else "bearish"
    else:
        trend = None

    return (
        round(current_close, 6),
        round(ema20, 6) if ema20 is not None else None,
        round(ema50, 6) if ema50 is not None else None,
        trend,
    )


def _ema_series(closes: list[float], span: int) -> float:
    """
    Compute EMA over the full closes list using the given span.
    Seed = simple average of the first `span` values.
    """
    mult = 2.0 / (span + 1)
    ema = sum(closes[:span]) / span
    for price in closes[span:]:
        ema = price * mult + ema * (1 - mult)
    return ema


def _compute_extension(
    current_close: Optional[float], ema50: Optional[float]
) -> tuple[Optional[float], Optional[str]]:
    """
    extension_ratio = current_close / ema50.
    Labels: parabolic >= 1.50, extended >= 1.30, normal otherwise.
    """
    if current_close is None or ema50 is None or ema50 == 0:
        return None, None

    ratio = round(current_close / ema50, 6)

    if ratio >= 1.50:
        label = "parabolic"
    elif ratio >= 1.30:
        label = "extended"
    else:
        label = "normal"

    return ratio, label


def _compute_distribution_days(
    ticker: str, test_date: date, db_session
) -> Optional[int]:
    """
    Count distribution days in the 25 most recent trading days on or before
    test_date.

    A distribution day = close < prior_day_close AND volume > 20-day avg volume.

    Fetches 45 rows to give enough runway for both the 25-day observation
    window and the 20-day volume average lookback.
    Returns None if fewer than 26 rows available.
    """
    rows = db_session.execute(
        text(
            "SELECT date, close, volume FROM price_history "
            "WHERE ticker = :ticker AND date <= :dt "
            "  AND close IS NOT NULL AND volume IS NOT NULL "
            "ORDER BY date DESC "
            "LIMIT 45"
        ),
        {"ticker": ticker, "dt": test_date},
    ).fetchall()

    # Need at least 26 rows: 25 observation days + 1 prior-day close for day 1,
    # plus extra for the 20-day volume average.
    if len(rows) < 26:
        return None

    # rows is newest-first; reverse to chronological order
    rows = list(reversed(rows))

    distribution_days = 0
    # Observe the 25 most recent days = rows[-25:]
    # For each day at index i (in the reversed list), we need:
    #   prior_day_close = rows[i-1].close
    #   20-day avg volume = mean of rows[i-20 : i].volume  (up to 20 prior days)
    obs_start = len(rows) - 25

    for i in range(obs_start, len(rows)):
        if i == 0:
            continue  # no prior day

        prior_close = float(rows[i - 1][1])
        current_close_day = float(rows[i][1])
        current_volume = float(rows[i][2])

        # 20-day average volume: up to 20 rows immediately before day i
        vol_window_start = max(0, i - 20)
        vol_window = [float(rows[j][2]) for j in range(vol_window_start, i)]

        if not vol_window:
            continue

        avg_volume = sum(vol_window) / len(vol_window)

        if current_close_day < prior_close and current_volume > avg_volume:
            distribution_days += 1

    return distribution_days
