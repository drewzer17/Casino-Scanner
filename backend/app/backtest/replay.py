"""
replay.py — Phase 4 backtesting layer.

Runs a full single-date replay across the entire ticker universe:
  reconstruct_factors → score_historical + score_path_safety → measure_outcome (all windows)
  → bulk-insert into backtest_runs + backtest_results.

Usage:
    python -m app.backtest.replay 2025-01-02
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

_BACKEND_DIR = Path(__file__).resolve().parent.parent.parent
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))

import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.backtest.factor_reconstruction import reconstruct_factors
from app.backtest.historical_scorer import score_historical
from app.backtest.outcome_measurement import measure_outcome
from app.backtest.path_safety_scorer import score_path_safety

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:uIcMzUUNlqmhekvgoKBcQxRIQOoajQyu@nozomi.proxy.rlwy.net:46336/railway",
)

DEFAULT_HOLD_DAYS   = [5, 10, 15, 21]
DEFAULT_STRIKE_PCTS = [0.0, 0.01, 0.02, 0.03]
PROGRESS_EVERY = 100


# ─────────────────────────────────────────────────────────────────────────────
# Market context helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_vix(test_date: date, db_session) -> tuple:
    """
    Return (vix_close, vix_regime) for test_date.
    Looks back up to 5 trading days to handle non-trading days.
    """
    row = db_session.execute(
        text(
            "SELECT close FROM price_history "
            "WHERE ticker = '^VIX' AND date <= :d AND close IS NOT NULL "
            "ORDER BY date DESC LIMIT 1"
        ),
        {"d": test_date},
    ).fetchone()

    if row is None:
        return None, "Unknown"

    vix = float(row[0])
    if vix < 15:
        regime = "Low"
    elif vix < 20:
        regime = "Normal"
    elif vix < 30:
        regime = "Elevated"
    else:
        regime = "High"

    return round(vix, 2), regime


def _ema(closes: list, period: int) -> Optional[float]:
    """Exponential moving average over a list of closes (oldest first)."""
    if len(closes) < period:
        return None
    k = 2.0 / (period + 1)
    val = closes[0]
    for c in closes[1:]:
        val = c * k + val * (1.0 - k)
    return val


def _get_spy_regime(test_date: date, db_session) -> dict:
    """
    Return SPY regime fields for test_date.

    Queries up to 260 closes on or before test_date to compute EMA50/EMA200
    and derive the 20-day return.

    Returns:
        spy_above_ema50   bool | None
        spy_above_ema200  bool | None
        spy_20d_return    float | None  (percentage)
    """
    rows = db_session.execute(
        text(
            "SELECT close FROM price_history "
            "WHERE ticker = 'SPY' AND date <= :d AND close IS NOT NULL "
            "ORDER BY date DESC LIMIT 260"
        ),
        {"d": test_date},
    ).fetchall()

    if not rows:
        return {"spy_above_ema50": None, "spy_above_ema200": None, "spy_20d_return": None}

    # rows are newest-first; reverse for EMA computation (oldest first)
    closes = [float(r[0]) for r in rows]
    current_close = closes[0]
    closes_asc = list(reversed(closes))

    ema50  = _ema(closes_asc, 50)
    ema200 = _ema(closes_asc, 200)

    spy_above_ema50:  Optional[bool] = (current_close > ema50)  if ema50  is not None else None
    spy_above_ema200: Optional[bool] = (current_close > ema200) if ema200 is not None else None

    spy_20d_return: Optional[float] = None
    if len(closes) > 20:
        close_20d_ago = closes[20]   # index 20 = 20 rows back in newest-first list
        if close_20d_ago and close_20d_ago != 0:
            spy_20d_return = round((current_close - close_20d_ago) / close_20d_ago * 100.0, 4)

    return {
        "spy_above_ema50":  spy_above_ema50,
        "spy_above_ema200": spy_above_ema200,
        "spy_20d_return":   spy_20d_return,
    }


def _check_earnings(ticker: str, test_date: date, hold_days: int, db_session) -> dict:
    """
    Check whether an earnings event falls within the holding window.

    end_date = test_date + hold_days * 2 calendar days
    (trading days × 2 ≈ calendar days, conservative upper bound).

    Returns:
        earnings_in_window  bool
        days_to_earnings    int | None  (calendar days from test_date to first earnings)
    """
    end_date = test_date + timedelta(days=hold_days * 2)

    rows = db_session.execute(
        text(
            "SELECT earnings_date FROM earnings_history "
            "WHERE ticker = :ticker "
            "  AND earnings_date > :start "
            "  AND earnings_date <= :end "
            "ORDER BY earnings_date ASC "
            "LIMIT 1"
        ),
        {"ticker": ticker, "start": test_date, "end": end_date},
    ).fetchall()

    if rows:
        first_date = rows[0][0]
        days_to = (first_date - test_date).days
        return {"earnings_in_window": True, "days_to_earnings": days_to}

    return {"earnings_in_window": False, "days_to_earnings": None}


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def single_date_replay(
    test_date: date,
    db_session,
    hold_days: list  = DEFAULT_HOLD_DAYS,
    strike_pcts: list = DEFAULT_STRIKE_PCTS,
) -> int:
    """
    Run a full replay for one test_date across the entire ticker universe.

    Returns the run_id of the inserted backtest_runs row.
    """
    t_start = datetime.now()
    logger.info("Replay start — test_date=%s  hold_days=%s  strike_pcts=%s",
                test_date, hold_days, strike_pcts)

    # ── Load VIX for this date ────────────────────────────────────────────────
    vix_on_entry, vix_regime = _get_vix(test_date, db_session)
    logger.info("VIX on %s: %.2f (%s)", test_date, vix_on_entry or 0, vix_regime)

    # ── SPY regime ────────────────────────────────────────────────────────────
    spy_regime = _get_spy_regime(test_date, db_session)
    logger.info(
        "SPY on %s: above_ema50=%s  above_ema200=%s  20d_return=%.2f%%",
        test_date,
        spy_regime["spy_above_ema50"],
        spy_regime["spy_above_ema200"],
        spy_regime["spy_20d_return"] or 0.0,
    )

    # ── Load universe ─────────────────────────────────────────────────────────
    rows = db_session.execute(
        text("SELECT DISTINCT ticker FROM ticker_universe WHERE active = TRUE ORDER BY ticker")
    ).fetchall()
    tickers = [r[0] for r in rows]
    total = len(tickers)
    logger.info("Universe: %d active tickers", total)

    # ── Counters ──────────────────────────────────────────────────────────────
    grade_counts = {"A": 0, "B": 0, "C": 0, "F": 0}
    graded       = 0
    skipped      = 0
    result_rows: list = []
    breadth_bullish = 0   # tickers with trend == 'bullish' (EMA20 > EMA50 proxy)
    breadth_total   = 0

    for i, ticker in enumerate(tickers, 1):
        # ── Reconstruct factors ───────────────────────────────────────────────
        try:
            factors = reconstruct_factors(ticker, test_date, db_session)
        except Exception as exc:
            logger.warning("reconstruct_factors failed %s: %s", ticker, exc)
            skipped += 1
            continue

        if factors["available_factors"] == 0:
            skipped += 1
            continue

        # ── Score (legacy + path safety) ──────────────────────────────────────
        scored    = score_historical(factors)
        ps_scored = score_path_safety(factors)

        grade = scored["grade"]
        grade_counts[grade] = grade_counts.get(grade, 0) + 1
        graded += 1

        # Track market breadth (EMA20 > EMA50 proxy = trend 'bullish')
        if factors.get("trend") is not None:
            breadth_total += 1
            if factors["trend"] == "bullish":
                breadth_bullish += 1

        entry_close = factors.get("current_close")
        if entry_close is None:
            skipped += 1
            continue

        # ── Measure outcomes — all hold_days × strike_pcts ───────────────────
        for hd in hold_days:
            # Earnings check is per hold_days window (5d vs 21d window may differ)
            earnings = _check_earnings(ticker, test_date, hd, db_session)

            for sp in strike_pcts:
                try:
                    outcome = measure_outcome(ticker, test_date, entry_close, hd, sp, db_session)
                except Exception as exc:
                    logger.warning("measure_outcome failed %s hd=%d sp=%.2f: %s", ticker, hd, sp, exc)
                    continue

                result_rows.append(
                    _to_tuple(scored, ps_scored, outcome, vix_on_entry, vix_regime,
                              spy_regime, earnings, None)
                )

        if i % PROGRESS_EVERY == 0:
            elapsed = (datetime.now() - t_start).total_seconds()
            logger.info("[%d/%d] graded=%d skipped=%d  grades=%s  %.0fs elapsed",
                        i, total, graded, skipped, grade_counts, elapsed)

    # ── Compute market breadth ────────────────────────────────────────────────
    market_breadth_50: Optional[float] = None
    if breadth_total > 0:
        market_breadth_50 = round(breadth_bullish / breadth_total * 100.0, 2)
    logger.info("Market breadth (%%above EMA50 proxy): %.1f%%  (%d/%d bullish)",
                market_breadth_50 or 0.0, breadth_bullish, breadth_total)

    # Backfill market_breadth_50 into all result rows (computed after the loop)
    result_rows = [r[:-1] + (market_breadth_50,) for r in result_rows]

    # ── Insert backtest_runs row ───────────────────────────────────────────────
    run_id = _insert_run(
        db_session, test_date, hold_days, strike_pcts,
        total, graded, grade_counts,
    )
    logger.info("Inserted backtest_runs row: run_id=%d", run_id)

    # ── Bulk-insert backtest_results ──────────────────────────────────────────
    if result_rows:
        _bulk_insert_results(result_rows, run_id)
        logger.info("Inserted %d backtest_results rows", len(result_rows))
    else:
        logger.warning("No result rows to insert — check price_history / iv_history coverage")

    elapsed_total = (datetime.now() - t_start).total_seconds()
    logger.info(
        "Replay complete — run_id=%d  graded=%d/%d  results=%d  time=%.0fs",
        run_id, graded, total, len(result_rows), elapsed_total,
    )
    return run_id


# ─────────────────────────────────────────────────────────────────────────────
# Private helpers
# ─────────────────────────────────────────────────────────────────────────────

def _to_tuple(
    scored: dict,
    ps_scored: dict,
    outcome: dict,
    vix_on_entry: Optional[float],
    vix_regime: Optional[str],
    spy_regime: dict,
    earnings: dict,
    market_breadth_50: Optional[float],
) -> tuple:
    """Flatten scored + ps_scored + outcome + market context into a flat tuple (no run_id yet)."""
    return (
        # identity / legacy grade
        scored["ticker"],
        scored["test_date"],
        scored.get("grade"),
        scored.get("vrp_state"),
        scored.get("iv_rank"),
        scored.get("vrp_spread"),
        scored.get("extension_ratio"),
        scored.get("distribution_days"),
        scored.get("trend"),
        scored.get("available_factors"),
        json.dumps(scored.get("missing_factors", [])),
        json.dumps(scored.get("fail_reasons", [])),
        # outcome — CSP
        outcome.get("strike"),
        outcome.get("strike_pct"),
        outcome.get("hold_days"),
        outcome.get("touched"),
        outcome.get("breached"),
        outcome.get("time_to_touch"),
        outcome.get("time_to_breach"),
        outcome.get("mae_pct"),
        outcome.get("mfe_pct"),
        outcome.get("final_close"),
        outcome.get("final_distance_pct"),
        outcome.get("complete"),
        # rv20 / current_iv
        scored.get("rv20"),
        scored.get("current_iv"),
        # path safety
        ps_scored.get("path_safety_grade"),
        ps_scored.get("path_safety_version"),
        ps_scored.get("threshold_profile"),
        ps_scored.get("extension_gate"),
        ps_scored.get("rv20_gate"),
        ps_scored.get("modifier_score"),
        ps_scored.get("path_pain_flag"),
        ps_scored.get("gate_ceiling"),
        # outcome — CC
        outcome.get("upside_touched"),
        outcome.get("upside_breached"),
        outcome.get("time_to_upside_touch"),
        outcome.get("time_to_upside_breach"),
        outcome.get("upside_overshoot_pct"),
        outcome.get("stock_return_pct"),
        # outcome — directional
        outcome.get("max_up_move_pct"),
        outcome.get("max_down_move_pct"),
        outcome.get("hit_5up"),
        outcome.get("hit_8up"),
        outcome.get("hit_10up"),
        outcome.get("hit_5down"),
        outcome.get("hit_8down"),
        outcome.get("hit_10down"),
        outcome.get("time_to_5up"),
        outcome.get("time_to_8up"),
        outcome.get("time_to_10up"),
        outcome.get("time_to_5down"),
        outcome.get("time_to_8down"),
        outcome.get("time_to_10down"),
        # VIX regime
        vix_on_entry,
        vix_regime,
        # SPY regime
        spy_regime.get("spy_above_ema50"),
        spy_regime.get("spy_above_ema200"),
        spy_regime.get("spy_20d_return"),
        # earnings window
        earnings.get("earnings_in_window"),
        earnings.get("days_to_earnings"),
        # market breadth (backfilled after loop)
        market_breadth_50,
    )


def _insert_run(
    db_session,
    test_date: date,
    hold_days: list,
    strike_pcts: list,
    total: int,
    graded: int,
    grade_counts: dict,
) -> int:
    result = db_session.execute(
        text(
            "INSERT INTO backtest_runs "
            "(test_date, hold_days, strike_pcts, total_tickers, graded_tickers, "
            " grade_a, grade_b, grade_c, grade_f, factors_available, parameters) "
            "VALUES (:td, :hd, :sp, :tot, :gr, :ga, :gb, :gc, :gf, 'partial_v1', :params) "
            "RETURNING id"
        ),
        {
            "td":     test_date,
            "hd":     hold_days,
            "sp":     strike_pcts,
            "tot":    total,
            "gr":     graded,
            "ga":     grade_counts.get("A", 0),
            "gb":     grade_counts.get("B", 0),
            "gc":     grade_counts.get("C", 0),
            "gf":     grade_counts.get("F", 0),
            "params": json.dumps({
                "hold_days":   hold_days,
                "strike_pcts": strike_pcts,
                "grade_basis": "partial_v1",
            }),
        },
    )
    run_id = result.fetchone()[0]
    db_session.commit()
    return run_id


def _bulk_insert_results(rows: list, run_id: int) -> None:
    """Prepend run_id and bulk-insert via psycopg2 execute_values."""
    tagged = [(run_id,) + r for r in rows]

    raw = psycopg2.connect(DATABASE_URL)
    try:
        with raw.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO backtest_results (
                    run_id, ticker, test_date, grade, vrp_state,
                    iv_rank, vrp_spread, extension_ratio, distribution_days, trend,
                    available_factors, missing_factors, fail_reasons,
                    strike, strike_pct, hold_days,
                    touched, breached, time_to_touch, time_to_breach,
                    mae_pct, mfe_pct, final_close, final_distance_pct, complete,
                    rv20, current_iv,
                    path_safety_grade, path_safety_version, threshold_profile,
                    extension_gate, rv20_gate, modifier_score, path_pain_flag, gate_ceiling,
                    upside_touched, upside_breached, time_to_upside_touch, time_to_upside_breach,
                    upside_overshoot_pct, stock_return_pct,
                    max_up_move_pct, max_down_move_pct,
                    hit_5up, hit_8up, hit_10up,
                    hit_5down, hit_8down, hit_10down,
                    time_to_5up, time_to_8up, time_to_10up,
                    time_to_5down, time_to_8down, time_to_10down,
                    vix_on_entry, vix_regime,
                    spy_above_ema50, spy_above_ema200, spy_20d_return,
                    earnings_in_window, days_to_earnings,
                    market_breadth_50
                ) VALUES %s
                """,
                tagged,
                page_size=500,
            )
        raw.commit()
    finally:
        raw.close()


def _ensure_backtest_tables(engine) -> None:
    """Create backtest tables + indexes if they don't exist; add new columns idempotently."""
    ddls = [
        # ── Core tables ───────────────────────────────────────────────────────
        """CREATE TABLE IF NOT EXISTS backtest_runs (
            id SERIAL PRIMARY KEY,
            test_date DATE NOT NULL,
            hold_days INTEGER[] NOT NULL,
            strike_pcts DOUBLE PRECISION[] NOT NULL,
            total_tickers INTEGER,
            graded_tickers INTEGER,
            grade_a INTEGER,
            grade_b INTEGER,
            grade_c INTEGER,
            grade_f INTEGER,
            factors_available TEXT DEFAULT 'partial_v1',
            parameters JSONB,
            created_at TIMESTAMP DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS backtest_results (
            id SERIAL PRIMARY KEY,
            run_id INTEGER REFERENCES backtest_runs(id),
            ticker VARCHAR(20) NOT NULL,
            test_date DATE NOT NULL,
            grade VARCHAR(1),
            vrp_state VARCHAR(20),
            iv_rank DOUBLE PRECISION,
            vrp_spread DOUBLE PRECISION,
            extension_ratio DOUBLE PRECISION,
            distribution_days INTEGER,
            trend VARCHAR(10),
            available_factors INTEGER,
            missing_factors JSONB,
            fail_reasons JSONB,
            strike DOUBLE PRECISION,
            strike_pct DOUBLE PRECISION,
            hold_days INTEGER,
            touched BOOLEAN,
            breached BOOLEAN,
            time_to_touch INTEGER,
            time_to_breach INTEGER,
            mae_pct DOUBLE PRECISION,
            mfe_pct DOUBLE PRECISION,
            final_close DOUBLE PRECISION,
            final_distance_pct DOUBLE PRECISION,
            complete BOOLEAN
        )""",
        "CREATE INDEX IF NOT EXISTS idx_backtest_results_run    ON backtest_results(run_id)",
        "CREATE INDEX IF NOT EXISTS idx_backtest_results_grade  ON backtest_results(grade)",
        "CREATE INDEX IF NOT EXISTS idx_backtest_results_ticker ON backtest_results(ticker, test_date)",
        # ── rv20 / current_iv (added in v2) ───────────────────────────────────
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS rv20 DOUBLE PRECISION",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS current_iv DOUBLE PRECISION",
        # ── Path Safety columns (v3) ──────────────────────────────────────────
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS path_safety_grade VARCHAR(2)",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS path_safety_version VARCHAR(30)",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS threshold_profile VARCHAR(30)",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS extension_gate VARCHAR(20)",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS rv20_gate VARCHAR(20)",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS modifier_score DOUBLE PRECISION",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS path_pain_flag BOOLEAN",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS gate_ceiling VARCHAR(2)",
        # ── CC outcome columns (v3) ───────────────────────────────────────────
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS upside_touched BOOLEAN",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS upside_breached BOOLEAN",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS time_to_upside_touch INTEGER",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS time_to_upside_breach INTEGER",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS upside_overshoot_pct DOUBLE PRECISION",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS stock_return_pct DOUBLE PRECISION",
        # ── Directional columns (v3) ──────────────────────────────────────────
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS max_up_move_pct DOUBLE PRECISION",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS max_down_move_pct DOUBLE PRECISION",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS hit_5up BOOLEAN",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS hit_8up BOOLEAN",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS hit_10up BOOLEAN",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS hit_5down BOOLEAN",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS hit_8down BOOLEAN",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS hit_10down BOOLEAN",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS time_to_5up INTEGER",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS time_to_8up INTEGER",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS time_to_10up INTEGER",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS time_to_5down INTEGER",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS time_to_8down INTEGER",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS time_to_10down INTEGER",
        # ── VIX regime columns (v3) ───────────────────────────────────────────
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS vix_on_entry DOUBLE PRECISION",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS vix_regime VARCHAR(20)",
        # ── SPY regime + market breadth columns (v4) ──────────────────────────
        "ALTER TABLE backtest_runs ADD COLUMN IF NOT EXISTS market_context TEXT",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS spy_above_ema50 BOOLEAN",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS spy_above_ema200 BOOLEAN",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS spy_20d_return DOUBLE PRECISION",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS market_breadth_50 DOUBLE PRECISION",
        # ── Earnings window columns (v5) ──────────────────────────────────────
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS earnings_in_window BOOLEAN",
        "ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS days_to_earnings INTEGER",
    ]
    with engine.begin() as conn:
        for ddl in ddls:
            conn.execute(text(ddl))


# ─────────────────────────────────────────────────────────────────────────────
# CLI runner
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    date_str = sys.argv[1] if len(sys.argv) > 1 else "2025-01-02"
    try:
        test_date = date.fromisoformat(date_str)
    except ValueError:
        print(f"ERROR: invalid date '{date_str}' — use YYYY-MM-DD")
        sys.exit(1)

    engine = create_engine(DATABASE_URL, pool_pre_ping=True)

    # Ensure backtest tables exist without importing models (avoids Python 3.9
    # incompatibility with `X | None` union syntax in models.py)
    _ensure_backtest_tables(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        run_id = single_date_replay(test_date, session)

        # ── Summary query ──────────────────────────────────────────────────────
        print(f"\n{'='*60}")
        print(f"REPLAY SUMMARY — test_date={test_date}  run_id={run_id}")
        print(f"{'='*60}")

        row = session.execute(
            text("SELECT total_tickers, graded_tickers, grade_a, grade_b, grade_c, grade_f "
                 "FROM backtest_runs WHERE id = :rid"),
            {"rid": run_id},
        ).fetchone()
        print(f"  Total tickers:   {row[0]}")
        print(f"  Graded:          {row[1]}")
        print(f"  Grade A:         {row[2]}")
        print(f"  Grade B:         {row[3]}")
        print(f"  Grade C:         {row[4]}")
        print(f"  Grade F:         {row[5]}")

        result_count = session.execute(
            text("SELECT COUNT(*) FROM backtest_results WHERE run_id = :rid"),
            {"rid": run_id},
        ).scalar()
        print(f"  Result rows:     {result_count:,}")

        # Legacy grade touch/breach rates
        print(f"\n  Legacy grade — Touch/Breach rates (21d ATM CSP, strike_pct=0.0):")
        rates = session.execute(text("""
            SELECT grade,
                   COUNT(*) as n,
                   ROUND(100.0 * SUM(CASE WHEN touched  THEN 1 ELSE 0 END) / COUNT(*), 1) as touch_pct,
                   ROUND(100.0 * SUM(CASE WHEN breached THEN 1 ELSE 0 END) / COUNT(*), 1) as breach_pct,
                   ROUND(AVG(mae_pct)::numeric, 2) as avg_mae
            FROM backtest_results
            WHERE run_id = :rid AND hold_days = 21 AND strike_pct = 0.0
            GROUP BY grade ORDER BY grade
        """), {"rid": run_id}).fetchall()
        print(f"  {'Grade':>5}  {'N':>5}  {'Touch%':>7}  {'Breach%':>8}  {'AvgMAE':>8}")
        for r in rates:
            print(f"  {r[0]:>5}  {r[1]:>5}  {str(r[2]):>7}  {str(r[3]):>8}  {str(r[4]):>8}")

        # Path Safety grade assignment rates
        print(f"\n  Path Safety grade — Assigned% (21d 2% OTM CSP):")
        ps_rates = session.execute(text("""
            SELECT path_safety_grade,
                   COUNT(*) as n,
                   ROUND(100.0 * SUM(CASE WHEN final_distance_pct < 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as assigned_pct,
                   ROUND(AVG(mae_pct)::numeric, 2) as avg_mae
            FROM backtest_results
            WHERE run_id = :rid AND hold_days = 21 AND strike_pct = 0.02
              AND path_safety_grade IS NOT NULL
            GROUP BY path_safety_grade ORDER BY path_safety_grade
        """), {"rid": run_id}).fetchall()
        print(f"  {'PS Grade':>8}  {'N':>5}  {'Assigned%':>10}  {'AvgMAE':>8}")
        for r in ps_rates:
            print(f"  {r[0]:>8}  {r[1]:>5}  {str(r[2]):>10}  {str(r[3]):>8}")

        print(f"\nDone.\n")
    finally:
        session.close()
