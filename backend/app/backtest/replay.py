"""
replay.py — Phase 4 backtesting layer.

Runs a full single-date replay across the entire ticker universe:
  reconstruct_factors → score_historical → measure_outcome (all windows)
  → bulk-insert into backtest_runs + backtest_results.

Usage:
    python -m app.backtest.replay 2025-01-02
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path

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

DEFAULT_HOLD_DAYS  = [5, 10, 15, 21]
DEFAULT_STRIKE_PCTS = [0.0, 0.01, 0.02, 0.03]
PROGRESS_EVERY = 100


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def single_date_replay(
    test_date: date,
    db_session,
    hold_days: list[int]   = DEFAULT_HOLD_DAYS,
    strike_pcts: list[float] = DEFAULT_STRIKE_PCTS,
) -> int:
    """
    Run a full replay for one test_date across the entire ticker universe.

    Returns the run_id of the inserted backtest_runs row.
    """
    t_start = datetime.now()
    logger.info("Replay start — test_date=%s  hold_days=%s  strike_pcts=%s",
                test_date, hold_days, strike_pcts)

    # ── Load universe ─────────────────────────────────────────────────────────
    rows = db_session.execute(
        text("SELECT DISTINCT ticker FROM ticker_universe WHERE active = TRUE ORDER BY ticker")
    ).fetchall()
    tickers = [r[0] for r in rows]
    total = len(tickers)
    logger.info("Universe: %d active tickers", total)

    # ── Counters ──────────────────────────────────────────────────────────────
    grade_counts = {"A": 0, "B": 0, "C": 0, "F": 0}
    graded = 0
    skipped = 0
    result_rows: list[tuple] = []

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

        # ── Score ─────────────────────────────────────────────────────────────
        scored = score_historical(factors)
        grade = scored["grade"]
        grade_counts[grade] = grade_counts.get(grade, 0) + 1
        graded += 1

        entry_close = factors.get("current_close")
        if entry_close is None:
            skipped += 1
            continue

        # ── Measure outcomes — all hold_days × strike_pcts ───────────────────
        for hd in hold_days:
            for sp in strike_pcts:
                try:
                    outcome = measure_outcome(ticker, test_date, entry_close, hd, sp, db_session)
                except Exception as exc:
                    logger.warning("measure_outcome failed %s hd=%d sp=%.2f: %s", ticker, hd, sp, exc)
                    continue

                result_rows.append(_to_tuple(scored, outcome))

        if i % PROGRESS_EVERY == 0:
            elapsed = (datetime.now() - t_start).total_seconds()
            logger.info("[%d/%d] graded=%d skipped=%d  grades=%s  %.0fs elapsed",
                        i, total, graded, skipped, grade_counts, elapsed)

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

def _to_tuple(scored: dict, outcome: dict) -> tuple:
    """Flatten scored + outcome into a flat tuple for bulk insert (no run_id yet)."""
    return (
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
        scored.get("rv20"),
        scored.get("current_iv"),
    )


def _insert_run(
    db_session,
    test_date: date,
    hold_days: list[int],
    strike_pcts: list[float],
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


def _bulk_insert_results(rows: list[tuple], run_id: int) -> None:
    """Prepend run_id and bulk-insert via psycopg2 execute_values."""
    # Prepend run_id to every row
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
                    rv20, current_iv
                ) VALUES %s
                """,
                tagged,
                page_size=500,
            )
        raw.commit()
    finally:
        raw.close()


def _ensure_backtest_tables(engine) -> None:
    """Create backtest tables + indexes if they don't exist (idempotent)."""
    ddls = [
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

        # Touch/breach rates by grade for 21-day ATM
        print(f"\n  Touch/Breach rates (21d ATM CSP, strike_pct=0.0):")
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

        print(f"\nDone.\n")
    finally:
        session.close()
