"""
Batch replay runner — chains multiple replay dates unattended.
Usage: cd backend && python -m app.backtest.batch_replay
"""

import os
import sys
from datetime import date
from pathlib import Path

_BACKEND_DIR = Path(__file__).resolve().parent.parent.parent
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))

from sqlalchemy import create_engine
from app.backtest.replay import single_date_replay, _ensure_backtest_tables

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:uIcMzUUNlqmhekvgoKBcQxRIQOoajQyu@nozomi.proxy.rlwy.net:46336/railway",
)

# Phase 1: Targeted regime-diverse dates (run these first)
TARGETED_DATES = [
    date(2022, 6, 1),    # Rate hike bear, deep stress
    date(2022, 10, 3),   # Bear market bottom, peak pessimism (Oct 1 is Saturday)
    date(2023, 1, 3),    # Early recovery from 2022 bear
    date(2023, 11, 1),   # AI rally ignition, strong momentum
    date(2024, 8, 1),    # August VIX spike, Japan carry trade unwind
    date(2024, 11, 1),   # Post-election Trump rally, euphoria
    date(2025, 4, 1),    # Tariff escalation deepening
    date(2020, 3, 16),   # COVID crash, extreme tail event
]

# Phase 2: Holdback validation (run AFTER probability engine is built)
HOLDBACK_DATES = [
    date(2023, 4, 3),    # SVB/regional bank crisis aftermath
    date(2024, 2, 1),    # Mid-cycle, digesting rate expectations
    date(2025, 3, 3),    # Different tariff phase
    date(2023, 8, 1),    # Late summer quiet
]

# Phase 3: Monthly backfill for density (run after panel is live)
# Fill remaining months from Jan 2023 - Dec 2025 not already covered
# Will be generated programmatically later


def run_batch(dates_to_run: list) -> None:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    _ensure_backtest_tables(engine)

    total = len(dates_to_run)
    completed = 0
    failed = []

    for i, d in enumerate(dates_to_run, 1):
        print(f"\n{'='*60}")
        print(f"BATCH: Starting date {i}/{total}: {d}")
        print(f"{'='*60}")
        try:
            from sqlalchemy.orm import sessionmaker
            Session = sessionmaker(bind=engine)
            session = Session()
            try:
                run_id = single_date_replay(d, session)
                print(f"BATCH: Completed {d} -> run_id={run_id}")
                completed += 1
            finally:
                session.close()
        except Exception as e:
            print(f"BATCH: FAILED {d} -> {e}")
            failed.append((d, str(e)))
            continue

    print(f"\n{'='*60}")
    print(f"BATCH COMPLETE: {completed}/{total} dates succeeded")
    if failed:
        print(f"FAILED dates ({len(failed)}):")
        for d, err in failed:
            print(f"  {d}: {err}")
    print(f"{'='*60}")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "targeted"
    if mode == "targeted":
        print("Running Phase 1: Targeted regime dates")
        run_batch(TARGETED_DATES)
    elif mode == "holdback":
        print("Running Phase 2: Holdback validation dates")
        run_batch(HOLDBACK_DATES)
    elif mode == "all":
        print("Running Phase 1 + Phase 2")
        run_batch(TARGETED_DATES + HOLDBACK_DATES)
    else:
        print(f"Unknown mode: {mode}. Use targeted, holdback, or all")
        sys.exit(1)
