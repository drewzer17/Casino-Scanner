"""
backfill_sectors.py — Populate sector + industry in ticker_universe from yfinance.

Usage:
    cd backend && python -m app.scripts.backfill_sectors

ETFs return None for sector — they are silently skipped (sector left NULL).
Safe to re-run: uses UPDATE, so duplicate runs are idempotent.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

_BACKEND_DIR = Path(__file__).resolve().parent.parent.parent
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))

import psycopg2
import yfinance as yf

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:uIcMzUUNlqmhekvgoKBcQxRIQOoajQyu@nozomi.proxy.rlwy.net:46336/railway",
)

BATCH_SIZE    = 5
SLEEP_BETWEEN = 1.0   # seconds between batches
PROGRESS_EVERY = 100


def backfill_sectors() -> None:
    conn = psycopg2.connect(DATABASE_URL)
    try:
        # Ensure columns exist (idempotent)
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE ticker_universe ADD COLUMN IF NOT EXISTS sector VARCHAR(50)")
            cur.execute("ALTER TABLE ticker_universe ADD COLUMN IF NOT EXISTS industry VARCHAR(100)")
            cur.execute("ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS sector VARCHAR(50)")
        conn.commit()
        print("Schema columns confirmed.")

        # Load all tickers that still need sector data
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ticker FROM ticker_universe WHERE active = TRUE ORDER BY ticker"
            )
            tickers = [r[0] for r in cur.fetchall()]

        total        = len(tickers)
        found        = 0
        no_sector    = 0
        failed       = 0

        print(f"Backfilling sectors for {total} tickers...")

        for batch_start in range(0, total, BATCH_SIZE):
            batch = tickers[batch_start : batch_start + BATCH_SIZE]

            for symbol in batch:
                try:
                    info   = yf.Ticker(symbol).info
                    sector   = info.get("sector")    or None
                    industry = info.get("industry")  or None

                    if sector is None:
                        # ETF or data gap — leave NULL, don't overwrite with garbage
                        no_sector += 1
                    else:
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE ticker_universe SET sector = %s, industry = %s WHERE ticker = %s",
                                (sector, industry, symbol),
                            )
                        conn.commit()
                        found += 1

                except Exception as exc:
                    failed += 1
                    # Don't print every failure — too noisy for bad tickers
                    pass

            processed = min(batch_start + BATCH_SIZE, total)
            if processed % PROGRESS_EVERY == 0 or processed == total:
                print(
                    f"  [{processed}/{total}]  sector_found={found}  "
                    f"no_sector={no_sector}  failed={failed}"
                )

            if batch_start + BATCH_SIZE < total:
                time.sleep(SLEEP_BETWEEN)

        print(
            f"\nDone. total={total}  sector_found={found}  "
            f"no_sector={no_sector}  failed={failed}"
        )

    finally:
        conn.close()


if __name__ == "__main__":
    backfill_sectors()
