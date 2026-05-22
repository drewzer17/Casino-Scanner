"""
backfill_earnings.py — Backfill historical earnings dates from yfinance.

Creates earnings_history table and populates it with past earnings dates
for all tickers in ticker_universe. Future estimated dates are excluded
to prevent look-ahead bias in backtesting.

Usage:
    cd backend && python -m app.scripts.backfill_earnings
"""
from __future__ import annotations

import os
import sys
import time
from datetime import date
from pathlib import Path

_BACKEND_DIR = Path(__file__).resolve().parent.parent.parent
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))

import psycopg2
import psycopg2.extras
import yfinance as yf
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:uIcMzUUNlqmhekvgoKBcQxRIQOoajQyu@nozomi.proxy.rlwy.net:46336/railway",
)

BATCH_SIZE     = 5
SLEEP_BETWEEN  = 1.0   # seconds between batches
PROGRESS_EVERY = 50
TODAY          = date.today()


# ─────────────────────────────────────────────────────────────────────────────
# Schema
# ─────────────────────────────────────────────────────────────────────────────

def _ensure_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS earnings_history (
                id           SERIAL PRIMARY KEY,
                ticker       VARCHAR(20) NOT NULL,
                earnings_date DATE NOT NULL,
                source       VARCHAR(20) DEFAULT 'yfinance',
                created_at   TIMESTAMP DEFAULT NOW(),
                UNIQUE(ticker, earnings_date)
            )
        """))
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_earnings_history_ticker_date "
            "ON earnings_history(ticker, earnings_date)"
        ))
    print("Table earnings_history ready.")


# ─────────────────────────────────────────────────────────────────────────────
# Fetch
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_earnings(ticker: str) -> list:
    """
    Return list of past earnings dates (as date objects) for ticker.
    Excludes any date > TODAY to prevent look-ahead bias.
    Returns empty list on failure.
    """
    try:
        ed = yf.Ticker(ticker).earnings_dates
        if ed is None or len(ed) == 0:
            return []
        dates = []
        for ts in ed.index:
            try:
                d = ts.date() if hasattr(ts, "date") else ts.to_pydatetime().date()
                if d <= TODAY:
                    dates.append(d)
            except Exception:
                continue
        return dates
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Insert
# ─────────────────────────────────────────────────────────────────────────────

def _bulk_insert(raw_conn, records: list) -> int:
    """
    Bulk-insert (ticker, earnings_date) pairs.
    ON CONFLICT DO NOTHING — safe to re-run.
    Returns number of rows actually inserted.
    """
    if not records:
        return 0
    with raw_conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO earnings_history (ticker, earnings_date, source)
            VALUES %s
            ON CONFLICT (ticker, earnings_date) DO NOTHING
            """,
            records,
            page_size=500,
        )
        inserted = cur.rowcount
    raw_conn.commit()
    return inserted


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    _ensure_table(engine)

    # Load universe
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT DISTINCT ticker FROM ticker_universe WHERE active = TRUE ORDER BY ticker")
        ).fetchall()
    tickers = [r[0] for r in rows]
    total = len(tickers)
    print(f"Universe: {total} active tickers")

    raw = psycopg2.connect(DATABASE_URL)
    try:
        processed   = 0
        inserted    = 0
        failures    = 0
        no_data     = 0

        for batch_start in range(0, total, BATCH_SIZE):
            batch = tickers[batch_start: batch_start + BATCH_SIZE]
            records: list = []

            for ticker in batch:
                dates = _fetch_earnings(ticker)
                if not dates:
                    no_data += 1
                else:
                    for d in dates:
                        records.append((ticker, d, "yfinance"))
                processed += 1

            n = _bulk_insert(raw, records)
            inserted += n

            if processed % PROGRESS_EVERY == 0 or processed == total:
                print(f"  [{processed}/{total}]  inserted={inserted:,}  no_data={no_data}  failures={failures}")

            if batch_start + BATCH_SIZE < total:
                time.sleep(SLEEP_BETWEEN)

    finally:
        raw.close()

    print(f"\nDone.")
    print(f"  Tickers processed : {processed:,}")
    print(f"  Earnings rows inserted : {inserted:,}")
    print(f"  No data / empty  : {no_data:,}")
    print(f"  Failures         : {failures:,}")

    # Verify
    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT COUNT(*) as total_rows, COUNT(DISTINCT ticker) as tickers, "
            "MIN(earnings_date), MAX(earnings_date) FROM earnings_history"
        )).fetchone()
    print(f"\nVerification:")
    print(f"  Total rows   : {row[0]:,}")
    print(f"  Tickers      : {row[1]:,}")
    print(f"  Date range   : {row[2]} → {row[3]}")


if __name__ == "__main__":
    main()
