"""
Backfill price_history table with daily OHLCV data from yfinance.

Usage:
    python -m app.scripts.backfill_prices

Reads the ticker universe from the DB (same source as engine.py).
Downloads daily OHLCV from 2007-01-01 to today in batches of 50 tickers.
Inserts with ON CONFLICT (ticker, date) DO NOTHING — safe to re-run.
Prints progress every batch and a quality report at the end.
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path

# ── path setup so this runs as: python -m app.scripts.backfill_prices ─────────
_BACKEND_DIR = Path(__file__).resolve().parent.parent.parent
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))

import yfinance as yf
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── config ────────────────────────────────────────────────────────────────────

START_DATE   = "2007-01-01"
END_DATE     = date.today().isoformat()
BATCH_SIZE   = 50

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:uIcMzUUNlqmhekvgoKBcQxRIQOoajQyu@nozomi.proxy.rlwy.net:46336/railway",
)


# ── helpers ───────────────────────────────────────────────────────────────────

def get_engine():
    return create_engine(DATABASE_URL, pool_pre_ping=True)


def load_tickers(engine) -> list[str]:
    """Load full ticker universe from ticker_universe table."""
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT DISTINCT ticker FROM ticker_universe ORDER BY ticker")).fetchall()
    tickers = [r[0] for r in rows]
    logger.info("Universe: %d tickers from ticker_universe table", len(tickers))
    return tickers


def fetch_batch(tickers: list[str], start: str, end: str) -> dict[str, list[dict]]:
    """Download OHLCV for a batch of tickers. Returns {ticker: [row_dict, ...]}."""
    joined = " ".join(tickers)
    try:
        df = yf.download(
            joined,
            start=start,
            end=end,
            auto_adjust=False,
            progress=False,
            threads=True,
            group_by="ticker",
        )
    except Exception as exc:
        logger.error("yfinance batch download failed: %s", exc)
        return {}

    result: dict[str, list[dict]] = {}

    if len(tickers) == 1:
        # Single-ticker: flat columns (Open, High, Low, Close, Adj Close, Volume)
        ticker = tickers[0]
        rows = []
        for idx, row in df.iterrows():
            rows.append({
                "ticker": ticker,
                "date": idx.date(),
                "open": _safe_float(row.get("Open")),
                "high": _safe_float(row.get("High")),
                "low": _safe_float(row.get("Low")),
                "close": _safe_float(row.get("Close")),
                "adj_close": _safe_float(row.get("Adj Close")),
                "volume": _safe_int(row.get("Volume")),
            })
        if rows:
            result[ticker] = rows
    else:
        # Multi-ticker: MultiIndex columns (metric, ticker)
        for ticker in tickers:
            if ticker not in df.columns.get_level_values(1):
                continue
            try:
                sub = df.xs(ticker, axis=1, level=1).dropna(how="all")
            except Exception:
                continue
            rows = []
            for idx, row in sub.iterrows():
                rows.append({
                    "ticker": ticker,
                    "date": idx.date(),
                    "open": _safe_float(row.get("Open")),
                    "high": _safe_float(row.get("High")),
                    "low": _safe_float(row.get("Low")),
                    "close": _safe_float(row.get("Close")),
                    "adj_close": _safe_float(row.get("Adj Close")),
                    "volume": _safe_int(row.get("Volume")),
                })
            if rows:
                result[ticker] = rows

    return result


def _safe_float(v) -> float | None:
    try:
        f = float(v)
        return None if (f != f) else f  # NaN check
    except (TypeError, ValueError):
        return None


def _safe_int(v) -> int | None:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def insert_batch(engine, records: list[dict]) -> int:
    """Insert records with ON CONFLICT DO NOTHING. Returns rows inserted."""
    if not records:
        return 0
    sql = text(
        "INSERT INTO price_history (ticker, date, open, high, low, close, adj_close, volume, source) "
        "VALUES (:ticker, :date, :open, :high, :low, :close, :adj_close, :volume, 'yfinance') "
        "ON CONFLICT (ticker, date) DO NOTHING"
    )
    inserted = 0
    with engine.begin() as conn:
        for rec in records:
            result = conn.execute(sql, rec)
            inserted += result.rowcount
    return inserted


# ── main ──────────────────────────────────────────────────────────────────────

def run():
    engine = get_engine()
    tickers = load_tickers(engine)

    if not tickers:
        logger.error("No tickers found in ticker_universe table. Aborting.")
        sys.exit(1)

    batches = [tickers[i:i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]
    total_batches = len(batches)

    total_inserted = 0
    total_failures: list[tuple[str, str]] = []
    tickers_done = 0

    logger.info("Starting backfill: %d tickers, %d batches, %s → %s",
                len(tickers), total_batches, START_DATE, END_DATE)

    for batch_idx, batch in enumerate(batches, 1):
        logger.info("Batch %d/%d: fetching %d tickers (done=%d, failures=%d so far)...",
                    batch_idx, total_batches, len(batch), tickers_done, len(total_failures))

        data = fetch_batch(batch, START_DATE, END_DATE)

        # Track which tickers in this batch returned no data
        for ticker in batch:
            if ticker not in data:
                total_failures.append((ticker, "no data returned"))

        # Flatten all records from this batch
        all_records = [rec for rows in data.values() for rec in rows]

        try:
            inserted = insert_batch(engine, all_records)
            total_inserted += inserted
        except Exception as exc:
            logger.error("Batch %d insert failed: %s", batch_idx, exc)
            for ticker in data:
                total_failures.append((ticker, f"insert error: {exc}"))

        tickers_done += len(batch)
        logger.info("  → inserted %d rows this batch (total=%d)", inserted if 'inserted' in dir() else 0, total_inserted)

    # ── quality report ────────────────────────────────────────────────────────
    logger.info("\n%s", "=" * 60)
    logger.info("BACKFILL COMPLETE")
    logger.info("=" * 60)
    logger.info("Total rows inserted: %d", total_inserted)
    logger.info("Tickers with failures: %d", len(total_failures))
    if total_failures:
        logger.info("Failed tickers (first 20):")
        for ticker, reason in total_failures[:20]:
            logger.info("  %s: %s", ticker, reason)

    print("\n--- QUALITY CHECK QUERIES ---\n")
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM price_history")).scalar()
        print(f"Total rows:          {count:,}")

        distinct = conn.execute(text("SELECT COUNT(DISTINCT ticker) FROM price_history")).scalar()
        print(f"Distinct tickers:    {distinct:,}")

        minmax = conn.execute(text("SELECT MIN(date), MAX(date) FROM price_history")).fetchone()
        print(f"Date range:          {minmax[0]} → {minmax[1]}")

        print("\nTop 10 tickers by row count:")
        rows = conn.execute(text(
            "SELECT ticker, COUNT(*) AS rows, MIN(date), MAX(date) "
            "FROM price_history GROUP BY ticker ORDER BY rows DESC LIMIT 10"
        )).fetchall()
        for r in rows:
            print(f"  {r[0]:<8}  {r[1]:>5} rows  {r[2]} → {r[3]}")

        print("\nBottom 10 tickers by row count (sparse coverage):")
        rows = conn.execute(text(
            "SELECT ticker, COUNT(*) AS rows, MIN(date), MAX(date) "
            "FROM price_history GROUP BY ticker ORDER BY rows ASC LIMIT 10"
        )).fetchall()
        for r in rows:
            print(f"  {r[0]:<8}  {r[1]:>5} rows  {r[2]} → {r[3]}")

        sparse = conn.execute(text(
            "SELECT COUNT(DISTINCT ticker) FROM price_history "
            "GROUP BY ticker HAVING COUNT(*) < 1000"
        )).fetchall()
        print(f"\nTickers with < 1000 rows (suspicious gaps): {len(sparse)}")

        bad = conn.execute(text(
            "SELECT COUNT(*) FROM price_history WHERE close IS NULL OR volume = 0"
        )).scalar()
        print(f"Rows with NULL close or zero volume:        {bad:,}")

    print("\nDone.")


if __name__ == "__main__":
    run()
