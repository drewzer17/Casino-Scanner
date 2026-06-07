"""
update_prices.py — One-time / on-demand price history updater.

Fetches missing daily OHLCV rows from yfinance for every ticker already
present in price_history and inserts them.  Safe to re-run: only rows
newer than the stored MAX(date) are inserted.
"""

import logging
import time
from datetime import date, timedelta

logger = logging.getLogger(__name__)


def update_price_history() -> dict:
    """Download and insert missing price_history rows for all tracked tickers.

    Returns a summary dict: {"updated": int, "rows_inserted": int, "skipped": int, "failed": list}
    """
    import yfinance as yf
    from sqlalchemy import text
    from .database import SessionLocal

    db = SessionLocal()
    today = date.today()
    # Consider "current" if max date is today or yesterday (market may be closed)
    cutoff = today - timedelta(days=1)

    summary = {"updated": 0, "rows_inserted": 0, "skipped": 0, "failed": []}

    try:
        # 1. Distinct tickers in price_history
        rows = db.execute(text("SELECT DISTINCT ticker FROM price_history ORDER BY ticker")).fetchall()
        tickers = [r[0] for r in rows]
        total = len(tickers)
        logger.info("update_price_history: %d tickers to check", total)

        BATCH = 100

        for batch_start in range(0, total, BATCH):
            batch = tickers[batch_start: batch_start + BATCH]
            logger.info(
                "update_price_history: batch %d-%d / %d",
                batch_start + 1, batch_start + len(batch), total,
            )

            for ticker in batch:
                try:
                    # 2. Most recent stored date
                    row = db.execute(
                        text("SELECT MAX(date) FROM price_history WHERE ticker = :t"),
                        {"t": ticker},
                    ).fetchone()
                    last_date = row[0] if row and row[0] else None

                    if last_date and last_date >= cutoff:
                        summary["skipped"] += 1
                        continue  # already current

                    fetch_start = (last_date + timedelta(days=1)) if last_date else date(2020, 1, 1)

                    # 3. Download from yfinance
                    df = yf.download(
                        ticker,
                        start=fetch_start.isoformat(),
                        end=(today + timedelta(days=1)).isoformat(),  # end is exclusive
                        auto_adjust=True,
                        progress=False,
                        threads=False,
                    )

                    if df is None or df.empty:
                        summary["skipped"] += 1
                        continue

                    # Flatten MultiIndex columns if present (single-ticker download may produce them)
                    if hasattr(df.columns, "levels"):
                        df.columns = df.columns.get_level_values(0)

                    inserted = 0
                    for idx, row_data in df.iterrows():
                        row_date = idx.date() if hasattr(idx, "date") else idx

                        # Skip rows not newer than what we already have
                        if last_date and row_date <= last_date:
                            continue

                        try:
                            db.execute(
                                text(
                                    "INSERT INTO price_history "
                                    "(ticker, date, open, high, low, close, adj_close, volume, source) "
                                    "VALUES (:ticker, :date, :open, :high, :low, :close, :adj_close, :volume, 'yfinance') "
                                    "ON CONFLICT DO NOTHING"
                                ),
                                {
                                    "ticker":    ticker,
                                    "date":      row_date,
                                    "open":      float(row_data.get("Open",  row_data.iloc[0])),
                                    "high":      float(row_data.get("High",  row_data.iloc[1])),
                                    "low":       float(row_data.get("Low",   row_data.iloc[2])),
                                    "close":     float(row_data.get("Close", row_data.iloc[3])),
                                    "adj_close": float(row_data.get("Close", row_data.iloc[3])),
                                    "volume":    int(row_data.get("Volume", 0) or 0),
                                },
                            )
                            inserted += 1
                        except Exception as row_exc:
                            logger.warning(
                                "update_price_history: row insert failed %s %s: %s",
                                ticker, row_date, row_exc,
                            )

                    if inserted:
                        db.commit()
                        summary["updated"] += 1
                        summary["rows_inserted"] += inserted
                        logger.debug(
                            "update_price_history: %s +%d rows (last=%s)",
                            ticker, inserted, row_date,
                        )
                    else:
                        summary["skipped"] += 1

                except Exception as exc:
                    logger.warning("update_price_history: %s failed: %s", ticker, exc)
                    summary["failed"].append(ticker)
                    try:
                        db.rollback()
                    except Exception:
                        pass

            # 7. Pause between batches to avoid rate-limiting
            if batch_start + BATCH < total:
                time.sleep(2)

    except Exception as outer_exc:
        logger.exception("update_price_history: outer failure: %s", outer_exc)
    finally:
        db.close()

    logger.info(
        "update_price_history complete: updated=%d rows_inserted=%d skipped=%d failed=%d",
        summary["updated"], summary["rows_inserted"], summary["skipped"], len(summary["failed"]),
    )
    if summary["failed"]:
        logger.warning("update_price_history: failed tickers: %s", summary["failed"])

    return summary
