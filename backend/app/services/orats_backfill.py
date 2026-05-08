"""ORATS historical IV backfill service.

Populates iv_history table with ~252 trading days of daily IV data per ticker
using the ORATS /datav2/hist/ivrank endpoint.  This is a one-time backfill —
after it runs, the live scanner maintains iv_history going forward via daily scans.

Triggered via POST /api/admin/backfill-iv-history.
"""
from __future__ import annotations

import logging
import time
from datetime import date, timedelta

import httpx
from sqlalchemy import text as _text
from sqlalchemy.orm import Session

from ..config import settings

logger = logging.getLogger(__name__)

_ORATS_BASE = "https://api.orats.io"
_BATCH_SIZE = 10       # ORATS max tickers per call
_RATE_SLEEP = 0.65     # seconds between calls — stays comfortably under 100 req/min
_MAX_RETRIES = 3


def _fetch_ivrank_batch(
    tickers: list[str],
    start_date: str,
    end_date: str,
) -> list[dict]:
    """Call ORATS /datav2/hist/ivrank for up to 10 tickers.

    Returns list of row dicts from the ``data`` array in the ORATS response.
    Raises RuntimeError on 401 (bad key) — caller should abort immediately.
    """
    key = settings.orats_api_key
    if not key:
        raise RuntimeError(
            "ORATS_API_KEY invalid or not loaded — check Railway env var"
        )

    params = {
        "token": key,
        "tickers": ",".join(tickers),
        "startDate": start_date,
        "endDate": end_date,
    }

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            with httpx.Client(timeout=30.0, follow_redirects=True) as client:
                resp = client.get(f"{_ORATS_BASE}/datav2/hist/ivrank", params=params)

            if resp.status_code == 401:
                logger.error(
                    "ORATS 401 Unauthorized — ORATS_API_KEY invalid or not loaded. "
                    "Check Railway env var ORATS_API_KEY."
                )
                raise RuntimeError("ORATS 401 Unauthorized — check ORATS_API_KEY")

            if resp.status_code == 429:
                wait = 60 * attempt
                logger.warning(
                    "ORATS rate limit hit — sleeping %ds (attempt %d/%d)",
                    wait, attempt, _MAX_RETRIES,
                )
                time.sleep(wait)
                continue

            resp.raise_for_status()
            payload = resp.json()
            return payload.get("data") or []

        except RuntimeError:
            raise  # propagate 401 immediately
        except (httpx.NetworkError, httpx.TimeoutException) as exc:
            logger.warning(
                "ORATS network error attempt %d/%d for %s: %s",
                attempt, _MAX_RETRIES, tickers, exc,
            )
            if attempt < _MAX_RETRIES:
                time.sleep(2 ** attempt)
            else:
                raise

    return []


def backfill_iv_history(db: Session) -> dict:
    """Backfill iv_history table with 1 year of ORATS historical IV per ticker.

    Reads the active ticker universe from DB, batches 10 tickers per ORATS call,
    and upserts results into iv_history.  Safe to re-run — idempotent via
    ON CONFLICT (ticker, recorded_date) DO UPDATE.

    Returns summary dict with counts for logging/reporting.
    """
    from ..universe import load_universe_from_db

    key = settings.orats_api_key
    if not key:
        logger.error("ORATS_API_KEY missing — cannot run IV history backfill")
        return {
            "error": "ORATS_API_KEY not set — add it to Railway environment variables",
            "tickers_processed": 0,
            "rows_written": 0,
        }

    tickers = load_universe_from_db(db)
    logger.info("ORATS backfill: starting for %d tickers", len(tickers))

    end_date = date.today()
    start_date = end_date - timedelta(days=365)
    start_str = start_date.isoformat()
    end_str = end_date.isoformat()

    rows_written = 0
    tickers_with_data: set[str] = set()
    tickers_no_data: list[str] = []
    error_batches: list[str] = []
    batches_processed = 0

    for batch_start in range(0, len(tickers), _BATCH_SIZE):
        batch = tickers[batch_start : batch_start + _BATCH_SIZE]

        try:
            rows = _fetch_ivrank_batch(batch, start_str, end_str)
        except RuntimeError:
            # 401 or missing key — abort immediately, no point continuing
            raise
        except Exception as exc:
            logger.warning("ORATS batch failed %s: %s", batch, exc)
            error_batches.extend(batch)
            time.sleep(_RATE_SLEEP)
            continue

        if not rows:
            logger.warning("ORATS returned no data for batch: %s", batch)
            tickers_no_data.extend(batch)
            time.sleep(_RATE_SLEEP)
            continue

        batch_rows = 0
        batch_tickers: set[str] = set()

        for row in rows:
            ticker = (row.get("ticker") or "").upper()
            trade_date = row.get("tradeDate")
            iv_raw = row.get("iv")

            if not ticker or not trade_date or iv_raw is None:
                continue

            try:
                iv_val = float(iv_raw)
                if iv_val <= 0:
                    continue
                # ORATS returns IV as decimal (0.25 = 25%).  If somehow > 3.0
                # (300% IV — physically impossible for normal equities), treat
                # as a percentage and normalise.
                if iv_val > 3.0:
                    iv_val = iv_val / 100.0

                db.execute(
                    _text(
                        "INSERT INTO iv_history (ticker, iv, recorded_date) "
                        "VALUES (:ticker, :iv, :date) "
                        "ON CONFLICT (ticker, recorded_date) DO UPDATE SET iv = EXCLUDED.iv"
                    ),
                    {"ticker": ticker, "iv": iv_val, "date": trade_date},
                )
                batch_tickers.add(ticker)
                batch_rows += 1

            except Exception as exc:
                logger.debug(
                    "iv_history insert failed %s %s: %s", ticker, trade_date, exc
                )

        try:
            db.commit()
        except Exception as exc:
            logger.warning("ORATS batch commit failed: %s", exc)
            db.rollback()
            batch_rows = 0

        rows_written += batch_rows
        tickers_with_data.update(batch_tickers)
        batches_processed += 1

        # Any tickers in this batch that came back empty
        missing = [t for t in batch if t not in batch_tickers]
        if missing:
            tickers_no_data.extend(missing)

        logger.info(
            "ORATS backfill: batch %d-%d done — %d rows this batch, %d total rows, %d tickers so far",
            batch_start + 1,
            batch_start + len(batch),
            batch_rows,
            rows_written,
            len(tickers_with_data),
        )

        time.sleep(_RATE_SLEEP)

    summary = {
        "tickers_processed": len(tickers_with_data),
        "rows_written": rows_written,
        "batches_processed": batches_processed,
        "tickers_no_data_count": len(tickers_no_data),
        "tickers_no_data": tickers_no_data[:50],  # cap for readability
        "error_count": len(error_batches),
    }
    logger.info(
        "ORATS backfill complete — %d tickers, %d rows written, "
        "%d tickers with no data, %d error batches",
        len(tickers_with_data),
        rows_written,
        len(tickers_no_data),
        len(error_batches),
    )
    return summary
