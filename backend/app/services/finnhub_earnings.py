"""Finnhub earnings calendar integration.

One API call fetches the entire market's earnings dates for the next
90 days.  Results are stored in the ``earnings_calendar`` PostgreSQL
table.  The cache is never refreshed automatically; use the
POST /api/refresh-earnings endpoint (or the "Run Earnings" UI button)
to populate or update it on demand.

Public entry points
-------------------
get_earnings_lookup(db)
    Read-only.  Returns {TICKER: days_until_earnings} for all tickers
    with an upcoming earnings date in the cache.

refresh_earnings_cache(db)
    Force-fetch from Finnhub and upsert the full result into the DB.
    Called by the POST /api/refresh-earnings route.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

import httpx
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..config import settings

logger = logging.getLogger(__name__)

_FINNHUB_BASE = "https://finnhub.io/api/v1"
_DAYS_AHEAD = 90


# ── Finnhub fetch ──────────────────────────────────────────────────────────────

def _fetch_from_finnhub(days_ahead: int = _DAYS_AHEAD) -> dict[str, date]:
    """Call Finnhub /calendar/earnings for today → today+days_ahead.

    Returns {TICKER: next_earnings_date}.  Returns {} on any error so
    the caller can decide how to handle a missing response.
    """
    api_key = settings.finnhub_api_key
    if not api_key:
        logger.warning("FINNHUB_API_KEY not set — skipping earnings fetch")
        return {}

    today = date.today()
    to_date = today + timedelta(days=days_ahead)
    params = {
        "from":  today.isoformat(),
        "to":    to_date.isoformat(),
        "token": api_key,
    }

    try:
        resp = httpx.get(
            f"{_FINNHUB_BASE}/calendar/earnings",
            params=params,
            timeout=20.0,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("Finnhub earnings fetch failed: %s", exc)
        return {}

    events = data.get("earningsCalendar") or []
    result: dict[str, date] = {}
    for ev in events:
        sym = (ev.get("symbol") or "").upper().strip()
        date_str = ev.get("date") or ""
        if not sym or not date_str:
            continue
        try:
            earn_date = date.fromisoformat(date_str)
        except ValueError:
            continue
        # Keep the earliest date if a symbol appears more than once
        if sym not in result or earn_date < result[sym]:
            result[sym] = earn_date

    logger.info("Finnhub earnings fetch: %d tickers with upcoming earnings", len(result))
    return result


# ── Cache refresh ──────────────────────────────────────────────────────────────

def refresh_earnings_cache(db: Session) -> int:
    """Fetch from Finnhub and upsert every result into earnings_calendar.

    Returns the number of rows upserted.  On fetch failure the existing
    cache is left intact and 0 is returned.
    """
    earnings_map = _fetch_from_finnhub()
    if not earnings_map:
        logger.warning("refresh_earnings_cache: nothing fetched, cache unchanged")
        return 0

    now = datetime.utcnow()
    upserted = 0
    for ticker, earn_date in earnings_map.items():
        db.execute(
            text("""
                INSERT INTO earnings_calendar (ticker, next_earnings_date, fetched_at)
                VALUES (:ticker, :earn_date, :now)
                ON CONFLICT (ticker) DO UPDATE
                  SET next_earnings_date = EXCLUDED.next_earnings_date,
                      fetched_at         = EXCLUDED.fetched_at
            """),
            {"ticker": ticker, "earn_date": earn_date, "now": now},
        )
        upserted += 1

    db.commit()
    logger.info("refresh_earnings_cache: upserted %d rows", upserted)
    return upserted


# ── Cache read (read-only — no automatic refresh) ──────────────────────────────

def get_earnings_lookup(db: Session) -> dict[str, int]:
    """Return {TICKER: days_until_earnings} from the DB cache.

    Read-only — never triggers a Finnhub fetch automatically.  Use the
    POST /api/refresh-earnings endpoint (or the "Run Earnings" UI button)
    to populate / refresh the cache on demand.

    Only dates today or in the future are included; past earnings (if
    somehow still in the cache) are filtered out.
    """
    today = date.today()
    try:
        rows = db.execute(
            text("""
                SELECT ticker, next_earnings_date
                FROM earnings_calendar
                WHERE next_earnings_date >= :today
            """),
            {"today": today},
        ).fetchall()
    except Exception as exc:
        logger.warning("earnings_calendar read failed: %s", exc)
        return {}

    lookup: dict[str, int] = {}
    for r in rows:
        if r.next_earnings_date is None:
            continue
        days = (r.next_earnings_date - today).days
        if 0 <= days <= _DAYS_AHEAD:
            lookup[r.ticker] = days

    logger.info("get_earnings_lookup: %d tickers loaded from cache", len(lookup))
    return lookup
