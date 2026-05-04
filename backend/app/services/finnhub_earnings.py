"""Finnhub earnings calendar integration.

One API call fetches the entire market's earnings dates for the next
90 days.  Results are cached in the ``earnings_calendar`` PostgreSQL
table with a 24-hour TTL.

Public entry points
-------------------
get_earnings_lookup(db)
    Returns {TICKER: days_until_earnings} for all tickers that have an
    upcoming earnings date in the cache.  If the cache is stale (>24h)
    it refreshes synchronously before returning.  Gracefully returns
    whatever is in the cache (possibly stale) if the API is unavailable.

refresh_earnings_cache(db)
    Force-fetch from Finnhub and upsert the full result into the DB.
    Called internally by get_earnings_lookup when cache is stale.
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
_CACHE_TTL_HOURS = 24
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


# ── Cache read (with stale-on-read refresh) ────────────────────────────────────

_last_refresh_attempt: datetime | None = None  # in-process guard so we don't
                                                # hammer finnhub if DB writes fail


def get_earnings_lookup(db: Session) -> dict[str, int]:
    """Return {TICKER: days_until_earnings} from the DB cache.

    If the cache is empty or the most-recently-fetched row is older than
    24 hours, a synchronous refresh is triggered first.  On failure the
    stale/empty cache is returned rather than raising.

    Only dates today or in the future are included; past earnings (if
    somehow still in the cache) are filtered out.
    """
    global _last_refresh_attempt

    # ── Staleness check ────────────────────────────────────────────────────────
    try:
        row = db.execute(
            text("SELECT MAX(fetched_at) AS latest FROM earnings_calendar")
        ).fetchone()
        latest_fetch: datetime | None = row.latest if row else None
    except Exception as exc:
        logger.warning("earnings_calendar staleness check failed: %s", exc)
        latest_fetch = None

    needs_refresh = (
        latest_fetch is None
        or (datetime.utcnow() - latest_fetch) > timedelta(hours=_CACHE_TTL_HOURS)
    )

    # Rate-limit in-process refresh attempts to once per 10 minutes even if DB
    # writes fail, so a broken finnhub key doesn't spam the API on every scan.
    if needs_refresh:
        now = datetime.utcnow()
        if _last_refresh_attempt is None or (now - _last_refresh_attempt) > timedelta(minutes=10):
            _last_refresh_attempt = now
            try:
                refresh_earnings_cache(db)
            except Exception as exc:
                logger.warning("earnings refresh failed, using stale cache: %s", exc)

    # ── Read cache ─────────────────────────────────────────────────────────────
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
