"""Finnhub earnings calendar integration.

Fetches earnings in 7-day chunks to avoid Finnhub's 1500-entry
response cap (which truncates data when a single large window is
requested).  Results are stored in the ``earnings_calendar``
PostgreSQL table.  The cache is never refreshed automatically; use
the POST /api/refresh-earnings endpoint (or the "Run Earnings" UI
button) to populate or update it on demand.

Public entry points
-------------------
get_earnings_lookup(db)
    Read-only.  Returns {TICKER: days_until_earnings} for all tickers
    with an upcoming earnings date in the cache (today or later).

refresh_earnings_cache(db)
    Force-fetch from Finnhub (chunked) and upsert into the DB.
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
_DAYS_AHEAD   = 90          # fetch window used by refresh_earnings_cache
_CHUNK_DAYS   = 3           # request chunk size — 7-day windows hit the 1500-entry
                            # cap during peak Q1/Q3 earnings weeks; 3-day keeps
                            # each chunk well under the cap (~500 entries max)


# ── Finnhub fetch (chunked) ────────────────────────────────────────────────────

def _fetch_from_finnhub(days_ahead: int = _DAYS_AHEAD) -> dict[str, date]:
    """Fetch Finnhub /calendar/earnings in 7-day chunks.

    Finnhub caps responses at 1500 entries and returns them in
    reverse-chronological order, so a single 90-day window silently
    drops near-term dates (e.g. the current week's earnings).
    Chunking keeps each request comfortably under the cap.

    Returns {TICKER: next_earnings_date} for the full window.
    If ALL chunks fail, returns {}.  Partial data from successful
    chunks is preserved even when individual chunks error.
    """
    api_key = settings.finnhub_api_key
    if not api_key:
        logger.warning("FINNHUB_API_KEY not set — skipping earnings fetch")
        return {}

    today     = date.today()
    end_date  = today + timedelta(days=days_ahead)
    chunk_sz  = timedelta(days=_CHUNK_DAYS)

    combined: dict[str, date] = {}
    chunk_start = today
    chunks_ok   = 0
    chunks_fail = 0

    while chunk_start <= end_date:
        chunk_end = min(chunk_start + chunk_sz - timedelta(days=1), end_date)

        try:
            resp = httpx.get(
                f"{_FINNHUB_BASE}/calendar/earnings",
                params={
                    "from":  chunk_start.isoformat(),
                    "to":    chunk_end.isoformat(),
                    "token": api_key,
                },
                timeout=20.0,
            )
            resp.raise_for_status()
            events = resp.json().get("earningsCalendar") or []

            if len(events) >= 1500:
                logger.warning(
                    "Finnhub chunk %s→%s returned %d entries (may be capped — "
                    "consider reducing _CHUNK_DAYS)",
                    chunk_start, chunk_end, len(events),
                )

            chunk_tickers = 0
            for ev in events:
                sym      = (ev.get("symbol") or "").upper().strip()
                date_str = ev.get("date") or ""
                if not sym or not date_str:
                    continue
                try:
                    earn_date = date.fromisoformat(date_str)
                except ValueError:
                    continue
                # Keep the earliest date if a symbol appears in multiple chunks
                if sym not in combined or earn_date < combined[sym]:
                    combined[sym] = earn_date
                    chunk_tickers += 1

            logger.info(
                "Finnhub chunk %s→%s: %d raw events, %d new/updated tickers",
                chunk_start, chunk_end, len(events), chunk_tickers,
            )
            chunks_ok += 1

        except Exception as exc:
            logger.error(
                "Finnhub chunk %s→%s failed (continuing): %s",
                chunk_start, chunk_end, exc,
            )
            chunks_fail += 1

        chunk_start = chunk_end + timedelta(days=1)

    logger.info(
        "Finnhub chunked fetch complete: %d chunks ok, %d failed, "
        "%d total tickers across %d-day window",
        chunks_ok, chunks_fail, len(combined), days_ahead,
    )
    return combined


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

    Returns all tickers whose next_earnings_date is today or later.
    No upper-bound cutoff is applied here — the fetch window used by
    refresh_earnings_cache is the authoritative source of truth for
    how far ahead the cache extends.
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
        if days >= 0:
            lookup[r.ticker] = days

    logger.info("get_earnings_lookup: %d tickers loaded from cache", len(lookup))
    return lookup


def get_earnings_date_lookup(db: Session) -> dict[str, date]:
    """Return {TICKER: next_earnings_date} from the DB cache.

    Companion to get_earnings_lookup — returns the actual date object instead
    of days-until, which is needed for the Phase 2 Risk Quality earnings overlap
    checks (earnings_date vs best_expiry comparison).
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
        logger.warning("earnings_calendar date read failed: %s", exc)
        return {}

    return {
        r.ticker: r.next_earnings_date
        for r in rows
        if r.next_earnings_date is not None
    }
