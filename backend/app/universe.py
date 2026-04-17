"""Universe management: CSV sync + database loader.

The CSV file (backend/data/ticker_universe.csv) is the source of truth.
On startup it is synced into the ticker_universe table.  The scan reads from
the DB so the universe can be extended without redeploying.
"""
from __future__ import annotations

import csv
import json
import logging
from functools import lru_cache
from pathlib import Path

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
_CSV_PATH = _DATA_DIR / "ticker_universe.csv"
_JSON_PATH = _DATA_DIR / "universe.json"


# ── CSV → DB sync ──────────────────────────────────────────────────────────────

def sync_universe_from_csv(db: Session) -> int:
    """Read ticker_universe.csv and insert any rows not already in the DB.

    Does NOT delete rows present in the DB but absent from the CSV — this
    preserves any 'custom' entries added through the dashboard.

    Returns the number of new rows inserted.
    """
    from . import models
    from sqlalchemy import select

    if not _CSV_PATH.exists():
        logger.warning("ticker_universe.csv not found at %s — skipping sync", _CSV_PATH)
        return 0

    csv_rows: list[tuple[str, str]] = []
    with _CSV_PATH.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticker = row.get("ticker", "").strip().upper()
            source = row.get("source", "").strip().lower()
            if ticker and source:
                csv_rows.append((ticker, source))

    existing: set[tuple[str, str]] = set(
        db.execute(
            select(models.TickerUniverse.ticker, models.TickerUniverse.source)
        ).all()
    )

    inserted = 0
    for ticker, source in csv_rows:
        if (ticker, source) not in existing:
            db.add(models.TickerUniverse(ticker=ticker, source=source, active=True))
            inserted += 1

    if inserted:
        db.commit()

    logger.info(
        "universe sync: %d CSV rows, %d already in DB, %d inserted",
        len(csv_rows), len(existing), inserted,
    )
    return inserted


# ── DB loaders ────────────────────────────────────────────────────────────────

def load_universe_from_db(db: Session) -> list[str]:
    """Return sorted list of distinct active tickers from ticker_universe table."""
    from . import models
    from sqlalchemy import select

    tickers = db.execute(
        select(models.TickerUniverse.ticker)
        .where(models.TickerUniverse.active == True)  # noqa: E712
        .distinct()
    ).scalars().all()
    return sorted(set(tickers))


def universe_size_from_db(db: Session) -> int:
    """Count distinct active tickers in the universe table."""
    from . import models
    from sqlalchemy import select, func, distinct

    result = db.execute(
        select(func.count(distinct(models.TickerUniverse.ticker)))
        .where(models.TickerUniverse.active == True)  # noqa: E712
    ).scalar()
    return int(result or 0)


def ticker_sources_from_db(db: Session) -> dict[str, list[str]]:
    """Return {ticker: [source, ...]} for all active universe entries."""
    from . import models
    from sqlalchemy import select

    rows = db.execute(
        select(models.TickerUniverse.ticker, models.TickerUniverse.source)
        .where(models.TickerUniverse.active == True)  # noqa: E712
    ).all()
    mapping: dict[str, list[str]] = {}
    for ticker, source in rows:
        mapping.setdefault(ticker, []).append(source)
    return mapping


# ── Legacy JSON loader (fallback) ─────────────────────────────────────────────

@lru_cache(maxsize=1)
def load_universe() -> list[str]:
    """Load from the legacy universe.json file.  Used as a fallback only."""
    with _JSON_PATH.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    tickers = payload.get("tickers", [])
    seen: set[str] = set()
    unique: list[str] = []
    for t in tickers:
        t = t.strip().upper()
        if t and t not in seen:
            seen.add(t)
            unique.append(t)
    return unique


def universe_size() -> int:
    return len(load_universe())
