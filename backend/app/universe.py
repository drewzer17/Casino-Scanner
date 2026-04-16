"""Load the ticker universe from static JSON."""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

_DATA_PATH = Path(__file__).resolve().parent.parent / "data" / "universe.json"


@lru_cache(maxsize=1)
def load_universe() -> list[str]:
    with _DATA_PATH.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    tickers = payload.get("tickers", [])
    # dedupe while preserving order
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
