#!/usr/bin/env python3
"""
Fetch sector and industry data for all universe tickers using yfinance.

Usage:
    pip install yfinance
    python fetch_sectors.py

Run from any directory. Reads ticker_universe.csv and writes/updates
sector_map.csv in backend/data/. Safe to re-run — skips tickers already
present in sector_map.csv.
"""

import csv
import time
from pathlib import Path

try:
    import yfinance as yf
except ImportError:
    raise SystemExit("yfinance not installed. Run: pip install yfinance")

SCRIPT_DIR   = Path(__file__).parent
DATA_DIR     = SCRIPT_DIR.parent / "data"
UNIVERSE_CSV = DATA_DIR / "ticker_universe.csv"
SECTOR_CSV   = DATA_DIR / "sector_map.csv"
DELAY        = 0.5  # seconds between API calls


def load_universe():
    with open(UNIVERSE_CSV) as f:
        return [row["ticker"].strip() for row in csv.DictReader(f)]


def load_existing():
    if not SECTOR_CSV.exists():
        return {}
    with open(SECTOR_CSV) as f:
        return {row["ticker"]: row for row in csv.DictReader(f)}


def main():
    universe = load_universe()
    existing = load_existing()

    already_done = set(existing.keys())
    to_fetch = [t for t in universe if t not in already_done]

    print(f"Universe:      {len(universe)} tickers")
    print(f"Already done:  {len(already_done)}")
    print(f"Need to fetch: {len(to_fetch)}")

    new_results = {}
    errors = []

    for i, ticker in enumerate(to_fetch, 1):
        try:
            info = yf.Ticker(ticker).info
            sector   = info.get("sector", "") or ""
            industry = info.get("industry", "") or ""
            new_results[ticker] = {"ticker": ticker, "sector": sector, "industry": industry}
        except Exception as e:
            errors.append(ticker)
            new_results[ticker] = {"ticker": ticker, "sector": "", "industry": ""}

        if i % 100 == 0:
            covered = sum(1 for r in new_results.values() if r["sector"])
            print(f"  Progress: {i}/{len(to_fetch)} | covered this batch: {covered}")

        time.sleep(DELAY)

    # Merge and write
    merged = {**existing, **new_results}
    all_tickers = sorted(merged.keys())

    with open(SECTOR_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["ticker", "sector", "industry"])
        writer.writeheader()
        for ticker in all_tickers:
            writer.writerow(merged[ticker])

    total_covered = sum(1 for r in merged.values() if r["sector"])
    print(f"\nDone. {total_covered}/{len(merged)} tickers have sector data.")
    if errors:
        print(f"Errors ({len(errors)}): {', '.join(errors[:20])}{'...' if len(errors) > 20 else ''}")
    print(f"Written: {SECTOR_CSV}")


if __name__ == "__main__":
    main()
