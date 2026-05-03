#!/usr/bin/env python3
"""
capture_prices.py — one-time idempotent script to capture current stock prices
as "price_at_add" for every sitrep ticker that doesn't yet have one.

Uses Tradier /v1/markets/quotes (same endpoint as the scanner engine).
Batches up to QUOTE_BATCH (20) symbols per API call.

Usage:
    DATABASE_URL=postgresql://... TRADIER_API_KEY=... python3 capture_prices.py

IDEMPOTENT — only fills NULL price_at_add values. Re-running is safe.
"""
from __future__ import annotations

import os
import sys

import httpx
import psycopg2

# ── Config ────────────────────────────────────────────────────────────────────

QUOTE_BATCH  = 20      # Tradier max symbols per quotes call
TRADIER_BASE = "https://sandbox.tradier.com"

_DEFAULT_DB = (
    "postgresql://postgres:uIcMzUUNlqmhekvgoKBcQxRIQOoajQyu"
    "@nozomi.proxy.rlwy.net:46336/railway"
)


# ── Tradier helpers (self-contained — no backend import needed) ───────────────

def _headers(api_key: str) -> dict:
    return {"Authorization": f"Bearer {api_key}", "Accept": "application/json"}


def fetch_quotes(symbols: list[str], api_key: str) -> dict[str, dict]:
    """Return {ticker: quote_dict} for up to QUOTE_BATCH symbols."""
    with httpx.Client(timeout=10.0) as client:
        resp = client.get(
            TRADIER_BASE + "/v1/markets/quotes",
            params={"symbols": ",".join(symbols)},
            headers=_headers(api_key),
        )
        resp.raise_for_status()
        data = resp.json()

    quote = (data.get("quotes") or {}).get("quote")
    if not quote:
        return {}
    if isinstance(quote, dict):
        quote = [quote]
    return {q["symbol"]: q for q in quote if q.get("symbol")}


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    db_url  = os.environ.get("DATABASE_URL") or _DEFAULT_DB
    api_key = os.environ.get("TRADIER_API_KEY", "").strip().lstrip("=")

    if not api_key:
        print("ERROR: TRADIER_API_KEY env var not set.", file=sys.stderr)
        sys.exit(1)

    print("Connecting to database…")
    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    cur  = conn.cursor()

    # Fetch all tickers that still need a price (idempotent guard)
    cur.execute("SELECT ticker FROM sitreps WHERE price_at_add IS NULL ORDER BY ticker")
    need_price = [row[0] for row in cur.fetchall()]
    print(f"Tickers needing price_at_add: {len(need_price)}")

    if not need_price:
        print("Nothing to do — all sitrep tickers already have price_at_add set.")
        cur.close()
        conn.close()
        return

    captured = 0
    failed:  list[str] = []

    for batch_start in range(0, len(need_price), QUOTE_BATCH):
        batch = need_price[batch_start : batch_start + QUOTE_BATCH]
        batch_label = f"{batch_start + 1}–{batch_start + len(batch)}"
        try:
            quotes = fetch_quotes(batch, api_key)
        except Exception as exc:
            print(f"  [WARN] batch {batch_label} fetch failed: {exc} — marking as failed")
            failed.extend(batch)
            continue

        for ticker in batch:
            q = quotes.get(ticker, {})
            price = q.get("last") or q.get("prevclose")
            if not price:
                print(f"  [WARN] {ticker}: no price in response")
                failed.append(ticker)
                continue

            cur.execute(
                """
                UPDATE sitreps
                   SET price_at_add      = %s,
                       price_at_add_date = CURRENT_DATE
                 WHERE ticker = %s
                   AND price_at_add IS NULL
                """,
                (float(price), ticker),
            )
            if cur.rowcount > 0:
                captured += 1
                print(f"  {ticker}: ${float(price):.2f}")
            else:
                print(f"  {ticker}: already set, skipped")

    conn.commit()
    cur.close()
    conn.close()

    print(f"\n=== PRICE CAPTURE REPORT ===")
    print(f"  Captured:  {captured}")
    print(f"  Failed ({len(failed)}): {', '.join(failed) if failed else 'none'}")
    print(f"=== END REPORT ===\n")


if __name__ == "__main__":
    main()
