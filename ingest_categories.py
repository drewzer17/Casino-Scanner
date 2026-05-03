#!/usr/bin/env python3
"""
ingest_categories.py — one-time script to:
  1. Run migration 002 (idempotent — ADD COLUMN IF NOT EXISTS)
  2. Update sitreps rows with category + subcategory from v2 JSON
  3. Merge 4 orphaned tickers back into v2 and write the canonical
     backend/data/ai_buildout_universe.json (replacing the old one)

Usage:
    DATABASE_URL=postgresql://... python3 ingest_categories.py \
        --v2 /path/to/ai_buildout_universe_v2.json

Defaults:
    --v2   : Downloads/ai_buildout_universe_v2.json (relative to HOME)
    DATABASE_URL: public Railway proxy if not set
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import psycopg2

# ── Paths ─────────────────────────────────────────────────────────────────────

_REPO_ROOT  = Path(__file__).resolve().parent
_LIVE_JSON  = _REPO_ROOT / "backend" / "data" / "ai_buildout_universe.json"
_MIGRATION  = _REPO_ROOT / "migrations" / "002_add_categories_and_prices.sql"

_DEFAULT_DB = (
    "postgresql://postgres:uIcMzUUNlqmhekvgoKBcQxRIQOoajQyu"
    "@nozomi.proxy.rlwy.net:46336/railway"
)

# 4 orphaned tickers present in the live DB but absent from the v2 JSON.
# Source values confirmed from PDF batch headers; spec D2 takes precedence.
_ORPHANS = [
    {
        "ticker":       "IREN",
        "company_name": "IREN Limited",
        "source":       "COMPLETE",
        "category":     "99. Uncategorized",
        "subcategory":  "Pending categorization",
    },
    {
        "ticker":       "J",
        "company_name": "Jacobs Solutions",
        "source":       "COMPLETE",
        "category":     "99. Uncategorized",
        "subcategory":  "Pending categorization",
    },
    {
        "ticker":       "NVT",
        "company_name": "nVent Electric",
        "source":       "Combined v2",
        "category":     "99. Uncategorized",
        "subcategory":  "Pending categorization",
    },
    {
        "ticker":       "S",
        "company_name": "SentinelOne",
        "source":       "COMPLETE",
        "category":     "99. Uncategorized",
        "subcategory":  "Pending categorization",
    },
]
_ORPHAN_TICKERS = {o["ticker"] for o in _ORPHANS}

# v2 source_pdf values → live JSON source field
_SOURCE_MAP = {
    "Combined v2":      "Combined v2",
    "COMPLETE":         "COMPLETE",
    "Final Supplement": "Final Supplement",
}


def run_migration(cur) -> None:
    sql = _MIGRATION.read_text()
    cur.execute(sql)
    print(f"[migration] {_MIGRATION.name} applied.")


def load_v2(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)


def ingest(db_url: str, v2_path: Path) -> None:
    v2 = load_v2(v2_path)
    v2_tickers: list[dict] = v2.get("tickers", [])
    print(f"[v2] Loaded {len(v2_tickers)} tickers from {v2_path.name}")

    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    cur  = conn.cursor()

    # ── Step 1: migration ────────────────────────────────────────────────────
    run_migration(cur)
    conn.commit()

    # ── Step 2: update sitreps with category + subcategory ──────────────────
    updated       = 0
    not_in_sitrep = []  # v2 tickers with no sitrep row (expected: 7 Final Supplement)

    for t in v2_tickers:
        ticker      = t["ticker"]
        category    = t.get("category", "")
        subcategory = t.get("subcategory", "")

        cur.execute(
            """
            UPDATE sitreps
               SET category    = %s,
                   subcategory = %s
             WHERE ticker = %s
            """,
            (category, subcategory, ticker),
        )
        if cur.rowcount == 0:
            not_in_sitrep.append(ticker)
        else:
            updated += 1

    # Update orphaned tickers already in sitreps with their placeholder category
    for o in _ORPHANS:
        cur.execute(
            """
            UPDATE sitreps
               SET category    = %s,
                   subcategory = %s
             WHERE ticker = %s
            """,
            (o["category"], o["subcategory"], o["ticker"]),
        )
        if cur.rowcount > 0:
            updated += 1

    conn.commit()
    print(f"[sitreps] {updated} rows updated with category/subcategory.")

    # ── Step 3: build and write merged JSON ─────────────────────────────────
    # Build a lookup from v2 for fast access
    v2_by_ticker = {t["ticker"]: t for t in v2_tickers}

    # Convert v2 entries to the live-JSON schema (source_pdf → source, keep category/subcategory)
    merged: list[dict] = []
    for t in v2_tickers:
        merged.append({
            "ticker":       t["ticker"],
            "company_name": t["company_name"],
            "source":       _SOURCE_MAP.get(t.get("source_pdf", ""), t.get("source_pdf", "")),
            "category":     t.get("category", ""),
            "subcategory":  t.get("subcategory", ""),
        })

    # Insert orphans in alphabetical order
    for o in _ORPHANS:
        entry = {
            "ticker":       o["ticker"],
            "company_name": o["company_name"],
            "source":       o["source"],
            "category":     o["category"],
            "subcategory":  o["subcategory"],
        }
        # Find the correct alphabetical insertion point
        idx = 0
        for i, m in enumerate(merged):
            if m["ticker"] < o["ticker"]:
                idx = i + 1
        merged.insert(idx, entry)

    # Build merged JSON structure preserving v2 categories list
    new_json = {
        "metadata": {
            **v2.get("metadata", {}),
            "total_tickers": len(merged),
        },
        "categories": v2.get("categories", []),
        "tickers": merged,
    }

    _LIVE_JSON.write_text(json.dumps(new_json, indent=2))
    print(f"[json] Wrote {len(merged)} tickers → {_LIVE_JSON}")

    # ── Step 4: report ───────────────────────────────────────────────────────
    print("\n=== INGEST REPORT ===")
    print(f"  Sitrep rows updated: {updated}")
    print(f"\n  Tickers per category:")
    cat_counts: dict[str, int] = {}
    for t in merged:
        c = t.get("category") or "— (none)"
        cat_counts[c] = cat_counts.get(c, 0) + 1
    for c in sorted(cat_counts):
        print(f"    {c}: {cat_counts[c]}")
    if not_in_sitrep:
        print(f"\n  v2 tickers with no sitrep row ({len(not_in_sitrep)}) — expected (Final Supplement):")
        for t in sorted(not_in_sitrep):
            print(f"    {t}")
    print("=== END REPORT ===\n")

    cur.close()
    conn.close()


def main() -> None:
    default_v2 = Path.home() / "Downloads" / "ai_buildout_universe_v2.json"

    parser = argparse.ArgumentParser(description="Ingest category data into sitreps and update universe JSON")
    parser.add_argument("--v2",  type=Path, default=default_v2,
                        help=f"Path to ai_buildout_universe_v2.json (default: {default_v2})")
    parser.add_argument("--db",  default=os.environ.get("DATABASE_URL", _DEFAULT_DB),
                        help="PostgreSQL connection string")
    args = parser.parse_args()

    if not args.v2.exists():
        print(f"ERROR: v2 JSON not found at {args.v2}", file=sys.stderr)
        sys.exit(1)

    ingest(args.db, args.v2)


if __name__ == "__main__":
    main()
