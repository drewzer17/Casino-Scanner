#!/usr/bin/env python3
"""
etl_parse_supplement.py
Parse AI_Buildout_Final_Supplement_v2.pdf → insert 7 sitreps into Postgres.

Reuses all parsing functions from etl_parse_sitreps.py — same regex,
same section extraction, same hidden-angles / kill-probability / winners
parsers. No changes to master parser.

Usage:
    DATABASE_URL=postgresql://... python3 etl_parse_supplement.py

After inserting rows, also sets category + subcategory from the
canonical ai_buildout_universe.json (populated by Phase D ingest).
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path

import psycopg2
import pypdf

# ── Import all parsing logic from the master ETL (do NOT modify it) ────────────
sys.path.insert(0, str(Path(__file__).resolve().parent))
import etl_parse_sitreps as master   # noqa: E402

# ── Config ─────────────────────────────────────────────────────────────────────
_REPO_ROOT   = Path(__file__).resolve().parent
PDF_PATH     = Path("/Users/andrewreberm4/Downloads/AI_Buildout_Final_Supplement_v2.pdf")
REPORT_PATH  = _REPO_ROOT / "etl_supplement_report.txt"
UNIVERSE_JSON = _REPO_ROOT / "backend" / "data" / "ai_buildout_universe.json"
PDF_FILENAME  = "AI_Buildout_Final_Supplement_v2.pdf"
SOURCE_PDF    = "Final Supplement v2"

EXPECTED_TICKERS = {"CMI", "CAT", "D", "SBGSY", "ABB", "FTV", "GE"}
MIN_BLOCKS = 5  # hard stop if too few blocks found

_DEFAULT_DB = (
    "postgresql://postgres:uIcMzUUNlqmhekvgoKBcQxRIQOoajQyu"
    "@nozomi.proxy.rlwy.net:46336/railway"
)
DATABASE_URL = os.environ.get("DATABASE_URL", _DEFAULT_DB)
if "railway.internal" in DATABASE_URL:
    DATABASE_URL = _DEFAULT_DB

# ── DB insert (same SQL as master, verbatim) ───────────────────────────────────
INSERT_SQL = """
INSERT INTO sitreps (
    ticker, company_name, last_updated,
    what_they_do, hidden_angles, bear_case, contrarian_case, what_kills_it,
    kill_probability_low, kill_probability_high, kill_horizon_months, kill_components,
    winners, source_pdf, raw_text, sections_parsed, parse_warnings
) VALUES (
    %(ticker)s, %(company_name)s, CURRENT_DATE,
    %(what_they_do)s, %(hidden_angles)s, %(bear_case)s, %(contrarian_case)s, %(what_kills_it)s,
    %(kill_probability_low)s, %(kill_probability_high)s, %(kill_horizon_months)s, %(kill_components)s,
    %(winners)s, %(source_pdf)s, %(raw_text)s, %(sections_parsed)s, %(parse_warnings)s
)
ON CONFLICT (ticker) DO UPDATE SET
    company_name          = EXCLUDED.company_name,
    last_updated          = CURRENT_DATE,
    what_they_do          = EXCLUDED.what_they_do,
    hidden_angles         = EXCLUDED.hidden_angles,
    bear_case             = EXCLUDED.bear_case,
    contrarian_case       = EXCLUDED.contrarian_case,
    what_kills_it         = EXCLUDED.what_kills_it,
    kill_probability_low  = EXCLUDED.kill_probability_low,
    kill_probability_high = EXCLUDED.kill_probability_high,
    kill_horizon_months   = EXCLUDED.kill_horizon_months,
    kill_components       = EXCLUDED.kill_components,
    winners               = EXCLUDED.winners,
    source_pdf            = EXCLUDED.source_pdf,
    raw_text              = EXCLUDED.raw_text,
    sections_parsed       = EXCLUDED.sections_parsed,
    parse_warnings        = EXCLUDED.parse_warnings,
    updated_at            = NOW();
"""


# ── PDF extraction (skip page 1 — cover page) ─────────────────────────────────

def extract_pdf_text_skip_cover(path: Path) -> str:
    reader = pypdf.PdfReader(str(path))
    print(f"  Total pages in PDF: {len(reader.pages)}")
    # Page 0 = cover ("AI Buildout Master Sitreps — Final Supplement (v2)")
    # Pages 1+ = sitreps
    pages = [p.extract_text() or "" for p in reader.pages[1:]]
    return "\n".join(pages)


# ── Category assignment from universe JSON ─────────────────────────────────────

def load_category_map() -> dict[str, dict]:
    """Return {ticker: {category, subcategory}} from the canonical universe JSON."""
    if not UNIVERSE_JSON.exists():
        print(f"  WARNING: {UNIVERSE_JSON} not found — categories will not be set")
        return {}
    with open(UNIVERSE_JSON) as f:
        data = json.load(f)
    result: dict[str, dict] = {}
    for t in data.get("tickers", []):
        ticker = t.get("ticker")
        if ticker:
            result[ticker] = {
                "category":    t.get("category") or "",
                "subcategory": t.get("subcategory") or "",
            }
    return result


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:

    # 1. Read PDF — skip cover page ─────────────────────────────────────────────
    print(f"Reading: {PDF_PATH}", flush=True)
    if not PDF_PATH.exists():
        print(f"ERROR: PDF not found at {PDF_PATH}", file=sys.stderr)
        sys.exit(1)

    full_text = extract_pdf_text_skip_cover(PDF_PATH)
    print(f"  Extracted {len(full_text):,} characters (pages 2+)", flush=True)

    # 2. Split into sitrep blocks ────────────────────────────────────────────────
    blocks = master.split_blocks(full_text)
    print(f"  Sitrep blocks found: {len(blocks)}", flush=True)
    for company, ticker, _ in blocks:
        print(f"    {ticker:6s}  {company}")

    if len(blocks) < MIN_BLOCKS:
        msg = (
            f"STOP: Only {len(blocks)} blocks found — expected {MIN_BLOCKS}.\n"
            "Block-splitting regex may be wrong. Aborting."
        )
        print(msg, file=sys.stderr)
        REPORT_PATH.write_text(msg)
        sys.exit(2)

    # 3. Parse each block ────────────────────────────────────────────────────────
    print("\nParsing sitrep blocks ...", flush=True)
    parsed_rows: list[dict] = []

    for company, ticker, raw_block in blocks:
        row = master.parse_sitrep(company, ticker, raw_block)
        # Override source_pdf to identify supplement origin
        row["source_pdf"] = SOURCE_PDF
        parsed_rows.append(row)

        warns = json.loads(row["parse_warnings"]) if row["parse_warnings"] else []
        warn_str = f"  {len(warns)} warning(s)" if warns else ""
        print(f"  {ticker:6s}  {row['sections_parsed']}/7 sections{warn_str}", flush=True)

        # HARD RULE 3: stop on any below-7/7 parse and show warnings
        if row["sections_parsed"] < 7:
            print(f"\nSTOP: {ticker} parsed only {row['sections_parsed']}/7 sections.", file=sys.stderr)
            print("Parse warnings:", file=sys.stderr)
            for w in warns:
                print(f"  {w}", file=sys.stderr)
            print("\nAborting before any DB writes.", file=sys.stderr)
            sys.exit(3)

    print(f"\nAll {len(parsed_rows)} blocks parsed at 7/7. Proceeding to DB.", flush=True)

    # 4. Connect to DB ───────────────────────────────────────────────────────────
    print(f"\nConnecting to DB ...", flush=True)
    try:
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=20)
        conn.autocommit = False
        cur = conn.cursor()
    except Exception as exc:
        print(f"ERROR: Cannot connect to DB: {exc}", file=sys.stderr)
        sys.exit(1)

    # 5. Insert / upsert rows ────────────────────────────────────────────────────
    print(f"Inserting {len(parsed_rows)} rows ...", flush=True)
    inserted = 0
    for row in parsed_rows:
        cur.execute(INSERT_SQL, row)
        inserted += 1
    conn.commit()
    print(f"  {inserted} rows inserted/updated.", flush=True)

    # 6. Set categories from universe JSON ───────────────────────────────────────
    print("\nSetting categories from universe JSON ...", flush=True)
    cat_map = load_category_map()
    cat_updated = 0
    cat_missing = []

    for row in parsed_rows:
        ticker = row["ticker"]
        cat_data = cat_map.get(ticker)
        if cat_data and cat_data["category"]:
            cur.execute(
                "UPDATE sitreps SET category = %s, subcategory = %s WHERE ticker = %s",
                (cat_data["category"], cat_data["subcategory"], ticker),
            )
            if cur.rowcount > 0:
                cat_updated += 1
                print(f"  {ticker}: {cat_data['category']}")
        else:
            cat_missing.append(ticker)
            print(f"  {ticker}: no category in JSON — leaving NULL")

    conn.commit()
    print(f"  Categories set: {cat_updated}")

    # 7. DB state summary ─────────────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM sitreps")
    total_rows = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM sitreps WHERE category IS NOT NULL")
    rows_with_cat = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM sitreps WHERE price_at_add IS NULL")
    rows_missing_price = cur.fetchone()[0]

    cur.close()
    conn.close()

    # 8. Build report ─────────────────────────────────────────────────────────────
    kp_stats = {
        "range":      sum(1 for r in parsed_rows if r["kill_probability_low"]  is not None),
        "horizon":    sum(1 for r in parsed_rows if r["kill_horizon_months"]   is not None),
        "components": sum(1 for r in parsed_rows if r["kill_components"]       is not None),
    }
    winners_tickers   = 0
    winners_rationale = 0
    for r in parsed_rows:
        if r["winners"]:
            for w in json.loads(r["winners"]):
                if w.get("ticker"):
                    winners_tickers += 1
                else:
                    winners_rationale += 1

    header_tickers = "  ".join(
        f"{'TICKER':<8} {'SECTIONS':<10} {'CATEGORY':<45} WARNINGS"
        .split()[::-1][::-1]  # just formatting
    )
    parse_lines = []
    for r in parsed_rows:
        ticker = r["ticker"]
        sec    = f"{r['sections_parsed']}/7"
        cat    = cat_map.get(ticker, {}).get("category", "—") or "—"
        warns  = json.loads(r["parse_warnings"]) if r["parse_warnings"] else []
        parse_lines.append(f"  {ticker:<8} {sec:<10} {cat:<45} {len(warns)}")

    report = "\n".join([
        "=== FINAL SUPPLEMENT ETL REPORT ===",
        f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"Source PDF: {PDF_FILENAME}",
        "",
        f"EXPECTED TICKERS: {', '.join(sorted(EXPECTED_TICKERS))}",
        "",
        "PARSE RESULTS:",
        f"  {'TICKER':<8} {'SECTIONS':<10} {'CATEGORY':<45} WARNINGS",
    ] + parse_lines + [
        "",
        "DATABASE STATE AFTER INGESTION:",
        f"  Total rows in sitreps table: {total_rows}  (expected 181)",
        f"  Rows with category set:      {rows_with_cat}  (expected 181)",
        f"  Rows missing price_at_add:   {rows_missing_price}  (expected ~7 — new rows)",
        "",
        "KILL PROBABILITY EXTRACTION:",
        f"  Range parsed:      {kp_stats['range']}/{len(parsed_rows)}",
        f"  Horizon parsed:    {kp_stats['horizon']}/{len(parsed_rows)}",
        f"  Components parsed: {kp_stats['components']}/{len(parsed_rows)}",
        "",
        "WINNERS EXTRACTION:",
        f"  Tickers identified:     {winners_tickers}",
        f"  Rationale-only entries: {winners_rationale}",
        "",
        "=== END REPORT ===",
    ])

    print("\n" + report, flush=True)
    REPORT_PATH.write_text(report)
    print(f"\nReport written to: {REPORT_PATH}", flush=True)


if __name__ == "__main__":
    main()
