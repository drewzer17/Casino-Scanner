#!/usr/bin/env python3
"""
ai_overview_pdf.py
==================
Pulls the AI Overview / AI Buildout universe out of Casino Scanner and writes a
PDF that lists every company grouped by the category it sits under.

It does NOT assume a name count. Whatever is in the data source is what gets
printed, and the true count is reported at the top of the PDF and in the console.
That way if you think it's 194 but the file says 191, you'll see the gap.

DATA SOURCE (auto-detected, in this order):
  1. Postgres table `tickers_ai_buildout` if DATABASE_URL is set and the table
     exists (this is Casino Scanner's working copy, loaded from v2 JSON).
  2. The JSON file the /ai-overview route reads at request time.

You can force a source with --source db  or  --source json.

USAGE (run from the Casino Scanner repo root):
  pip install reportlab psycopg2-binary
  python ai_overview_pdf.py
  python ai_overview_pdf.py --source json --json backend/data/ai_buildout_universe_v2.json
  python ai_overview_pdf.py --source db --out ai_overview.pdf

Field names are normalized, so it works whether the source uses
category/Category, company_name/name, sub_category/subcategory, etc.
"""

import os
import sys
import json
import argparse
from collections import defaultdict, OrderedDict

# ---------------------------------------------------------------------------
# Defaults — adjust these paths if your repo layout differs
# ---------------------------------------------------------------------------
DEFAULT_JSON_CANDIDATES = [
    "backend/data/ai_buildout_universe_v2.json",   # enriched v2 (has category)
    "data/ai_buildout_universe_v2.json",
    "backend/data/ai_buildout_universe.json",       # original route file
    "data/ai_buildout_universe.json",
]
DEFAULT_TABLE = "tickers_ai_buildout"
DEFAULT_OUT = "ai_overview_by_category.pdf"
UNCATEGORIZED = "(uncategorized)"


# ---------------------------------------------------------------------------
# Field normalization — tolerate schema drift across v1 / v2 / DB
# ---------------------------------------------------------------------------
def _pick(d, *keys, default=""):
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    # case-insensitive fallback
    lower = {str(k).lower(): v for k, v in d.items()}
    for k in keys:
        v = lower.get(k.lower())
        if v not in (None, ""):
            return v
    return default


def normalize(rec):
    return {
        "ticker": str(_pick(rec, "ticker", "symbol")).strip().upper(),
        "company_name": str(_pick(rec, "company_name", "company", "name")).strip(),
        "category": str(_pick(rec, "category", "cat", default=UNCATEGORIZED)).strip() or UNCATEGORIZED,
        "subcategory": str(_pick(rec, "subcategory", "sub_category", "subcat")).strip(),
        "source": str(_pick(rec, "source", "source_pdf")).strip(),
    }


# ---------------------------------------------------------------------------
# Source 1: Postgres
# ---------------------------------------------------------------------------
def load_from_db(table=DEFAULT_TABLE):
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        return None
    try:
        import psycopg2
        import psycopg2.extras
    except ImportError:
        print("psycopg2 not installed; skipping DB. (pip install psycopg2-binary)", file=sys.stderr)
        return None
    try:
        conn = psycopg2.connect(dsn)
    except Exception as e:
        print(f"Could not connect to DATABASE_URL: {e}", file=sys.stderr)
        return None
    try:
        cur = conn.cursor()
        # confirm table exists
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)",
            (table,),
        )
        if not cur.fetchone()[0]:
            print(f"Table '{table}' not found in DB; falling back to JSON.", file=sys.stderr)
            return None
        dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        # only active rows if the column exists, else all rows
        dict_cur.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
            (table,),
        )
        cols = {r["column_name"] for r in dict_cur.fetchall()}
        where = "WHERE active = TRUE" if "active" in cols else ""
        dict_cur.execute(f"SELECT * FROM {table} {where}")
        rows = [dict(r) for r in dict_cur.fetchall()]
        print(f"Loaded {len(rows)} rows from DB table '{table}'.")
        return rows
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Source 2: JSON file (route data source)
# ---------------------------------------------------------------------------
def load_from_json(path=None):
    candidates = [path] if path else DEFAULT_JSON_CANDIDATES
    for p in candidates:
        if p and os.path.exists(p):
            with open(p, "r") as f:
                data = json.load(f)
            # v2 master has {"tickers": [...]}, route file may be a bare list
            rows = data["tickers"] if isinstance(data, dict) and "tickers" in data else data
            print(f"Loaded {len(rows)} rows from JSON file '{p}'.")
            return rows
    print("No JSON data file found. Looked in: " + ", ".join(c for c in candidates if c),
          file=sys.stderr)
    return None


# ---------------------------------------------------------------------------
# Build the PDF
# ---------------------------------------------------------------------------
def build_pdf(records, out_path):
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.units import inch
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak,
    )

    rows = [normalize(r) for r in records]
    rows = [r for r in rows if r["ticker"]]  # drop blanks

    # group by category, keep tickers sorted A-Z within each
    by_cat = defaultdict(list)
    for r in rows:
        by_cat[r["category"]].append(r)
    for cat in by_cat:
        by_cat[cat].sort(key=lambda r: r["ticker"])
    # categories sorted by size desc, then name
    ordered_cats = OrderedDict(
        sorted(by_cat.items(), key=lambda kv: (-len(kv[1]), kv[0].lower()))
    )

    total = len(rows)
    n_cats = len(ordered_cats)

    styles = getSampleStyleSheet()
    h1 = styles["Title"]
    sub = ParagraphStyle("sub", parent=styles["Normal"], fontSize=9,
                         textColor=colors.grey, spaceAfter=14)
    cat_head = ParagraphStyle("cathead", parent=styles["Heading2"], fontSize=13,
                              textColor=colors.HexColor("#8b5cf6"), spaceBefore=14,
                              spaceAfter=6, keepWithNext=True)
    cell = ParagraphStyle("cell", parent=styles["Normal"], fontSize=9, leading=11)

    doc = SimpleDocTemplate(
        out_path, pagesize=letter,
        leftMargin=0.7 * inch, rightMargin=0.7 * inch,
        topMargin=0.7 * inch, bottomMargin=0.7 * inch,
        title="AI Overview Universe by Category",
    )
    story = []

    story.append(Paragraph("AI Overview Universe — by Category", h1))
    story.append(Paragraph(
        f"{total} companies across {n_cats} categories. "
        f"Pulled directly from Casino Scanner.", sub))

    # ---- summary table: category -> count ----
    summary_data = [["Category", "Count"]]
    for cat, items in ordered_cats.items():
        summary_data.append([cat, str(len(items))])
    summary_data.append(["TOTAL", str(total)])

    summary = Table(summary_data, colWidths=[5.3 * inch, 0.9 * inch], repeatRows=1)
    summary.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a1c1f")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("ROWBACKGROUNDS", (0, 1), (-1, -2), [colors.white, colors.HexColor("#f3f0fa")]),
        ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#e8e0fb")),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
        ("ALIGN", (1, 0), (1, -1), "RIGHT"),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cccccc")),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    story.append(summary)
    story.append(PageBreak())

    # ---- one section per category ----
    has_sub = any(r["subcategory"] for r in rows)
    for cat, items in ordered_cats.items():
        story.append(Paragraph(f"{cat}  ({len(items)})", cat_head))
        if has_sub:
            header = ["Ticker", "Company", "Subcategory"]
            widths = [0.9 * inch, 3.3 * inch, 2.9 * inch]
        else:
            header = ["Ticker", "Company"]
            widths = [0.9 * inch, 6.2 * inch]
        data = [header]
        for r in items:
            row = [r["ticker"], Paragraph(r["company_name"], cell)]
            if has_sub:
                row.append(Paragraph(r["subcategory"], cell))
            data.append(row)
        t = Table(data, colWidths=widths, repeatRows=1)
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a1c1f")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTNAME", (0, 1), (0, -1), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f3f0fa")]),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#dddddd")),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ]))
        story.append(t)
        story.append(Spacer(1, 6))

    doc.build(story)
    print(f"Wrote {out_path}  ({total} companies, {n_cats} categories).")


# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Export Casino Scanner AI overview universe to PDF, grouped by category.")
    ap.add_argument("--source", choices=["auto", "db", "json"], default="auto")
    ap.add_argument("--json", help="explicit path to the universe JSON file")
    ap.add_argument("--table", default=DEFAULT_TABLE, help="Postgres table name")
    ap.add_argument("--out", default=DEFAULT_OUT, help="output PDF path")
    args = ap.parse_args()

    records = None
    if args.source in ("auto", "db"):
        records = load_from_db(args.table)
    if records is None and args.source in ("auto", "json"):
        records = load_from_json(args.json)

    if not records:
        print("ERROR: no data loaded. Set DATABASE_URL or point --json at the universe file.",
              file=sys.stderr)
        sys.exit(1)

    build_pdf(records, args.out)


if __name__ == "__main__":
    main()
