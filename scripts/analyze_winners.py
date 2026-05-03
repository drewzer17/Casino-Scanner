#!/usr/bin/env python3
"""
analyze_winners.py
──────────────────
One-time winner-frequency analysis across all sitreps.

Reads the `sitreps` table from Railway, tallies how often each ticker
appears as a "winner" (beneficiary when that company fails), and writes
a markdown report broken down by overall, per-category, and per-lens.

Usage:
    python3 scripts/analyze_winners.py

No arguments needed — DB URL and data files are resolved automatically.

READ-ONLY.  No database writes, no git commits.
"""
from __future__ import annotations

import json
import os
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

import psycopg2
import psycopg2.extras

# ── Paths ──────────────────────────────────────────────────────────────────────
REPO_ROOT   = Path(__file__).resolve().parent.parent
DATA_DIR    = REPO_ROOT / "backend" / "data"
OUTPUT_DIR  = REPO_ROOT / "output"
UNIVERSE    = DATA_DIR / "ai_buildout_universe.json"
LENSES      = DATA_DIR / "ai_overview_lenses.json"
TODAY       = date.today().isoformat()
OUTPUT_FILE = OUTPUT_DIR / f"winner-frequency-analysis-{TODAY}.md"

# ── DB connection (same public Railway proxy used by ETL scripts) ───────────────
_DEFAULT_DB = (
    "postgresql://postgres:uIcMzUUNlqmhekvgoKBcQxRIQOoajQyu"
    "@nozomi.proxy.rlwy.net:46336/railway"
)
DATABASE_URL = os.environ.get("DATABASE_URL", _DEFAULT_DB)
if "railway.internal" in DATABASE_URL:
    DATABASE_URL = _DEFAULT_DB

# ── Helpers ────────────────────────────────────────────────────────────────────

def progress(msg: str) -> None:
    print(msg, flush=True)

def bar(done: int, total: int, width: int = 30) -> str:
    filled = int(width * done / max(total, 1))
    return "▓" * filled + "░" * (width - filled)

def fmt_sources(sources: list[str], limit: int = 12) -> str:
    if len(sources) <= limit:
        return ", ".join(sorted(sources))
    shown = sorted(sources)[:limit]
    return ", ".join(shown) + f" (+{len(sources) - limit} more)"

# ── Step 1: load metadata JSON files ──────────────────────────────────────────

progress("Loading metadata files…")

# universe: ticker → { category, subcategory, company_name }
universe_meta: dict[str, dict] = {}
if UNIVERSE.exists():
    with open(UNIVERSE) as f:
        univ = json.load(f)
    for t in univ.get("tickers", []):
        sym = (t.get("ticker") or "").upper()
        if sym:
            universe_meta[sym] = {
                "category":    t.get("category") or "",
                "subcategory": t.get("subcategory") or "",
                "company":     t.get("company_name") or t.get("name") or "",
            }
else:
    progress(f"  ⚠  Universe file not found: {UNIVERSE}")

# lenses: ticker → primary lens name, and lens → [tickers]
ticker_primary_lens: dict[str, str]       = {}  # source ticker → primary lens name
lens_tickers:        dict[str, list[str]] = {}  # lens name → [tickers]

if LENSES.exists():
    with open(LENSES) as f:
        lenses_data = json.load(f)
    lens_names_map: dict[str, list[str]] = {}  # ticker → [lens names]
    for lens in lenses_data.get("lenses", []):
        name = lens.get("name", "")
        tickers_in_lens = [t.upper() for t in lens.get("tickers", [])]
        lens_tickers[name] = tickers_in_lens
        for sym in tickers_in_lens:
            lens_names_map.setdefault(sym, []).append(name)
        for sym in lens.get("primary_for", []):
            ticker_primary_lens[sym.upper()] = name
    # Auto-assign primary for single-lens tickers
    for sym, names in lens_names_map.items():
        if sym not in ticker_primary_lens and len(names) == 1:
            ticker_primary_lens[sym] = names[0]
else:
    progress(f"  ⚠  Lenses file not found: {LENSES}")

progress(f"  Universe: {len(universe_meta)} tickers  |  Lenses: {len(lens_tickers)} defined")

# ── Step 2: query sitreps ──────────────────────────────────────────────────────

progress("\nConnecting to Railway DB…")
conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
conn.autocommit = True
cur = conn.cursor()

cur.execute("""
    SELECT ticker, company_name, winners, category, subcategory
    FROM   sitreps
    ORDER  BY ticker
""")
rows = cur.fetchall()
cur.close()
conn.close()
progress(f"  Fetched {len(rows)} sitrep rows.\n")

# ── Step 3: build frequency maps ──────────────────────────────────────────────

progress(f"Analyzing {len(rows)} sitreps…")

# winner_ticker → { count, sources: set of source tickers }
overall: dict[str, dict] = defaultdict(lambda: {"count": 0, "sources": set()})

# category → { winner_ticker → { count, sources } }
by_category: dict[str, dict[str, dict]] = defaultdict(
    lambda: defaultdict(lambda: {"count": 0, "sources": set()})
)

# lens → { winner_ticker → { count, sources } }
by_lens: dict[str, dict[str, dict]] = defaultdict(
    lambda: defaultdict(lambda: {"count": 0, "sources": set()})
)

sitrep_tickers: set[str] = set()  # all source tickers in DB
errors: list[str] = []

# winner_ticker → { src_ticker → {rationale, company, lens, category} }
# Only the FIRST occurrence per (winner, source) pair is stored.
winner_detail: dict[str, dict[str, dict]] = defaultdict(dict)

for i, row in enumerate(rows):
    src = (row["ticker"] or "").upper()
    sitrep_tickers.add(src)

    # Progress bar every 20 rows
    if i % 20 == 0 or i == len(rows) - 1:
        pct = int((i + 1) / len(rows) * 100)
        sys.stdout.write(f"\r  {bar(i + 1, len(rows))} {pct}%  ({i + 1}/{len(rows)})")
        sys.stdout.flush()

    # Parse winners field
    raw_winners = row.get("winners")
    if not raw_winners:
        continue

    if isinstance(raw_winners, str):
        try:
            raw_winners = json.loads(raw_winners)
        except Exception:
            errors.append(f"{src}: JSON parse error on winners field")
            continue

    if not isinstance(raw_winners, list):
        continue

    # Collect unique winner tickers from this sitrep (count each once per source)
    seen_winners: set[str] = set()
    src_company = (row.get("company_name") or universe_meta.get(src, {}).get("company") or "")
    cat = (row.get("category") or universe_meta.get(src, {}).get("category") or "").strip()
    lens = ticker_primary_lens.get(src, "")

    for w in raw_winners:
        if isinstance(w, dict):
            wticker   = (w.get("ticker") or "").upper().strip()
            rationale = (w.get("rationale") or "").strip()
        elif isinstance(w, str):
            wticker   = w.upper().strip()
            rationale = ""
        else:
            continue
        if not wticker or wticker in seen_winners:
            continue
        seen_winners.add(wticker)

        # Overall
        overall[wticker]["count"] += 1
        overall[wticker]["sources"].add(src)

        # Per-source rationale detail (first occurrence wins)
        if src not in winner_detail[wticker]:
            winner_detail[wticker][src] = {
                "rationale": rationale,
                "company":   src_company,
                "lens":      lens,
                "category":  cat,
            }

        # Per category — use sitrep's own stored category, fall back to universe
        if cat:
            by_category[cat][wticker]["count"] += 1
            by_category[cat][wticker]["sources"].add(src)

        # Per lens — use primary lens of the SOURCE ticker
        if lens:
            by_lens[lens][wticker]["count"] += 1
            by_lens[lens][wticker]["sources"].add(src)

print()  # newline after progress bar

total_unique_winners = len(overall)
progress(f"\n  Done. {total_unique_winners} unique winner tickers across {len(rows)} sitreps.")

# ── Step 4: build report ───────────────────────────────────────────────────────

def top_n(freq_dict: dict[str, dict], n: int = 20) -> list[tuple[str, int, list[str]]]:
    """Return list of (ticker, count, sorted_sources) sorted by count desc."""
    return sorted(
        [(t, d["count"], sorted(d["sources"])) for t, d in freq_dict.items()],
        key=lambda x: (-x[1], x[0]),
    )[:n]

lines: list[str] = []

lines += [
    f"# Winner Frequency Analysis",
    f"",
    f"**Generated:** {TODAY}  ",
    f"**Total sitreps analyzed:** {len(rows)}  ",
    f"**Total unique winner tickers named:** {total_unique_winners}",
    f"",
    f"> These are tickers that appear as beneficiaries (\"who wins if this fails\")",
    f"> across the AI buildout universe sitreps. High frequency = convergence",
    f"> beneficiary — wins across many independent failure scenarios.",
    f"",
]

# ── Overall top 20 ────────────────────────────────────────────────────────────
lines += [
    "## Overall Top 20 Most-Named Winners",
    "",
    "| Rank | Winner | Times Named | Source Tickers |",
    "|------|--------|-------------|----------------|",
]
for rank, (wticker, count, sources) in enumerate(top_n(overall, 20), 1):
    company = universe_meta.get(wticker, {}).get("company", "")
    display = f"{wticker}" + (f" — {company}" if company else "")
    lines.append(f"| {rank} | **{display}** | {count} | {fmt_sources(sources)} |")

lines += [""]

# ── Per category ──────────────────────────────────────────────────────────────
lines += [
    "## Top 5 Per Category",
    "",
    "_Only categories with 3+ sitreps shown._",
    "",
]

# Count how many source sitreps each category has
cat_sitrep_counts: dict[str, int] = defaultdict(int)
for row in rows:
    cat = (row.get("category") or universe_meta.get((row["ticker"] or "").upper(), {}).get("category") or "").strip()
    if cat:
        cat_sitrep_counts[cat] += 1

for cat in sorted(by_category.keys()):
    if cat_sitrep_counts.get(cat, 0) < 3:
        continue
    lines += [
        f"### {cat}",
        f"_({cat_sitrep_counts[cat]} source sitreps in category)_",
        "",
        "| Rank | Winner | Count | Source Tickers |",
        "|------|--------|-------|----------------|",
    ]
    for rank, (wticker, count, sources) in enumerate(top_n(by_category[cat], 5), 1):
        company = universe_meta.get(wticker, {}).get("company", "")
        display = f"{wticker}" + (f" — {company}" if company else "")
        lines.append(f"| {rank} | **{display}** | {count} | {fmt_sources(sources, 8)} |")
    lines += [""]

# ── Per lens ──────────────────────────────────────────────────────────────────
lines += [
    "## Top 5 Per Lens",
    "",
    "_Only lenses with 3+ source tickers shown._",
    "",
]

for lens_name in sorted(by_lens.keys()):
    lens_src_count = len(by_lens[lens_name])  # distinct source tickers that contributed
    if lens_src_count < 3:
        continue
    lines += [
        f"### {lens_name}",
        f"_({lens_src_count} source tickers with winners in this lens)_",
        "",
        "| Rank | Winner | Count | Source Tickers |",
        "|------|--------|-------|----------------|",
    ]
    for rank, (wticker, count, sources) in enumerate(top_n(by_lens[lens_name], 5), 1):
        company = universe_meta.get(wticker, {}).get("company", "")
        display = f"{wticker}" + (f" — {company}" if company else "")
        lines.append(f"| {rank} | **{display}** | {count} | {fmt_sources(sources, 8)} |")
    lines += [""]

# ── Patterns and observations ────────────────────────────────────────────────��

# Cross-cutting: winners named in 3+ different categories
winner_cat_breadth: dict[str, set[str]] = defaultdict(set)
for cat, freq in by_category.items():
    for wticker in freq:
        winner_cat_breadth[wticker].add(cat)

cross_cutting = sorted(
    [(t, cats) for t, cats in winner_cat_breadth.items() if len(cats) >= 3],
    key=lambda x: (-len(x[1]), x[0]),
)[:8]

# Category-specific: winners named in only 1 category, with count >= 3
category_specific = sorted(
    [(t, list(cats)[0], overall[t]["count"])
     for t, cats in winner_cat_breadth.items()
     if len(cats) == 1 and overall[t]["count"] >= 3],
    key=lambda x: (-x[2], x[0]),
)[:8]

# Winners without their own sitrep (potential additions to universe)
no_sitrep_winners = sorted(
    [(t, overall[t]["count"], sorted(overall[t]["sources"]))
     for t in overall
     if t not in sitrep_tickers and overall[t]["count"] >= 3],
    key=lambda x: (-x[1], x[0]),
)[:10]

# Winners not in the universe JSON either
not_in_universe = [t for t, _, _ in no_sitrep_winners if t not in universe_meta]

lines += [
    "## Patterns and Observations",
    "",
]

if cross_cutting:
    lines += ["**Cross-cutting beneficiaries** (named in 3+ different categories):", ""]
    for wticker, cats in cross_cutting:
        company = universe_meta.get(wticker, {}).get("company", "")
        cat_list = ", ".join(sorted(cats))
        lines.append(f"- **{wticker}**{' (' + company + ')' if company else ''} — "
                      f"{overall[wticker]['count']} total mentions across "
                      f"{len(cats)} categories: {cat_list}")
    lines += [""]

if category_specific:
    lines += ["**Category-specific beneficiaries** (3+ mentions but in only one category):", ""]
    for wticker, cat, count in category_specific:
        company = universe_meta.get(wticker, {}).get("company", "")
        lines.append(f"- **{wticker}**{' (' + company + ')' if company else ''} — "
                      f"{count} mentions, all within: {cat}")
    lines += [""]

if no_sitrep_winners:
    lines += [
        "**Named 3+ times but have no sitrep yet** (potential universe additions):",
        "",
    ]
    for wticker, count, sources in no_sitrep_winners:
        company = universe_meta.get(wticker, {}).get("company", "")
        in_univ = "✓ in universe" if wticker in universe_meta else "⚠ not in universe"
        lines.append(f"- **{wticker}**{' (' + company + ')' if company else ''} — "
                      f"{count} mentions [{in_univ}] — named by: {fmt_sources(sources, 6)}")
    lines += [""]

# ── Errors ────────────────────────────────────────────────────────────────────
if errors:
    lines += ["## Errors", ""]
    for e in errors:
        lines.append(f"- {e}")
    lines += [""]

# ── Write markdown file ───────────────────────────────────────────────────────
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")

# ── Write winner-frequency.json ───────────────────────────────────────────────
from datetime import datetime as _dt

top5 = top_n(overall, 5)

# Cross-cutting: 5+ categories, top 10 by sectors_count then mentions
cross_cutting_json = sorted(
    [
        (t, cats, overall[t]["count"])
        for t, cats in winner_cat_breadth.items()
        if len(cats) >= 5
    ],
    key=lambda x: (-len(x[1]), -x[2], x[0]),
)[:10]

def _build_sources_list(wticker: str) -> list[dict]:
    """Return sorted source list for a winner ticker."""
    sources = []
    for src_t, detail in winner_detail[wticker].items():
        sources.append({
            "source_ticker":   src_t,
            "source_company":  detail["company"],
            "source_lens":     detail["lens"],
            "source_category": detail["category"],
            "rationale":       detail["rationale"],
        })
    # Sort by lens (primary grouping key), then ticker alphabetically
    sources.sort(key=lambda x: (x["source_lens"] or "\xff", x["source_ticker"]))
    return sources

def _winner_obj(wticker: str, count: int, cats_set: set) -> dict:
    """Build a fully-enriched winner object."""
    in_univ = wticker in sitrep_tickers
    um      = universe_meta.get(wticker, {})
    return {
        "ticker":       wticker,
        "company_name": um.get("company", ""),
        "mentions":     count,
        "in_universe":  in_univ,
        "primary_lens": ticker_primary_lens.get(wticker) if in_univ else None,
        "category":     um.get("category") or None,
        "subcategory":  um.get("subcategory") or None,
        "sectors_count": len(cats_set),
        "sectors":      sorted(cats_set),
        "sources":      _build_sources_list(wticker),
    }

# all_winners_with_sources: every winner ticker → full enriched object
all_winners: dict[str, dict] = {}
for wticker, d in overall.items():
    cats = winner_cat_breadth.get(wticker, set())
    all_winners[wticker] = _winner_obj(wticker, d["count"], cats)

freq_json = {
    "generated_at":            _dt.utcnow().isoformat() + "Z",
    "total_sitreps_analyzed":  len(rows),
    "total_unique_winners":    total_unique_winners,
    "top_5_overall": [
        _winner_obj(wticker, count, winner_cat_breadth.get(wticker, set()))
        for wticker, count, _ in top5
    ],
    "cross_cutting_winners": [
        _winner_obj(t, count, cats)
        for t, cats, count in cross_cutting_json
    ],
    "all_winners_with_sources": all_winners,
}

JSON_OUT = REPO_ROOT / "backend" / "data" / "winner-frequency.json"
JSON_OUT.parent.mkdir(parents=True, exist_ok=True)
JSON_OUT.write_text(json.dumps(freq_json, indent=2), encoding="utf-8")
progress(f"  JSON written to: {JSON_OUT} ({JSON_OUT.stat().st_size // 1024} KB)")

# ── Terminal summary ──────────────────────────────────────────────────────────
print(f"\nAnalysis complete. Report saved to:\n  {OUTPUT_FILE}\n")
print(f"Top 5 overall winners:")
for rank, (wticker, count, _) in enumerate(top5, 1):
    company = universe_meta.get(wticker, {}).get("company", "")
    label = f"{wticker}" + (f" ({company})" if company else "")
    print(f"  {rank}. {label}: {count} mention{'s' if count != 1 else ''}")

if no_sitrep_winners:
    print(f"\n{len(no_sitrep_winners)} winner(s) named 3+ times with no sitrep — "
          f"see 'Patterns' section.")
if errors:
    print(f"\n{len(errors)} parse error(s) — see 'Errors' section in report.")
