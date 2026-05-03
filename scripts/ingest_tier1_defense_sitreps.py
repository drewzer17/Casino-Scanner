#!/usr/bin/env python3
"""
ingest_tier1_defense_sitreps.py
──────────────────────────��──────
INSERT/UPSERT 14 sitreps from AI_Buildout_Tier1_Defense_v2.pdf
into the sitreps table on Railway.

Maps the v2 JSON schema to the existing DB column layout.
Safe to re-run: uses ON CONFLICT (ticker) DO UPDATE.

Usage:
    python3 scripts/ingest_tier1_defense_sitreps.py
"""
from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path

import psycopg2
import psycopg2.extras

REPO_ROOT  = Path(__file__).resolve().parent.parent
INPUT_FILE = REPO_ROOT / "backend" / "data" / "ai_buildout_tier1_defense_sitreps.json"

_DEFAULT_DB = (
    "postgresql://postgres:uIcMzUUNlqmhekvgoKBcQxRIQOoajQyu"
    "@nozomi.proxy.rlwy.net:46336/railway"
)
DATABASE_URL = os.environ.get("DATABASE_URL", _DEFAULT_DB)
if "railway.internal" in DATABASE_URL:
    DATABASE_URL = _DEFAULT_DB

# Category / subcategory per the task spec (not in the input JSON)
CATEGORY_MAP: dict[str, tuple[str, str]] = {
    "AAPL": ("16. Hyperscalers & Mega-Cap Cloud",        "Vertical AI Stack (Silicon + OS + Apps)"),
    "TSLA": ("02. AI Silicon & Analog",                  "Vertical AI Compute (Dojo + Inference)"),
    "TXN":  ("02. AI Silicon & Analog",                  "Analog & Mixed Signal (Industrial)"),
    "MARA": ("13. Bitcoin Miners (Repurposed AI Compute)","Pure-Play Bitcoin (Slow AI Pivot)"),
    "COHU": ("05. Semiconductor Test & Inspection",      "Test Handlers (Final Test Automation)"),
    "WTS":  ("10. Cooling & Thermal",                    "Water Systems & Plumbing (Data Center)"),
    "NXPI": ("02. AI Silicon & Analog",                  "Automotive & Edge AI Semis"),
    "AEP":  ("12. Power Generation & Nuclear",           "Transmission Utility (Multi-State)"),
    "DUK":  ("12. Power Generation & Nuclear",           "Utility (Carolinas + Southeast)"),
    "SO":   ("12. Power Generation & Nuclear",           "Utility + New Nuclear (Vogtle)"),
    "ENR":  ("11. Power Infrastructure & Equipment",     "Heavy Gas Turbines & Grid Tech"),
    "RTX":  ("19. Defense & Aerospace",                  "Defense Conglomerate (Engines + Avionics + Missiles)"),
    "LMT":  ("19. Defense & Aerospace",                  "Top-Tier Defense Prime (F-35 + Missiles + Space)"),
    "NOC":  ("19. Defense & Aerospace",                  "Defense Prime (B-21 + Space + Mission Systems)"),
}


def build_raw_text(s: dict) -> str:
    """Concatenate all text sections into a single raw_text blob."""
    parts = []
    if s.get("what_they_do"):
        parts.append(f"WHAT THEY DO\n{s['what_they_do']}")
    if s.get("bear_case"):
        parts.append(f"BEAR CASE\n{s['bear_case']}")
    if s.get("contrarian_case"):
        parts.append(f"CONTRARIAN CASE\n{s['contrarian_case']}")
    if s.get("what_kills_it"):
        parts.append(f"WHAT KILLS IT\n{s['what_kills_it']}")
    kp = s.get("kill_probability", {})
    if kp.get("raw_text"):
        parts.append(f"KILL PROBABILITY\n{kp['raw_text']}")
    w = s.get("winners", {})
    if w.get("raw_text"):
        parts.append(f"WINNERS\n{w['raw_text']}")
    return "\n\n".join(parts)


def transform_winners(w: dict) -> list[dict]:
    """
    Input:  {"raw_text": "...", "parsed_tickers": [{"ticker": "TSM", "context_sentence": "..."}, ...]}
    Output: [{"ticker": "TSM", "rationale": "..."}, ...]
    """
    result = []
    for pt in w.get("parsed_tickers", []):
        ticker = (pt.get("ticker") or "").strip().upper()
        rationale = (pt.get("context_sentence") or "").strip()
        if ticker:
            result.append({"ticker": ticker, "rationale": rationale})
    return result


UPSERT_SQL = """
INSERT INTO sitreps (
    ticker, company_name, last_updated,
    what_they_do, hidden_angles,
    bear_case, contrarian_case, what_kills_it,
    kill_probability_low, kill_probability_high, kill_horizon_months,
    kill_components, winners,
    source_pdf, raw_text, sections_parsed, parse_warnings,
    category, subcategory
)
VALUES (
    %(ticker)s, %(company_name)s, %(last_updated)s,
    %(what_they_do)s, %(hidden_angles)s,
    %(bear_case)s, %(contrarian_case)s, %(what_kills_it)s,
    %(kill_probability_low)s, %(kill_probability_high)s, %(kill_horizon_months)s,
    %(kill_components)s, %(winners)s,
    %(source_pdf)s, %(raw_text)s, %(sections_parsed)s, %(parse_warnings)s,
    %(category)s, %(subcategory)s
)
ON CONFLICT (ticker) DO UPDATE SET
    company_name          = EXCLUDED.company_name,
    last_updated          = EXCLUDED.last_updated,
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
    category              = EXCLUDED.category,
    subcategory           = EXCLUDED.subcategory,
    updated_at            = NOW()
"""

print("Loading input file…")
with open(INPUT_FILE) as f:
    data = json.load(f)

sitreps = data["sitreps"]
print(f"  {len(sitreps)} sitreps to process.")

print("Connecting to Railway DB…")
conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = False
cur = conn.cursor()

today = date.today()
ok = 0
errors = []

for s in sitreps:
    ticker = s["ticker"].upper().strip()
    cat, subcat = CATEGORY_MAP.get(ticker, ("", ""))
    kp = s.get("kill_probability", {})
    w  = s.get("winners", {})

    row = {
        "ticker":               ticker,
        "company_name":         s.get("company_name", ""),
        "last_updated":         today,
        "what_they_do":         s.get("what_they_do", ""),
        "hidden_angles":        json.dumps(s.get("hidden_angles", [])),
        "bear_case":            s.get("bear_case", ""),
        "contrarian_case":      s.get("contrarian_case", ""),
        "what_kills_it":        s.get("what_kills_it", ""),
        "kill_probability_low":  kp.get("low"),
        "kill_probability_high": kp.get("high"),
        "kill_horizon_months":   kp.get("horizon_months"),
        "kill_components":      json.dumps(kp.get("components", [])),
        "winners":              json.dumps(transform_winners(w)),
        "source_pdf":           "AI_Buildout_Tier1_Defense_v2.pdf",
        "raw_text":             build_raw_text(s),
        "sections_parsed":      7,
        "parse_warnings":       json.dumps([]),
        "category":             cat,
        "subcategory":          subcat,
    }

    try:
        cur.execute(UPSERT_SQL, row)
        ok += 1
        print(f"  ✓ {ticker}")
    except Exception as e:
        errors.append(f"{ticker}: {e}")
        print(f"  ✗ {ticker}: {e}")

if errors:
    conn.rollback()
    print(f"\n{len(errors)} ERROR(S) — rolled back. Fix and re-run.")
    for e in errors:
        print(f"  {e}")
else:
    conn.commit()
    print(f"\nCommitted {ok} sitreps.")

cur.close()
conn.close()

# Verify
print("\nVerifying row count…")
conn2 = psycopg2.connect(DATABASE_URL)
cur2 = conn2.cursor()
cur2.execute("SELECT COUNT(*) FROM sitreps WHERE source_pdf = 'AI_Buildout_Tier1_Defense_v2.pdf'")
count = cur2.fetchone()[0]
cur2.close(); conn2.close()
print(f"  Rows with source_pdf = 'AI_Buildout_Tier1_Defense_v2.pdf': {count}")
assert count == 14, f"Expected 14, got {count}"
print("  ✓ Verification passed.")
