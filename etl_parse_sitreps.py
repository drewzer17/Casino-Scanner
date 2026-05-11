#!/usr/bin/env python3
"""
etl_parse_sitreps.py
Parse AI_Buildout_Ultimate_Master_Final.pdf → sitreps table in Postgres.
Phase A: ETL only. No routes, no templates.

Usage:
    DATABASE_URL=postgresql://... python3 etl_parse_sitreps.py
"""
from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import psycopg2
import psycopg2.extras
import pypdf

# ── Config ─────────────────────────────────────────────────────────────────────
PDF_PATH = Path("/Users/andrewreberm4/Downloads/AI_Buildout_Ultimate_Master_Final.pdf")
MIGRATION_SQL = Path(__file__).parent / "migrations" / "001_create_sitreps.sql"
REPORT_PATH = Path(__file__).parent / "etl_report.txt"
PDF_FILENAME = "AI_Buildout_Ultimate_Master_Final_177names.pdf"

# Public Railway URL for local execution; falls back to DATABASE_URL env var
_DEFAULT_DB = (
    "postgresql://postgres:uIcMzUUNlqmhekvgoKBcQxRIQOoajQyu"
    "@nozomi.proxy.rlwy.net:46336/railway"
)
DATABASE_URL = os.environ.get("DATABASE_URL", _DEFAULT_DB)
# If DATABASE_URL points to the internal Railway hostname, swap to the public proxy
if "railway.internal" in DATABASE_URL:
    DATABASE_URL = _DEFAULT_DB

# ── Regex patterns ─────────────────────────────────────────────────────────────

# Block separator: matches "Company Name — TICKER" on its own line.
# Fixes vs naive pattern:
#   [A-Za-z]  — allow lowercase start (nVent Electric)
#   [0-9]     — allow digits in name  (Hut 8)
#   [1,6]     — allow 1-char tickers  (J = Jacobs Solutions, S = SentinelOne)
BLOCK_SEP = re.compile(
    r"^([A-Za-z][A-Za-z0-9 &\.\-\'\`\/\(\)]+?)\s+[—–-]+\s+([A-Z]{1,6})\s*$",
    re.MULTILINE,
)

# Page header noise to strip from extracted text before parsing sections
PAGE_HEADER = re.compile(
    r"Prepared for Bob[^\n]*\nPage \d+\n?",
    re.MULTILINE,
)

# Section header definitions ─────────────────────────────────────────────────
# Each entry: (strict_patterns, fallback_patterns)
# strict  → no warning logged
# fallback → logged to parse_warnings as {section, "fallback", actual_text}
# neither  → section MISSING, sections_parsed not incremented

SECTION_DEFS: dict[str, tuple[list, list]] = {
    "what_they_do": (
        [re.compile(r"^What they do\.$", re.MULTILINE)],
        [re.compile(r"^What\s+they\s+do[\.\:]?\s*$", re.MULTILINE | re.IGNORECASE)],
    ),
    "bear_case": (
        [
            re.compile(r"^The bear case\.$", re.MULTILINE),
            re.compile(r"^Bear case\.$",     re.MULTILINE),
        ],
        [re.compile(r"^(?:The )?bear case[\.\:]\s*$", re.MULTILINE | re.IGNORECASE)],
    ),
    "contrarian_case": (
        [re.compile(r"^The contrarian case\.$", re.MULTILINE)],
        [
            re.compile(r"^The contrarian case[\.\:]\s*$",           re.MULTILINE | re.IGNORECASE),
            re.compile(r"^The contrarian (?:case|read|view)\s+\S",  re.MULTILINE | re.IGNORECASE),
            re.compile(r"^Contrarian (?:case|view|read)[\.\:]\s*$", re.MULTILINE | re.IGNORECASE),
        ],
    ),
    "what_kills_it": (
        [re.compile(r"^What kills it\.$", re.MULTILINE)],
        [re.compile(r"^What kills it[\.\:]\s*$", re.MULTILINE | re.IGNORECASE)],
    ),
    "kill_probability": (
        [re.compile(r"^Probability of kill scenario\.$", re.MULTILINE)],
        [
            re.compile(r"^Probability of kill[^\n]*$", re.MULTILINE | re.IGNORECASE),
            re.compile(r"^Kill probability[^\n]*$",    re.MULTILINE | re.IGNORECASE),
        ],
    ),
    "winners": (
        [re.compile(r"^Who wins if the kill fires\.$", re.MULTILINE)],
        [
            re.compile(r"^Who wins if[^\n]*$",  re.MULTILINE | re.IGNORECASE),
            re.compile(r"^Winners if[^\n]*$",   re.MULTILINE | re.IGNORECASE),
        ],
    ),
}

# Ordered section names — used to bound content extraction
ORDERED_SECTIONS = [
    "what_they_do",
    "bear_case",
    "contrarian_case",
    "what_kills_it",
    "kill_probability",
    "winners",
]

# 7 sections counted for sections_parsed (hidden_angles is derived from what_they_do)
ALL_SEVEN = [
    "what_they_do",
    "hidden_angles",   # derived — not in ORDERED_SECTIONS
    "bear_case",
    "contrarian_case",
    "what_kills_it",
    "kill_probability",
    "winners",
]

# ── Kill probability sub-parsers ───────────────────────────────────────────────

# Overall range: "35-45%" or "35%-45%" — find the first in the section
_RANGE_PAT = re.compile(r"(\d{1,3})\s*%?\s*[-–]\s*(\d{1,3})\s*%")
# Horizon: "24 months" / "36 months" / "2 years" — first occurrence
_HORIZON_PAT = re.compile(r"over\s+(\d+(?:\.\d+)?)\s*(month|year)s?", re.IGNORECASE)
# Component line: text description, tilde, range%
# Matches: "some description text ~30-35%"  or  "some description ~30-35%;"
_COMP_PAT = re.compile(
    r"([A-Za-z][^~;\n]{10,150}?)\s*~\s*(\d{1,3})\s*[-–]\s*(\d{1,3})\s*%",
)


def parse_kill_probability(text: str) -> tuple[dict, list]:
    result = {
        "kill_probability_low":  None,
        "kill_probability_high": None,
        "kill_horizon_months":   None,
        "kill_components":       None,
    }
    warnings: list[dict] = []

    # ── Overall range ──
    m = _RANGE_PAT.search(text)
    if m:
        lo, hi = int(m.group(1)), int(m.group(2))
        # Sanity: lo <= hi and both 0–100
        if 0 <= lo <= hi <= 100:
            result["kill_probability_low"]  = lo
            result["kill_probability_high"] = hi
        else:
            warnings.append({"section": "kill_probability", "issue": "range_values_out_of_bounds",
                              "raw": m.group()})
    else:
        warnings.append({"section": "kill_probability", "issue": "range_not_found"})

    # ── Horizon ──
    m = _HORIZON_PAT.search(text)
    if m:
        n    = float(m.group(1))
        unit = m.group(2).lower()
        result["kill_horizon_months"] = int(n * 12) if "year" in unit else int(n)
    else:
        warnings.append({"section": "kill_probability", "issue": "horizon_not_found"})

    # ── Components ──
    # Try to find "Component breakdown:" block first for better accuracy
    cb_start = text.lower().find("component breakdown")
    cb_text  = text[cb_start:] if cb_start >= 0 else text
    components: list[dict] = []
    seen_descs: set[str] = set()
    for cm in _COMP_PAT.finditer(cb_text):
        desc = cm.group(1).strip().rstrip(",;. (")
        plo  = int(cm.group(2))
        phi  = int(cm.group(3))
        key  = desc[:40].lower()
        if key in seen_descs:
            continue
        seen_descs.add(key)
        if 0 <= plo <= phi <= 100 and len(desc) >= 10:
            components.append({"description": desc, "pct_low": plo, "pct_high": phi})
    if components:
        result["kill_components"] = components

    return result, warnings


# ── Winners parser ─────────────────────────────────────────────────────────────

# Tickers appear as "(AAPL)" or standalone uppercase words
_TICKER_IN_PAREN = re.compile(r"\(([A-Z]{1,6})\)")

# Common uppercase acronyms that are NOT tickers
_NOT_TICKER = frozenset({
    "AI", "CEO", "CFO", "CTO", "COO", "US", "EU", "UK", "IPO", "GPU", "CPU",
    "AWS", "OCI", "GCP", "CXL", "HPC", "API", "ETF", "HBM", "PCIe", "ADR",
    "TAM", "EPS", "EBIT", "EBITDA", "ASP", "SaaS", "PaaS", "IaaS", "SOX",
    "CPI", "GDP", "USD", "YoY", "QoQ", "TTM", "GAAP", "NVLINK", "UALink",
    "CHIPS", "OSAT", "DDR", "NVMe", "SSD", "HDD", "LNG", "ESG", "WTI",
    "LLM", "CDU", "PDU", "ERCOT", "PPA", "ARR", "MRR", "RPO", "CAD",
    "OEM", "JV", "SPV", "PE", "VC", "ML", "DLT", "PCB", "FPGA", "ASIC",
    "DSP", "DCI", "CPO", "AEC", "ACC", "SMR", "HPC", "CapEx", "OpEx",
    "DDTL", "HALEU", "SMR", "BWR", "PWR", "IRA", "DoD", "FAA", "FCC",
    "FY", "Q1", "Q2", "Q3", "Q4", "H1", "H2", "IPP", "MW", "GW", "KW",
    "JIT", "SKU", "ERP", "CRM", "SOC", "NOC", "SLA", "SLO", "MLOps",
    "ONNX", "MTIA", "TPU", "NPU", "DPU", "AEP", "EPC", "EDR", "XDR",
    "SIEM", "IAM", "PAM", "MFA", "SSO", "VPN", "DLP", "WAF", "DRAM",
    "NAND", "RAM", "ROM", "GNS", "MAC", "USB", "PCI", "UPS", "PDU",
    "ILM", "HSM", "RAID", "NAS", "SAN", "OCP", "COSM", "BTC", "ETH",
    "NFT", "APR", "APY", "AMM", "DEX", "CEX", "CBDC", "SPAC", "PIPE",
    "RIF", "TLT", "SPY", "QQQ", "IWM", "XLF", "XLK", "XLE", "XLP",
    "AND", "FOR", "THE", "NOT", "ARE", "WAS", "HAS", "HAD", "MAY",
    "CAN", "ALL", "ANY", "NEW", "OLD", "BIG", "KEY", "TOP", "LOW",
    "HIGH", "NET", "TAX", "LAW", "WAR", "OIL", "GAS", "ACT", "TWO",
})


def parse_winners(text: str) -> list[dict]:
    winners: list[dict] = []
    seen: set[str] = set()

    for m in _TICKER_IN_PAREN.finditer(text):
        t = m.group(1)
        if t in _NOT_TICKER or t in seen:
            continue
        if len(t) < 1 or len(t) > 6:
            continue
        seen.add(t)
        # Rationale: enclosing sentence (~200 chars back, ~100 chars forward from match)
        start    = text.rfind("\n", 0, m.start())
        start    = start + 1 if start >= 0 else max(0, m.start() - 200)
        end      = text.find("\n", m.end())
        end      = end if end >= 0 else min(len(text), m.end() + 100)
        rationale = text[start:end].strip()
        winners.append({"ticker": t, "rationale": rationale})

    if not winners:
        # No parenthetical tickers — store full text as rationale-only
        winners.append({"ticker": None, "rationale": text.strip()[:1000]})

    return winners


# ── Hidden angles parser ───────────────────────────────────────────────────────

_HIDDEN_ANGLE_HEADER = re.compile(
    r"^Hidden angle \d+\s*[—–-]+\s*(.+?)\s*$",
    re.MULTILINE,
)


def parse_hidden_angles(text: str) -> list[dict]:
    angles: list[dict] = []
    matches = list(_HIDDEN_ANGLE_HEADER.finditer(text))
    for i, m in enumerate(matches):
        title    = m.group(1).strip()
        body_start = m.end()
        body_end   = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body       = text[body_start:body_end].strip()
        angles.append({"title": title, "body": body})
    return angles


# ── Section extraction ─────────────────────────────────────────────────────────

def find_section_match(text: str, section_name: str) -> tuple | None:
    """
    Try strict patterns first, then fallback.
    Returns (match_start, match_end, used_fallback, actual_text) or None.
    """
    strict_pats, fallback_pats = SECTION_DEFS[section_name]
    for pat in strict_pats:
        m = pat.search(text)
        if m:
            return m.start(), m.end(), False, m.group()
    for pat in fallback_pats:
        m = pat.search(text)
        if m:
            return m.start(), m.end(), True, m.group()
    return None


def extract_sections(text: str) -> tuple[dict, list]:
    """
    Locate all section headers in the block text and return:
    - sections dict: {name: content_text}
    - warnings list: [{section, type, actual_text}]
    """
    warnings: list[dict] = []
    found: dict[str, tuple] = {}   # {section_name: (marker_start, content_start)}

    for sname in ORDERED_SECTIONS:
        result = find_section_match(text, sname)
        if result is None:
            continue
        marker_start, header_end, used_fallback, actual = result
        if used_fallback:
            warnings.append({
                "section":     sname,
                "type":        "fallback",
                "actual_text": actual.strip()[:200],
            })
        # For inline contrarian starts ("The contrarian case is not..."),
        # the header text IS the first sentence of the content, so we use
        # marker_start as the content start to avoid losing that sentence.
        is_inline = (
            sname == "contrarian_case"
            and used_fallback
            and not actual.strip().endswith(".")
        )
        content_start = marker_start if is_inline else header_end
        found[sname] = (marker_start, content_start)

    # Sort by position so we can compute content boundaries correctly
    ordered = sorted(found.items(), key=lambda kv: kv[1][0])

    sections: dict[str, str] = {}
    for i, (sname, (marker_start, content_start)) in enumerate(ordered):
        # Content extends to the start of the next section marker
        next_marker = ordered[i + 1][1][0] if i + 1 < len(ordered) else len(text)
        content = text[content_start:next_marker].strip()
        if content:
            sections[sname] = content

    return sections, warnings


# ── Main sitrep parser ─────────────────────────────────────────────────────────

def parse_sitrep(company_name: str, ticker: str, raw_block: str) -> dict:
    """
    Parse one sitrep block into a DB-ready dict.
    Always stores raw_text.
    sections_parsed counts out of 7.
    """
    # Strip page-header noise
    clean = PAGE_HEADER.sub("", raw_block)

    sections, warnings = extract_sections(clean)

    sections_parsed = 0

    # what_they_do
    what_they_do = sections.get("what_they_do")
    if what_they_do:
        sections_parsed += 1

    # hidden_angles — derived from what_they_do
    hidden_angles_list: list[dict] = []
    if what_they_do:
        hidden_angles_list = parse_hidden_angles(what_they_do)
    if hidden_angles_list:
        sections_parsed += 1

    # bear_case
    bear_case = sections.get("bear_case")
    if bear_case:
        sections_parsed += 1

    # contrarian_case
    contrarian_case = sections.get("contrarian_case")
    if contrarian_case:
        sections_parsed += 1

    # what_kills_it
    what_kills_it = sections.get("what_kills_it")
    if what_kills_it:
        sections_parsed += 1

    # kill_probability
    kill_prob_data = {
        "kill_probability_low":  None,
        "kill_probability_high": None,
        "kill_horizon_months":   None,
        "kill_components":       None,
    }
    kp_content = sections.get("kill_probability")
    if kp_content:
        sections_parsed += 1
        kp_parsed, kp_warns = parse_kill_probability(kp_content)
        kill_prob_data = kp_parsed
        warnings.extend(kp_warns)

    # winners
    winners_list: list[dict] = []
    winners_content = sections.get("winners")
    if winners_content:
        sections_parsed += 1
        winners_list = parse_winners(winners_content)

    return {
        "ticker":               ticker,
        "company_name":         company_name,
        "what_they_do":         what_they_do,
        "hidden_angles":        json.dumps(hidden_angles_list)  if hidden_angles_list  else None,
        "bear_case":            bear_case,
        "contrarian_case":      contrarian_case,
        "what_kills_it":        what_kills_it,
        "kill_probability_low":  kill_prob_data["kill_probability_low"],
        "kill_probability_high": kill_prob_data["kill_probability_high"],
        "kill_horizon_months":   kill_prob_data["kill_horizon_months"],
        "kill_components":       json.dumps(kill_prob_data["kill_components"]) if kill_prob_data["kill_components"] else None,
        "winners":              json.dumps(winners_list)          if winners_list          else None,
        "source_pdf":           PDF_FILENAME,
        "raw_text":             raw_block,
        "sections_parsed":      sections_parsed,
        "parse_warnings":       json.dumps(warnings)              if warnings              else None,
    }


# ── PDF extraction ─────────────────────────────────────────────────────────────

def extract_pdf_text(path: Path) -> str:
    reader   = pypdf.PdfReader(str(path))
    pages    = [p.extract_text() or "" for p in reader.pages]
    return "\n".join(pages)


def split_blocks(full_text: str) -> list[tuple[str, str, str]]:
    """
    Split full PDF text into sitrep blocks.
    Returns list of (company_name, ticker, block_text).
    """
    matches = list(BLOCK_SEP.finditer(full_text))
    blocks: list[tuple[str, str, str]] = []
    for i, m in enumerate(matches):
        company  = m.group(1).strip()
        ticker   = m.group(2).strip()
        start    = m.start()
        end      = matches[i + 1].start() if i + 1 < len(matches) else len(full_text)
        raw_text = full_text[start:end].strip()
        blocks.append((company, ticker, raw_text))
    return blocks


# ── DB helpers ─────────────────────────────────────────────────────────────────

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


# ── Report generation ──────────────────────────────────────────────────────────

def build_report(
    blocks_found:   int,
    parsed_rows:    list[dict],
    fallback_counts: dict[str, int],
    kp_stats:        dict,
    winners_stats:   dict,
) -> str:
    total       = len(parsed_rows)
    perfect     = sum(1 for r in parsed_rows if r["sections_parsed"] == 7)
    partial     = sum(1 for r in parsed_rows if 4 <= r["sections_parsed"] <= 6)
    failed      = sum(1 for r in parsed_rows if r["sections_parsed"] < 4)

    below_7 = [r for r in parsed_rows if r["sections_parsed"] < 7]
    below_7.sort(key=lambda r: r["sections_parsed"])

    lines = [
        "=== ETL PARSE REPORT ===",
        f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"Source PDF: {PDF_FILENAME}",
        "",
        "SUMMARY:",
        f"  Total sitrep blocks found:    {blocks_found}",
        f"  Expected:                     177",
        f"  Successfully parsed (7/7):    {perfect}",
        f"  Partial parse (4-6 sections): {partial}",
        f"  Failed (<4 sections):         {failed}",
        "",
        "TICKERS BELOW 7/7 (REVIEW NEEDED):",
    ]

    if below_7:
        lines.append(f"  {'TICKER':<10} {'SECTIONS':<10} WARNINGS")
        for r in below_7:
            warns = json.loads(r["parse_warnings"]) if r["parse_warnings"] else []
            warn_str = "; ".join(
                f"{w.get('section','?')}:{w.get('type','?')}" for w in warns
            )[:80]
            lines.append(f"  {r['ticker']:<10} {r['sections_parsed']:<10} {warn_str}")
    else:
        lines.append("  (none — all tickers parsed 7/7)")

    lines += [
        "",
        "FALLBACK FIRES BY SECTION:",
        f"  what_they_do:     {fallback_counts.get('what_they_do', 0)}",
        f"  hidden_angles:    {fallback_counts.get('hidden_angles', 0)}",
        f"  bear_case:        {fallback_counts.get('bear_case', 0)}",
        f"  contrarian_case:  {fallback_counts.get('contrarian_case', 0)}",
        f"  what_kills_it:    {fallback_counts.get('what_kills_it', 0)}",
        f"  kill_probability: {fallback_counts.get('kill_probability', 0)}",
        f"  winners:          {fallback_counts.get('winners', 0)}",
        "",
        "KILL PROBABILITY EXTRACTION:",
        f"  Range parsed:      {kp_stats['range']}/{total}",
        f"  Horizon parsed:    {kp_stats['horizon']}/{total}",
        f"  Components parsed: {kp_stats['components']}/{total}",
        "",
        "WINNERS EXTRACTION:",
        f"  Tickers identified:     {winners_stats['tickers']}",
        f"  Rationale-only entries: {winners_stats['rationale_only']}",
        "",
        "=== END REPORT ===",
    ]
    return "\n".join(lines)


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    # 1. Read PDF
    print(f"Reading PDF: {PDF_PATH} ...", flush=True)
    if not PDF_PATH.exists():
        print(f"ERROR: PDF not found at {PDF_PATH}", file=sys.stderr)
        sys.exit(1)
    full_text = extract_pdf_text(PDF_PATH)
    print(f"  Extracted {len(full_text):,} characters from {PDF_PATH.name}", flush=True)

    # 2. Split into sitrep blocks
    blocks = split_blocks(full_text)
    print(f"  Sitrep blocks found: {len(blocks)}", flush=True)

    # HARD RULE 5: stop if < 170 blocks
    if len(blocks) < 170:
        msg = (
            f"STOP: Only {len(blocks)} sitrep blocks found — expected >= 170.\n"
            "The block-splitting regex is likely wrong. Aborting before any DB writes."
        )
        print(msg, file=sys.stderr)
        REPORT_PATH.write_text(msg)
        sys.exit(2)

    # 3. Parse each block
    print("Parsing sitrep blocks ...", flush=True)
    parsed_rows: list[dict] = []
    fallback_counts: dict[str, int] = {s: 0 for s in ALL_SEVEN}

    for company, ticker, raw_block in blocks:
        row = parse_sitrep(company, ticker, raw_block)
        parsed_rows.append(row)

        # Tally fallbacks from parse_warnings
        if row["parse_warnings"]:
            for w in json.loads(row["parse_warnings"]):
                if w.get("type") == "fallback":
                    sec = w.get("section", "")
                    if sec in fallback_counts:
                        fallback_counts[sec] += 1

    # 4. Connect to DB and run migration
    print(f"Connecting to DB ...", flush=True)
    try:
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=20)
        conn.autocommit = False
        cur = conn.cursor()
    except Exception as exc:
        print(f"ERROR: Cannot connect to DB: {exc}", file=sys.stderr)
        sys.exit(1)

    print("Running migration (001_create_sitreps.sql) ...", flush=True)
    migration_sql = MIGRATION_SQL.read_text()
    cur.execute(migration_sql)
    conn.commit()

    # 5. Insert rows
    print(f"Inserting {len(parsed_rows)} rows (ON CONFLICT DO UPDATE) ...", flush=True)
    inserted = 0
    failed_inserts: list[tuple[str, str]] = []
    for row in parsed_rows:
        try:
            cur.execute(INSERT_SQL, row)
            inserted += 1
        except Exception as exc:
            conn.rollback()
            failed_inserts.append((row["ticker"], str(exc)[:120]))
            # Reconnect cursor after rollback failure
            try:
                cur = conn.cursor()
            except Exception:
                pass
    conn.commit()
    cur.close()
    conn.close()

    if failed_inserts:
        print(f"  WARNING: {len(failed_inserts)} insert failures:", file=sys.stderr)
        for t, e in failed_inserts:
            print(f"    {t}: {e}", file=sys.stderr)

    print(f"  {inserted}/{len(parsed_rows)} rows inserted/updated.", flush=True)

    # 6. Build stats for report
    kp_stats = {
        "range":      sum(1 for r in parsed_rows if r["kill_probability_low"]  is not None),
        "horizon":    sum(1 for r in parsed_rows if r["kill_horizon_months"]    is not None),
        "components": sum(1 for r in parsed_rows if r["kill_components"]        is not None),
    }
    winners_tickers     = 0
    winners_rationale   = 0
    for r in parsed_rows:
        if r["winners"]:
            for w in json.loads(r["winners"]):
                if w.get("ticker"):
                    winners_tickers += 1
                else:
                    winners_rationale += 1
    winners_stats = {"tickers": winners_tickers, "rationale_only": winners_rationale}

    # 7. Build and emit report
    report = build_report(
        blocks_found    = len(blocks),
        parsed_rows     = parsed_rows,
        fallback_counts = fallback_counts,
        kp_stats        = kp_stats,
        winners_stats   = winners_stats,
    )
    print("\n" + report, flush=True)
    REPORT_PATH.write_text(report)
    print(f"\nReport written to: {REPORT_PATH}", flush=True)


if __name__ == "__main__":
    main()
