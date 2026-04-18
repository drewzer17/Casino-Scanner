import React, { useState } from "react";
import CrossConflictWarning from "./CrossConflictWarning.jsx";

const DTE_RANGES = [
  { label: "≤3",    min: 0,  max: 3  },
  { label: "4-7",   min: 4,  max: 7  },
  { label: "10-17", min: 10, max: 17 },
  { label: "21-30", min: 21, max: 30 },
  { label: "31-61", min: 31, max: 61 },
  { label: "61+",   min: 62, max: Infinity },
];

function dteInAny(dte, dteSelected) {
  if (dteSelected.size === 0) return true;
  if (dte == null) return false;
  for (const label of dteSelected) {
    const r = DTE_RANGES.find(r => r.label === label);
    if (r && dte >= r.min && dte <= r.max) return true;
  }
  return false;
}
const OTM_LEVELS = ["ATM", "1", "2", "3", "4", "5"];

function guessStrikeIncrement(price) {
  if (!price) return 5;
  if (price < 5)    return 0.5;
  if (price < 25)   return 1;
  if (price < 50)   return 2.5;
  if (price < 500)  return 5;
  if (price < 1000) return 10;
  return 25;
}

function calcOtmLevel(strike, price, isCSP) {
  if (strike == null || !price) return null;
  const inc = guessStrikeIncrement(price);
  const diff = isCSP ? (price - strike) : (strike - price);
  return Math.round(diff / inc);
}

function otmLevelKey(level) {
  if (level == null || level <= 0) return "ATM";
  return String(Math.min(level, 5));
}

function getOtmCallsFromExpiry(row, dteSelected) {
  const allExp = row.expiry_data || [];
  if (!allExp.length) return [];
  const expiries = allExp.filter(e => dteInAny(e.dte, dteSelected));
  if (!expiries.length) return [];
  const best = [...expiries].sort((a, b) => (b.atm_call_prem ?? 0) - (a.atm_call_prem ?? 0))[0];
  const result = [];
  (best.calls || []).forEach((s, idx) => {
    if (s.prem != null)
      result.push({ level: idx + 1, premium: s.prem, premiumPct: row.price ? s.prem / row.price : null, strike: s.strike, expiry: best.expiry, dte: best.dte });
  });
  return result;
}

function getOtmPutsFromExpiry(row, dteSelected) {
  const allExp = row.expiry_data || [];
  if (!allExp.length) return [];
  const expiries = allExp.filter(e => dteInAny(e.dte, dteSelected));
  if (!expiries.length) return [];
  const best = [...expiries].sort((a, b) => (b.atm_put_prem ?? 0) - (a.atm_put_prem ?? 0))[0];
  const result = [];
  (best.puts || []).forEach((s, idx) => {
    if (s.prem != null)
      result.push({ level: idx + 1, premium: s.prem, premiumPct: row.price ? s.prem / row.price : null, strike: s.strike, expiry: best.expiry, dte: best.dte });
  });
  return result;
}

function fmt(v, digits = 2) {
  if (v == null) return "—";
  return Number(v).toFixed(digits);
}

function fmtExpiry(exp) {
  if (!exp) return "—";
  const [y, m, d] = exp.split("-").map(Number);
  return new Date(y, m - 1, d).toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

// ── Data extractors ───────────────────────────────────────────────

function getCallData(row, dteSelected) {
  if (dteSelected.size === 0) {
    // ALL: use stored best call directly
    if (row.atm_call_premium != null) {
      return {
        premium: row.atm_call_premium,
        premiumPct: row.premium_pct,
        strike: row.best_strike,
        expiry: row.best_expiry,
        dte: row.best_dte,
      };
    }
    const entries = (row.expiry_data || []).filter(e => e.atm_call_prem != null);
    if (!entries.length) return null;
    entries.sort((a, b) => (b.atm_call_prem ?? 0) - (a.atm_call_prem ?? 0));
    const e = entries[0];
    return {
      premium: e.atm_call_prem,
      premiumPct: (e.atm_call_prem && row.price) ? e.atm_call_prem / row.price : null,
      strike: e.atm_strike,
      expiry: e.expiry,
      dte: e.dte,
    };
  }
  // Range-filtered: prefer expiry_data entries within any selected range
  const entries = (row.expiry_data || []).filter(
    e => e.atm_call_prem != null && dteInAny(e.dte, dteSelected)
  );
  if (entries.length) {
    entries.sort((a, b) => (b.atm_call_prem ?? 0) - (a.atm_call_prem ?? 0));
    const e = entries[0];
    return {
      premium: e.atm_call_prem,
      premiumPct: (e.atm_call_prem && row.price) ? e.atm_call_prem / row.price : null,
      strike: e.atm_strike,
      expiry: e.expiry,
      dte: e.dte,
    };
  }
  // Fall back to stored best if it fits
  if (row.best_dte != null && dteInAny(row.best_dte, dteSelected) && row.atm_call_premium != null) {
    return {
      premium: row.atm_call_premium,
      premiumPct: row.premium_pct,
      strike: row.best_strike,
      expiry: row.best_expiry,
      dte: row.best_dte,
    };
  }
  return null;
}

function getPutData(row, dteSelected) {
  if (dteSelected.size === 0) {
    // ALL: prefer stored atm_put_premium directly
    if (row.atm_put_premium != null) {
      return {
        premium: row.atm_put_premium,
        premiumPct: (row.atm_put_premium && row.price) ? row.atm_put_premium / row.price : null,
        strike: row.best_put_strike,
        expiry: row.best_put_expiry,
        dte: row.best_put_dte,
      };
    }
    const entries = (row.expiry_data || []).filter(e => e.atm_put_prem != null);
    if (!entries.length) return null;
    entries.sort((a, b) => (b.atm_put_prem ?? 0) - (a.atm_put_prem ?? 0));
    const e = entries[0];
    return {
      premium: e.atm_put_prem,
      premiumPct: (e.atm_put_prem && row.price) ? e.atm_put_prem / row.price : null,
      strike: e.atm_strike,
      expiry: e.expiry,
      dte: e.dte,
    };
  }
  // Range-filtered: prefer expiry_data entries within any selected range
  const entries = (row.expiry_data || []).filter(
    e => e.atm_put_prem != null && dteInAny(e.dte, dteSelected)
  );
  if (entries.length) {
    entries.sort((a, b) => (b.atm_put_prem ?? 0) - (a.atm_put_prem ?? 0));
    const e = entries[0];
    return {
      premium: e.atm_put_prem,
      premiumPct: (e.atm_put_prem && row.price) ? e.atm_put_prem / row.price : null,
      strike: e.atm_strike,
      expiry: e.expiry,
      dte: e.dte,
    };
  }
  // Fall back to stored put if it fits
  if (row.best_put_dte != null && dteInAny(row.best_put_dte, dteSelected) && row.atm_put_premium != null) {
    return {
      premium: row.atm_put_premium,
      premiumPct: (row.atm_put_premium && row.price) ? row.atm_put_premium / row.price : null,
      strike: row.best_put_strike,
      expiry: row.best_put_expiry,
      dte: row.best_put_dte,
    };
  }
  return null;
}

// ── Columns ───────────────────────────────────────────────────────

const COLS = [
  { key: "ticker",     label: "Ticker",    align: "left" },
  { key: "type",       label: "Type",      align: "left" },
  { key: "otm",        label: "OTM",       align: "center" },
  { key: "price",      label: "Price",     align: "right", compact: true },
  { key: "strike",     label: "Strike",    align: "right", compact: true },
  { key: "premium",    label: "Premium $", align: "right" },
  { key: "spread",     label: "Spread",    align: "right" },
  { key: "bid_ask",    label: "Bid/Mark",  align: "right" },
  { key: "premiumPct", label: "Prem %",    align: "right", compact: true },
  { key: "dte",        label: "DTE",       align: "right", compact: true },
  { key: "oi",         label: "OI",        align: "right", compact: true },
  { key: "r2_dist",    label: "R2 Dist",   align: "right" },
  { key: "r1_dist",    label: "R1 Dist",   align: "right" },
  { key: "s1_dist",    label: "S1 Dist",   align: "right" },
  { key: "s2_dist",    label: "S2 Dist",   align: "right" },
  { key: "score",      label: "Score",     align: "right" },
  { key: "asymmetric", label: "ASYMMETRIC",  align: "center" },
];

function cellValue(item, key) {
  switch (key) {
    case "ticker":     return (
      <span>
        {item.sma_golden_cross === true && item.sma_regime === "DOWNTREND" && <CrossConflictWarning />}
        {item.ticker}
        {item.company_name && (
          <span className="company-name company-name-table">{item.company_name}</span>
        )}
      </span>
    );
    case "type":       return (
      <span className={`prem-type-badge prem-type-${item._type.toLowerCase()}`}>
        {item._type}
      </span>
    );
    case "price":      return item.price != null ? `$${fmt(item.price)}` : "—";
    case "premium":    return item._d.premium != null ? `$${fmt(item._d.premium)}` : "—";
    case "spread": {
      const pct = item.bid_ask_spread_pct;
      if (pct == null || item.atm_call_premium == null) return <span className="text-muted-sm">N/A</span>;
      const val = pct * 100;
      const dollarSpread = pct * item.atm_call_premium;
      const cls = val <= 5 ? "spread-tight" : val <= 15 ? "spread-ok" : "spread-wide";
      return (
        <span>
          <span className="spread-dollar">${dollarSpread.toFixed(2)}</span>
          <br />
          <span className={cls}>{val.toFixed(1)}%</span>
        </span>
      );
    }
    case "otm": {
      const lvl = item._otmLevel;
      if (lvl == null) return "—";
      if (lvl <= 0) return <span className="otm-atm">ATM</span>;
      if (lvl === 1) return <span className="otm-1">1</span>;
      return <span className="otm-2plus">{Math.min(lvl, 5)}{lvl >= 5 ? "+" : ""}</span>;
    }
    case "bid_ask": {
      const mid = item._d.premium;
      const spr = item.bid_ask_spread_pct;
      if (mid == null) return "—";
      const bid = spr != null ? mid * (1 - spr / 2) : null;
      return <span style={{ fontSize: "0.82em", whiteSpace: "nowrap" }}>{bid != null ? `$${fmt(bid)}/$${fmt(mid)}` : `—/$${fmt(mid)}`}</span>;
    }
    case "premiumPct": return item._d.premiumPct != null
      ? <span style={{ fontSize: "0.88em" }}>{fmt(item._d.premiumPct * 100)}%</span> : "—";
    case "strike":     return item._d.strike != null ? `$${fmt(item._d.strike, 0)}` : "—";
    case "dte":        return item._d.dte != null ? `${item._d.dte}d` : "—";
    case "oi":
      return item.open_interest != null
        ? item.open_interest >= 1000
          ? `${(item.open_interest / 1000).toFixed(1)}K`
          : String(item.open_interest)
        : "—";
    case "r2_dist": {
      if (item.resistance_2 == null || item.price == null || item.price <= 0) return <span className="text-muted-sm">PD</span>;
      const dist = ((item.resistance_2 - item.price) / item.price) * 100;
      const cls = dist <= 8 ? "s1dist-tight" : dist <= 15 ? "s1dist-ok" : "s1dist-wide";
      return <span className={cls}>{dist.toFixed(1)}%</span>;
    }
    case "r1_dist": {
      if (item.resistance_1 == null || item.price == null || item.price <= 0) return <span className="text-muted-sm">PD</span>;
      const dist = ((item.resistance_1 - item.price) / item.price) * 100;
      const cls = dist <= 8 ? "s1dist-tight" : dist <= 15 ? "s1dist-ok" : "s1dist-wide";
      return <span className={cls}>{dist.toFixed(1)}%</span>;
    }
    case "s1_dist": {
      if (item.support_1 == null || item.price == null || item.price <= 0) return "—";
      const dist = ((item.price - item.support_1) / item.price) * 100;
      const cls = dist <= 8 ? "s1dist-tight" : dist <= 15 ? "s1dist-ok" : "s1dist-wide";
      return <span className={cls}>{dist.toFixed(1)}%</span>;
    }
    case "s2_dist": {
      if (item.support_2 == null || item.price == null || item.price <= 0) return "—";
      const dist = ((item.price - item.support_2) / item.price) * 100;
      const cls = dist <= 8 ? "s1dist-tight" : dist <= 15 ? "s1dist-ok" : "s1dist-wide";
      return <span className={cls}>{dist.toFixed(1)}%</span>;
    }
    case "score":
      if (item._type === "CC")
        return item.cc_score != null ? <span className="score-cc">{item.cc_score}</span> : "—";
      return item.csp_score != null ? <span className="score-csp">{item.csp_score}</span> : "—";
    case "asymmetric": {
      if (!item.asymmetric_any_flag || !item.asymmetric_type) return "—";
      const label = item.asymmetric_type === "ALL_THREE"
        ? "CC+CSP+IV RAMP"
        : item.asymmetric_type.replace("IV_RAMP", "IV RAMP");
      return <span className="prem-asym-badge">{label}</span>;
    }
    default: return "—";
  }
}

function sortValue(item, key) {
  switch (key) {
    case "ticker":     return item.ticker;
    case "type":       return item._type;
    case "price":      return item.price ?? -1;
    case "premium":    return item._d.premium ?? -1;
    case "spread":     return item.bid_ask_spread_pct != null ? item.bid_ask_spread_pct : Infinity;
    case "otm":        return item._otmLevel ?? -1;
    case "bid_ask":    return (item._d.premium != null && item.bid_ask_spread_pct != null) ? item._d.premium * (1 - item.bid_ask_spread_pct / 2) : -1;
    case "premiumPct": return item._d.premiumPct ?? -1;
    case "strike":     return item._d.strike ?? -1;
    case "dte":        return item._d.dte ?? 9999;
    case "oi":         return item.open_interest ?? -1;
    case "r2_dist":
      return (item.resistance_2 != null && item.price > 0)
        ? ((item.resistance_2 - item.price) / item.price) * 100 : Infinity;
    case "r1_dist":
      return (item.resistance_1 != null && item.price > 0)
        ? ((item.resistance_1 - item.price) / item.price) * 100 : Infinity;
    case "s1_dist":
      return (item.support_1 != null && item.price > 0)
        ? ((item.price - item.support_1) / item.price) * 100
        : -1;
    case "s2_dist":
      return (item.support_2 != null && item.price > 0)
        ? ((item.price - item.support_2) / item.price) * 100
        : -1;
    case "score": return item._type === "CC" ? (item.cc_score ?? -1) : (item.csp_score ?? -1);
    case "asymmetric": return item.asymmetric_any_flag ? 1 : 0;
    default: return 0;
  }
}

// ── Exclusion diagnostic ──────────────────────────────────────────

function groupByReason(rows) {
  const groups = {};
  for (const r of rows) {
    // Bucket by leading phrase (before first parenthesis or colon detail)
    const cat = r._reason.replace(/\s*[\(\(].*$/, "").trim();
    if (!groups[cat]) groups[cat] = [];
    groups[cat].push(r);
  }
  return groups;
}

function ExclusionTable({ allExcluded }) {
  const [expandedGroup, setExpandedGroup] = useState(null);
  const groups = groupByReason(allExcluded);
  const cats = Object.keys(groups).sort((a, b) => groups[b].length - groups[a].length);

  return (
    <div className="excl-wrap">
      <div className="excl-summary">
        {cats.map(cat => (
          <button
            key={cat}
            className={`excl-group-btn${expandedGroup === cat ? " active" : ""}`}
            onClick={() => setExpandedGroup(expandedGroup === cat ? null : cat)}
          >
            {cat} <span className="excl-count">{groups[cat].length}</span>
          </button>
        ))}
      </div>
      {expandedGroup && groups[expandedGroup] && (
        <table className="excl-table">
          <thead>
            <tr>
              <th>Ticker</th>
              <th>Price</th>
              <th>Company</th>
              <th>Reason</th>
            </tr>
          </thead>
          <tbody>
            {groups[expandedGroup]
              .sort((a, b) => (a.ticker < b.ticker ? -1 : 1))
              .map(r => (
                <tr key={r.ticker}>
                  <td className="excl-ticker">{r.ticker}</td>
                  <td className="excl-price">{r.price != null ? `$${r.price.toFixed(0)}` : "—"}</td>
                  <td className="excl-company">{r.company_name || ""}</td>
                  <td className="excl-reason">{r._reason}</td>
                </tr>
              ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

// ── Component ─────────────────────────────────────────────────────

export default function PremiumScanner({ rows, onRowClick, allScanRows = [], excludedRows = [] }) {
  const [dteSelected, setDteSelected] = useState(new Set()); // empty = ALL
  const [typeFilter, setTypeFilter] = useState("ALL");
  const [otmSelected, setOtmSelected] = useState(new Set()); // empty = ALL
  const [sortCol, setSortCol] = useState("premium");
  const [sortAsc, setSortAsc] = useState(false);
  const [showExcl, setShowExcl] = useState(false);
  const [showAll, setShowAll] = useState(false);

  const toggleDte = (label) => {
    setDteSelected(prev => {
      const next = new Set(prev);
      if (next.has(label)) next.delete(label);
      else next.add(label);
      return next;
    });
  };

  const toggleOtm = (level) => {
    setOtmSelected(prev => {
      const next = new Set(prev);
      if (next.has(level)) next.delete(level);
      else next.add(level);
      return next;
    });
  };

  const handleSort = (key) => {
    if (sortCol === key) {
      setSortAsc(v => !v);
    } else {
      setSortCol(key);
      setSortAsc(key === "ticker" || key === "type");
    }
  };

  const baseRows = showAll ? allScanRows : rows;

  // Expand each ticker into rows: ATM + each OTM level available in expiry_data
  const items = [];
  for (const row of baseRows) {
    if (typeFilter !== "CSP") {
      const callD = getCallData(row, dteSelected);
      if (callD) {
        if (otmSelected.size === 0 || otmSelected.has("ATM"))
          items.push({ ...row, _d: callD, _type: "CC", _key: `${row.ticker}-CC-ATM`, _otmLevel: 0 });
      }
      for (const oc of getOtmCallsFromExpiry(row, dteSelected)) {
        const key = otmLevelKey(oc.level);
        if (otmSelected.size === 0 || otmSelected.has(key))
          items.push({ ...row, _d: oc, _type: "CC", _key: `${row.ticker}-CC-${oc.level}`, _otmLevel: oc.level });
      }
    }
    if (typeFilter !== "CC") {
      const putD = getPutData(row, dteSelected);
      if (putD) {
        if (otmSelected.size === 0 || otmSelected.has("ATM"))
          items.push({ ...row, _d: putD, _type: "CSP", _key: `${row.ticker}-CSP-ATM`, _otmLevel: 0 });
      }
      for (const op of getOtmPutsFromExpiry(row, dteSelected)) {
        const key = otmLevelKey(op.level);
        if (otmSelected.size === 0 || otmSelected.has(key))
          items.push({ ...row, _d: op, _type: "CSP", _key: `${row.ticker}-CSP-${op.level}`, _otmLevel: op.level });
      }
    }
  }

  // ── Internal exclusions (rows that passed Dashboard but have no items) ──
  const passedTickerSet = new Set(items.map(i => i.ticker));
  const internalExcluded = baseRows
    .filter(r => !passedTickerSet.has(r.ticker))
    .map(r => {
      const callAny = getCallData(r, new Set());
      const putAny  = getPutData(r, new Set());
      if (!callAny && !putAny)
        return { ticker: r.ticker, price: r.price, company_name: r.company_name,
                 _reason: "No premium data (options chain not fetched or null)" };
      const callInDte = getCallData(r, dteSelected);
      const putInDte  = getPutData(r, dteSelected);
      if (!callInDte && !putInDte && dteSelected.size > 0)
        return { ticker: r.ticker, price: r.price, company_name: r.company_name,
                 _reason: `DTE filter (${[...dteSelected].join(", ")} — no expiry fits)` };
      if (otmSelected.size > 0)
        return { ticker: r.ticker, price: r.price, company_name: r.company_name,
                 _reason: `OTM filter (only ${[...otmSelected].join(", ")} OTM selected)` };
      if (typeFilter !== "ALL")
        return { ticker: r.ticker, price: r.price, company_name: r.company_name,
                 _reason: `Type filter (${typeFilter} only)` };
      return { ticker: r.ticker, price: r.price, company_name: r.company_name,
               _reason: "Unknown internal filter" };
    });

  const allExcluded = [...(showAll ? [] : excludedRows), ...internalExcluded];

  const sorted = [...items].sort((a, b) => {
    const av = sortValue(a, sortCol);
    const bv = sortValue(b, sortCol);
    if (av < bv) return sortAsc ? -1 : 1;
    if (av > bv) return sortAsc ? 1 : -1;
    return 0;
  });

  const uniqueTickers = new Set(sorted.map(i => i.ticker)).size;

  const totalInScan    = allScanRows.length;
  const inTable        = passedTickerSet.size;
  const hiddenCount    = totalInScan - inTable;

  return (
    <div>
      {/* ── Diagnostic banner ── */}
      <div className="excl-banner">
        <span className="excl-stat">Scanned: <strong>{totalInScan}</strong></span>
        <span className="excl-sep">·</span>
        <span className="excl-stat">In table: <strong>{inTable}</strong></span>
        <span className="excl-sep">·</span>
        <span className="excl-stat excl-hidden">Hidden: <strong>{hiddenCount}</strong></span>
        {allExcluded.length > 0 && (
          <button
            className={`excl-toggle-btn${showExcl ? " active" : ""}`}
            onClick={() => setShowExcl(v => !v)}
          >
            {showExcl ? "Hide Exclusions ▲" : "Show Exclusions ▼"}
          </button>
        )}
        <button
          className="excl-toggle-btn"
          style={showAll ? { background: "#c0392b", borderColor: "#c0392b", color: "#fff" } : {}}
          onClick={() => setShowAll(v => !v)}
        >
          {showAll ? "SHOW ALL (on)" : "SHOW ALL"}
        </button>
      </div>
      {showExcl && <ExclusionTable allExcluded={allExcluded} />}

      <div className="dte-filter-row">
        <span className="dte-filter-label">Type</span>
        {["ALL", "CC", "CSP"].map(opt => (
          <button
            key={opt}
            className={`dte-filter-btn type-filter-btn-${opt.toLowerCase()}${typeFilter === opt ? " active" : ""}`}
            onClick={() => setTypeFilter(opt)}
          >{opt}</button>
        ))}
      </div>
      <div className="dte-filter-row">
        <span className="dte-filter-label">OTM</span>
        {OTM_LEVELS.map(lvl => (
          <button
            key={lvl}
            className={`dte-filter-btn${otmSelected.has(lvl) ? " active" : ""}`}
            onClick={() => toggleOtm(lvl)}
          >{lvl}</button>
        ))}
        <button
          className={`dte-filter-btn${otmSelected.size === 0 ? " active" : ""}`}
          onClick={() => setOtmSelected(new Set())}
        >ALL</button>
      </div>
      <div className="dte-filter-row">
        <span className="dte-filter-label">DTE</span>
        {DTE_RANGES.map(r => (
          <button
            key={r.label}
            className={`dte-filter-btn${dteSelected.has(r.label) ? " active" : ""}`}
            onClick={() => toggleDte(r.label)}
          >{r.label}</button>
        ))}
        <button
          className={`dte-filter-btn${dteSelected.size === 0 ? " active" : ""}`}
          onClick={() => setDteSelected(new Set())}
        >ALL</button>
        <span className="dte-filter-count">{sorted.length} rows · {uniqueTickers} tickers</span>
      </div>
      <div className="prem-scanner-wrap">
        {sorted.length === 0 ? (
          <div className="empty">
            No tickers match this DTE filter.
            {dteSelected.size > 0 && [...dteSelected].some(l => l === "≤3" || l === "4-7")
              ? " Run an Extensive Scan to populate short-term weekly data."
              : ""}
          </div>
        ) : (
          <table className="prem-scanner-table">
            <thead>
              <tr>
                {COLS.map(col => (
                  <th
                    key={col.key}
                    className={`prem-scanner-th${col.align === "right" ? " right" : ""}${sortCol === col.key ? " sorted" : ""}${col.compact ? " compact-col" : ""}`}
                    onClick={() => handleSort(col.key)}
                  >
                    {col.label}
                    {sortCol === col.key && (
                      <span className="sort-arrow">{sortAsc ? " ▲" : " ▼"}</span>
                    )}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {sorted.map(item => (
                <tr
                  key={item._key}
                  className="prem-scanner-row"
                  onClick={() => onRowClick && onRowClick(item)}
                >
                  {COLS.map(col => (
                    <td
                      key={col.key}
                      className={`prem-scanner-td${col.align === "right" ? " right" : ""}${col.key === "ticker" ? " ticker-col" : ""}${col.key === "premium" ? " prem-col" : ""}${col.compact ? " compact-col" : ""}`}
                    >
                      {cellValue(item, col.key)}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
