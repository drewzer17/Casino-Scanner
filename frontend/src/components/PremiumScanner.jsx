import React, { useState, useEffect } from "react";
import CrossConflictWarning from "./CrossConflictWarning.jsx";
import ResearchAsterisk from "./ResearchAsterisk.jsx";
import ScrollArrows from "./ScrollArrows.jsx";
import { GRADE_COLORS, formatFailKey } from "./RiskQualityScanner.jsx";

const VRP_COLORS_PS = { Rich: "#4ade80", Moderate: "#facc15", Weak: "#fb923c", Negative: "#f87171" };

// Risk Quality inline cells for Premium Scanner expand mode
function RqGradeCell({ grade }) {
  if (!grade) return <span style={{ color: "var(--text-muted)" }}>—</span>;
  return (
    <span style={{
      display: "inline-block",
      background: GRADE_COLORS[grade],
      color: "#fff",
      fontWeight: 700,
      fontSize: "11px",
      padding: "2px 7px",
      borderRadius: "999px",
      minWidth: 20,
      textAlign: "center",
    }}>{grade}</span>
  );
}
function RqVrpCell({ state, spread }) {
  if (!state) return <span style={{ color: "var(--text-muted)" }}>—</span>;
  const sign = spread != null && spread >= 0 ? "+" : "";
  return (
    <div style={{ textAlign: "center" }}>
      <span style={{
        display: "inline-block",
        background: VRP_COLORS_PS[state] ?? "#6b7280",
        color: state === "Rich" || state === "Negative" ? "#fff" : "#000",
        fontSize: "10px",
        fontWeight: 700,
        padding: "2px 5px",
        borderRadius: "3px",
      }}>{state}</span>
      {spread != null && (
        <div style={{ fontSize: "10px", color: "var(--text-muted)", marginTop: 1 }}>
          {sign}{spread.toFixed(1)}
        </div>
      )}
    </div>
  );
}
function RqStrategyCell({ strategyType, secondaryEdge }) {
  if (!strategyType) return <span style={{ color: "var(--text-muted)" }}>—</span>;
  const hasSecTech = (secondaryEdge || []).includes("TechnicalLocation");
  if (strategyType === "Event Ramp") return (
    <span style={{ fontSize: "11px", fontWeight: 700, color: "#f59e0b" }}>
      {hasSecTech ? "Event+Tech" : "Event"}
    </span>
  );
  if (strategyType === "Technical Location") return (
    <span style={{ fontSize: "11px", fontWeight: 700, color: "#4a90d9" }}>Tech</span>
  );
  return <span style={{ fontSize: "11px", color: "var(--text-muted)" }}>Income</span>;
}

const DTE_RANGES = [
  { label: "≤3",    min: 0,  max: 3  },
  { label: "4-7",   min: 4,  max: 7  },
  { label: "10-17", min: 10, max: 17 },
  { label: "21-30", min: 21, max: 30 },
  { label: "31-61", min: 31, max: 61 },
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
  { key: "earnings",   label: "EARNINGS",  align: "center" },
  { key: "type",       label: "Type",      align: "left" },
  { key: "otm",        label: "OTM",       align: "center" },
  { key: "price",      label: "Price",     align: "right", compact: true },
  { key: "strike",     label: "Strike",    align: "right", compact: true },
  { key: "premium",    label: "Prem $",    align: "right" },
  { key: "spread",     label: "Spread",    align: "right" },
  { key: "bid_ask",    label: "Bid/Mark",  align: "right" },
  { key: "premiumPct", label: "Prem %",    align: "right", compact: true },
  { key: "dte",        label: "DTE",       align: "right", compact: true },
  { key: "oi",         label: "OI",        align: "right", compact: true },
  { key: "s1_dist",    label: "S1",        align: "right", compact: true },
  { key: "s2_dist",    label: "S2",        align: "right", compact: true, groupEnd: true },
  { key: "r2_dist",    label: "R2",        align: "right", compact: true },
  { key: "r1_dist",    label: "R1",        align: "right", compact: true },
  { key: "score",      label: "Score",     align: "right" },
  { key: "asymmetric", label: "ASYMMETRIC",  align: "center" },
];

function cellValue(item, key, onResearch) {
  switch (key) {
    case "ticker":     return (
      <span>
        {item.sma_golden_cross === true && item.sma_regime === "DOWNTREND" && <CrossConflictWarning />}
        <ResearchAsterisk ticker={item.ticker} hasSitrep={item.has_sitrep} onResearch={onResearch} isDefense={item.is_defense} />
        {item.ticker}
        {item.has_sitrep && item.primary_lens && (
          <span style={{
            display: "inline-block",
            background: item.is_defense ? "#fbbf24" : "#8b5cf6",
            color: item.is_defense ? "#000" : "#fff",
            fontSize: "10px",
            fontWeight: 500,
            padding: "2px 6px",
            borderRadius: "3px",
            marginLeft: "6px",
            marginRight: "4px",
            cursor: "default",
            whiteSpace: "nowrap",
            verticalAlign: "middle",
          }}>
            {item.primary_lens}
          </span>
        )}
        {item.company_name && (
          <span className="company-name company-name-table">{item.company_name}</span>
        )}
      </span>
    );
    case "earnings": {
      const days = item.earnings_days;
      const dte  = item._d.dte;
      if (days == null) return <span className="earn-col-unknown">E ?</span>;
      if (days <= 7 || (dte != null && days <= dte))
        return <span className="earn-col-hot">E {days}</span>;
      return <span className="earn-col-neutral">E {days}</span>;
    }
    case "type":       return (
      <span className={`prem-type-badge prem-type-${item._type.toLowerCase()}`}>
        {item._type}
      </span>
    );
    case "price":      return item.price != null ? `$${fmt(item.price)}` : "—";
    case "premium":    return item._d.premium != null ? `$${fmt(item._d.premium)}` : "—";
    case "spread": {
      const pct = item.bid_ask_spread_pct;
      if (pct == null || item._d.premium == null) return <span className="text-muted-sm">N/A</span>;
      const val = pct * 100;
      const dollarSpread = pct * item._d.premium;
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
      const isCSPItem = item._type === "CSP";
      const sk = item._d.strike;
      const distPct = sk != null && item.price ? (isCSPItem ? (item.price - sk) : (sk - item.price)) / item.price * 100 : null;
      const sign = isCSPItem ? "-" : "+";
      const distStr = distPct != null ? ` ${sign}${distPct.toFixed(1)}%` : "";
      const distCls = distPct == null ? "" : distPct >= 3 ? "spread-tight" : distPct >= 1 ? "spread-ok" : "spread-wide";
      if (lvl <= 0) return <span className="otm-atm">ATM{distStr && <span className={distCls}>{distStr}</span>}</span>;
      if (lvl === 1) return <span className="otm-1">1 OTM{distStr && <span className={distCls}>{distStr}</span>}</span>;
      const n = Math.min(lvl, 5);
      return <span className="otm-2plus">{n}{lvl >= 5 ? "+" : ""} OTM{distStr && <span className={distCls}>{distStr}</span>}</span>;
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
    case "strike":
      return item._d.strike != null ? `$${fmt(item._d.strike, 0)}` : "—";
    case "dte":
      if (item._isLeaps)
        return <span className="leaps-dte-badge">{item._d.dte != null ? `${item._d.dte}d ` : ""}LEAPS</span>;
      return item._d.dte != null ? `${item._d.dte}d` : "—";
    case "oi":
      return item.open_interest != null
        ? item.open_interest >= 1000
          ? `${(item.open_interest / 1000).toFixed(1)}K`
          : String(item.open_interest)
        : "—";
    case "r2_dist": {
      if (item.resistance_2 == null || item.price == null || item.price <= 0) return <span style={{ color: "#a855f7", fontWeight: "bold" }}>PD</span>;
      const dist = ((item.resistance_2 - item.price) / item.price) * 100;
      const cls = dist <= 8 ? "s1dist-tight" : dist <= 15 ? "s1dist-ok" : "s1dist-wide";
      return <span className={cls}>{dist.toFixed(1)}%</span>;
    }
    case "r1_dist": {
      if (item.resistance_1 == null || item.price == null || item.price <= 0) return <span style={{ color: "#a855f7", fontWeight: "bold" }}>PD</span>;
      const dist = ((item.resistance_1 - item.price) / item.price) * 100;
      const cls = dist <= 8 ? "s1dist-tight" : dist <= 15 ? "s1dist-ok" : "s1dist-wide";
      return <span className={cls}>{dist.toFixed(1)}%</span>;
    }
    case "s1_dist": {
      if (item.support_1 == null || item.price == null || item.price <= 0) return <span style={{ color: "#ef4444", fontWeight: "bold" }}>FF</span>;
      const dist = ((item.price - item.support_1) / item.price) * 100;
      const cls = dist <= 8 ? "s1dist-tight" : dist <= 15 ? "s1dist-ok" : "s1dist-wide";
      return <span className={cls}>{dist.toFixed(1)}%</span>;
    }
    case "s2_dist": {
      if (item.support_2 == null || item.price == null || item.price <= 0) return <span style={{ color: "#ef4444", fontWeight: "bold" }}>FF</span>;
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
    case "earnings":   return item.earnings_days ?? Infinity;
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

// ── Earnings bucket filter ────────────────────────────────────────

const EARN_BUCKETS = [
  { key: "0-3d",   label: "0-3d",   test: d => d != null && d >= 0 && d <= 3  },
  { key: "3-7d",   label: "3-7d",   test: d => d != null && d >  3 && d <= 7  },
  { key: "7-14d",  label: "7-14d",  test: d => d != null && d >  7 && d <= 14 },
  { key: "14-21d", label: "14-21d", test: d => d != null && d > 14 && d <= 21 },
  { key: "21+",    label: "21+",    test: d => d != null && d > 21             },
  { key: "none",   label: "None",   test: d => d == null                       },
];

function earnBucketMatch(days, buckets) {
  for (const key of buckets) {
    const b = EARN_BUCKETS.find(x => x.key === key);
    if (b && b.test(days)) return true;
  }
  return false;
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

function fmtLeapsTs(iso) {
  if (!iso) return "Never scanned";
  const d = new Date(iso);
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric" }) +
    ", " + d.toLocaleTimeString("en-US", { hour: "numeric", minute: "2-digit" });
}

export default function PremiumScanner({ rows, onRowClick, allScanRows = [], excludedRows = [], onResearch, onAddToPositions, leapsRows = [], leapsScannedAt = null }) {
  const [dteSelected, setDteSelected] = useState(new Set()); // empty = ALL
  const [typeFilter, setTypeFilter] = useState("ALL");
  const [otmSelected, setOtmSelected] = useState(new Set()); // empty = ALL
  const [sortCol, setSortCol] = useState("premium");
  const [sortAsc, setSortAsc] = useState(false);
  const [showExcl, setShowExcl] = useState(false);
  const [showAll, setShowAll] = useState(false);
  const [earnBuckets, setEarnBuckets] = useState(new Set());
  const [showRiskCols, setShowRiskCols] = useState(false); // Risk Quality expand toggle
  const [showLeaps, setShowLeaps] = useState(false);
  const [visibleCount, setVisibleCount] = useState(100);

  useEffect(() => { setVisibleCount(100); }, [dteSelected, typeFilter, otmSelected, earnBuckets, showAll, showLeaps]);

  const toggleDte = (label) => {
    setDteSelected(prev => {
      if (prev.size === 1 && prev.has(label)) return new Set(); // deselect → ALL
      return new Set([label]); // exclusive: only one range active at a time
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
    if (key === "earnings") {
      if (sortCol !== "earnings") { setSortCol("earnings"); setSortAsc(true); }
      else if (sortAsc) { setSortAsc(false); }
      else { setSortCol("premium"); setSortAsc(false); }
      return;
    }
    if (sortCol === key) {
      setSortAsc(v => !v);
    } else {
      setSortCol(key);
      setSortAsc(key === "ticker" || key === "type");
    }
  };

  const baseRows = showAll ? allScanRows : rows;

  // Expand each ticker into rows: ATM + each OTM level available in expiry_data
  // getCallData/getPutData handle all DTE filtering internally via expiry_data.
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

  // Append LEAPS items when toggle is ON (bypass DTE filter — LEAPS are 180-365 DTE)
  if (showLeaps && leapsRows.length > 0) {
    for (const row of leapsRows) {
      if (typeFilter !== "CSP") {
        const callD = getCallData(row, new Set());
        if (callD) {
          if (otmSelected.size === 0 || otmSelected.has("ATM"))
            items.push({ ...row, _d: callD, _type: "CC", _key: `${row.ticker}-LEAPS-CC-ATM`, _otmLevel: 0, _isLeaps: true });
        }
        for (const oc of getOtmCallsFromExpiry(row, new Set())) {
          const key = otmLevelKey(oc.level);
          if (otmSelected.size === 0 || otmSelected.has(key))
            items.push({ ...row, _d: oc, _type: "CC", _key: `${row.ticker}-LEAPS-CC-${oc.level}`, _otmLevel: oc.level, _isLeaps: true });
        }
      }
      if (typeFilter !== "CC") {
        const putD = getPutData(row, new Set());
        if (putD) {
          if (otmSelected.size === 0 || otmSelected.has("ATM"))
            items.push({ ...row, _d: putD, _type: "CSP", _key: `${row.ticker}-LEAPS-CSP-ATM`, _otmLevel: 0, _isLeaps: true });
        }
        for (const op of getOtmPutsFromExpiry(row, new Set())) {
          const key = otmLevelKey(op.level);
          if (otmSelected.size === 0 || otmSelected.has(key))
            items.push({ ...row, _d: op, _type: "CSP", _key: `${row.ticker}-LEAPS-CSP-${op.level}`, _otmLevel: op.level, _isLeaps: true });
        }
      }
    }
  }

  const earnFiltered = earnBuckets.size === 0
    ? items
    : items.filter(i => i._isLeaps || earnBucketMatch(i.earnings_days, earnBuckets));

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

  const sorted = [...earnFiltered].sort((a, b) => {
    if (sortCol === "earnings") {
      const an = a.earnings_days == null;
      const bn = b.earnings_days == null;
      if (an && bn) return 0;
      if (sortAsc) { if (an) return 1; if (bn) return -1; }
      else         { if (an) return -1; if (bn) return 1; }
    }
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
        <button
          className="excl-toggle-btn"
          style={showRiskCols ? { background: "#5b21b6", borderColor: "#5b21b6", color: "#fff" } : {}}
          onClick={() => setShowRiskCols(v => !v)}
          title="Show/hide Risk Quality columns (Grade, VRP, Strategy)"
        >
          {showRiskCols ? "Risk ▼" : "Risk ▶"}
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
        <button
          className={`leaps-toggle-btn${showLeaps ? " active" : ""}`}
          onClick={() => setShowLeaps(v => !v)}
          title="Show LEAPS (180–365 DTE) from the most recent LEAPS scan"
        >LEAPS</button>
        {showLeaps && (
          <span className="leaps-scan-ts">Last: {fmtLeapsTs(leapsScannedAt)}</span>
        )}
        <span className="dte-filter-count">{sorted.length} rows · {uniqueTickers} tickers</span>
      </div>
      <div className="dte-filter-row">
        <span className="dte-filter-label">EARNINGS</span>
        <button
          className={`dte-filter-btn${earnBuckets.size === 0 ? " active" : ""}`}
          onClick={() => setEarnBuckets(new Set())}
        >ALL</button>
        {EARN_BUCKETS.map(b => (
          <button
            key={b.key}
            className={`dte-filter-btn${earnBuckets.has(b.key) ? " active" : ""}`}
            onClick={() => setEarnBuckets(prev => {
              const n = new Set(prev);
              n.has(b.key) ? n.delete(b.key) : n.add(b.key);
              return n;
            })}
          >{b.label}</button>
        ))}
      </div>
      <ScrollArrows>
        {sorted.length === 0 ? (
          <div className="empty">
            No tickers match this DTE filter.
            {dteSelected.size > 0 && [...dteSelected].some(l => l === "≤3" || l === "4-7")
              ? " Run an Extensive Scan to populate short-term weekly data."
              : ""}
          </div>
        ) : (() => {
          // Build visible columns: Risk cols insert after Ticker when expanded
          const RQ_COLS = [
            { key: "rq_grade",    label: "Grade",    align: "center" },
            { key: "rq_vrp",      label: "VRP",      align: "center" },
            { key: "rq_strategy", label: "Strategy", align: "center" },
          ];
          const visibleCols = showRiskCols
            ? [COLS[0], ...RQ_COLS, ...COLS.slice(1)]
            : COLS;

          return (
            <table className="prem-scanner-table">
              <thead>
                <tr>
                  {visibleCols.map(col => (
                    <th
                      key={col.key}
                      className={`prem-scanner-th${col.align === "right" ? " right" : col.align === "center" ? " center" : ""}${sortCol === col.key ? " sorted" : ""}${col.compact ? " compact-col" : ""}`}
                      onClick={() => col.key.startsWith("rq_") ? undefined : handleSort(col.key)}
                      style={{
                        ...(col.groupEnd ? { borderRight: "1px solid rgba(255,255,255,0.15)" } : {}),
                        ...(col.key.startsWith("rq_") ? { cursor: "default", color: "#7c3aed" } : {}),
                      }}
                    >
                      {col.label}
                      {sortCol === col.key && !col.key.startsWith("rq_") && (
                        <span className="sort-arrow">{sortAsc ? " ▲" : " ▼"}</span>
                      )}
                    </th>
                  ))}
                  {onAddToPositions && <th className="prem-scanner-th row-action-th"></th>}
                </tr>
              </thead>
              <tbody>
                {sorted.slice(0, visibleCount).map(item => {
                  const grade = item.risk_grade;
                  const rowStyle = showRiskCols
                    ? { opacity: grade === "F" ? 0.45 : grade === "C" ? 0.7 : 1.0, transition: "opacity 0.15s" }
                    : {};
                  return (
                    <tr
                      key={item._key}
                      className={`prem-scanner-row${item._isLeaps ? " leaps-row" : ""}`}
                      style={rowStyle}
                      onMouseEnter={e => { if (showRiskCols && rowStyle.opacity < 1) e.currentTarget.style.opacity = "1"; }}
                      onMouseLeave={e => { if (showRiskCols && rowStyle.opacity < 1) e.currentTarget.style.opacity = String(rowStyle.opacity); }}
                      onClick={() => onRowClick && onRowClick(item)}
                    >
                      {visibleCols.map(col => {
                        if (col.key === "rq_grade") return (
                          <td key="rq_grade" className="prem-scanner-td center">
                            <RqGradeCell grade={item.risk_grade} />
                          </td>
                        );
                        if (col.key === "rq_vrp") return (
                          <td key="rq_vrp" className="prem-scanner-td center">
                            <RqVrpCell state={item.vrp_state} spread={item.vrp_spread} />
                          </td>
                        );
                        if (col.key === "rq_strategy") return (
                          <td key="rq_strategy" className="prem-scanner-td center">
                            <RqStrategyCell strategyType={item.strategy_type} secondaryEdge={item.secondary_edge} />
                          </td>
                        );
                        return (
                          <td
                            key={col.key}
                            className={`prem-scanner-td${col.align === "right" ? " right" : col.align === "center" ? " center" : ""}${col.key === "ticker" ? " ticker-col" : ""}${col.key === "premium" ? " prem-col" : ""}${col.compact ? " compact-col" : ""}`}
                            style={col.groupEnd ? { borderRight: "1px solid rgba(255,255,255,0.15)" } : undefined}
                          >
                            {cellValue(item, col.key, onResearch)}
                          </td>
                        );
                      })}
                      {onAddToPositions && (
                        <td className="prem-scanner-td row-action-td" onClick={e => e.stopPropagation()}>
                          <button
                            className="row-add-pos-btn"
                            onClick={() => onAddToPositions(item.ticker)}
                            title={`Add ${item.ticker} to positions`}
                          >+</button>
                        </td>
                      )}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          );
        })()}
      </ScrollArrows>
      {sorted.length > visibleCount && (
        <div style={{ textAlign: "center", padding: "10px 0 4px" }}>
          <button className="excl-toggle-btn" onClick={() => setVisibleCount(v => v + 100)}>
            Show 100 more ({sorted.length - visibleCount} remaining)
          </button>
        </div>
      )}
    </div>
  );
}
