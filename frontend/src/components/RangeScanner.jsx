import React, { useState } from "react";
import CrossConflictWarning from "./CrossConflictWarning.jsx";
import ResearchAsterisk from "./ResearchAsterisk.jsx";
import ScrollArrows from "./ScrollArrows.jsx";

function fmt(v, digits = 2) {
  if (v == null) return "—";
  return Number(v).toFixed(digits);
}

function rangeScore(row) {
  if (row.resistance_1 == null || row.support_1 == null || row.price == null) return null;
  const span = row.resistance_1 - row.support_1;
  if (span <= 0) return null;
  return Math.max(0, Math.min(100, ((row.price - row.support_1) / span) * 100));
}

const COLS = [
  { key: "ticker",       label: "Ticker",       align: "left" },
  { key: "earnings",     label: "EARNINGS",     align: "center" },
  { key: "price",        label: "Price",        align: "right" },
  { key: "support_1",    label: "S1",           align: "right" },
  { key: "resistance_1", label: "R1",           align: "right" },
  { key: "range_score",  label: "Range Score",  align: "right" },
  { key: "signal",       label: "Signal",       align: "center" },
  { key: "cross",        label: "Cross",        align: "left" },
  { key: "premium",      label: "Premium $",    align: "right" },
  { key: "cc_score",     label: "CC Score",     align: "right" },
  { key: "csp_score",    label: "CSP Score",    align: "right" },
];

function cellValue(row, key, onResearch) {
  const rs = rangeScore(row);
  switch (key) {
    case "ticker":
      return (
        <span>
          {row.sma_golden_cross === true && row.sma_regime === "DOWNTREND" && <CrossConflictWarning />}
          <ResearchAsterisk ticker={row.ticker} hasSitrep={row.has_sitrep} onResearch={onResearch} isDefense={row.is_defense} />
          {row.ticker}
          {row.company_name && (
            <span className="company-name company-name-table">{row.company_name}</span>
          )}
        </span>
      );
    case "earnings": {
      const days = row.earnings_days;
      const dte  = row.best_dte;
      if (days == null) return <span className="earn-col-unknown">E ?</span>;
      if (days <= 7 || (dte != null && days <= dte))
        return <span className="earn-col-hot">E {days}</span>;
      return <span className="earn-col-neutral">E {days}</span>;
    }
    case "price":
      return row.price != null ? `$${fmt(row.price)}` : "—";
    case "support_1":
      return row.support_1 != null ? `$${fmt(row.support_1)}` : "—";
    case "resistance_1":
      return row.resistance_1 != null
        ? `$${fmt(row.resistance_1)}`
        : <span className="rs-discovery-label">Discovery</span>;
    case "range_score":
      if (row.resistance_1 == null) return <span className="rs-signal rs-pd">PD</span>;
      return rs != null ? rs.toFixed(0) : "—";
    case "signal": {
      if (row.resistance_1 == null) return <span className="rs-signal rs-pd">PD</span>;
      if (rs == null) return <span className="rs-signal rs-neutral">—</span>;
      if (rs <= 30)   return <span className="rs-signal rs-csp">CSP</span>;
      if (rs >= 70)   return <span className="rs-signal rs-cc">CC</span>;
      return <span className="rs-signal rs-neutral">—</span>;
    }
    case "cross":
      if (row.sma_golden_cross === true)
        return <span className="rs-cross rs-golden">Golden Cross</span>;
      if (row.sma_golden_cross === false)
        return <span className="rs-cross rs-death">Death Cross</span>;
      return "—";
    case "premium":
      return row.atm_call_premium != null ? `$${fmt(row.atm_call_premium)}` : "—";
    case "cc_score":
      return row.cc_score != null ? <span className="score-cc">{row.cc_score}</span> : "—";
    case "csp_score":
      return row.csp_score != null ? <span className="score-csp">{row.csp_score}</span> : "—";
    default:
      return "—";
  }
}

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

function sortValue(row, key) {
  const rs = rangeScore(row);
  switch (key) {
    case "ticker":       return row.ticker;
    case "earnings":     return row.earnings_days ?? Infinity;
    case "price":        return row.price ?? -1;
    case "support_1":    return row.support_1 ?? -1;
    case "resistance_1": return row.resistance_1 ?? Infinity;
    case "range_score":  return rs ?? -1;
    case "signal":       return rs ?? -1;
    case "cross":        return row.sma_golden_cross == null ? 0 : row.sma_golden_cross ? 1 : -1;
    case "premium":      return row.atm_call_premium ?? -1;
    case "cc_score":     return row.cc_score ?? -1;
    case "csp_score":    return row.csp_score ?? -1;
    default:             return 0;
  }
}

export default function RangeScanner({ rows, onRowClick, onResearch, onAddToPositions }) {
  const [sortCol, setSortCol] = useState("range_score");
  const [sortAsc, setSortAsc] = useState(true);
  const [earnBuckets, setEarnBuckets] = useState(new Set());

  const handleSort = (key) => {
    if (key === "earnings") {
      if (sortCol !== "earnings") { setSortCol("earnings"); setSortAsc(true); }
      else if (sortAsc) { setSortAsc(false); }
      else { setSortCol("range_score"); setSortAsc(true); }
      return;
    }
    if (sortCol === key) {
      setSortAsc(v => !v);
    } else {
      setSortCol(key);
      setSortAsc(key === "ticker" || key === "range_score" || key === "signal");
    }
  };

  const earnFiltered = earnBuckets.size === 0
    ? rows
    : rows.filter(r => earnBucketMatch(r.earnings_days, earnBuckets));

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
    if (typeof av === "string" && typeof bv === "string")
      return sortAsc ? av.localeCompare(bv) : bv.localeCompare(av);
    if (av < bv) return sortAsc ? -1 : 1;
    if (av > bv) return sortAsc ? 1 : -1;
    return 0;
  });

  return (
    <div>
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
          <div className="empty">No tickers match the current filters.</div>
      ) : (
        <table className="prem-scanner-table">
          <thead>
            <tr>
              {COLS.map(col => (
                <th
                  key={col.key}
                  className={`prem-scanner-th${col.align === "right" ? " right" : col.align === "center" ? " center" : ""}${sortCol === col.key ? " sorted" : ""}`}
                  onClick={() => handleSort(col.key)}
                >
                  {col.label}
                  {sortCol === col.key && (
                    <span className="sort-arrow">{sortAsc ? " ▲" : " ▼"}</span>
                  )}
                </th>
              ))}
              {onAddToPositions && <th className="prem-scanner-th row-action-th"></th>}
            </tr>
          </thead>
          <tbody>
            {sorted.map(row => (
              <tr
                key={row.ticker}
                className="prem-scanner-row"
                onClick={() => onRowClick && onRowClick(row)}
              >
                {COLS.map(col => (
                  <td
                    key={col.key}
                    className={`prem-scanner-td${col.align === "right" ? " right" : col.align === "center" ? " center" : ""}${col.key === "ticker" ? " ticker-col" : ""}`}
                  >
                    {cellValue(row, col.key, onResearch)}
                  </td>
                ))}
                {onAddToPositions && (
                  <td className="prem-scanner-td row-action-td" onClick={e => e.stopPropagation()}>
                    <button
                      className="row-add-pos-btn"
                      onClick={() => onAddToPositions(row.ticker)}
                      title={`Add ${row.ticker} to positions`}
                    >+</button>
                  </td>
                )}
              </tr>
            ))}
          </tbody>
        </table>
      )}
      </ScrollArrows>
    </div>
  );
}
