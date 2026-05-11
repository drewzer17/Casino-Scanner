import React, { useState } from "react";
import ResearchAsterisk from "./ResearchAsterisk.jsx";
import ScrollArrows from "./ScrollArrows.jsx";

function velCell(v) {
  if (v == null) return <span className="text-muted-sm">—</span>;
  const cls = v > 0 ? "vel-pos" : v < 0 ? "vel-neg" : "text-muted-sm";
  return <span className={cls}>{v >= 0 ? "+" : ""}{v.toFixed(1)}%</span>;
}

function rampScoreCell(s) {
  if (!s) return <span className="text-muted-sm">0</span>;
  const cls = s >= 60 ? "ramp-hi" : s >= 30 ? "ramp-mid" : "text-muted-sm";
  return <span className={cls}>{s}</span>;
}

const COLS = [
  { key: "ticker",          label: "TICKER",      align: "left" },
  { key: "earnings",        label: "EARNINGS",    align: "center" },
  { key: "price",           label: "PRICE",       align: "right" },
  { key: "iv",              label: "IV",          align: "right" },
  { key: "iv_rank",         label: "IV RANK",     align: "right" },
  { key: "iv_velocity_5d",  label: "5D VEL",      align: "right" },
  { key: "iv_velocity_10d", label: "10D VEL",     align: "right" },
  { key: "iv_velocity_20d", label: "20D VEL",     align: "right" },
  { key: "iv_ramp_score",   label: "RAMP SCORE",  align: "right" },
  { key: "cross",           label: "CROSS",       align: "center" },
  { key: "trend",           label: "TREND",       align: "center" },
  { key: "cc_score",        label: "CC SCORE",    align: "right" },
];

function cellValue(row, key, onResearch) {
  switch (key) {
    case "ticker": return (
      <span>
        <ResearchAsterisk ticker={row.ticker} hasSitrep={row.has_sitrep} onResearch={onResearch} isDefense={row.is_defense} />
        {row.ticker}
        {row.company_name && (
          <span className="company-name company-name-table">{row.company_name}</span>
        )}
        {row.iv_ramp_flag && (
          <span className="regime-tag regime-ivramp" style={{ marginLeft: 6 }}>IV RAMP ↑</span>
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
    case "price":           return row.price != null ? `$${Number(row.price).toFixed(2)}` : "—";
    case "iv":              return row.iv != null ? `${(row.iv * 100).toFixed(1)}%` : "—";
    case "iv_rank":         return row.iv_rank != null ? `${Math.round(row.iv_rank)}%` : "—";
    case "iv_velocity_5d":  return velCell(row.iv_velocity_5d);
    case "iv_velocity_10d": return velCell(row.iv_velocity_10d);
    case "iv_velocity_20d": return velCell(row.iv_velocity_20d);
    case "iv_ramp_score":   return rampScoreCell(row.iv_ramp_score);
    case "cross":
      if (row.sma_golden_cross === true)  return <span className="rs-cross rs-golden">Golden</span>;
      if (row.sma_golden_cross === false) return <span className="rs-cross rs-death">Death</span>;
      return "—";
    case "trend": {
      if (!row.sma_regime) return "—";
      const cls = row.sma_regime === "UPTREND" ? "regime-up"
        : row.sma_regime === "DOWNTREND" ? "regime-dn" : "regime-mid";
      return <span className={`regime-tag ${cls}`}>{row.sma_regime}</span>;
    }
    case "cc_score": return row.cc_score != null
      ? <span className="score-cc">{row.cc_score}</span> : "—";
    default: return "—";
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
  switch (key) {
    case "ticker":          return row.ticker;
    case "earnings":        return row.earnings_days ?? Infinity;
    case "price":           return row.price ?? -1;
    case "iv":              return row.iv ?? -1;
    case "iv_rank":         return row.iv_rank ?? -1;
    case "iv_velocity_5d":  return row.iv_velocity_5d ?? -Infinity;
    case "iv_velocity_10d": return row.iv_velocity_10d ?? -Infinity;
    case "iv_velocity_20d": return row.iv_velocity_20d ?? -Infinity;
    case "iv_ramp_score":   return row.iv_ramp_score ?? -1;
    case "cross":           return row.sma_golden_cross == null ? 0 : row.sma_golden_cross ? 1 : -1;
    case "trend":           return row.sma_regime ?? "";
    case "cc_score":        return row.cc_score ?? -1;
    default: return 0;
  }
}

export default function IvRampScanner({ rows, onRowClick, onResearch, onAddToPositions }) {
  const [sortCol, setSortCol] = useState("iv_ramp_score");
  const [sortAsc, setSortAsc] = useState(false);
  const [flaggedOnly, setFlaggedOnly] = useState(true);
  const [earnBuckets, setEarnBuckets] = useState(new Set());

  const withHistory = rows.filter(r => r.iv_velocity_5d != null).length;
  const showBanner = rows.length === 0 || withHistory / rows.length < 0.8;

  const displayRows = flaggedOnly ? rows.filter(r => r.iv_ramp_flag) : rows;

  const earnFiltered = earnBuckets.size === 0
    ? displayRows
    : displayRows.filter(r => earnBucketMatch(r.earnings_days, earnBuckets));

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

  const handleSort = (key) => {
    if (key === "earnings") {
      if (sortCol !== "earnings") { setSortCol("earnings"); setSortAsc(true); }
      else if (sortAsc) { setSortAsc(false); }
      else { setSortCol("iv_ramp_score"); setSortAsc(false); }
      return;
    }
    if (sortCol === key) setSortAsc(v => !v);
    else { setSortCol(key); setSortAsc(false); }
  };

  const flagCount = rows.filter(r => r.iv_ramp_flag).length;

  return (
    <div>
      {showBanner && (
        <div className="ivramp-banner">
          IV Ramp detection requires 10+ days of IV history. Data collection started Apr 16, 2026. Full accuracy begins ~May 1, 2026.
        </div>
      )}
      <div className="dte-filter-row">
        <button
          className={`dte-filter-btn${flaggedOnly ? " active" : ""}`}
          onClick={() => setFlaggedOnly(true)}
        >Flagged Only ({flagCount})</button>
        <button
          className={`dte-filter-btn${!flaggedOnly ? " active" : ""}`}
          onClick={() => setFlaggedOnly(false)}
        >Show All ({rows.length})</button>
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
            {flaggedOnly
              ? "No tickers flagged for IV ramp. Check back after more IV history accumulates."
              : "No tickers match current filters."}
          </div>
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
