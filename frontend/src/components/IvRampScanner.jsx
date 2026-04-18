import React, { useState } from "react";

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

function cellValue(row, key) {
  switch (key) {
    case "ticker": return (
      <span>
        {row.ticker}
        {row.company_name && (
          <span className="company-name company-name-table">{row.company_name}</span>
        )}
        {row.iv_ramp_flag && (
          <span className="regime-tag regime-ivramp" style={{ marginLeft: 6 }}>IV RAMP ↑</span>
        )}
      </span>
    );
    case "price":           return row.price != null ? `$${Number(row.price).toFixed(2)}` : "—";
    case "iv":              return row.iv != null ? `${(row.iv * 100).toFixed(1)}%` : "—";
    case "iv_rank":         return row.iv_rank != null ? `${Math.round(row.iv_rank)}%` : "—";
    case "iv_velocity_5d":  return velCell(row.iv_velocity_5d);
    case "iv_velocity_10d": return velCell(row.iv_velocity_10d);
    case "iv_velocity_20d": return velCell(row.iv_velocity_20d);
    case "iv_ramp_score":   return rampScoreCell(row.iv_ramp_score);
    case "cross": {
      if (row.sma_golden_cross === true) return (
        <span>
          <span className="rs-cross rs-golden">Golden</span>
          {row.sma_regime === "DOWNTREND" && (
            <span className="cross-conflict-warn" title="Golden cross with downtrend — cross is fresh but price hasn't confirmed. Higher risk setup.">⚠️</span>
          )}
        </span>
      );
      if (row.sma_golden_cross === false) return <span className="rs-cross rs-death">Death</span>;
      return "—";
    }
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

function sortValue(row, key) {
  switch (key) {
    case "ticker":          return row.ticker;
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

export default function IvRampScanner({ rows, onRowClick }) {
  const [sortCol, setSortCol] = useState("iv_ramp_score");
  const [sortAsc, setSortAsc] = useState(false);
  const [flaggedOnly, setFlaggedOnly] = useState(true);

  const withHistory = rows.filter(r => r.iv_velocity_5d != null).length;
  const showBanner = rows.length === 0 || withHistory / rows.length < 0.8;

  const displayRows = flaggedOnly ? rows.filter(r => r.iv_ramp_flag) : rows;

  const sorted = [...displayRows].sort((a, b) => {
    const av = sortValue(a, sortCol);
    const bv = sortValue(b, sortCol);
    if (typeof av === "string" && typeof bv === "string")
      return sortAsc ? av.localeCompare(bv) : bv.localeCompare(av);
    if (av < bv) return sortAsc ? -1 : 1;
    if (av > bv) return sortAsc ? 1 : -1;
    return 0;
  });

  const handleSort = (key) => {
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
      <div className="prem-scanner-wrap">
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
                      {cellValue(row, col.key)}
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
