import React, { useState } from "react";

function fmt(v, digits = 2) {
  if (v == null) return "—";
  return Number(v).toFixed(digits);
}

function fmtExpiry(exp) {
  if (!exp) return "—";
  const [y, m, d] = exp.split("-").map(Number);
  return new Date(y, m - 1, d).toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

const COLS = [
  { key: "ticker",      label: "Ticker",     align: "left"  },
  { key: "price",       label: "Price",      align: "right" },
  { key: "atm_call_premium", label: "Premium $", align: "right" },
  { key: "premium_pct", label: "Premium %",  align: "right" },
  { key: "best_strike", label: "Strike",     align: "right" },
  { key: "best_expiry", label: "Expiry",     align: "right" },
  { key: "best_dte",    label: "DTE",        align: "right" },
  { key: "open_interest", label: "OI",       align: "right" },
];

function cellValue(row, key) {
  switch (key) {
    case "ticker":           return row.ticker;
    case "price":            return row.price != null ? `$${fmt(row.price, 2)}` : "—";
    case "atm_call_premium": return row.atm_call_premium != null ? `$${fmt(row.atm_call_premium, 2)}` : "—";
    case "premium_pct":      return row.premium_pct != null ? `${fmt(row.premium_pct * 100, 2)}%` : "—";
    case "best_strike":      return row.best_strike != null ? `$${fmt(row.best_strike, 0)}` : "—";
    case "best_expiry":      return fmtExpiry(row.best_expiry);
    case "best_dte":         return row.best_dte != null ? `${row.best_dte}d` : "—";
    case "open_interest":    return row.open_interest != null
      ? row.open_interest >= 1000
        ? `${(row.open_interest / 1000).toFixed(1)}K`
        : String(row.open_interest)
      : "—";
    default: return "—";
  }
}

function sortValue(row, key) {
  switch (key) {
    case "ticker":           return row.ticker;
    case "price":            return row.price ?? -1;
    case "atm_call_premium": return row.atm_call_premium ?? -1;
    case "premium_pct":      return row.premium_pct ?? -1;
    case "best_strike":      return row.best_strike ?? -1;
    case "best_expiry":      return row.best_expiry ?? "";
    case "best_dte":         return row.best_dte ?? 9999;
    case "open_interest":    return row.open_interest ?? -1;
    default: return 0;
  }
}

export default function PremiumScanner({ rows, onRowClick }) {
  const [sortCol, setSortCol] = useState("atm_call_premium");
  const [sortAsc, setSortAsc] = useState(false);

  const handleSort = (key) => {
    if (sortCol === key) {
      setSortAsc(v => !v);
    } else {
      setSortCol(key);
      // Ticker and expiry sort ascending by default; everything else descending
      setSortAsc(key === "ticker" || key === "best_expiry");
    }
  };

  const sorted = [...rows].sort((a, b) => {
    const av = sortValue(a, sortCol);
    const bv = sortValue(b, sortCol);
    if (av < bv) return sortAsc ? -1 : 1;
    if (av > bv) return sortAsc ? 1 : -1;
    return 0;
  });

  if (sorted.length === 0) {
    return <div className="empty">No premium data available. Run a scan first.</div>;
  }

  return (
    <div className="prem-scanner-wrap">
      <table className="prem-scanner-table">
        <thead>
          <tr>
            {COLS.map(col => (
              <th
                key={col.key}
                className={`prem-scanner-th${col.align === "right" ? " right" : ""}${sortCol === col.key ? " sorted" : ""}`}
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
                  className={`prem-scanner-td${col.align === "right" ? " right" : ""}${col.key === "ticker" ? " ticker-col" : ""}${col.key === "atm_call_premium" ? " prem-col" : ""}`}
                >
                  {cellValue(row, col.key)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
