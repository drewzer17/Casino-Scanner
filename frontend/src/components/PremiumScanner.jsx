import React, { useState } from "react";

const DTE_OPTS = [1, 2, 3, 4, 5, 6, 7, 10, 14, 21, 28, "ALL"];

function fmt(v, digits = 2) {
  if (v == null) return "—";
  return Number(v).toFixed(digits);
}

function fmtExpiry(exp) {
  if (!exp) return "—";
  const [y, m, d] = exp.split("-").map(Number);
  return new Date(y, m - 1, d).toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

function getRowData(row, dteFilter) {
  if (dteFilter === "ALL") {
    return {
      premium: row.atm_call_premium,
      premiumPct: row.premium_pct,
      strike: row.best_strike,
      expiry: row.best_expiry,
      dte: row.best_dte,
    };
  }
  // Search expiry_data for entries with dte <= dteFilter
  const entries = (row.expiry_data || []).filter(
    e => e.dte != null && e.dte <= dteFilter && e.atm_call_prem != null
  );
  if (entries.length > 0) {
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
  // Fall back to stored best if it fits the DTE filter
  if (row.best_dte != null && row.best_dte <= dteFilter) {
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

const COLS = [
  { key: "ticker",     label: "Ticker",    align: "left" },
  { key: "price",      label: "Price",     align: "right" },
  { key: "premium",    label: "Premium $", align: "right" },
  { key: "premiumPct", label: "Premium %", align: "right" },
  { key: "strike",     label: "Strike",    align: "right" },
  { key: "expiry",     label: "Expiry",    align: "right" },
  { key: "dte",        label: "DTE",       align: "right" },
  { key: "oi",         label: "OI",        align: "right" },
];

function cellValue(enriched, key) {
  switch (key) {
    case "ticker":     return enriched.ticker;
    case "price":      return enriched.price != null ? `$${fmt(enriched.price)}` : "—";
    case "premium":    return enriched._d.premium != null ? `$${fmt(enriched._d.premium)}` : "—";
    case "premiumPct": return enriched._d.premiumPct != null ? `${fmt(enriched._d.premiumPct * 100)}%` : "—";
    case "strike":     return enriched._d.strike != null ? `$${fmt(enriched._d.strike, 0)}` : "—";
    case "expiry":     return fmtExpiry(enriched._d.expiry);
    case "dte":        return enriched._d.dte != null ? `${enriched._d.dte}d` : "—";
    case "oi":
      return enriched.open_interest != null
        ? enriched.open_interest >= 1000
          ? `${(enriched.open_interest / 1000).toFixed(1)}K`
          : String(enriched.open_interest)
        : "—";
    default: return "—";
  }
}

function sortValue(enriched, key) {
  switch (key) {
    case "ticker":     return enriched.ticker;
    case "price":      return enriched.price ?? -1;
    case "premium":    return enriched._d.premium ?? -1;
    case "premiumPct": return enriched._d.premiumPct ?? -1;
    case "strike":     return enriched._d.strike ?? -1;
    case "expiry":     return enriched._d.expiry ?? "";
    case "dte":        return enriched._d.dte ?? 9999;
    case "oi":         return enriched.open_interest ?? -1;
    default: return 0;
  }
}

export default function PremiumScanner({ rows, onRowClick }) {
  const [dteFilter, setDteFilter] = useState("ALL");
  const [sortCol, setSortCol] = useState("premium");
  const [sortAsc, setSortAsc] = useState(false);

  const handleSort = (key) => {
    if (sortCol === key) {
      setSortAsc(v => !v);
    } else {
      setSortCol(key);
      setSortAsc(key === "ticker" || key === "expiry");
    }
  };

  const enriched = rows
    .map(row => {
      const d = getRowData(row, dteFilter);
      if (!d) return null;
      return { ...row, _d: d };
    })
    .filter(Boolean);

  const sorted = [...enriched].sort((a, b) => {
    const av = sortValue(a, sortCol);
    const bv = sortValue(b, sortCol);
    if (av < bv) return sortAsc ? -1 : 1;
    if (av > bv) return sortAsc ? 1 : -1;
    return 0;
  });

  return (
    <div>
      <div className="dte-filter-row">
        <span className="dte-filter-label">DTE ≤</span>
        {DTE_OPTS.map(opt => (
          <button
            key={opt}
            className={`dte-filter-btn${dteFilter === opt ? " active" : ""}`}
            onClick={() => setDteFilter(opt)}
          >{opt}</button>
        ))}
        <span className="dte-filter-count">{sorted.length} tickers</span>
      </div>
      <div className="prem-scanner-wrap">
        {sorted.length === 0 ? (
          <div className="empty">
            No tickers match this DTE filter.
            {dteFilter !== "ALL" && dteFilter <= 7
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
                      className={`prem-scanner-td${col.align === "right" ? " right" : ""}${col.key === "ticker" ? " ticker-col" : ""}${col.key === "premium" ? " prem-col" : ""}`}
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
