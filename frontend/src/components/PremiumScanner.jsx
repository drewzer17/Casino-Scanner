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

// ── Data extractors ───────────────────────────────────────────────

function getCallData(row, dteFilter) {
  if (dteFilter === "ALL") {
    // Use stored best call (always available from normal scan)
    if (row.atm_call_premium != null) {
      return {
        premium: row.atm_call_premium,
        premiumPct: row.premium_pct,
        strike: row.best_strike,
        expiry: row.best_expiry,
        dte: row.best_dte,
      };
    }
    // Fall back to best entry in expiry_data
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
  // DTE-filtered: prefer expiry_data entries within window
  const entries = (row.expiry_data || []).filter(
    e => e.dte != null && e.dte <= dteFilter && e.atm_call_prem != null
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
  if (row.best_dte != null && row.best_dte <= dteFilter && row.atm_call_premium != null) {
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

function getPutData(row, dteFilter) {
  if (dteFilter === "ALL") {
    // Prefer stored atm_put_premium (from normal scan)
    if (row.atm_put_premium != null) {
      return {
        premium: row.atm_put_premium,
        premiumPct: (row.atm_put_premium && row.price) ? row.atm_put_premium / row.price : null,
        strike: row.best_put_strike,
        expiry: row.best_put_expiry,
        dte: row.best_put_dte,
      };
    }
    // Fall back to best entry in expiry_data
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
  // DTE-filtered: prefer expiry_data entries within window
  const entries = (row.expiry_data || []).filter(
    e => e.atm_put_prem != null && e.dte != null && e.dte <= dteFilter
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
  // Fall back to stored put if it fits the DTE filter
  if (row.best_put_dte != null && row.best_put_dte <= dteFilter && row.atm_put_premium != null) {
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
  { key: "price",      label: "Price",     align: "right" },
  { key: "premium",    label: "Premium $", align: "right" },
  { key: "spread",     label: "Spread",    align: "right" },
  { key: "premiumPct", label: "Premium %", align: "right" },
  { key: "strike",     label: "Strike",    align: "right" },
  { key: "expiry",     label: "Expiry",    align: "right" },
  { key: "dte",        label: "DTE",       align: "right" },
  { key: "oi",         label: "OI",        align: "right" },
  { key: "r2_dist",    label: "R2 Dist",   align: "right" },
  { key: "r1_dist",    label: "R1 Dist",   align: "right" },
  { key: "s1_dist",    label: "S1 Dist",   align: "right" },
  { key: "s2_dist",    label: "S2 Dist",   align: "right" },
  { key: "cc_score",   label: "CC Score",  align: "right" },
  { key: "csp_score",  label: "CSP Score",   align: "right" },
  { key: "asymmetric", label: "ASYMMETRIC",  align: "center" },
];

function cellValue(item, key) {
  switch (key) {
    case "ticker":     return (
      <span>
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
    case "premiumPct": return item._d.premiumPct != null ? `${fmt(item._d.premiumPct * 100)}%` : "—";
    case "strike":     return item._d.strike != null ? `$${fmt(item._d.strike, 0)}` : "—";
    case "expiry":     return fmtExpiry(item._d.expiry);
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
    case "cc_score":  return item.cc_score != null
      ? <span className="score-cc">{item.cc_score}</span> : "—";
    case "csp_score": return item.csp_score != null
      ? <span className="score-csp">{item.csp_score}</span> : "—";
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
    case "premiumPct": return item._d.premiumPct ?? -1;
    case "strike":     return item._d.strike ?? -1;
    case "expiry":     return item._d.expiry ?? "";
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
    case "cc_score":   return item.cc_score ?? -1;
    case "csp_score":  return item.csp_score ?? -1;
    case "asymmetric": return item.asymmetric_any_flag ? 1 : 0;
    default: return 0;
  }
}

// ── Component ─────────────────────────────────────────────────────

export default function PremiumScanner({ rows, onRowClick }) {
  const [dteFilter, setDteFilter] = useState("ALL");
  const [typeFilter, setTypeFilter] = useState("ALL");
  const [sortCol, setSortCol] = useState("premium");
  const [sortAsc, setSortAsc] = useState(false);

  const handleSort = (key) => {
    if (sortCol === key) {
      setSortAsc(v => !v);
    } else {
      setSortCol(key);
      setSortAsc(key === "ticker" || key === "type" || key === "expiry");
    }
  };

  // Expand each ticker into up to 2 items: one CC, one CSP
  const items = [];
  for (const row of rows) {
    const callD = getCallData(row, dteFilter);
    const putD  = getPutData(row, dteFilter);
    if (callD && typeFilter !== "CSP") items.push({ ...row, _d: callD, _type: "CC",  _key: `${row.ticker}-CC` });
    if (putD  && typeFilter !== "CC")  items.push({ ...row, _d: putD,  _type: "CSP", _key: `${row.ticker}-CSP` });
  }

  const sorted = [...items].sort((a, b) => {
    const av = sortValue(a, sortCol);
    const bv = sortValue(b, sortCol);
    if (av < bv) return sortAsc ? -1 : 1;
    if (av > bv) return sortAsc ? 1 : -1;
    return 0;
  });

  const uniqueTickers = new Set(sorted.map(i => i.ticker)).size;

  return (
    <div>
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
        <span className="dte-filter-label">DTE ≤</span>
        {DTE_OPTS.map(opt => (
          <button
            key={opt}
            className={`dte-filter-btn${dteFilter === opt ? " active" : ""}`}
            onClick={() => setDteFilter(opt)}
          >{opt}</button>
        ))}
        <span className="dte-filter-count">{sorted.length} rows · {uniqueTickers} tickers</span>
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
              {sorted.map(item => (
                <tr
                  key={item._key}
                  className="prem-scanner-row"
                  onClick={() => onRowClick && onRowClick(item)}
                >
                  {COLS.map(col => (
                    <td
                      key={col.key}
                      className={`prem-scanner-td${col.align === "right" ? " right" : ""}${col.key === "ticker" ? " ticker-col" : ""}${col.key === "premium" ? " prem-col" : ""}`}
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
