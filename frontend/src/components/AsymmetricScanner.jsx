import React, { useState } from "react";

const SETUP_COLORS = {
  CC:      "asym-cc",
  CSP:     "asym-csp",
  IV_RAMP: "asym-ivramp",
};

function TypeBadge({ type }) {
  if (!type) return null;
  const parts = type === "ALL_THREE" ? ["CC", "CSP", "IV_RAMP"] : type.split("+");
  return (
    <span className="asym-type-wrap">
      {parts.map(p => (
        <span key={p} className={`asym-type-badge ${SETUP_COLORS[p] || ""}`}>
          {p === "IV_RAMP" ? "IV RAMP" : p}
        </span>
      ))}
    </span>
  );
}

function generateWhy(row) {
  const type = row.asymmetric_type || "";
  const price = row.price;

  if (type.includes("CC")) {
    const parts = [];
    if (row.sma_regime === "UPTREND") parts.push("Uptrend");
    if (row.sma_golden_cross) parts.push("golden cross");
    if (row.resistance_1 == null) {
      parts.push("price discovery");
    } else if (row.resistance_1 && price) {
      const dist = ((row.resistance_1 - price) / price * 100).toFixed(1);
      parts.push(`${dist}% from R1`);
    }
    if (row.iv_rank != null) parts.push(`IV rank ${Math.round(row.iv_rank)}`);
    if (row.bid_ask_spread_pct != null) {
      const spr = row.bid_ask_spread_pct * 100;
      parts.push(spr <= 3 ? "tight spread" : `${spr.toFixed(1)}% spread`);
    }
    if (row.support_1 && price) {
      const dist = ((price - row.support_1) / price * 100).toFixed(1);
      parts.push(`S1 floor ${dist}% below`);
    }
    return parts.join(", ");
  }

  if (type.includes("CSP")) {
    const parts = ["Golden cross intact"];
    if (row.support_1 && price) {
      const dist = ((price - row.support_1) / price * 100).toFixed(1);
      parts.push(`${dist}% from S1`);
    }
    if (row.support_1_strength != null) {
      parts.push(`support tested ${row.support_1_strength}×`);
    }
    if (row.iv_rank != null) parts.push(`IV rank ${Math.round(row.iv_rank)}`);
    if (row.atm_put_premium != null) parts.push(`put premium $${row.atm_put_premium.toFixed(2)}`);
    return parts.join(", ");
  }

  if (type.includes("IV_RAMP")) {
    const parts = [];
    if (row.iv_rank != null) parts.push(`IV rank ${Math.round(row.iv_rank)}`);
    if (row.iv_velocity_10d != null) {
      parts.push(`climbing ${Math.abs(row.iv_velocity_10d).toFixed(1)}% over 10d`);
    }
    parts.push("golden cross, uptrend, premiums about to expand");
    return parts.join(", ");
  }

  return "Multiple convergent setups";
}

const COLS = [
  { key: "ticker",        label: "TICKER",      align: "left" },
  { key: "type",          label: "SETUP TYPE",  align: "left" },
  { key: "price",         label: "PRICE",       align: "right" },
  { key: "premium",       label: "PREMIUM $",   align: "right" },
  { key: "spread",        label: "SPREAD",      align: "right" },
  { key: "iv_rank",       label: "IV RANK",     align: "right" },
  { key: "s1_dist",       label: "S1 DIST",     align: "right" },
  { key: "r1_dist",       label: "R1 DIST",     align: "right" },
  { key: "cross",         label: "CROSS",       align: "center" },
  { key: "trend",         label: "TREND",       align: "center" },
  { key: "cc_score",      label: "CC SCORE",    align: "right" },
  { key: "csp_score",     label: "CSP SCORE",   align: "right" },
  { key: "iv_ramp_score", label: "RAMP SCORE",  align: "right" },
  { key: "why",           label: "WHY",         align: "left",  noSort: true },
];

function cellValue(row, key) {
  const price = row.price;
  switch (key) {
    case "ticker": return (
      <span>
        {row.ticker}
        {row.company_name && (
          <span className="company-name company-name-table">{row.company_name}</span>
        )}
      </span>
    );
    case "type": return <TypeBadge type={row.asymmetric_type} />;
    case "price": return price != null ? `$${Number(price).toFixed(2)}` : "—";
    case "premium": {
      const prem = row.asymmetric_type === "CSP"
        ? row.atm_put_premium
        : row.atm_call_premium;
      return prem != null ? `$${Number(prem).toFixed(2)}` : "—";
    }
    case "spread": {
      const pct = row.bid_ask_spread_pct;
      if (pct == null || row.atm_call_premium == null) return <span className="text-muted-sm">N/A</span>;
      const val = pct * 100;
      const dollar = (pct * row.atm_call_premium).toFixed(2);
      const cls = val <= 5 ? "spread-tight" : val <= 15 ? "spread-ok" : "spread-wide";
      return (
        <span>
          <span className="spread-dollar">${dollar}</span>
          <br />
          <span className={cls}>{val.toFixed(1)}%</span>
        </span>
      );
    }
    case "iv_rank": return row.iv_rank != null ? `${Math.round(row.iv_rank)}%` : "—";
    case "s1_dist": {
      if (!row.support_1 || !price || price <= 0) return "—";
      const dist = ((price - row.support_1) / price) * 100;
      const cls = dist <= 5 ? "s1dist-tight" : dist <= 15 ? "s1dist-ok" : "s1dist-wide";
      return <span className={cls}>{dist.toFixed(1)}%</span>;
    }
    case "r1_dist": {
      if (!row.resistance_1) return <span className="text-muted-sm">PD</span>;
      if (!price || price <= 0) return "—";
      const dist = ((row.resistance_1 - price) / price) * 100;
      const cls = dist <= 5 ? "s1dist-tight" : dist <= 15 ? "s1dist-ok" : "s1dist-wide";
      return <span className={cls}>{dist.toFixed(1)}%</span>;
    }
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
    case "cc_score":  return row.cc_score != null
      ? <span className="score-cc">{row.cc_score}</span> : "—";
    case "csp_score": return row.csp_score != null
      ? <span className="score-csp">{row.csp_score}</span> : "—";
    case "iv_ramp_score": {
      if (!row.iv_ramp_score) return <span className="text-muted-sm">—</span>;
      const cls = row.iv_ramp_score >= 60 ? "ramp-hi" : row.iv_ramp_score >= 30 ? "ramp-mid" : "text-muted-sm";
      return <span className={cls}>{row.iv_ramp_score}</span>;
    }
    case "why": return <span className="asym-why">{generateWhy(row)}</span>;
    default: return "—";
  }
}

function sortValue(row, key) {
  const price = row.price;
  switch (key) {
    case "ticker":        return row.ticker;
    case "type":          return row.asymmetric_type || "";
    case "price":         return price ?? -1;
    case "premium": {
      const prem = row.asymmetric_type === "CSP" ? row.atm_put_premium : row.atm_call_premium;
      return prem ?? -1;
    }
    case "spread":        return row.bid_ask_spread_pct ?? Infinity;
    case "iv_rank":       return row.iv_rank ?? -1;
    case "s1_dist":
      return (row.support_1 && price && price > 0)
        ? ((price - row.support_1) / price) * 100 : Infinity;
    case "r1_dist":
      return (row.resistance_1 && price && price > 0)
        ? ((row.resistance_1 - price) / price) * 100 : Infinity;
    case "cross":         return row.sma_golden_cross == null ? 0 : row.sma_golden_cross ? 1 : -1;
    case "trend":         return row.sma_regime ?? "";
    case "cc_score":      return row.cc_score ?? -1;
    case "csp_score":     return row.csp_score ?? -1;
    case "iv_ramp_score": return row.iv_ramp_score ?? -1;
    default: return 0;
  }
}

const SETUP_MODES = [
  { key: "all",    label: "ALL SETUPS",     cls: "asym-mode-all" },
  { key: "cc",     label: "CC SETUPS",      cls: "asym-mode-cc" },
  { key: "csp",    label: "CSP SETUPS",     cls: "asym-mode-csp" },
  { key: "ivramp", label: "IV RAMP SETUPS", cls: "asym-mode-ivramp" },
];

export default function AsymmetricScanner({ rows, onRowClick }) {
  const [setupMode, setSetupMode] = useState("all");
  const [sortCol, setSortCol]     = useState("premium");
  const [sortAsc, setSortAsc]     = useState(false);

  const flagged = rows.filter(r => r.asymmetric_any_flag);

  const modeFiltered = flagged.filter(r => {
    if (setupMode === "cc")     return r.asymmetric_cc_flag;
    if (setupMode === "csp")    return r.asymmetric_csp_flag;
    if (setupMode === "ivramp") return r.asymmetric_ivramp_flag;
    return true;
  });

  const sorted = [...modeFiltered].sort((a, b) => {
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

  const counts = {
    all:    flagged.length,
    cc:     flagged.filter(r => r.asymmetric_cc_flag).length,
    csp:    flagged.filter(r => r.asymmetric_csp_flag).length,
    ivramp: flagged.filter(r => r.asymmetric_ivramp_flag).length,
  };

  return (
    <div>
      <div className="asym-mode-bar">
        {SETUP_MODES.map(m => (
          <button
            key={m.key}
            className={`asym-mode-btn ${m.cls}${setupMode === m.key ? " active" : ""}`}
            onClick={() => setSetupMode(m.key)}
          >
            {m.label} ({counts[m.key]})
          </button>
        ))}
      </div>
      <div className="prem-scanner-wrap">
        {sorted.length === 0 ? (
          <div className="empty">
            {flagged.length === 0
              ? "No asymmetric setups detected in current scan. All criteria must converge simultaneously."
              : "No setups match this filter."}
          </div>
        ) : (
          <table className="prem-scanner-table">
            <thead>
              <tr>
                {COLS.map(col => (
                  <th
                    key={col.key}
                    className={`prem-scanner-th${col.align === "right" ? " right" : col.align === "center" ? " center" : ""}${sortCol === col.key ? " sorted" : ""}`}
                    onClick={col.noSort ? undefined : () => handleSort(col.key)}
                    style={col.noSort ? { cursor: "default" } : {}}
                  >
                    {col.label}
                    {!col.noSort && sortCol === col.key && (
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
                      className={`prem-scanner-td${col.align === "right" ? " right" : col.align === "center" ? " center" : ""}${col.key === "ticker" ? " ticker-col" : ""}${col.key === "why" ? " asym-why-col" : ""}`}
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
