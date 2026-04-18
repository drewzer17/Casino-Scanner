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

function hasOtmLevel(row, levelKey) {
  if (levelKey === "ATM") return true;
  const level = parseInt(levelKey);
  if (isNaN(level)) return false;
  if (row.asymmetric_cc_flag || row.asymmetric_ivramp_flag) {
    if (level === 1 && row.premium_otm1 != null) return true;
    const entry = (row.expiry_data || []).find(e => e.expiry === row.best_expiry)
      || (row.expiry_data || [])[0];
    if (entry?.calls?.[level - 1]?.prem != null) return true;
  }
  if (row.asymmetric_csp_flag) {
    const entry = (row.expiry_data || []).find(e => e.expiry === row.best_put_expiry)
      || (row.expiry_data || [])[0];
    if (entry?.puts?.[level - 1]?.prem != null) return true;
  }
  return false;
}

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

function fmtExpiry(exp) {
  if (!exp) return "—";
  const [y, m, d] = exp.split("-").map(Number);
  return new Date(y, m - 1, d).toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

function getRelevantDte(row) {
  return row.asymmetric_type === "CSP" ? row.best_put_dte : row.best_dte;
}
function getRelevantExpiry(row) {
  return row.asymmetric_type === "CSP" ? row.best_put_expiry : row.best_expiry;
}

function guessStrikeIncrement(price) {
  if (!price) return 5;
  if (price < 5)   return 0.5;
  if (price < 25)  return 1;
  if (price < 50)  return 2.5;
  if (price < 200) return 5;
  return 10;
}

function calcOtmLevel(strike, price, isCSP) {
  if (strike == null || !price) return null;
  const inc = guessStrikeIncrement(price);
  const diff = isCSP ? (price - strike) : (strike - price);
  return Math.round(diff / inc);
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

// ── Mini OTM expansion ───────────────────────────────────────────────────────

function PremiumSection({ title, expLabel, dte, tableRows }) {
  return (
    <div className="asym-mini-wrap">
      <div className="asym-mini-title">
        {title}
        {expLabel && ` · ${expLabel}`}
        {dte != null && ` · ${dte}d`}
      </div>
      <table className="asym-mini-table">
        <thead>
          <tr>
            <th>Level</th>
            <th>Strike</th>
            <th>Premium/sh</th>
            <th>Per Contract</th>
          </tr>
        </thead>
        <tbody>
          {tableRows.map(r => (
            <tr key={r.label}>
              <td><span className={r.cls}>{r.label}</span></td>
              <td>{r.strike != null ? `$${r.strike.toFixed(2)}` : "—"}</td>
              <td>{r.prem != null ? `$${r.prem.toFixed(2)}` : "—"}</td>
              <td>{r.prem != null ? `$${(r.prem * 100).toFixed(0)}` : "—"}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <div className="asym-mini-note">3–4 OTM: open full detail</div>
    </div>
  );
}

function AsymExpansion({ row, onFullDetail }) {
  const inc = guessStrikeIncrement(row.price);

  const showCalls = row.asymmetric_cc_flag || row.asymmetric_ivramp_flag;
  const showPuts  = row.asymmetric_csp_flag;

  const callRows = [
    { label: "ATM",   cls: "otm-atm",   strike: row.best_strike,
      prem: row.atm_call_premium },
    { label: "1 OTM", cls: "otm-1",     strike: row.best_strike != null ? row.best_strike + inc : null,
      prem: row.premium_otm1 },
    { label: "2 OTM", cls: "otm-2plus", strike: row.best_strike != null ? row.best_strike + 2 * inc : null,
      prem: row.premium_otm2 },
  ];

  // Pull put OTM levels from expiry_data (populated by extensive scan)
  const putExpEntry = (row.expiry_data || []).find(e => e.expiry === row.best_put_expiry)
    || (row.expiry_data || [])[0];
  const otmPuts = putExpEntry?.puts || [];

  const putRows = [
    { label: "ATM",   cls: "otm-atm",
      strike: row.best_put_strike,
      prem: row.atm_put_premium },
    { label: "1 OTM", cls: "otm-1",
      strike: otmPuts[0]?.strike ?? (row.best_put_strike != null ? row.best_put_strike - inc : null),
      prem: otmPuts[0]?.prem ?? null },
    { label: "2 OTM", cls: "otm-2plus",
      strike: otmPuts[1]?.strike ?? (row.best_put_strike != null ? row.best_put_strike - 2 * inc : null),
      prem: otmPuts[1]?.prem ?? null },
  ];

  // Fall back to calls if no flags are set (shouldn't happen)
  const renderCalls = showCalls || !showPuts;
  const renderPuts  = showPuts;

  return (
    <div className="asym-expansion" onClick={e => e.stopPropagation()}>
      {renderCalls && (
        <PremiumSection
          title="Call Premiums"
          expLabel={fmtExpiry(row.best_expiry)}
          dte={row.best_dte}
          tableRows={callRows}
        />
      )}
      {renderPuts && (
        <PremiumSection
          title="Put Premiums"
          expLabel={fmtExpiry(row.best_put_expiry)}
          dte={row.best_put_dte}
          tableRows={putRows}
        />
      )}
      <button
        className="asym-full-btn"
        onClick={e => { e.stopPropagation(); onFullDetail(); }}
      >
        Full Detail →
      </button>
    </div>
  );
}

// ── Table columns ────────────────────────────────────────────────────────────

const COLS = [
  { key: "ticker",        label: "TICKER",      align: "left" },
  { key: "type",          label: "SETUP TYPE",  align: "left" },
  { key: "price",         label: "PRICE",       align: "right" },
  { key: "premium",       label: "PREMIUM $",   align: "right" },
  { key: "strike",        label: "STRIKE",      align: "right" },
  { key: "otm",           label: "OTM",         align: "center" },
  { key: "dte",           label: "DTE",         align: "right" },
  { key: "expiry",        label: "EXPIRY",      align: "right" },
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
  const isCSP = row.asymmetric_type === "CSP";

  switch (key) {
    case "ticker": return (
      <span>
        {row.sma_golden_cross === true && row.sma_regime === "DOWNTREND" && <CrossConflictWarning />}
        {row.ticker}
        {row.company_name && (
          <span className="company-name company-name-table">{row.company_name}</span>
        )}
      </span>
    );
    case "type": return <TypeBadge type={row.asymmetric_type} />;
    case "price": return price != null ? `$${Number(price).toFixed(2)}` : "—";
    case "premium": {
      const prem = isCSP ? row.atm_put_premium : row.atm_call_premium;
      return prem != null ? `$${Number(prem).toFixed(2)}` : "—";
    }
    case "strike": {
      const s = isCSP ? row.best_put_strike : row.best_strike;
      return s != null ? `$${s.toFixed(2)}` : "—";
    }
    case "otm": {
      const s = isCSP ? row.best_put_strike : row.best_strike;
      if (s == null || !price) return "—";
      const level = calcOtmLevel(s, price, isCSP);
      if (level == null) return "—";
      const label = level <= 0 ? "ATM" : `${level} OTM`;
      const cls   = level <= 0 ? "otm-atm" : level === 1 ? "otm-1" : "otm-2plus";
      return <span className={cls}>{label}</span>;
    }
    case "dte": {
      const dte = getRelevantDte(row);
      return dte != null ? `${dte}d` : "—";
    }
    case "expiry": return fmtExpiry(getRelevantExpiry(row));
    case "spread": {
      const pct = row.bid_ask_spread_pct;
      if (pct == null || row.atm_call_premium == null) return <span className="text-muted-sm">N/A</span>;
      const val    = pct * 100;
      const dollar = (pct * row.atm_call_premium).toFixed(2);
      const cls    = val <= 5 ? "spread-tight" : val <= 15 ? "spread-ok" : "spread-wide";
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
      const cls  = dist <= 5 ? "s1dist-tight" : dist <= 15 ? "s1dist-ok" : "s1dist-wide";
      return <span className={cls}>{dist.toFixed(1)}%</span>;
    }
    case "r1_dist": {
      if (!row.resistance_1) return <span className="text-muted-sm">PD</span>;
      if (!price || price <= 0) return "—";
      const dist = ((row.resistance_1 - price) / price) * 100;
      const cls  = dist <= 5 ? "s1dist-tight" : dist <= 15 ? "s1dist-ok" : "s1dist-wide";
      return <span className={cls}>{dist.toFixed(1)}%</span>;
    }
    case "cross": {
      if (row.sma_golden_cross === true)  return <span className="rs-cross rs-golden">Golden</span>;
      if (row.sma_golden_cross === false) return <span className="rs-cross rs-death">Death</span>;
      return "—";
    }
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
  const isCSP = row.asymmetric_type === "CSP";

  switch (key) {
    case "ticker":        return row.ticker;
    case "type":          return row.asymmetric_type || "";
    case "price":         return price ?? -1;
    case "premium": {
      const prem = isCSP ? row.atm_put_premium : row.atm_call_premium;
      return prem ?? -1;
    }
    case "strike": {
      const s = isCSP ? row.best_put_strike : row.best_strike;
      return s ?? -1;
    }
    case "otm": {
      const s = isCSP ? row.best_put_strike : row.best_strike;
      return calcOtmLevel(s, price, isCSP) ?? 99;
    }
    case "dte":    return getRelevantDte(row) ?? 9999;
    case "expiry": return getRelevantExpiry(row) ?? "";
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
  const [dteSelected, setDteSelected] = useState(new Set());
  const [otmSelected, setOtmSelected] = useState(new Set());
  const [sortCol, setSortCol]     = useState("premium");
  const [sortAsc, setSortAsc]     = useState(false);
  const [expandedRow, setExpandedRow] = useState(null);

  const toggleDte = (label) => setDteSelected(prev => {
    const next = new Set(prev);
    if (next.has(label)) next.delete(label); else next.add(label);
    return next;
  });
  const toggleOtm = (level) => setOtmSelected(prev => {
    const next = new Set(prev);
    if (next.has(level)) next.delete(level); else next.add(level);
    return next;
  });

  const flagged = rows.filter(r => r.asymmetric_any_flag);

  const modeFiltered = flagged.filter(r => {
    if (setupMode === "cc")     return r.asymmetric_cc_flag;
    if (setupMode === "csp")    return r.asymmetric_csp_flag;
    if (setupMode === "ivramp") return r.asymmetric_ivramp_flag;
    return true;
  });

  const dteFiltered = modeFiltered.filter(r =>
    dteInAny(getRelevantDte(r), dteSelected)
  );

  const otmFiltered = dteFiltered.filter(r => {
    if (otmSelected.size === 0) return true;
    return [...otmSelected].some(lvl => hasOtmLevel(r, lvl));
  });

  const sorted = [...otmFiltered].sort((a, b) => {
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
      </div>
      <div className="prem-scanner-wrap">
        {sorted.length === 0 ? (
          <div className="empty">
            {flagged.length === 0
              ? "No asymmetric setups detected in current scan. All criteria must converge simultaneously."
              : "No setups match current filters."}
          </div>
        ) : (
          <table className="prem-scanner-table">
            <thead>
              <tr>
                <th className="prem-scanner-th" style={{ width: 28 }} />
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
              {sorted.map(row => {
                const isExpanded = expandedRow === row.ticker;
                return (
                  <React.Fragment key={row.ticker}>
                    <tr className={`prem-scanner-row${isExpanded ? " asym-row-expanded" : ""}`}>
                      <td className="prem-scanner-td" style={{ textAlign: "center", padding: "0 4px" }}>
                        <button
                          style={{ background: "none", border: "none", cursor: "pointer", color: "#aaa", fontSize: "0.7em", padding: "2px 4px" }}
                          onClick={() => setExpandedRow(isExpanded ? null : row.ticker)}
                          title="Show OTM premiums"
                        >{isExpanded ? "▼" : "▶"}</button>
                      </td>
                      {COLS.map(col => (
                        <td
                          key={col.key}
                          className={`prem-scanner-td${col.align === "right" ? " right" : col.align === "center" ? " center" : ""}${col.key === "ticker" ? " ticker-col" : ""}${col.key === "why" ? " asym-why-col" : ""}`}
                          onClick={col.key === "ticker" ? (e) => { e.stopPropagation(); onRowClick && onRowClick(row); } : undefined}
                          style={col.key === "ticker" ? { cursor: "pointer" } : undefined}
                        >
                          {cellValue(row, col.key)}
                        </td>
                      ))}
                    </tr>
                    {isExpanded && (
                      <tr className="asym-expansion-row">
                        <td colSpan={COLS.length + 1} className="asym-expansion-cell">
                          <AsymExpansion
                            row={row}
                            onFullDetail={() => onRowClick && onRowClick(row)}
                          />
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                );
              })}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
