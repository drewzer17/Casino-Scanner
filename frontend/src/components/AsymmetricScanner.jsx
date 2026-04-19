import React, { useState } from "react";
import CrossConflictWarning from "./CrossConflictWarning.jsx";

// ── Constants ────────────────────────────────────────────────────────────────

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

const EVAL_LEVELS = [
  { key: 0, label: "ATM" },
  { key: 1, label: "1 OTM" },
  { key: 2, label: "2 OTM" },
  { key: 3, label: "3 OTM" },
];

const SETUP_COLORS = {
  CC:      "asym-cc",
  CSP:     "asym-csp",
  IV_RAMP: "asym-ivramp",
};

// ── Premium / strike extractors ──────────────────────────────────────────────

function getCallPrem(row, level) {
  if (level === 0) return row.atm_call_premium;
  if (level === 1 && row.premium_otm1 != null) return row.premium_otm1;
  if (level === 2 && row.premium_otm2 != null) return row.premium_otm2;
  const entry = (row.expiry_data || []).find(e => e.expiry === row.best_expiry)
    || (row.expiry_data || [])[0];
  return entry?.calls?.[level - 1]?.prem ?? null;
}

function getPutPrem(row, level) {
  if (level === 0) return row.atm_put_premium;
  const entry = (row.expiry_data || []).find(e => e.expiry === row.best_put_expiry)
    || (row.expiry_data || [])[0];
  return entry?.puts?.[level - 1]?.prem ?? null;
}

function getCallStrike(row, level) {
  if (level === 0) return row.best_strike;
  const entry = (row.expiry_data || []).find(e => e.expiry === row.best_expiry)
    || (row.expiry_data || [])[0];
  if (entry?.calls?.[level - 1]?.strike != null) return entry.calls[level - 1].strike;
  const inc = guessStrikeIncrement(row.price);
  return row.best_strike != null ? row.best_strike + level * inc : null;
}

function getPutStrike(row, level) {
  if (level === 0) return row.best_put_strike;
  const entry = (row.expiry_data || []).find(e => e.expiry === row.best_put_expiry)
    || (row.expiry_data || [])[0];
  if (entry?.puts?.[level - 1]?.strike != null) return entry.puts[level - 1].strike;
  const inc = guessStrikeIncrement(row.price);
  return row.best_put_strike != null ? row.best_put_strike - level * inc : null;
}

// ── Criteria evaluation ──────────────────────────────────────────────────────

function calcCCCriteria(row, callPrem) {
  const price  = row.price;
  const ivRank = row.iv_rank;
  const spread = row.bid_ask_spread_pct;
  const oi     = row.open_interest;
  const s1Dist = (row.support_1 != null && price > 0) ? (price - row.support_1) / price * 100 : null;
  const r1Dist = (row.resistance_1 != null && price > 0) ? (row.resistance_1 - price) / price * 100 : null;

  return [
    { label: "Golden cross",
      pass: row.sma_golden_cross === true,
      hint: `Golden cross: ${row.sma_golden_cross === true ? "yes" : "no"}` },
    { label: "Uptrend/PD",
      pass: row.sma_regime === "UPTREND" || row.resistance_1 == null,
      hint: `Trend: ${row.sma_regime ?? "—"}` },
    { label: "R1 dist ≤10%",
      pass: row.resistance_1 == null || (r1Dist != null && r1Dist <= 10),
      hint: r1Dist != null ? `R1 dist ${r1Dist.toFixed(1)}% > 10%` : "R1 dist: PD" },
    { label: "IV rank 40-80",
      pass: ivRank != null && ivRank >= 40 && ivRank <= 80,
      hint: ivRank != null ? (ivRank < 40 ? `IV rank ${Math.round(ivRank)} < 40` : `IV rank ${Math.round(ivRank)} > 80`) : "IV rank: N/A" },
    { label: "Spread ≤10%",
      pass: spread != null && spread <= 0.10,
      hint: spread != null ? `Spread ${(spread*100).toFixed(1)}% > 10%` : "Spread: N/A" },
    { label: "OI ≥200",
      pass: oi != null && oi >= 200,
      hint: `OI ${oi ?? "N/A"} < 200` },
    { label: "Call ≥$2",
      pass: callPrem != null && callPrem >= 2.00,
      hint: callPrem != null ? `Call $${callPrem.toFixed(2)} < $2` : "Call prem: N/A" },
    { label: "S1 dist ≤12%",
      pass: s1Dist != null && s1Dist <= 12,
      hint: s1Dist != null ? `S1 dist ${s1Dist.toFixed(1)}% > 12%` : "S1 dist: N/A" },
  ];
}

function calcCSPCriteria(row, putPrem) {
  const price  = row.price;
  const ivRank = row.iv_rank;
  const spread = row.bid_ask_spread_pct;
  const oi     = row.open_interest;
  const s1Dist = (row.support_1 != null && price > 0) ? (price - row.support_1) / price * 100 : null;

  return [
    { label: "Golden cross",
      pass: row.sma_golden_cross === true,
      hint: `Golden cross: ${row.sma_golden_cross === true ? "yes" : "no"}` },
    { label: "S1 dist ≤8%",
      pass: s1Dist != null && s1Dist <= 8,
      hint: s1Dist != null ? `S1 dist ${s1Dist.toFixed(1)}% > 8%` : "S1 dist: N/A" },
    { label: "S1 strength ≥8",
      pass: row.support_1_strength != null && row.support_1_strength >= 8,
      hint: `S1 strength ${row.support_1_strength ?? "N/A"} < 8` },
    { label: "IV rank ≥45",
      pass: ivRank != null && ivRank >= 45,
      hint: ivRank != null ? `IV rank ${Math.round(ivRank)} < 45` : "IV rank: N/A" },
    { label: "Spread ≤10%",
      pass: spread != null && spread <= 0.10,
      hint: spread != null ? `Spread ${(spread*100).toFixed(1)}% > 10%` : "Spread: N/A" },
    { label: "OI ≥200",
      pass: oi != null && oi >= 200,
      hint: `OI ${oi ?? "N/A"} < 200` },
    { label: "Put ≥$2",
      pass: putPrem != null && putPrem >= 2.00,
      hint: putPrem != null ? `Put $${putPrem.toFixed(2)} < $2` : "Put prem: N/A" },
  ];
}

function calcIVRampCriteria(row) {
  const ivRank = row.iv_rank;
  const spread = row.bid_ask_spread_pct;

  return [
    { label: "IV ramp flag",
      pass: row.iv_ramp_flag === true,
      hint: "IV ramp flag not set" },
    { label: "IV rank <40",
      pass: ivRank != null && ivRank < 40,
      hint: ivRank != null ? `IV rank ${Math.round(ivRank)} ≥ 40` : "IV rank: N/A" },
    { label: "10d IV rising",
      pass: row.iv_velocity_10d == null || row.iv_velocity_10d > 0,
      hint: `10d IV vel ${row.iv_velocity_10d?.toFixed(1) ?? "N/A"} ≤ 0` },
    { label: "20d IV rising",
      pass: row.iv_velocity_20d == null || row.iv_velocity_20d > 0,
      hint: `20d IV vel ${row.iv_velocity_20d?.toFixed(1) ?? "N/A"} ≤ 0` },
    { label: "Golden cross",
      pass: row.sma_golden_cross === true,
      hint: `Golden cross: ${row.sma_golden_cross === true ? "yes" : "no"}` },
    { label: "Uptrend",
      pass: row.sma_regime === "UPTREND",
      hint: `Trend: ${row.sma_regime ?? "—"}` },
    { label: "Spread ≤15%",
      pass: spread != null && spread <= 0.15,
      hint: spread != null ? `Spread ${(spread*100).toFixed(1)}% > 15%` : "Spread: N/A" },
  ];
}

function evalRow(row, callPrem, putPrem) {
  const ccC      = calcCCCriteria(row, callPrem);
  const cspC     = calcCSPCriteria(row, putPrem);
  const ivRampC  = calcIVRampCriteria(row);
  const ccFails      = ccC.filter(c => !c.pass).length;
  const cspFails     = cspC.filter(c => !c.pass).length;
  const ivRampFails  = ivRampC.filter(c => !c.pass).length;
  return {
    ccPass: ccFails === 0, cspPass: cspFails === 0, ivRampPass: ivRampFails === 0,
    ccFails, cspFails, ivRampFails,
    ccC, cspC, ivRampC,
  };
}

function getBestNearMiss(ev) {
  const options = [
    { type: "CC",      fails: ev.ccFails,     criteria: ev.ccC },
    { type: "CSP",     fails: ev.cspFails,    criteria: ev.cspC },
    { type: "IV_RAMP", fails: ev.ivRampFails, criteria: ev.ivRampC },
  ].filter(o => o.fails > 0);
  if (!options.length) return null;
  options.sort((a, b) => a.fails - b.fails);
  const best = options[0];
  return { setupType: best.type, fails: best.fails, failedCriteria: best.criteria.filter(c => !c.pass) };
}

// ── Display helpers ──────────────────────────────────────────────────────────

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

function NearMissBadge({ level, setupType }) {
  const style = level === 1
    ? { background: "#1e3a5f", border: "1px solid #2563eb", color: "#93c5fd" }
    : { background: "#3b2a00", border: "1px solid #ca8a04", color: "#fcd34d" };
  const setupStyle = SETUP_COLORS[setupType] ? undefined : {};
  return (
    <span className="asym-type-wrap">
      <span className="asym-type-badge" style={style}>NM{level}</span>
      {setupType && (
        <span className={`asym-type-badge ${SETUP_COLORS[setupType] || ""}`} style={setupStyle}>
          {setupType === "IV_RAMP" ? "IV RAMP" : setupType}
        </span>
      )}
    </span>
  );
}

function fmtExpiry(exp) {
  if (!exp) return "—";
  const [y, m, d] = exp.split("-").map(Number);
  return new Date(y, m - 1, d).toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

function getRelevantDte(row) {
  const type = row._nearMissInfo?.setupType ?? row.asymmetric_type;
  return type === "CSP" ? row.best_put_dte : row.best_dte;
}
function getRelevantExpiry(row) {
  const type = row._nearMissInfo?.setupType ?? row.asymmetric_type;
  return type === "CSP" ? row.best_put_expiry : row.best_expiry;
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
            <th>Level</th><th>Strike</th><th>Premium/sh</th><th>Per Contract</th>
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
  const nmType = row._nearMissInfo?.setupType;

  const showCalls = row.asymmetric_cc_flag || row.asymmetric_ivramp_flag
    || nmType === "CC" || nmType === "IV_RAMP";
  const showPuts  = row.asymmetric_csp_flag || nmType === "CSP";

  const callRows = [
    { label: "ATM",   cls: "otm-atm",   strike: row.best_strike, prem: row.atm_call_premium },
    { label: "1 OTM", cls: "otm-1",     strike: row.best_strike != null ? row.best_strike + inc : null, prem: row.premium_otm1 },
    { label: "2 OTM", cls: "otm-2plus", strike: row.best_strike != null ? row.best_strike + 2 * inc : null, prem: row.premium_otm2 },
  ];

  const putExpEntry = (row.expiry_data || []).find(e => e.expiry === row.best_put_expiry)
    || (row.expiry_data || [])[0];
  const otmPuts = putExpEntry?.puts || [];

  const putRows = [
    { label: "ATM",   cls: "otm-atm",   strike: row.best_put_strike, prem: row.atm_put_premium },
    { label: "1 OTM", cls: "otm-1",
      strike: otmPuts[0]?.strike ?? (row.best_put_strike != null ? row.best_put_strike - inc : null),
      prem: otmPuts[0]?.prem ?? null },
    { label: "2 OTM", cls: "otm-2plus",
      strike: otmPuts[1]?.strike ?? (row.best_put_strike != null ? row.best_put_strike - 2 * inc : null),
      prem: otmPuts[1]?.prem ?? null },
  ];

  const renderCalls = showCalls || !showPuts;

  return (
    <div className="asym-expansion" onClick={e => e.stopPropagation()}>
      {renderCalls && (
        <PremiumSection title="Call Premiums" expLabel={fmtExpiry(row.best_expiry)} dte={row.best_dte} tableRows={callRows} />
      )}
      {showPuts && (
        <PremiumSection title="Put Premiums" expLabel={fmtExpiry(row.best_put_expiry)} dte={row.best_put_dte} tableRows={putRows} />
      )}
      <button className="asym-full-btn" onClick={e => { e.stopPropagation(); onFullDetail(); }}>
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
  { key: "spread",        label: "SPREAD",      align: "right" },
  { key: "iv_rank",       label: "IV RANK",     align: "right" },
  { key: "s1_dist",       label: "S1",          align: "right" },
  { key: "r1_dist",       label: "R1",          align: "right" },
  { key: "cross",         label: "CROSS",       align: "center" },
  { key: "trend",         label: "TREND",       align: "center" },
  { key: "cc_score",      label: "CC SCORE",    align: "right" },
  { key: "csp_score",     label: "CSP SCORE",   align: "right" },
  { key: "iv_ramp_score", label: "RAMP SCORE",  align: "right" },
  { key: "why",           label: "WHY",         align: "left",  noSort: true },
];

function cellValue(row, key, evalOtmLevel = 0) {
  const price = row.price;
  const nmType = row._nearMissInfo?.setupType;
  const isCSP = nmType ? nmType === "CSP" : row.asymmetric_type === "CSP";

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
    case "type":
      if (row._nearMissInfo) return <NearMissBadge level={row._nearMissInfo.fails} setupType={nmType} />;
      return <TypeBadge type={row.asymmetric_type} />;
    case "price": return price != null ? `$${Number(price).toFixed(2)}` : "—";
    case "premium": {
      const prem = isCSP ? getPutPrem(row, evalOtmLevel) : getCallPrem(row, evalOtmLevel);
      return prem != null ? `$${Number(prem).toFixed(2)}` : "—";
    }
    case "strike": {
      const s = isCSP ? getPutStrike(row, evalOtmLevel) : getCallStrike(row, evalOtmLevel);
      if (s == null) return "—";
      const distPct = price ? (isCSP ? (price - s) : (s - price)) / price * 100 : null;
      const distCls = distPct == null ? "" : distPct >= 3 ? "spread-tight" : distPct >= 1 ? "spread-ok" : "spread-wide";
      return (
        <span>
          ${s.toFixed(2)}
          {distPct != null && <><br /><span className={distCls}>{isCSP ? "-" : "+"}{distPct.toFixed(1)}%</span></>}
        </span>
      );
    }
    case "otm": {
      if (evalOtmLevel === 0) return <span className="otm-atm">ATM</span>;
      const cls = evalOtmLevel === 1 ? "otm-1" : "otm-2plus";
      return <span className={cls}>{evalOtmLevel} OTM</span>;
    }
    case "dte": {
      const dte = getRelevantDte(row);
      return dte != null ? `${dte}d` : "—";
    }
    case "expiry": return fmtExpiry(getRelevantExpiry(row));
    case "spread": {
      const pct = row.bid_ask_spread_pct;
      const prem = isCSP ? getPutPrem(row, evalOtmLevel) : getCallPrem(row, evalOtmLevel);
      if (pct == null || prem == null) return <span className="text-muted-sm">N/A</span>;
      const val    = pct * 100;
      const dollar = (pct * prem).toFixed(2);
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
      if (!row.support_1 || !price || price <= 0) return <span style={{ color: "#ef4444", fontWeight: "bold" }}>FF</span>;
      const dist = ((price - row.support_1) / price) * 100;
      const cls  = dist <= 5 ? "s1dist-tight" : dist <= 15 ? "s1dist-ok" : "s1dist-wide";
      return <span className={cls}>{dist.toFixed(1)}%</span>;
    }
    case "r1_dist": {
      if (!row.resistance_1) return <span style={{ color: "#a855f7", fontWeight: "bold" }}>PD</span>;
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
    case "cc_score":  return row.cc_score != null ? <span className="score-cc">{row.cc_score}</span> : "—";
    case "csp_score": return row.csp_score != null ? <span className="score-csp">{row.csp_score}</span> : "—";
    case "iv_ramp_score": {
      if (!row.iv_ramp_score) return <span className="text-muted-sm">—</span>;
      const cls = row.iv_ramp_score >= 60 ? "ramp-hi" : row.iv_ramp_score >= 30 ? "ramp-mid" : "text-muted-sm";
      return <span className={cls}>{row.iv_ramp_score}</span>;
    }
    case "why": {
      if (row._nearMissInfo) {
        const hints = row._nearMissInfo.failedCriteria.map(c => c.hint).join(" · ");
        return <span style={{ fontSize: "0.82em", color: "#aaa" }}>{hints}</span>;
      }
      return "—";
    }
    default: return "—";
  }
}

function sortValue(row, key, evalOtmLevel = 0) {
  const price = row.price;
  const nmType = row._nearMissInfo?.setupType;
  const isCSP = nmType ? nmType === "CSP" : row.asymmetric_type === "CSP";

  switch (key) {
    case "ticker":   return row.ticker;
    case "type":     return row._nearMissInfo ? `ZNM${row._nearMissInfo.fails}` : (row.asymmetric_type || "");
    case "price":    return price ?? -1;
    case "premium":  return (isCSP ? getPutPrem(row, evalOtmLevel) : getCallPrem(row, evalOtmLevel)) ?? -1;
    case "strike":   return (isCSP ? getPutStrike(row, evalOtmLevel) : getCallStrike(row, evalOtmLevel)) ?? -1;
    case "otm": return evalOtmLevel;
    case "dte":    return getRelevantDte(row) ?? 9999;
    case "expiry": return getRelevantExpiry(row) ?? "";
    case "spread":        return row.bid_ask_spread_pct ?? Infinity;
    case "iv_rank":       return row.iv_rank ?? -1;
    case "s1_dist":
      return (row.support_1 && price && price > 0) ? ((price - row.support_1) / price) * 100 : Infinity;
    case "r1_dist":
      return (row.resistance_1 && price && price > 0) ? ((row.resistance_1 - price) / price) * 100 : Infinity;
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

// ── Main component ───────────────────────────────────────────────────────────

export default function AsymmetricScanner({ rows, onRowClick }) {
  const [setupMode, setSetupMode]       = useState("all");
  const [dteSelected, setDteSelected]   = useState(new Set());
  const [evalOtmLevel, setEvalOtmLevel] = useState(0);
  const [showNearMiss1, setShowNearMiss1] = useState(false);
  const [showNearMiss2, setShowNearMiss2] = useState(false);
  const [sortCol, setSortCol]           = useState("premium");
  const [sortAsc, setSortAsc]           = useState(false);
  const [expandedRow, setExpandedRow]   = useState(null);

  const toggleDte = (label) => setDteSelected(prev => {
    const next = new Set(prev); if (next.has(label)) next.delete(label); else next.add(label); return next;
  });

  // At OTM level > 0, only evaluate rows that have premium data at that level
  const eligibleRows = evalOtmLevel === 0 ? rows : rows.filter(row =>
    getCallPrem(row, evalOtmLevel) != null || getPutPrem(row, evalOtmLevel) != null
  );

  // Evaluate each row with the selected OTM level
  const evaluated = eligibleRows.map(row => {
    const callPrem = getCallPrem(row, evalOtmLevel);
    const putPrem  = getPutPrem(row, evalOtmLevel);
    const ev = evalRow(row, callPrem, putPrem);
    const anyPass = ev.ccPass || ev.cspPass || ev.ivRampPass;
    const types = [];
    if (ev.ccPass)     types.push("CC");
    if (ev.cspPass)    types.push("CSP");
    if (ev.ivRampPass) types.push("IV_RAMP");
    const asymmetric_type = types.length === 3 ? "ALL_THREE" : types.length >= 2 ? types.join("+") : types[0] ?? null;
    return { ...row, _ev: ev, _anyPass: anyPass, _evalType: asymmetric_type };
  });

  // Full asymmetric passes at this eval level
  const fullPass = evaluated.filter(r => r._anyPass);

  // Near miss rows (not full passes, best setup fails 1 or 2 criteria)
  const allNearMiss = evaluated
    .filter(r => !r._anyPass)
    .map(r => {
      const nm = getBestNearMiss(r._ev);
      if (!nm) return null;
      return { ...r, _nearMissInfo: nm };
    })
    .filter(Boolean);

  const nearMiss1All = allNearMiss.filter(r => r._nearMissInfo.fails === 1);
  const nearMiss2All = allNearMiss.filter(r => r._nearMissInfo.fails === 2);

  // Mode filter helper
  function modeMatch(row, mode) {
    if (mode === "all") return true;
    const type = row._nearMissInfo ? row._nearMissInfo.setupType : row._evalType;
    if (mode === "cc")     return type?.includes("CC");
    if (mode === "csp")    return type?.includes("CSP");
    if (mode === "ivramp") return type?.includes("IV_RAMP");
    return true;
  }

  // Apply filters to full passes
  const filteredPass = fullPass
    .filter(r => modeMatch(r, setupMode))
    .filter(r => dteInAny(getRelevantDte(r), dteSelected));

  // Apply filters to near miss rows
  function filterNearMiss(nmRows) {
    return nmRows
      .filter(r => modeMatch(r, setupMode))
      .filter(r => dteInAny(getRelevantDte(r), dteSelected));
  }

  const filteredNM1 = filterNearMiss(nearMiss1All);
  const filteredNM2 = filterNearMiss(nearMiss2All);

  const doSort = (arr) => [...arr].sort((a, b) => {
    const av = sortValue(a, sortCol, evalOtmLevel);
    const bv = sortValue(b, sortCol, evalOtmLevel);
    if (typeof av === "string" && typeof bv === "string")
      return sortAsc ? av.localeCompare(bv) : bv.localeCompare(av);
    if (av < bv) return sortAsc ? -1 : 1;
    if (av > bv) return sortAsc ? 1 : -1;
    return 0;
  });

  const sortedPass = doSort(filteredPass);
  const sortedNM1  = doSort(filteredNM1);
  const sortedNM2  = doSort(filteredNM2);

  const handleSort = (key) => {
    if (sortCol === key) setSortAsc(v => !v);
    else { setSortCol(key); setSortAsc(false); }
  };

  const counts = {
    all:    fullPass.length,
    cc:     fullPass.filter(r => r._evalType?.includes("CC")).length,
    csp:    fullPass.filter(r => r._evalType?.includes("CSP")).length,
    ivramp: fullPass.filter(r => r._evalType?.includes("IV_RAMP")).length,
  };

  const nm1Count = nearMiss1All.length;
  const nm2Count = nearMiss2All.length;

  const renderRow = (row) => {
    const isExpanded = expandedRow === row.ticker;
    // For full-pass rows, use _evalType; for near miss rows, use _nearMissInfo
    const rowForExpansion = row._nearMissInfo
      ? row
      : { ...row, asymmetric_cc_flag: row._ev?.ccPass, asymmetric_csp_flag: row._ev?.cspPass, asymmetric_ivramp_flag: row._ev?.ivRampPass };

    return (
      <React.Fragment key={`${row.ticker}-${row._nearMissInfo ? "nm" : "pass"}`}>
        <tr className={`prem-scanner-row${isExpanded ? " asym-row-expanded" : ""}${row._nearMissInfo ? " asym-near-miss-row" : ""}`}>
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
              {cellValue(row, col.key, evalOtmLevel)}
            </td>
          ))}
        </tr>
        {isExpanded && (
          <tr className="asym-expansion-row">
            <td colSpan={COLS.length + 1} className="asym-expansion-cell">
              <AsymExpansion row={rowForExpansion} onFullDetail={() => onRowClick && onRowClick(row)} />
            </td>
          </tr>
        )}
      </React.Fragment>
    );
  };

  const renderSeparator = (label, color) => (
    <tr key={`sep-${label}`}>
      <td colSpan={COLS.length + 1} style={{ padding: "6px 12px", fontSize: "0.75em", color, background: "#1a1a2e", borderTop: "1px solid #333", borderBottom: "1px solid #333", letterSpacing: "0.05em" }}>
        {label}
      </td>
    </tr>
  );

  const hasAnyRows = sortedPass.length > 0 || (showNearMiss1 && sortedNM1.length > 0) || (showNearMiss2 && sortedNM2.length > 0);

  return (
    <div>
      <div className="asym-mode-bar">
        {SETUP_MODES.map(m => (
          <button
            key={m.key}
            className={`asym-mode-btn ${m.cls}${setupMode === m.key ? " active" : ""}`}
            onClick={() => setSetupMode(m.key)}
          >
            {m.label} ({counts[m.key] ?? 0})
          </button>
        ))}
        <button
          className="asym-mode-btn"
          style={showNearMiss1
            ? { background: "#1e3a5f", borderColor: "#2563eb", color: "#93c5fd" }
            : { background: "transparent", borderColor: "#2563eb", color: "#60a5fa" }}
          onClick={() => setShowNearMiss1(v => !v)}
        >
          Near Miss 1 ({nm1Count})
        </button>
        <button
          className="asym-mode-btn"
          style={showNearMiss2
            ? { background: "#3b2a00", borderColor: "#ca8a04", color: "#fcd34d" }
            : { background: "transparent", borderColor: "#ca8a04", color: "#fbbf24" }}
          onClick={() => setShowNearMiss2(v => !v)}
        >
          Near Miss 2 ({nm2Count})
        </button>
      </div>
      <div className="dte-filter-row">
        <span className="dte-filter-label">EVALUATE AT</span>
        {EVAL_LEVELS.map(lv => (
          <button
            key={lv.key}
            className={`dte-filter-btn${evalOtmLevel === lv.key ? " active" : ""}`}
            onClick={() => setEvalOtmLevel(lv.key)}
          >{lv.label}</button>
        ))}
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
        {!hasAnyRows ? (
          <div className="empty">
            {fullPass.length === 0 && nm1Count === 0
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
              {sortedPass.map(renderRow)}
              {showNearMiss1 && sortedNM1.length > 0 && (
                <>
                  {renderSeparator("── NEAR MISS 1 (one criterion from full setup) ──", "#93c5fd")}
                  {sortedNM1.map(renderRow)}
                </>
              )}
              {showNearMiss2 && sortedNM2.length > 0 && (
                <>
                  {renderSeparator("── NEAR MISS 2 (two criteria from full setup) ──", "#fcd34d")}
                  {sortedNM2.map(renderRow)}
                </>
              )}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
