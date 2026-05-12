import React, { useState } from "react";
import EarningsTile from "./EarningsTile.jsx";
import ResearchAsterisk from "./ResearchAsterisk.jsx";
import ScrollArrows from "./ScrollArrows.jsx";

// ── DTE ranges (same as PremiumScanner, excluding LEAPS — those use the toggle) ──
const DTE_RANGES_RQ = [
  { label: "≤3",    min: 0,  max: 3  },
  { label: "4-7",   min: 4,  max: 7  },
  { label: "10-17", min: 10, max: 17 },
  { label: "21-30", min: 21, max: 30 },
  { label: "31-61", min: 31, max: 61 },
];

const OTM_LEVELS_RQ = ["ATM", "1", "2", "3", "4", "5"];

function guessStrikeInc(price) {
  if (!price) return 5;
  if (price < 5)    return 0.5;
  if (price < 25)   return 1;
  if (price < 50)   return 2.5;
  if (price < 500)  return 5;
  if (price < 1000) return 10;
  return 25;
}

function calcOtmLvl(strike, price, isCSP) {
  if (strike == null || !price) return null;
  const inc = guessStrikeInc(price);
  const diff = isCSP ? (price - strike) : (strike - price);
  return Math.round(diff / inc);
}

function otmKey(level) {
  if (level == null || level <= 0) return "ATM";
  return String(Math.min(level, 5));
}

function fmtLeapsTs(iso) {
  if (!iso) return "Never scanned";
  const d = new Date(iso);
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric" }) +
    ", " + d.toLocaleTimeString("en-US", { hour: "numeric", minute: "2-digit" });
}

// ── Grade & VRP helpers ───────────────────────────────────────────────────────

export const GRADE_ORDER = { A: 1, B: 2, C: 3, F: 4 };
export const GRADE_COLORS = { A: "#3fc27f", B: "#f0d000", C: "#f0a030", F: "#e0525e" };
const VRP_COLORS = { Rich: "#4ade80", Moderate: "#facc15", Weak: "#fb923c", Negative: "#f87171" };

const FAIL_LABELS = {
  earnings_overlap:           "Earnings overlap",
  weak_vrp_income_grind:      "Weak VRP (Income Grind)",
  extreme_backwardation:      "Extreme backwardation",
  put_skew_elevated:          "Elevated put skew (15-25)",
  extreme_put_skew:           "Extreme put skew (>25)",
  low_iv_rank_weak_vrp:       "Low IV Rank + Weak VRP",
  low_iv_rank_rich_vrp:       "Low IV Rank (VRP positive)",
  trend_csp:                  "Downtrend (CSP)",
  distribution_days:          "High distribution days",
  strike_misaligned:          "Strike not near S/R",
  mild_backwardation:         "Mild backwardation (5-15)",
  earnings_overlap_technical: "Earnings overlap (Tech Location)",
};

export function formatFailKey(k) {
  return FAIL_LABELS[k] || k.replace(/_/g, " ");
}

function GradePill({ grade }) {
  if (!grade) return <span className="rq-grade-null">—</span>;
  return (
    <span className="rq-grade-pill" style={{ background: GRADE_COLORS[grade] }}>
      {grade}
    </span>
  );
}

function VrpBadge({ state, spread, highBeta }) {
  if (!state) return <span className="rq-grade-null">—</span>;
  const sign = spread != null && spread >= 0 ? "+" : "";
  const spreadStr = spread != null ? `${sign}${spread.toFixed(1)}` : null;
  return (
    <div style={{ textAlign: "center" }}>
      <span className="rq-vrp-badge" style={{ background: VRP_COLORS[state] ?? "#6b7280" }}>
        {state}
      </span>
      {highBeta && (
        <span style={{
          marginLeft: "4px",
          fontSize: "9px",
          fontWeight: 800,
          color: "#4ade80",
          verticalAlign: "middle",
          letterSpacing: "0.02em",
        }}>β+</span>
      )}
      {spreadStr && <div className="rq-vrp-spread">{spreadStr}</div>}
    </div>
  );
}

function StrategyTag({ strategyType, secondaryEdge }) {
  if (!strategyType) return <span className="rq-grade-null">—</span>;

  const hasSecondaryTech = (secondaryEdge || []).includes("TechnicalLocation");

  if (strategyType === "Event Ramp") {
    return (
      <span className="rq-strategy-tag rq-strategy-event">
        {hasSecondaryTech ? "Event+Tech" : "Event"}
      </span>
    );
  }
  if (strategyType === "Technical Location") {
    return <span className="rq-strategy-tag rq-strategy-tech">Tech</span>;
  }
  // Income Grind
  return <span className="rq-strategy-tag rq-strategy-income">Income</span>;
}

function FailSummary({ row }) {
  const grade = row.risk_grade;
  if (!grade) return <span className="rq-grade-null">—</span>;
  if (grade === "A") return <span className="rq-fail-clean">Clean</span>;
  if (grade === "B") {
    const n = row.soft_fail_count ?? 0;
    return <span className="rq-fail-soft">{n} soft</span>;
  }
  if (grade === "C") {
    const strong = (row.strong_fail_reasons || [])[0];
    const text = strong ? formatFailKey(strong) : `${row.soft_fail_count ?? 0} soft`;
    return <span className="rq-fail-strong">{text}</span>;
  }
  // F
  const hard = (row.hard_fail_reasons || [])[0];
  const text = hard ? formatFailKey(hard) : "Hard fail";
  return <span className="rq-fail-hard">{text}</span>;
}

function rowOpacity(grade) {
  if (grade === "F") return 0.45;
  if (grade === "C") return 0.7;
  return 1.0;
}

// ── Sorting ──────────────────────────────────────────────────────────────────

const GRADE_SORT = { A: 1, B: 2, C: 3, F: 4, null: 5, undefined: 5 };

function sortRqRows(rows, col, asc) {
  const s = [...rows];
  s.sort((a, b) => {
    let av, bv;
    switch (col) {
      case "ticker":   av = a.ticker; bv = b.ticker; break;
      case "grade":    av = GRADE_SORT[a.risk_grade] ?? 5; bv = GRADE_SORT[b.risk_grade] ?? 5; break;
      case "vrp":      av = a.vrp_spread ?? -999; bv = b.vrp_spread ?? -999; break;
      case "strategy": av = a.strategy_type ?? ""; bv = b.strategy_type ?? ""; break;
      case "score":    av = a.score; bv = b.score; break;
      case "strike":   av = a._strike ?? -1; bv = b._strike ?? -1; break;
      case "dte":      av = a._dte ?? 9999; bv = b._dte ?? 9999; break;
      case "premium":  av = a._premium ?? -1; bv = b._premium ?? -1; break;
      case "iv_rank":  av = a.iv_rank ?? -1; bv = b.iv_rank ?? -1; break;
      default: return 0;
    }
    if (av < bv) return asc ? -1 : 1;
    if (av > bv) return asc ? 1 : -1;
    return 0;
  });
  return s;
}

// ── Column headers ────────────────────────────────────────────────────────────

const COLS = [
  { key: "ticker",   label: "Ticker",    align: "left"   },
  { key: "grade",    label: "Grade",     align: "center" },
  { key: "vrp",      label: "VRP",       align: "center" },
  { key: "strategy", label: "Strategy",  align: "center" },
  { key: "score",    label: "Score",     align: "right"  },
  { key: "strike",   label: "Strike",    align: "right"  },
  { key: "dte",      label: "DTE",       align: "right"  },
  { key: "premium",  label: "Prem $",    align: "right"  },
  { key: "iv_rank",  label: "IV Rank",   align: "right"  },
  { key: "fail",     label: "Fail Summary", align: "left" },
];

function enrichRow(row) {
  const isCC = (row.cc_score ?? 0) > (row.csp_score ?? 0);
  return {
    ...row,
    _isCC:    isCC,
    _strike:  isCC ? (row.best_strike ?? null)     : (row.best_put_strike ?? null),
    _dte:     isCC ? (row.best_dte ?? null)         : (row.best_put_dte ?? null),
    _premium: isCC ? (row.atm_call_premium ?? null) : (row.atm_put_premium ?? null),
  };
}

// ── Earnings filter ───────────────────────────────────────────────────────────

const EARN_BUCKETS = [
  { key: "0-7d",  label: "0-7d",  test: d => d != null && d >= 0 && d <= 7  },
  { key: "7-21d", label: "7-21d", test: d => d != null && d > 7  && d <= 21 },
  { key: "21+",   label: "21+",   test: d => d != null && d > 21             },
  { key: "none",  label: "None",  test: d => d == null                       },
];

// ── Component ─────────────────────────────────────────────────────────────────

export default function RiskQualityScanner({
  rows,
  onRowClick,
  onResearch,
  gradeFilter,
  vrpFilter,
  strategyFilter,
  onGradeFilter,
  onVrpFilter,
  onStrategyFilter,
  onAddToPositions,
  leapsRows = [],
  leapsScannedAt = null,
}) {
  const [sortCol, setSortCol] = useState("grade");
  const [sortAsc, setSortAsc] = useState(true);
  const [earnFilter, setEarnFilter] = useState(new Set());

  // New filters matching PremiumScanner
  const [typeFilter, setTypeFilter]     = useState("ALL");  // "ALL" | "CC" | "CSP"
  const [otmSelected, setOtmSelected]   = useState(new Set());
  const [dteSelected, setDteSelected]   = useState(new Set());
  const [showLeaps, setShowLeaps]       = useState(false);

  const handleSort = (key) => {
    if (key === "fail") return; // not sortable
    if (sortCol === key) setSortAsc(v => !v);
    else { setSortCol(key); setSortAsc(key === "ticker" || key === "grade" || key === "strategy"); }
  };

  const enriched = rows.map(enrichRow);

  const VRP_MAP = { Rich: "Rich", Mod: "Moderate", Weak: "Weak", Neg: "Negative" };
  const STRATEGY_MAP = { Income: "Income Grind", Event: "Event Ramp", Tech: "Technical Location" };

  // Apply RQ-specific filters
  const filtered = enriched.filter(r => {
    if (gradeFilter.size > 0 && !gradeFilter.has(r.risk_grade)) return false;
    if (vrpFilter.size > 0) {
      const allowed = new Set([...vrpFilter].map(k => VRP_MAP[k]));
      if (!allowed.has(r.vrp_state)) return false;
    }
    if (strategyFilter.size > 0) {
      const allowed = new Set([...strategyFilter].map(k => STRATEGY_MAP[k]));
      if (!allowed.has(r.strategy_type)) return false;
    }
    if (earnFilter.size > 0) {
      const match = [...earnFilter].some(k => {
        const b = EARN_BUCKETS.find(x => x.key === k);
        return b && b.test(r.earnings_days);
      });
      if (!match) return false;
    }
    // TYPE filter
    if (typeFilter !== "ALL") {
      if (typeFilter === "CC"  && !r._isCC) return false;
      if (typeFilter === "CSP" &&  r._isCC) return false;
    }
    // OTM filter
    if (otmSelected.size > 0) {
      const level = calcOtmLvl(r._strike, r.price, !r._isCC);
      const key = otmKey(level);
      if (!otmSelected.has(key)) return false;
    }
    // DTE filter
    if (dteSelected.size > 0) {
      const dte = r._dte;
      if (dte == null) return false;
      const inRange = [...dteSelected].some(lbl => {
        const range = DTE_RANGES_RQ.find(x => x.label === lbl);
        return range && dte >= range.min && dte <= range.max;
      });
      if (!inRange) return false;
    }
    return true;
  });

  // Default sort: grade asc, then score desc
  let sorted;
  if (sortCol === "grade") {
    sorted = [...filtered].sort((a, b) => {
      const ag = GRADE_SORT[a.risk_grade] ?? 5;
      const bg = GRADE_SORT[b.risk_grade] ?? 5;
      if (ag !== bg) return sortAsc ? ag - bg : bg - ag;
      return b.score - a.score; // within same grade, score desc
    });
  } else {
    sorted = sortRqRows(filtered, sortCol, sortAsc);
  }

  // Merge LEAPS rows when toggle is ON (always shown regardless of DTE/OTM/type filter)
  const enrichedLeaps = showLeaps && leapsRows.length > 0
    ? leapsRows.map(r => ({ ...enrichRow(r), _isLeaps: true }))
    : [];
  const displayRows = enrichedLeaps.length > 0 ? [...sorted, ...enrichedLeaps] : sorted;

  // Count grades for display
  const gradeCounts = { A: 0, B: 0, C: 0, F: 0 };
  for (const r of enriched) {
    if (r.risk_grade in gradeCounts) gradeCounts[r.risk_grade]++;
  }

  return (
    <div>
      {/* TODO Phase 5+: Regime indicator banner
          Position: top of scanner, above filter bar
          Will show Green/Yellow/Red from Market Health tab
          Affects which grades are tradeable (Red = no new positions) */}

      {/* ── RQ-specific filter rows ── */}
      <div className="dte-filter-row">
        <span className="dte-filter-label">GRADE</span>
        {["all", "A", "B", "C", "F"].map(opt => (
          <button
            key={opt}
            className={`dte-filter-btn rq-grade-btn-${opt}${opt === "all" ? (gradeFilter.size === 0 ? " active" : "") : (gradeFilter.has(opt) ? " active" : "")}`}
            style={gradeFilter.has(opt) && opt !== "all" ? { background: GRADE_COLORS[opt], borderColor: GRADE_COLORS[opt], color: "#fff" } : {}}
            onClick={() => {
              if (opt === "all") { onGradeFilter(new Set()); return; }
              const next = new Set(gradeFilter);
              if (next.has(opt)) next.delete(opt); else next.add(opt);
              onGradeFilter(next);
            }}
          >
            {opt === "all" ? "ALL" : opt}
            {opt !== "all" && <span className="dte-filter-count">{gradeCounts[opt] || 0}</span>}
          </button>
        ))}
      </div>
      <div className="dte-filter-row">
        <span className="dte-filter-label">VRP</span>
        {["all", "Rich", "Mod", "Weak", "Neg"].map(opt => (
          <button
            key={opt}
            className={`dte-filter-btn${opt === "all" ? (vrpFilter.size === 0 ? " active" : "") : (vrpFilter.has(opt) ? " active" : "")}`}
            style={vrpFilter.has(opt) && opt !== "all"
              ? { background: VRP_COLORS[{ Rich: "Rich", Mod: "Moderate", Weak: "Weak", Neg: "Negative" }[opt]] || "#6b7280", borderColor: "transparent", color: "#000", fontWeight: 700 }
              : {}}
            onClick={() => {
              if (opt === "all") { onVrpFilter(new Set()); return; }
              const next = new Set(vrpFilter);
              if (next.has(opt)) next.delete(opt); else next.add(opt);
              onVrpFilter(next);
            }}
          >
            {opt === "all" ? "ALL" : opt}
          </button>
        ))}
      </div>
      <div className="dte-filter-row">
        <span className="dte-filter-label">STRATEGY</span>
        {["all", "Income", "Event", "Tech"].map(opt => (
          <button
            key={opt}
            className={`dte-filter-btn${opt === "all" ? (strategyFilter.size === 0 ? " active" : "") : (strategyFilter.has(opt) ? " active" : "")}`}
            onClick={() => {
              if (opt === "all") { onStrategyFilter(new Set()); return; }
              const next = new Set(strategyFilter);
              if (next.has(opt)) next.delete(opt); else next.add(opt);
              onStrategyFilter(next);
            }}
          >
            {opt === "all" ? "ALL" : opt}
          </button>
        ))}
      </div>
      <div className="dte-filter-row">
        <span className="dte-filter-label">TYPE</span>
        {["ALL", "CC", "CSP"].map(opt => (
          <button
            key={opt}
            className={`dte-filter-btn${typeFilter === opt ? " active" : ""}`}
            onClick={() => setTypeFilter(opt)}
          >{opt}</button>
        ))}
      </div>
      <div className="dte-filter-row">
        <span className="dte-filter-label">OTM</span>
        {OTM_LEVELS_RQ.map(lvl => (
          <button
            key={lvl}
            className={`dte-filter-btn${otmSelected.has(lvl) ? " active" : ""}`}
            onClick={() => setOtmSelected(prev => {
              const n = new Set(prev);
              n.has(lvl) ? n.delete(lvl) : n.add(lvl);
              return n;
            })}
          >{lvl}</button>
        ))}
        <button
          className={`dte-filter-btn${otmSelected.size === 0 ? " active" : ""}`}
          onClick={() => setOtmSelected(new Set())}
        >ALL</button>
      </div>
      <div className="dte-filter-row">
        <span className="dte-filter-label">DTE</span>
        {DTE_RANGES_RQ.map(r => (
          <button
            key={r.label}
            className={`dte-filter-btn${dteSelected.has(r.label) ? " active" : ""}`}
            onClick={() => setDteSelected(prev => {
              const n = new Set(prev);
              n.has(r.label) ? n.delete(r.label) : n.add(r.label);
              return n;
            })}
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
      </div>
      <div className="dte-filter-row">
        <span className="dte-filter-label">EARNINGS</span>
        <button
          className={`dte-filter-btn${earnFilter.size === 0 ? " active" : ""}`}
          onClick={() => setEarnFilter(new Set())}
        >ALL</button>
        {EARN_BUCKETS.map(b => (
          <button
            key={b.key}
            className={`dte-filter-btn${earnFilter.has(b.key) ? " active" : ""}`}
            onClick={() => setEarnFilter(prev => {
              const n = new Set(prev);
              n.has(b.key) ? n.delete(b.key) : n.add(b.key);
              return n;
            })}
          >{b.label}</button>
        ))}
        <span className="dte-filter-count">{displayRows.length} rows · {new Set(displayRows.map(r => r.ticker)).size} tickers</span>
      </div>

      {/* ── Table ── */}
      <ScrollArrows>
        {displayRows.length === 0 ? (
          <div className="empty">No results match these filters.</div>
        ) : (
          <table className="prem-scanner-table rq-table">
            <thead>
              <tr>
                {COLS.map(col => (
                  <th
                    key={col.key}
                    className={`prem-scanner-th${col.align === "right" ? " right" : col.align === "center" ? " center" : ""}${sortCol === col.key ? " sorted" : ""}${col.key === "fail" ? "" : " rq-sortable"}`}
                    onClick={() => handleSort(col.key)}
                  >
                    {col.label}
                    {sortCol === col.key && col.key !== "fail" && (
                      <span className="sort-arrow">{sortAsc ? " ▲" : " ▼"}</span>
                    )}
                  </th>
                ))}
                {onAddToPositions && <th className="prem-scanner-th row-action-th"></th>}
              </tr>
            </thead>
            <tbody>
              {displayRows.map((row, idx) => {
                const opacity = row._isLeaps ? 1.0 : rowOpacity(row.risk_grade);
                const key = `${row.ticker}-${row._isLeaps ? "leaps-" : ""}${row.best_expiry || row.best_put_expiry || idx}`;
                return (
                  <tr
                    key={key}
                    className={`prem-scanner-row rq-row${row._isLeaps ? " leaps-row" : ""}`}
                    style={{ opacity }}
                    onMouseEnter={e => { if (opacity < 1) e.currentTarget.style.opacity = "1"; }}
                    onMouseLeave={e => { if (opacity < 1) e.currentTarget.style.opacity = String(opacity); }}
                    onClick={() => onRowClick && onRowClick(row)}
                  >
                    {/* Ticker */}
                    <td className="prem-scanner-td ticker-col">
                      <ResearchAsterisk ticker={row.ticker} hasSitrep={row.has_sitrep} onResearch={onResearch} isDefense={row.is_defense} />
                      <span style={{ fontWeight: 600 }}>{row.ticker}</span>
                      <EarningsTile days={row.earnings_days} inWindow={row.earnings_in_window} />
                      {row.company_name && (
                        <span className="company-name company-name-table">{row.company_name}</span>
                      )}
                    </td>
                    {/* Grade */}
                    <td className="prem-scanner-td center"><GradePill grade={row.risk_grade} /></td>
                    {/* VRP */}
                    <td className="prem-scanner-td center"><VrpBadge state={row.vrp_state} spread={row.vrp_spread} highBeta={row.high_beta_moderate} /></td>
                    {/* Strategy */}
                    <td className="prem-scanner-td center"><StrategyTag strategyType={row.strategy_type} secondaryEdge={row.secondary_edge} /></td>
                    {/* Score */}
                    <td className="prem-scanner-td right">
                      <span style={{ fontWeight: 700, color: row.score >= 60 ? "var(--green)" : row.score >= 40 ? "var(--yellow)" : "var(--text-muted)" }}>
                        {row.score.toFixed(0)}
                      </span>
                    </td>
                    {/* Strike */}
                    <td className="prem-scanner-td right">
                      {row._strike != null ? `$${row._strike.toFixed(0)}` : "—"}
                    </td>
                    {/* DTE */}
                    <td className="prem-scanner-td right">
                      {row._isLeaps
                        ? <span className="leaps-dte-badge">{row._dte != null ? `${row._dte}d ` : ""}LEAPS</span>
                        : (row._dte != null ? `${row._dte}d` : "—")}
                    </td>
                    {/* Premium $ */}
                    <td className="prem-scanner-td right prem-col">
                      {row._premium != null ? `$${row._premium.toFixed(2)}` : "—"}
                    </td>
                    {/* IV Rank */}
                    <td className="prem-scanner-td right">
                      {row.iv_rank != null ? `${row.iv_rank.toFixed(0)}` : "—"}
                    </td>
                    {/* Fail Summary */}
                    <td className="prem-scanner-td"><FailSummary row={row} /></td>
                    {/* Add to positions */}
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
                );
              })}
            </tbody>
          </table>
        )}
      </ScrollArrows>
    </div>
  );
}
