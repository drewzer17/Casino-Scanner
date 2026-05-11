import React, { useState } from "react";
import EarningsTile from "./EarningsTile.jsx";
import ResearchAsterisk from "./ResearchAsterisk.jsx";
import ScrollArrows from "./ScrollArrows.jsx";

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
}) {
  const [sortCol, setSortCol] = useState("grade");
  const [sortAsc, setSortAsc] = useState(true);
  const [earnFilter, setEarnFilter] = useState(new Set());

  const handleSort = (key) => {
    if (key === "fail") return; // not sortable
    if (sortCol === key) setSortAsc(v => !v);
    else { setSortCol(key); setSortAsc(key === "ticker" || key === "grade" || key === "strategy"); }
  };

  const enriched = rows.map(enrichRow);

  // Apply RQ-specific filters
  const filtered = enriched.filter(r => {
    if (gradeFilter !== "all" && r.risk_grade !== gradeFilter) return false;
    if (vrpFilter !== "all") {
      const map = { "Rich": "Rich", "Mod": "Moderate", "Weak": "Weak", "Neg": "Negative" };
      if (r.vrp_state !== map[vrpFilter]) return false;
    }
    if (strategyFilter !== "all") {
      const map = { "Income": "Income Grind", "Event": "Event Ramp", "Tech": "Technical Location" };
      if (r.strategy_type !== map[strategyFilter]) return false;
    }
    if (earnFilter.size > 0) {
      const match = [...earnFilter].some(k => {
        const b = EARN_BUCKETS.find(x => x.key === k);
        return b && b.test(r.earnings_days);
      });
      if (!match) return false;
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
            className={`dte-filter-btn rq-grade-btn-${opt}${gradeFilter === opt ? " active" : ""}`}
            style={gradeFilter === opt && opt !== "all" ? { background: GRADE_COLORS[opt], borderColor: GRADE_COLORS[opt], color: "#fff" } : {}}
            onClick={() => onGradeFilter(opt)}
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
            className={`dte-filter-btn${vrpFilter === opt ? " active" : ""}`}
            style={vrpFilter === opt && opt !== "all"
              ? { background: VRP_COLORS[{ Rich: "Rich", Mod: "Moderate", Weak: "Weak", Neg: "Negative" }[opt]] || "#6b7280", borderColor: "transparent", color: "#000", fontWeight: 700 }
              : {}}
            onClick={() => onVrpFilter(opt)}
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
            className={`dte-filter-btn${strategyFilter === opt ? " active" : ""}`}
            onClick={() => onStrategyFilter(opt)}
          >
            {opt === "all" ? "ALL" : opt}
          </button>
        ))}
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
        <span className="dte-filter-count">{sorted.length} rows · {new Set(sorted.map(r => r.ticker)).size} tickers</span>
      </div>

      {/* ── Table ── */}
      <ScrollArrows>
        {sorted.length === 0 ? (
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
              {sorted.map((row, idx) => {
                const opacity = rowOpacity(row.risk_grade);
                const key = `${row.ticker}-${row.best_expiry || row.best_put_expiry || idx}`;
                return (
                  <tr
                    key={key}
                    className="prem-scanner-row rq-row"
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
                      {row._dte != null ? `${row._dte}d` : "—"}
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
