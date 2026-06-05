import React, { useEffect, useRef, useState } from "react";
import { api } from "../api/client.js";
import EarningsTile from "./EarningsTile.jsx";
import ResearchButton from "./ResearchButton.jsx";
import { formatFailKey } from "./RiskQualityScanner.jsx";
import ProbabilityPanel from "./ProbabilityPanel.jsx";

function fmt(v, digits = 2) {
  if (v == null) return "—";
  return Number(v).toFixed(digits);
}

function fmtExpiry(exp) {
  if (!exp) return "—";
  const [y, m, d] = exp.split("-").map(Number);
  return new Date(y, m - 1, d).toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

function fmtDollar(v, digits = 2) {
  if (v == null) return "—";
  return `$${Number(v).toFixed(digits)}`;
}

function fmtSignedPct(v, digits = 1) {
  if (v == null) return "—";
  const sign = v >= 0 ? "+" : "";
  return `${sign}${Number(v).toFixed(digits)}%`;
}

function fmtYield(v) {
  if (v == null) return "—";
  return `${Number(v).toFixed(2)}%`;
}

const REGIME_CLASS = {
  UPTREND: "regime-up",
  DOWNTREND: "regime-dn",
  TRANSITIONAL: "regime-mid",
};

function LevelInput({ label, value, onChange, colorClass }) {
  return (
    <div className="level-row">
      <span className={`level-label ${colorClass}`}>{label}</span>
      <span className="level-prefix">$</span>
      <input
        className="level-input"
        type="number"
        step="0.01"
        value={value ?? ""}
        onChange={(e) => onChange(e.target.value ? Number(e.target.value) : null)}
        placeholder="—"
      />
    </div>
  );
}

function StrengthBar({ strength }) {
  if (strength == null) return null;
  // Typical range 1–30; cap at 30 for display
  const pct = Math.min(100, (strength / 30) * 100);
  return (
    <div className="strength-bar-wrap">
      <div className="strength-bar" style={{ width: `${pct}%` }} />
      <span className="strength-val">{strength.toFixed(1)}</span>
    </div>
  );
}

function WheelLeg({ label, suggestion, effectiveBasis, profitIfCalled }) {
  if (!suggestion) return (
    <div className="wheel-leg">
      <div className="wheel-leg-title">{label}</div>
      <div className="wheel-leg-empty">No chain data available</div>
    </div>
  );
  return (
    <div className="wheel-leg">
      <div className="wheel-leg-title">{label}</div>
      <div className="wheel-leg-row">
        <span className="wheel-key">Strike</span>
        <span className="wheel-val">{fmtDollar(suggestion.strike)}</span>
      </div>
      <div className="wheel-leg-row">
        <span className="wheel-key">Premium (mid)</span>
        <span className="wheel-val">{suggestion.premium != null ? fmtDollar(suggestion.premium) : "—"}</span>
      </div>
      {suggestion.bid != null && suggestion.ask != null && (
        <div className="wheel-leg-row">
          <span className="wheel-key">Bid / Ask</span>
          <span className="wheel-val">{fmtDollar(suggestion.bid)} / {fmtDollar(suggestion.ask)}</span>
        </div>
      )}
      {effectiveBasis != null && (
        <div className="wheel-leg-row">
          <span className="wheel-key">Effective basis</span>
          <span className="wheel-val hi">{fmtDollar(effectiveBasis)}</span>
        </div>
      )}
      {profitIfCalled != null && (
        <div className="wheel-leg-row">
          <span className="wheel-key">Profit if called</span>
          <span className="wheel-val hi">${fmt(profitIfCalled, 0)}</span>
        </div>
      )}
    </div>
  );
}

const OTM_LABELS = ["1 OTM", "2 OTM", "3 OTM", "4 OTM"];

// ── Risk Quality section helpers ─────────────────────────────────────────────

const GRADE_COLORS_MODAL = { A: "#3fc27f", B: "#f0d000", C: "#f0a030", F: "#e0525e" };
const VRP_COLORS_MODAL   = { Rich: "#4ade80", Moderate: "#facc15", Weak: "#fb923c", Negative: "#f87171" };

function RqGradePill({ grade }) {
  if (!grade) return <span className="rq-grade-null">—</span>;
  return (
    <span className="rq-grade-pill" style={{ background: GRADE_COLORS_MODAL[grade] }}>
      {grade}
    </span>
  );
}

function RqVrpPill({ state, spread }) {
  if (!state) return <span className="rq-grade-null">—</span>;
  const sign = spread != null && spread >= 0 ? "+" : "";
  const spreadStr = spread != null ? ` (${sign}${spread.toFixed(1)}pp)` : "";
  return (
    <span className="rq-vrp-badge" style={{ background: VRP_COLORS_MODAL[state] ?? "#6b7280" }}>
      {state}{spreadStr}
    </span>
  );
}

function RqStrategyPill({ strategyType, secondaryEdge }) {
  if (!strategyType) return <span className="rq-grade-null">—</span>;
  const hasSecTech = (secondaryEdge || []).includes("TechnicalLocation");
  if (strategyType === "Event Ramp")
    return <span className="rq-strategy-tag rq-strategy-event">{hasSecTech ? "Event + Tech" : "Event Ramp"}</span>;
  if (strategyType === "Technical Location")
    return <span className="rq-strategy-tag rq-strategy-tech">Technical Location</span>;
  return <span className="rq-strategy-tag rq-strategy-income">Income Grind</span>;
}

function RqFailList({ items, className, emptyText }) {
  if (!items || items.length === 0) return <span className="rq-grade-null">{emptyText ?? "None"}</span>;
  return (
    <ul className="rq-modal-fail-list">
      {items.map((k) => (
        <li key={k} className={className}>{formatFailKey(k)}</li>
      ))}
    </ul>
  );
}

function RiskQualitySection({ row }) {
  const hasData = !!row.risk_grade;

  return (
    <div className="rq-modal-section">
      <div className="modal-section-title">Risk Quality</div>

      {!hasData ? (
        <div className="rq-modal-nodata">Insufficient data — run a scan to populate Risk Quality scores.</div>
      ) : (
        <>
          {/* Summary row: Grade | VRP | Strategy | Realized Vol */}
          <div className="rq-modal-summary-row">
            <div className="rq-modal-kv">
              <span className="modal-key">Grade</span>
              <span className="modal-val"><RqGradePill grade={row.risk_grade} /></span>
            </div>
            <div className="rq-modal-kv">
              <span className="modal-key">VRP State</span>
              <span className="modal-val">
                <RqVrpPill state={row.vrp_state} spread={row.vrp_spread} />
                {row.high_beta_moderate && (
                  <div style={{ fontSize: "11px", fontStyle: "italic", color: "#4ade80", marginTop: "3px" }}>
                    High-beta Moderate — this spread may be more durable than Rich on a low-vol name
                  </div>
                )}
              </span>
            </div>
            <div className="rq-modal-kv">
              <span className="modal-key">Strategy</span>
              <span className="modal-val">
                <RqStrategyPill strategyType={row.strategy_type} secondaryEdge={row.secondary_edge} />
              </span>
            </div>
            <div className="rq-modal-kv">
              <span className="modal-key">Realized Vol</span>
              <span className="modal-val">
                {row.realized_vol_20d != null ? `${row.realized_vol_20d.toFixed(1)}%` : "—"}
                <span style={{ color: "var(--text-muted)", fontSize: "10px", marginLeft: "5px" }}>20d</span>
                {row.realized_vol_60d != null && (
                  <span style={{ marginLeft: "8px" }}>
                    {row.realized_vol_60d.toFixed(1)}%
                    <span style={{ color: "var(--text-muted)", fontSize: "10px", marginLeft: "3px" }}>60d</span>
                  </span>
                )}
              </span>
            </div>
          </div>

          {/* Factor Assessment */}
          <div className="rq-modal-factors">
            <div className="rq-modal-factor-col">
              <div className="rq-modal-factor-label rq-fail-hard-label">Hard Fails</div>
              <RqFailList items={row.hard_fail_reasons} className="rq-fail-hard" emptyText="None" />
            </div>
            <div className="rq-modal-factor-col">
              <div className="rq-modal-factor-label rq-fail-strong-label">Strong Fails</div>
              <RqFailList items={row.strong_fail_reasons} className="rq-fail-strong" emptyText="None" />
            </div>
            <div className="rq-modal-factor-col">
              <div className="rq-modal-factor-label rq-fail-soft-label">
                Soft Fails {row.soft_fail_count != null ? `(${row.soft_fail_count})` : ""}
              </div>
              <RqFailList items={row.soft_fail_details} className="rq-fail-soft" emptyText="None" />
            </div>
          </div>
        </>
      )}
    </div>
  );
}

function ChainsTable({ expirations, price }) {
  if (!expirations || expirations.length === 0) return null;

  const fmtP = (v) => (v == null ? "—" : `$${Number(v).toFixed(2)}`);
  const fmtExp = (exp) => {
    if (!exp) return "—";
    const [y, m, d] = exp.split("-").map(Number);
    return new Date(y, m - 1, d).toLocaleDateString("en-US", { month: "short", day: "numeric" });
  };
  const fmtProfit = (strike, prem) => {
    if (strike == null || prem == null || !price) return null;
    const profit = (strike - price) * 100 + prem * 100;
    const pct = (profit / (price * 100)) * 100;
    return `$${Math.round(profit).toLocaleString()} (${pct.toFixed(1)}%)`;
  };

  const firstValid = expirations.find(e => !e.error && e.calls?.length > 0);
  const callStrikes = firstValid?.calls?.map(c => c.strike) ?? [];
  const putStrikes = firstValid?.puts?.map(c => c.strike) ?? [];

  const tableSection = (title, dataKey, strikesArr, atmPremKey, showProfit) => (
    <div className="prem-exp-section" style={{ marginBottom: 12 }}>
      <div className="prem-exp-title">{title}</div>
      <table className="prem-exp-table">
        <thead>
          <tr>
            <th>Exp</th>
            <th>DTE</th>
            <th className="prem-exp-atm-col">
              <div>ATM</div>
              {expirations[0]?.atm_strike != null && (
                <div className="prem-exp-strike">${Number(expirations[0].atm_strike).toFixed(0)}</div>
              )}
            </th>
            {OTM_LABELS.map((l, i) => (
              <th key={i}>
                <div>{l}</div>
                {strikesArr[i] != null && <div className="prem-exp-strike">${Number(strikesArr[i]).toFixed(0)}</div>}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {expirations.map(e => (
            <tr key={e.expiry}>
              {e.error ? (
                <td colSpan={7} className="prem-exp-empty">{fmtExp(e.expiry)} ({e.dte}d) — {e.error}</td>
              ) : (
                <>
                  <td>{fmtExp(e.expiry)}</td>
                  <td className="prem-exp-dte">{e.dte}d</td>
                  <td className={`prem-exp-atm-col${e[atmPremKey] ? "" : " prem-exp-empty"}`}>
                    <div>{fmtP(e[atmPremKey])}</div>
                    {showProfit && fmtProfit(e.atm_strike, e[atmPremKey]) && (
                      <div className="prem-exp-profit">{fmtProfit(e.atm_strike, e[atmPremKey])}</div>
                    )}
                  </td>
                  {Array.from({ length: 4 }, (_, i) => {
                    const s = e[dataKey]?.[i];
                    const profitStr = showProfit ? fmtProfit(s?.strike, s?.prem) : null;
                    return (
                      <td key={i} className={s?.prem ? "" : "prem-exp-empty"}>
                        <div>{fmtP(s?.prem)}</div>
                        {profitStr && <div className="prem-exp-profit">{profitStr}</div>}
                      </td>
                    );
                  })}
                </>
              )}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );

  return (
    <div>
      {tableSection("Covered Calls ▲", "calls", callStrikes, "atm_call_prem", true)}
      {tableSection("Cash-Secured Puts ▼", "puts", putStrikes, "atm_put_prem", false)}
    </div>
  );
}

export default function TickerModal({ row, onClose, onResearch }) {
  const [wheel, setWheel] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [chains, setChains] = useState(null);
  const [chainsLoading, setChainsLoading] = useState(true);
  const [chainsError, setChainsError] = useState(null);

  // Live Chain state
  const [chainLoading, setChainLoading] = useState(false);
  const [chainTickers, setChainTickers] = useState([]);
  const [chainSelected, setChainSelected] = useState(null);
  const [chainData, setChainData] = useState(null);
  const [chainError, setChainError] = useState(null);
  const [showChain, setShowChain] = useState(false);

  // Editable S/R levels — seeded from wheel response on first load so display
  // and wheel suggestions always reference the same data source.
  const [overrides, setOverrides] = useState({
    support_1: null, support_2: null, resistance_1: null, resistance_2: null,
  });
  const initialLoadDone = useRef(false);
  const debounceRef = useRef(null);

  const fetchWheel = (s1, r1) => {
    setLoading(true);
    setError(null);
    api.wheel(row.ticker, s1, r1)
      .then((w) => {
        setWheel(w);
        // On first load, seed overrides from wheel (authoritative DB source).
        // On user-triggered refetches, keep their edits.
        if (!initialLoadDone.current) {
          initialLoadDone.current = true;
          setOverrides({
            support_1:    w.support_1    ?? null,
            support_2:    w.support_2    ?? null,
            resistance_1: w.resistance_1 ?? null,
            resistance_2: w.resistance_2 ?? null,
          });
        }
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  };

  // Initial fetch — pass no overrides so backend uses its own DB S/R values
  useEffect(() => {
    fetchWheel(null, null);
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Debounced refetch when overrides change
  const handleOverride = (key, val) => {
    const next = { ...overrides, [key]: val };
    setOverrides(next);
    clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => {
      fetchWheel(next.support_1, next.resistance_1);
    }, 600);
  };

  // Fetch live multi-expiry chain data
  useEffect(() => {
    setChainsLoading(true);
    api.chains(row.ticker)
      .then((data) => {
        setChains(data.expirations || []);
        setChainsLoading(false);
      })
      .catch((e) => {
        setChainsError(e.message);
        setChainsLoading(false);
      });
  }, [row.ticker]);

  // Live Chain handlers
  const handleLiveChain = () => {
    setShowChain(true);
    setChainLoading(true);
    setChainError(null);
    setChainData(null);
    api.chainTickers()
      .then(async (data) => {
        const tickers = data.tickers || [];
        setChainTickers(tickers);
        if (data.error || tickers.length === 0) {
          setChainError("no_tickers");
          return;
        }
        const match = tickers.find(t => t.ticker === row.ticker);
        if (match) {
          setChainSelected(match);
          const histData = await api.chainHistory(match.ticker);
          const analyses = histData.analyses || [];
          const analysis = analyses.find(a => a.expiry === match.expiry) || analyses[0] || null;
          setChainData(analysis);
        }
      })
      .catch(() => setChainError("fetch_failed"))
      .finally(() => setChainLoading(false));
  };

  const handleChainSelect = (entry) => {
    setChainSelected(entry);
    setChainData(null);
    setChainLoading(true);
    api.chainHistory(entry.ticker)
      .then((histData) => {
        const analyses = histData.analyses || [];
        const analysis = analyses.find(a => a.expiry === entry.expiry) || analyses[0] || null;
        setChainData(analysis);
      })
      .catch(() => setChainError("fetch_failed"))
      .finally(() => setChainLoading(false));
  };

  // Close on Escape
  useEffect(() => {
    const handler = (e) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [onClose]);

  const price = row.price;
  const distPct = (level) =>
    price && level ? (((level - price) / price) * 100).toFixed(1) + "%" : null;

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-panel" onClick={(e) => e.stopPropagation()}>
        {/* ── Header ── */}
        <div className="modal-header">
          <div style={{ flex: 1, minWidth: 0 }}>
            <div style={{ display: "flex", alignItems: "center", gap: "10px", flexWrap: "wrap" }}>
              <div className="modal-ticker">
                {row.ticker}
                <EarningsTile days={row.earnings_days} inWindow={row.earnings_in_window} />
              </div>
              {/* AI Universe metadata pills — only shown when ticker is in universe */}
              {(row.primary_lens || (row.lenses && row.lenses.length > 0) || row.category) && (
                <div style={{ display: "flex", flexWrap: "wrap", gap: "5px", alignItems: "center" }}>
                  {/* Primary lens — yellow for defense, purple otherwise */}
                  {row.primary_lens && (
                    <span style={{
                      background: row.is_defense ? "#fbbf24" : "#8b5cf6",
                      color: row.is_defense ? "#000" : "#fff",
                      fontWeight: 700,
                      fontSize: "12px",
                      padding: "3px 9px",
                      borderRadius: "4px",
                      cursor: "default",
                      whiteSpace: "nowrap",
                    }}>
                      {row.primary_lens}
                    </span>
                  )}
                  {/* Secondary lenses — faded purple */}
                  {(row.lenses || [])
                    .filter(l => l !== row.primary_lens)
                    .map(l => (
                      <span key={l} style={{
                        background: "rgba(139,92,246,0.3)",
                        color: "rgba(255,255,255,0.75)",
                        fontWeight: 400,
                        fontSize: "12px",
                        padding: "3px 9px",
                        borderRadius: "4px",
                        cursor: "default",
                        whiteSpace: "nowrap",
                      }}>
                        {l}
                      </span>
                    ))}
                  {/* Category — yellow */}
                  {row.category && (
                    <span style={{
                      background: "#fbbf24",
                      color: "#111",
                      fontSize: "11px",
                      fontWeight: 600,
                      padding: "3px 8px",
                      borderRadius: "4px",
                      cursor: "default",
                      whiteSpace: "nowrap",
                    }}>
                      {row.category}
                    </span>
                  )}
                  {/* Subcategory — teal */}
                  {row.subcategory && (
                    <span style={{
                      background: "#0891b2",
                      color: "#fff",
                      fontSize: "10px",
                      padding: "2px 7px",
                      borderRadius: "4px",
                      cursor: "default",
                      whiteSpace: "nowrap",
                    }}>
                      {row.subcategory}
                    </span>
                  )}
                </div>
              )}
            </div>
            <div className="modal-sub">
              {fmtDollar(price)} · Score {fmt(row.score, 0)} · {row.bucket?.replace(/_/g, " ")}
              {row.sma_regime && (
                <span className={`regime-tag ${REGIME_CLASS[row.sma_regime] ?? ""}`} style={{ marginLeft: 8 }}>
                  {row.sma_regime}
                </span>
              )}
            </div>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: "10px", flexShrink: 0 }}>
            <ResearchButton ticker={row.ticker} hasSitrep={row.has_sitrep} onResearch={onResearch} />
            <button
              onClick={handleLiveChain}
              style={{
                background: showChain ? "#1e3a2a" : "#1a2a1a",
                border: `1px solid ${showChain ? "#4ade80" : "#2d5a2d"}`,
                color: "#4ade80",
                fontSize: "12px",
                fontWeight: 600,
                padding: "4px 12px",
                borderRadius: "4px",
                cursor: "pointer",
                whiteSpace: "nowrap",
              }}
            >
              Live Chain
            </button>
            <button className="modal-close" onClick={onClose}>✕</button>
          </div>
        </div>

        <div className="modal-body">
          {/* ── Left column ── */}
          <div className="modal-col">
            {/* SMA */}
            <div className="modal-section">
              <div className="modal-section-title">Moving Averages</div>
              <div className="modal-kv-grid">
                <span className="modal-key">200-day SMA</span>
                <span className="modal-val">
                  {fmtDollar(row.sma_200)}
                  {row.price_vs_sma200_pct != null && (
                    <span className={row.price_vs_sma200_pct >= 0 ? "hi" : "lo"}>
                      {" "}{fmtSignedPct(row.price_vs_sma200_pct)} {row.price_vs_sma200_pct >= 0 ? "▲" : "▼"}
                    </span>
                  )}
                </span>
                <span className="modal-key">50-day SMA</span>
                <span className="modal-val">
                  {fmtDollar(row.sma_50)}
                  {row.price_vs_sma50_pct != null && (
                    <span className={row.price_vs_sma50_pct >= 0 ? "hi" : "lo"}>
                      {" "}{fmtSignedPct(row.price_vs_sma50_pct)} {row.price_vs_sma50_pct >= 0 ? "▲" : "▼"}
                    </span>
                  )}
                </span>
                <span className="modal-key">Cross</span>
                <span className="modal-val">
                  {row.sma_golden_cross === true && <span className="hi">Golden Cross ▲</span>}
                  {row.sma_golden_cross === false && <span className="lo">Death Cross ▼</span>}
                  {row.sma_golden_cross == null && "—"}
                </span>
              </div>
            </div>

            {/* Editable S/R */}
            <div className="modal-section">
              <div className="modal-section-title">Support / Resistance <span className="modal-hint">(edit to override)</span></div>

              <div className="sr-level-group">
                <div className="sr-level-header resist">Resistance</div>
                <LevelInput label="R2" value={overrides.resistance_2} onChange={(v) => handleOverride("resistance_2", v)} colorClass="resist" />
                {wheel?.resistance_2_strength && <StrengthBar strength={wheel.resistance_2_strength} />}
                {distPct(overrides.resistance_2) && <div className="level-dist">{distPct(overrides.resistance_2)} from price</div>}

                <LevelInput label="R1" value={overrides.resistance_1} onChange={(v) => handleOverride("resistance_1", v)} colorClass="resist" />
                {wheel?.resistance_1_strength && <StrengthBar strength={wheel.resistance_1_strength} />}
                {distPct(overrides.resistance_1) && <div className="level-dist">{distPct(overrides.resistance_1)} from price</div>}
              </div>

              {/* Price range indicator */}
              {price && (overrides.support_1 || overrides.resistance_1) && (
                <div className="price-range-bar">
                  <span className="prb-label support">S1</span>
                  <div className="prb-track">
                    {(() => {
                      const lo = overrides.support_1 ?? price * 0.9;
                      const hi = overrides.resistance_1 ?? price * 1.1;
                      const range = hi - lo;
                      if (range <= 0) return null;
                      const pct = ((price - lo) / range * 100).toFixed(1);
                      return (
                        <>
                          <div className="prb-fill support" style={{ width: `${pct}%` }} />
                          <div className="prb-marker" style={{ left: `${pct}%` }} title={`$${price.toFixed(2)}`} />
                          <div className="prb-fill resist" style={{ width: `${100 - pct}%` }} />
                        </>
                      );
                    })()}
                  </div>
                  <span className="prb-label resist">R1</span>
                </div>
              )}

              <div className="sr-level-group" style={{ marginTop: 8 }}>
                <div className="sr-level-header support">Support</div>
                <LevelInput label="S1" value={overrides.support_1} onChange={(v) => handleOverride("support_1", v)} colorClass="support" />
                {wheel?.support_1_strength && <StrengthBar strength={wheel.support_1_strength} />}
                {distPct(overrides.support_1) && <div className="level-dist">{distPct(overrides.support_1)} from price</div>}

                <LevelInput label="S2" value={overrides.support_2} onChange={(v) => handleOverride("support_2", v)} colorClass="support" />
                {wheel?.support_2_strength && <StrengthBar strength={wheel.support_2_strength} />}
                {distPct(overrides.support_2) && <div className="level-dist">{distPct(overrides.support_2)} from price</div>}
              </div>
            </div>
          </div>

          {/* ── Right column: Wheel Math ── */}
          <div className="modal-col">
            <div className="modal-section">
              <div className="modal-section-title">
                Wheel Suggestions
                {wheel?.expiration && <span className="modal-hint"> · exp {wheel.expiration}</span>}
                {loading && <span className="modal-hint"> · loading…</span>}
              </div>

              {error && <div className="modal-error">{error}</div>}

              {wheel && (
                <>
                  <div className="wheel-legs">
                    <WheelLeg
                      label="CSP — Cash-Secured Put"
                      suggestion={wheel.csp}
                      effectiveBasis={wheel.csp_effective_basis}
                    />
                    <WheelLeg
                      label="CC — Covered Call"
                      suggestion={wheel.cc}
                      profitIfCalled={wheel.cc_profit_if_called}
                    />
                  </div>

                  {wheel.combined_premium_per_share != null && (
                    <div className="wheel-summary">
                      <div className="modal-section-title" style={{ marginBottom: 8 }}>Combined Cycle</div>
                      <div className="modal-kv-grid">
                        <span className="modal-key">Combined premium</span>
                        <span className="modal-val hi">{fmtDollar(wheel.combined_premium_per_share)}/sh · ${fmt(wheel.combined_premium_per_share * 100, 0)}/contract</span>
                        <span className="modal-key">Capital required</span>
                        <span className="modal-val">${fmt(wheel.capital_required, 0)}</span>
                        <span className="modal-key">Monthly yield</span>
                        <span className="modal-val hi">{fmtYield(wheel.monthly_yield_pct)}</span>
                        <span className="modal-key">Annualized</span>
                        <span className="modal-val hi">{fmtYield(wheel.annualized_yield_pct)}</span>
                      </div>
                    </div>
                  )}

                  {wheel.combined_premium_per_share == null && !loading && (
                    <div className="wheel-nodata">
                      Chain data unavailable or no strikes near S/R levels.
                      {!overrides.support_1 && " Set a support level to get CSP suggestions."}
                    </div>
                  )}
                </>
              )}
            </div>
          </div>
        </div>

        {/* ── Risk Quality section (full width) ── */}
        <RiskQualitySection row={row} />

        {/* ── CSP Probability (full width) ── */}
        <div className="expiry-section">
          <div className="modal-section-title">CSP Probability</div>
          <ProbabilityPanel ticker={row.ticker} embedded={true} />
        </div>

        {/* ── Live Chain section (full width) ── */}
        {showChain && (
          <div style={{ borderTop: "1px solid #2a2a3a", padding: "16px 20px" }}>
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 12 }}>
              <div className="modal-section-title" style={{ margin: 0 }}>
                Live Chain
                {chainLoading && <span className="modal-hint"> · loading…</span>}
              </div>
              <button
                onClick={() => { setShowChain(false); setChainData(null); setChainSelected(null); }}
                style={{ background: "none", border: "none", color: "#888", cursor: "pointer", fontSize: 13, padding: "2px 6px" }}
              >
                ✕ Close
              </button>
            </div>

            {/* Error states */}
            {chainError === "no_tickers" && (
              <div style={{ fontSize: 13, color: "#f87171" }}>
                No chain data available. Upload screenshots at:{" "}
                <a href="https://web-production-3ee9e.up.railway.app" target="_blank" rel="noreferrer" style={{ color: "#818cf8" }}>
                  web-production-3ee9e.up.railway.app
                </a>
              </div>
            )}
            {chainError === "fetch_failed" && (
              <div style={{ fontSize: 13, color: "#f87171" }}>
                Failed to reach chain analyzer.{" "}
                <a href="https://web-production-3ee9e.up.railway.app" target="_blank" rel="noreferrer" style={{ color: "#818cf8" }}>
                  Open directly
                </a>
              </div>
            )}

            {/* Ticker dropdown */}
            {!chainError && !chainLoading && chainTickers.length > 0 && (
              <div style={{ marginBottom: 14 }}>
                <select
                  value={chainSelected?.label ?? ""}
                  onChange={(e) => {
                    const entry = chainTickers.find(t => t.label === e.target.value);
                    if (entry) handleChainSelect(entry);
                  }}
                  style={{
                    background: "#1a1a2e", color: "#e2e8f0", border: "1px solid #3a3a5a",
                    borderRadius: 4, padding: "4px 10px", fontSize: 13, cursor: "pointer",
                  }}
                >
                  <option value="">-- select --</option>
                  {chainTickers.map(t => (
                    <option key={t.label} value={t.label}>{t.label}</option>
                  ))}
                </select>
              </div>
            )}

            {/* Chain data tables */}
            {chainData && !chainLoading && (() => {
              const shares   = chainData.shares ?? 100;
              const basis    = chainData.basis ?? 0;
              const realized = chainData.realized ?? 0;
              const fmtD  = (v) => v == null ? "—" : `$${Number(v).toFixed(2)}`;
              const fmtDol = (v) => v == null ? "—" : `$${Math.round(v).toLocaleString()}`;
              const dolCol = (v) => ({ color: v == null ? "#94a3b8" : v >= 0 ? "#4ade80" : "#f87171", fontWeight: 700 });

              const ccRows = (chainData.cc_strikes || []).map(s => {
                const premD  = s.mid != null ? s.mid * shares : null;
                const stGain = s.strike != null ? (s.strike - basis) * shares : null;
                const total  = premD != null && stGain != null ? premD + stGain + realized : null;
                return { ...s, premD, stGain, total };
              }).sort((a, b) => (b.total ?? -Infinity) - (a.total ?? -Infinity));

              const cspRows = (chainData.csp_strikes || []).map(s => {
                const premD   = s.mid != null ? s.mid * shares : null;
                const beven   = s.mid != null && s.strike != null ? s.strike - s.mid : null;
                const cashReq = s.strike != null ? s.strike * shares : null;
                return { ...s, premD, beven, cashReq };
              }).sort((a, b) => (b.premD ?? -Infinity) - (a.premD ?? -Infinity));

              const TH = ({ children, left }) => (
                <th style={{ textAlign: left ? "left" : "right", padding: "4px 8px", fontSize: 11,
                  color: "#94a3b8", fontWeight: 600, borderBottom: "1px solid #2a2a3a", whiteSpace: "nowrap" }}>
                  {children}
                </th>
              );
              const TD = ({ children, style: s }) => (
                <td style={{ textAlign: "right", padding: "4px 8px", fontSize: 12, color: "#e2e8f0", ...s }}>{children}</td>
              );
              const TDL = ({ children }) => (
                <td style={{ textAlign: "left", padding: "4px 8px", fontSize: 12, fontWeight: 700, color: "#e2e8f0" }}>{children}</td>
              );
              const rowBorder = { borderBottom: "1px solid #1e1e2e" };

              return (
                <div>
                  {/* Info header */}
                  <div style={{ fontSize: 12, color: "#94a3b8", marginBottom: 14 }}>
                    <strong style={{ color: "#e2e8f0", fontSize: 13 }}>{chainData.ticker} · {chainData.expiry}</strong>
                    {"  "}basis: <strong style={{ color: "#e2e8f0" }}>${Number(basis).toFixed(2)}</strong>
                    {"  "}shares: <strong style={{ color: "#e2e8f0" }}>{shares}</strong>
                    {"  "}prior realized: <strong style={dolCol(realized)}>${Math.round(realized).toLocaleString()}</strong>
                    {chainData.analyzed_at && <>{"  "}updated: <span>{chainData.analyzed_at}</span></>}
                  </div>

                  {/* CC table */}
                  {ccRows.length > 0 && (
                    <div style={{ marginBottom: 20 }}>
                      <div style={{ fontSize: 13, fontWeight: 700, color: "#4ade80", marginBottom: 6 }}>Covered Call Strikes</div>
                      <table style={{ width: "100%", borderCollapse: "collapse" }}>
                        <thead>
                          <tr>
                            <TH left>STRIKE</TH>
                            <TH>BID</TH><TH>ASK</TH><TH>MID</TH>
                            <TH>PREMIUM $</TH><TH>STOCK GAIN</TH><TH>TOTAL IF CALLED</TH>
                          </tr>
                        </thead>
                        <tbody>
                          {ccRows.map((s, i) => (
                            <tr key={i} style={rowBorder}>
                              <TDL>${s.strike != null ? Number(s.strike).toFixed(2) : "—"}</TDL>
                              <TD>{fmtD(s.bid)}</TD>
                              <TD>{fmtD(s.ask)}</TD>
                              <TD>{fmtD(s.mid)}</TD>
                              <TD style={dolCol(s.premD)}>{fmtDol(s.premD)}</TD>
                              <TD style={dolCol(s.stGain)}>{fmtDol(s.stGain)}</TD>
                              <TD style={{ ...dolCol(s.total), fontSize: 13 }}>{fmtDol(s.total)}</TD>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}

                  {/* CSP table */}
                  {cspRows.length > 0 && (
                    <div>
                      <div style={{ fontSize: 13, fontWeight: 700, color: "#60a5fa", marginBottom: 6 }}>Cash Secured Put Strikes</div>
                      <table style={{ width: "100%", borderCollapse: "collapse" }}>
                        <thead>
                          <tr>
                            <TH left>STRIKE</TH>
                            <TH>BID</TH><TH>ASK</TH><TH>MID</TH>
                            <TH>PREMIUM $</TH><TH>BREAKEVEN</TH><TH>CASH REQUIRED</TH>
                          </tr>
                        </thead>
                        <tbody>
                          {cspRows.map((s, i) => (
                            <tr key={i} style={rowBorder}>
                              <TDL>${s.strike != null ? Number(s.strike).toFixed(2) : "—"}</TDL>
                              <TD>{fmtD(s.bid)}</TD>
                              <TD>{fmtD(s.ask)}</TD>
                              <TD>{fmtD(s.mid)}</TD>
                              <TD style={dolCol(s.premD)}>{fmtDol(s.premD)}</TD>
                              <TD>{s.beven != null ? `$${Number(s.beven).toFixed(2)}` : "—"}</TD>
                              <TD>{fmtDol(s.cashReq)}</TD>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}

                  {ccRows.length === 0 && cspRows.length === 0 && (
                    <div style={{ color: "#94a3b8", fontSize: 13 }}>No strike data for this expiry.</div>
                  )}
                </div>
              );
            })()}
          </div>
        )}

        {/* ── Live multi-expiry table (full width, fetched on demand) ── */}
        <div className="expiry-section">
          <div className="modal-section-title">
            Options Premiums by Expiry
            {chainsLoading && <span className="modal-hint"> · loading…</span>}
          </div>
          {chainsError && <div className="modal-error">{chainsError}</div>}
          {!chainsLoading && !chainsError && (!chains || chains.length === 0) && (
            <div className="modal-hint" style={{ padding: "8px 0" }}>No expirations in 7-45d window</div>
          )}
          {chainsLoading && (
            <div className="chains-spinner">
              <span className="spinner-dot" /><span className="spinner-dot" /><span className="spinner-dot" />
            </div>
          )}
          {chains && chains.length > 0 && (
            <ChainsTable expirations={chains} price={row.price} />
          )}
        </div>
      </div>
    </div>
  );
}
