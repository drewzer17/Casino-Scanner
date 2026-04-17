import React, { useEffect, useRef, useState } from "react";
import { api } from "../api/client.js";

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

function ChainsTable({ expirations }) {
  if (!expirations || expirations.length === 0) return null;

  const fmtP = (v) => (v == null ? "—" : `$${Number(v).toFixed(2)}`);
  const fmtExp = (exp) => {
    if (!exp) return "—";
    const [y, m, d] = exp.split("-").map(Number);
    return new Date(y, m - 1, d).toLocaleDateString("en-US", { month: "short", day: "numeric" });
  };

  const firstValid = expirations.find(e => !e.error && e.calls?.length > 0);
  const callStrikes = firstValid?.calls?.map(c => c.strike) ?? [];
  const putStrikes = firstValid?.puts?.map(c => c.strike) ?? [];

  const tableSection = (title, dataKey, strikesArr, atmPremKey) => (
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
                    {fmtP(e[atmPremKey])}
                  </td>
                  {Array.from({ length: 4 }, (_, i) => {
                    const s = e[dataKey]?.[i];
                    return (
                      <td key={i} className={s?.prem ? "" : "prem-exp-empty"}>
                        {fmtP(s?.prem)}
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
      {tableSection("Covered Calls ▲", "calls", callStrikes, "atm_call_prem")}
      {tableSection("Cash-Secured Puts ▼", "puts", putStrikes, "atm_put_prem")}
    </div>
  );
}

export default function TickerModal({ row, onClose }) {
  const [wheel, setWheel] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [chains, setChains] = useState(null);
  const [chainsLoading, setChainsLoading] = useState(true);
  const [chainsError, setChainsError] = useState(null);

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
          <div>
            <div className="modal-ticker">{row.ticker}</div>
            <div className="modal-sub">
              {fmtDollar(price)} · Score {fmt(row.score, 0)} · {row.bucket?.replace(/_/g, " ")}
              {row.sma_regime && (
                <span className={`regime-tag ${REGIME_CLASS[row.sma_regime] ?? ""}`} style={{ marginLeft: 8 }}>
                  {row.sma_regime}
                </span>
              )}
            </div>
          </div>
          <button className="modal-close" onClick={onClose}>✕</button>
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
            <ChainsTable expirations={chains} />
          )}
        </div>
      </div>
    </div>
  );
}
