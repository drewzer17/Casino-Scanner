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

export default function TickerModal({ row, onClose }) {
  const [wheel, setWheel] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // Editable level overrides (null = use DB value)
  const [overrides, setOverrides] = useState({
    support_1: row.support_1,
    support_2: row.support_2,
    resistance_1: row.resistance_1,
    resistance_2: row.resistance_2,
  });
  const debounceRef = useRef(null);

  const fetchWheel = (s1, r1) => {
    setLoading(true);
    setError(null);
    api.wheel(row.ticker, s1, r1)
      .then(setWheel)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  };

  // Initial fetch
  useEffect(() => {
    fetchWheel(overrides.support_1, overrides.resistance_1);
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
                {row.resistance_2_strength && <StrengthBar strength={row.resistance_2_strength} />}
                {distPct(overrides.resistance_2) && <div className="level-dist">{distPct(overrides.resistance_2)} from price</div>}

                <LevelInput label="R1" value={overrides.resistance_1} onChange={(v) => handleOverride("resistance_1", v)} colorClass="resist" />
                {row.resistance_1_strength && <StrengthBar strength={row.resistance_1_strength} />}
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
                {row.support_1_strength && <StrengthBar strength={row.support_1_strength} />}
                {distPct(overrides.support_1) && <div className="level-dist">{distPct(overrides.support_1)} from price</div>}

                <LevelInput label="S2" value={overrides.support_2} onChange={(v) => handleOverride("support_2", v)} colorClass="support" />
                {row.support_2_strength && <StrengthBar strength={row.support_2_strength} />}
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

        {/* ── Expiry table (full width) ── */}
        {row.expiry_data && row.expiry_data.length > 0 && (
          <div className="expiry-section">
            <div className="modal-section-title">Options Premiums by Expiry</div>
            <table className="expiry-table">
              <thead>
                <tr>
                  <th>Expiry</th>
                  <th>DTE</th>
                  <th>ATM Strike</th>
                  <th>ATM Prem</th>
                  <th>1 OTM</th>
                  <th>2 OTM</th>
                </tr>
              </thead>
              <tbody>
                {row.expiry_data.map((e) => (
                  <tr key={e.expiry} className={e.expiry === row.best_expiry ? "expiry-best" : ""}>
                    <td>{fmtExpiry(e.expiry)}</td>
                    <td>{e.dte}d</td>
                    <td>{e.atm_strike != null ? `$${e.atm_strike.toFixed(0)}` : "—"}</td>
                    <td>{e.atm_prem != null ? `$${(e.atm_prem * 100).toFixed(2)}` : "—"}</td>
                    <td>{e.otm1_prem != null ? `$${(e.otm1_prem * 100).toFixed(2)}` : "—"}</td>
                    <td className={e.expiry === row.best_expiry ? "expiry-best-val" : ""}>
                      {e.otm2_prem != null ? `$${(e.otm2_prem * 100).toFixed(2)}` : "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
