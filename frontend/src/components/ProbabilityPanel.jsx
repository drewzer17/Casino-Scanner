import React, { useState, useEffect } from "react";

// ── Constants ─────────────────────────────────────────────────────────────────

const STRIKE_OPTIONS = [
  { label: "ATM (0%)", value: 0.0 },
  { label: "1% OTM",   value: 0.01 },
  { label: "2% OTM",   value: 0.02 },
  { label: "3% OTM",   value: 0.03 },
];

const HOLD_OPTIONS = [
  { label: "5d",  value: 5  },
  { label: "10d", value: 10 },
  { label: "15d", value: 15 },
  { label: "21d", value: 21 },
];

const GRADE_COLORS = { A: "#3fc27f", B: "#f0d000", C: "#f0a030", F: "#e0525e" };
const REGIME_BG    = { red: "#4a1010", yellow: "#4a3000", green: "#0d2e16" };
const REGIME_BORDER= { red: "#c62828", yellow: "#f57f17", green: "#2e7d32" };
const REGIME_DOT   = { red: "#ef5350", yellow: "#ffa726", green: "#66bb6a" };

// ── Helpers ───────────────────────────────────────────────────────────────────

function assignColor(v) {
  if (v == null) return "#888";
  if (v < 25) return "#4caf50";
  if (v < 40) return "#ffc107";
  return "#f44336";
}

function maeColor(v) {
  if (v == null) return "#888";
  if (v > -5)  return "#4caf50";
  if (v > -10) return "#ffc107";
  return "#f44336";
}

function ConfidenceDot({ confidence }) {
  const colors = { high: "#4caf50", medium: "#ffc107", low: "#ff9800", insufficient: "#666" };
  return (
    <span style={{
      display: "inline-block",
      width: 8, height: 8,
      borderRadius: "50%",
      background: colors[confidence] ?? "#666",
      marginRight: 4,
      verticalAlign: "middle",
    }} title={confidence} />
  );
}

// Horizontal comparison bar
function ComparisonBar({ label, assignedPct, isSelected, n, showKeepPremium = false }) {
  const display = showKeepPremium && assignedPct != null ? 100 - assignedPct : assignedPct;
  const color = showKeepPremium
    ? (display > 75 ? "#4caf50" : display > 60 ? "#ffc107" : "#f44336")
    : assignColor(assignedPct);
  const barWidth = display != null ? Math.min(100, display * 2) : 0;
  return (
    <div style={{
      padding: "4px 6px",
      borderRadius: 4,
      border: isSelected ? "1px solid #4caf50" : "1px solid transparent",
      background: isSelected ? "rgba(76,175,80,0.08)" : "transparent",
      marginBottom: 3,
    }}>
      <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 2 }}>
        <span style={{ fontSize: 11, color: "#bbb", minWidth: 60 }}>{label}</span>
        <div style={{ flex: 1, height: 6, background: "#333", borderRadius: 3, overflow: "hidden" }}>
          <div style={{ width: `${barWidth}%`, height: "100%", background: color, borderRadius: 3, transition: "width 0.3s" }} />
        </div>
        <span style={{ fontSize: 11, color, fontWeight: 700, minWidth: 36, textAlign: "right" }}>
          {display != null ? `${display.toFixed ? display.toFixed(1) : display}%` : "—"}
        </span>
      </div>
      {n != null && (
        <div style={{ fontSize: 10, color: "#666", paddingLeft: 66 }}>n={n}</div>
      )}
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

export default function ProbabilityPanel({ ticker, embedded = false, onClose }) {
  const [strikePct,       setStrikePct]       = useState(0.02);
  const [holdDays,        setHoldDays]        = useState(21);
  const [data,            setData]            = useState(null);
  const [loading,         setLoading]         = useState(true);
  const [error,           setError]           = useState(null);
  const [showKeepPremium, setShowKeepPremium] = useState(false);

  useEffect(() => {
    if (!ticker) return;
    setLoading(true);
    setError(null);
    fetch(
      `/api/probability/${ticker}?strike_pct=${strikePct}&hold_days=${holdDays}`,
      { credentials: "include" }
    )
      .then(r => r.ok ? r.json() : r.json().then(e => Promise.reject(e.detail ?? "Error")))
      .then(d => { setData(d); setLoading(false); })
      .catch(e => { setError(String(e)); setLoading(false); });
  }, [ticker, strikePct, holdDays]);

  // ── Shell wrapper ──────────────────────────────────────────────────────────
  const content = (
    <div style={{ fontFamily: "inherit" }}>
      {/* Selectors */}
      <div style={{ display: "flex", gap: 16, flexWrap: "wrap", marginBottom: 10, alignItems: "flex-end" }}>
        <div>
          <div style={{ fontSize: 11, color: "#888", marginBottom: 4, textTransform: "uppercase", letterSpacing: "0.5px" }}>Strike</div>
          <div style={{ display: "flex", gap: 4 }}>
            {STRIKE_OPTIONS.map(o => {
              const px = data?.current_price;
              const dollarStr = px ? ` ($${Math.round(px * (1 - o.value))})` : "";
              const label = o.value === 0 ? `ATM${dollarStr}` : `${(o.value * 100).toFixed(0)}%${dollarStr}`;
              return (
                <button key={o.value} onClick={() => setStrikePct(o.value)} style={{
                  padding: "4px 10px", fontSize: 12, borderRadius: 4, cursor: "pointer",
                  background: strikePct === o.value ? "#2e7d32" : "#1e1e2e",
                  color: strikePct === o.value ? "#fff" : "#aaa",
                  border: strikePct === o.value ? "1px solid #4caf50" : "1px solid #444",
                }}>{label}</button>
              );
            })}
          </div>
        </div>
        <div>
          <div style={{ fontSize: 11, color: "#888", marginBottom: 4, textTransform: "uppercase", letterSpacing: "0.5px" }}>Hold Window</div>
          <div style={{ display: "flex", gap: 4 }}>
            {HOLD_OPTIONS.map(o => (
              <button key={o.value} onClick={() => setHoldDays(o.value)} style={{
                padding: "4px 10px", fontSize: 12, borderRadius: 4, cursor: "pointer",
                background: holdDays === o.value ? "#1a3a5e" : "#1e1e2e",
                color: holdDays === o.value ? "#fff" : "#aaa",
                border: holdDays === o.value ? "1px solid #4a90d9" : "1px solid #444",
              }}>{o.label}</button>
            ))}
          </div>
        </div>
        <div style={{ marginLeft: "auto" }}>
          <button
            onClick={() => setShowKeepPremium(v => !v)}
            style={{
              padding: "4px 12px", fontSize: 12, borderRadius: 4, cursor: "pointer",
              background: showKeepPremium ? "#1a3a5e" : "transparent",
              color: showKeepPremium ? "#7ec8f7" : "#888",
              border: showKeepPremium ? "1px solid #4a90d9" : "1px solid #555",
            }}
          >
            {showKeepPremium ? "VIEW: Keep Premium %" : "VIEW: Assignment Risk"}
          </button>
        </div>
      </div>

      {loading && (
        <div style={{ color: "#666", fontSize: 13, padding: "20px 0" }}>Loading probabilities…</div>
      )}
      {error && (
        <div style={{ color: "#f44336", fontSize: 13, padding: "8px 0" }}>{error}</div>
      )}

      {!loading && !error && data && (() => {
        const { regime, grade, sector, industry, probabilities: probs,
                hold_window_comparison: holdComp, strike_comparison: strikeComp,
                industry_warning, earnings, current_price: price,
                parabolic } = data;
        const c = regime?.color ?? "yellow";

        return (
          <>
            {/* Regime banner */}
            {regime && (
              <div style={{
                background: REGIME_BG[c],
                border: `1px solid ${REGIME_BORDER[c]}`,
                borderRadius: 6,
                padding: "8px 12px",
                marginBottom: 8,
                display: "flex",
                alignItems: "center",
                gap: 10,
                flexWrap: "wrap",
              }}>
                <span style={{ color: REGIME_DOT[c], fontSize: 10 }}>●</span>
                <span style={{ fontWeight: 700, color: "#fff", fontSize: 13 }}>{regime.name}</span>
                <span style={{ color: "#ccc", fontSize: 12 }}>— {regime.desc}</span>
                <span style={{ color: "#aaa", fontSize: 11, marginLeft: "auto" }}>
                  ⏱ {regime.duration} · 🎯 {regime.strike}
                </span>
                {regime.vix != null && (
                  <span style={{ color: "#777", fontSize: 11 }}>
                    VIX {regime.vix.toFixed(1)} · SPY {regime.spy_above_ema50 ? "↑ above EMA50" : "↓ below EMA50"}
                  </span>
                )}
              </div>
            )}

            {/* Grade / sector bar */}
            <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap", marginBottom: 10, fontSize: 12 }}>
              {grade && (
                <span style={{
                  background: GRADE_COLORS[grade] ?? "#555",
                  color: "#000",
                  fontWeight: 800,
                  fontSize: 13,
                  padding: "2px 10px",
                  borderRadius: 4,
                }}>PS {grade}</span>
              )}
              {sector && <span style={{ color: "#aaa" }}>{sector}</span>}
              {industry && sector !== industry && <span style={{ color: "#777" }}>· {industry}</span>}
              {industry_warning && (
                <span style={{ color: "#f44336", fontSize: 11 }}>⚠ {industry_warning}</span>
              )}
              {earnings?.in_window && (
                <span style={{
                  background: "#7f1313", color: "#fff", fontSize: 11,
                  padding: "2px 8px", borderRadius: 4,
                }}>⚡ Earnings {earnings.days_until}d</span>
              )}
            </div>

            {/* Parabolic banner */}
            {parabolic?.is_parabolic && (
              <div style={{
                background: parabolic.is_extreme ? "rgba(180,0,0,0.18)" : "rgba(180,100,0,0.18)",
                border: `1px solid ${parabolic.is_extreme ? "#c62828" : "#e65100"}`,
                borderRadius: 6,
                padding: "8px 12px",
                marginBottom: 8,
                fontSize: 12,
              }}>
                <div style={{ fontWeight: 700, color: parabolic.is_extreme ? "#ef5350" : "#ff9800", marginBottom: 3 }}>
                  {parabolic.is_extreme ? "⚠" : "⚡"}{" "}
                  {parabolic.is_extreme
                    ? `Extreme Extension (${parabolic.extension_ratio.toFixed(2)}x) — highest risk bucket. 100% touch rate historically.`
                    : `Parabolic (ext ${parabolic.extension_ratio.toFixed(2)}x)`}
                </div>
                {!parabolic.is_extreme && parabolic.all_regimes?.avg_mae != null && parabolic.standard_f_comparison?.avg_mae != null && (
                  <div style={{ color: "#aaa" }}>
                    When wrong, avg dip{" "}
                    <span style={{ color: "#f44336", fontWeight: 700 }}>{parabolic.all_regimes.avg_mae}%</span>
                    {" "}vs{" "}
                    <span style={{ color: "#ffc107" }}>{parabolic.standard_f_comparison.avg_mae}%</span>
                    {" "}standard F-grade
                    {parabolic.all_regimes.n > 0 && <span style={{ color: "#555", marginLeft: 6 }}>n={parabolic.all_regimes.n}</span>}
                  </div>
                )}
              </div>
            )}

            {!probs ? (
              <div style={{ color: "#666", fontSize: 13, padding: "12px 0" }}>
                Insufficient backtest data for this regime + grade combination.
              </div>
            ) : (
              <>
                {/* Three cards */}
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8, marginBottom: 12 }}>

                  {/* Card 1: Assignment Risk / Keep Premium */}
                  <div style={{ background: "#12121e", border: "1px solid #333", borderRadius: 6, padding: "10px 12px" }}>
                    {(() => {
                      const raw = probs.assigned_pct;
                      const display = showKeepPremium && raw != null ? 100 - raw : raw;
                      const color = showKeepPremium
                        ? (display > 75 ? "#4caf50" : display > 60 ? "#ffc107" : "#f44336")
                        : assignColor(raw);
                      return (
                        <>
                          <div style={{ fontSize: 28, fontWeight: 800, color, lineHeight: 1 }}>
                            {display != null ? `${display.toFixed(1)}%` : "—"}
                          </div>
                          <div style={{ fontSize: 10, color: "#888", textTransform: "uppercase", letterSpacing: "0.5px", margin: "4px 0 8px" }}>
                            {showKeepPremium ? "Keep Premium" : "Assignment Risk"}
                          </div>
                        </>
                      );
                    })()}
                    <div style={{ fontSize: 11, color: "#aaa" }}>
                      <ConfidenceDot confidence={probs.confidence} />
                      n={probs.n} · {probs.confidence}
                    </div>
                    <div style={{ fontSize: 11, color: "#aaa", marginTop: 4 }}>
                      Touched: {probs.touch_pct != null ? `${probs.touch_pct}%` : "—"}
                    </div>
                    <div style={{ fontSize: 11, color: "#aaa" }}>
                      Recovered: {probs.recovery_rate != null ? `${probs.recovery_rate}%` : "—"}
                    </div>
                  </div>

                  {/* Card 2: Avg Worst Dip */}
                  <div style={{ background: "#12121e", border: "1px solid #333", borderRadius: 6, padding: "10px 12px" }}>
                    <div style={{ fontSize: 28, fontWeight: 800, color: maeColor(probs.avg_mae), lineHeight: 1 }}>
                      {probs.avg_mae != null ? `${probs.avg_mae}%` : "—"}
                    </div>
                    <div style={{ fontSize: 10, color: "#888", textTransform: "uppercase", letterSpacing: "0.5px", margin: "4px 0 8px" }}>
                      Avg Worst Dip (MAE)
                    </div>
                    <div style={{ fontSize: 11, color: "#aaa" }}>
                      {">10% dip: "}{probs.pct_worse_than_10 != null ? `${probs.pct_worse_than_10}%` : "—"}
                    </div>
                    <div style={{ fontSize: 11, color: "#aaa" }}>
                      {">15% dip: "}{probs.pct_worse_than_15 != null ? `${probs.pct_worse_than_15}%` : "—"}
                    </div>
                    <div style={{ fontSize: 11, color: "#aaa" }}>
                      {">20% dip: "}{probs.pct_worse_than_20 != null ? `${probs.pct_worse_than_20}%` : "—"}
                    </div>
                  </div>

                  {/* Card 3: Early Exit */}
                  <div style={{ background: "#12121e", border: "1px solid #333", borderRadius: 6, padding: "10px 12px" }}>
                    <div style={{ fontSize: 28, fontWeight: 800, color: "#4caf50", lineHeight: 1 }}>
                      {probs.runaway_pct != null ? `${probs.runaway_pct}%` : "—"}
                    </div>
                    <div style={{ fontSize: 10, color: "#888", textTransform: "uppercase", letterSpacing: "0.5px", margin: "4px 0 8px" }}>
                      Early Exit Opportunity
                    </div>
                    <div style={{ fontSize: 11, color: "#aaa", marginBottom: 6 }}>
                      Stock moved 4%+ above strike
                    </div>
                    {probs.recovery_rate != null && (
                      <div style={{ fontSize: 11, color: "#aaa" }}>
                        Recovery:{" "}
                        <span style={{
                          fontSize: 10, fontWeight: 700,
                          background: probs.recovery_rate > 50 ? "#1b3d1b" : probs.recovery_rate < 30 ? "#3d1b1b" : "#3d2e00",
                          color: probs.recovery_rate > 50 ? "#4caf50" : probs.recovery_rate < 30 ? "#f44336" : "#ffc107",
                          padding: "1px 6px", borderRadius: 3,
                        }}>
                          {probs.recovery_rate > 50 ? "bouncy" : probs.recovery_rate < 30 ? "drifty" : "mixed"}
                        </span>
                      </div>
                    )}
                    {probs.avg_mfe != null && (
                      <div style={{ fontSize: 11, color: "#aaa", marginTop: 4 }}>
                        Avg best move: +{probs.avg_mfe}%
                      </div>
                    )}
                  </div>
                </div>

                {/* Comparison bars */}
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>

                  {/* Hold window */}
                  <div>
                    <div style={{ fontSize: 11, color: "#888", textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: 6 }}>
                      {showKeepPremium ? "Keep Premium by Hold" : "Assignment by Hold"}
                    </div>
                    {holdComp && holdComp.length > 0 ? holdComp.map(row => (
                      <ComparisonBar
                        key={row.hold_days}
                        label={`${row.hold_days}d`}
                        assignedPct={row.assigned_pct}
                        isSelected={Math.abs(row.hold_days - holdDays) <= 3}
                        n={row.n}
                        showKeepPremium={showKeepPremium}
                      />
                    )) : (
                      <div style={{ color: "#666", fontSize: 12 }}>No comparison data</div>
                    )}
                  </div>

                  {/* Strike distance */}
                  <div>
                    <div style={{ fontSize: 11, color: "#888", textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: 6 }}>
                      {showKeepPremium ? "Keep Premium by Strike" : "Assignment by Strike"}
                    </div>
                    {strikeComp && strikeComp.length > 0 ? strikeComp.map(row => {
                      const dollarStr = price ? ` $${Math.round(price * (1 - row.strike_pct))}` : "";
                      const label = row.strike_pct === 0
                        ? `ATM${dollarStr}`
                        : `${(row.strike_pct * 100).toFixed(0)}%${dollarStr}`;
                      return (
                        <ComparisonBar
                          key={row.strike_pct}
                          label={label}
                          assignedPct={row.assigned_pct}
                          isSelected={Math.abs(row.strike_pct - strikePct) < 0.005}
                          n={row.n}
                          showKeepPremium={showKeepPremium}
                        />
                      );
                    }) : (
                      <div style={{ color: "#666", fontSize: 12 }}>No comparison data</div>
                    )}
                  </div>
                </div>

                {/* Parabolic detail section */}
                {parabolic?.is_parabolic && (
                  <div style={{
                    marginTop: 10,
                    background: "#12121e",
                    border: "1px solid #2a2a3a",
                    borderRadius: 6,
                    padding: "8px 12px",
                    fontSize: 12,
                  }}>
                    <div style={{ fontWeight: 700, color: "#888", textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: 6, fontSize: 10 }}>
                      Parabolic Detail
                    </div>
                    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 4 }}>
                      <div>
                        <span style={{ color: "#555" }}>All regimes</span>
                        {parabolic.all_regimes.n > 0 ? (
                          <span style={{ color: "#aaa", marginLeft: 6 }}>
                            n={parabolic.all_regimes.n} ·{" "}
                            <span style={{ color: parabolic.all_regimes.assigned_pct < 40 ? "#4caf50" : "#f44336" }}>
                              {parabolic.all_regimes.assigned_pct}% assign
                            </span>
                            {" · "}
                            <span style={{ color: "#f44336" }}>{parabolic.all_regimes.avg_mae}% MAE</span>
                            {" · "}
                            <span style={{ color: "#4caf50" }}>+{parabolic.all_regimes.avg_mfe}% MFE</span>
                          </span>
                        ) : <span style={{ color: "#555" }}> —</span>}
                      </div>
                      <div>
                        <span style={{ color: "#555" }}>Your regime</span>
                        {parabolic.current_regime.n > 0 ? (
                          <span style={{ color: "#aaa", marginLeft: 6 }}>
                            n={parabolic.current_regime.n} ·{" "}
                            <span style={{ color: parabolic.current_regime.assigned_pct < 40 ? "#4caf50" : "#f44336" }}>
                              {parabolic.current_regime.assigned_pct}% assign
                            </span>
                            {" · "}
                            <span style={{ color: "#f44336" }}>{parabolic.current_regime.avg_mae}% MAE</span>
                            {" · "}
                            <span style={{ color: "#4caf50" }}>+{parabolic.current_regime.avg_mfe}% MFE</span>
                            {" "}
                            <span style={{ color: "#555", fontSize: 10 }}>[{parabolic.current_regime.confidence}]</span>
                          </span>
                        ) : <span style={{ color: "#555" }}> insufficient data</span>}
                      </div>
                    </div>
                  </div>
                )}
              </>
            )}
          </>
        );
      })()}
    </div>
  );

  if (embedded) return content;

  return (
    <div style={{
      position: "fixed", inset: 0, background: "rgba(0,0,0,0.7)",
      display: "flex", alignItems: "center", justifyContent: "center",
      zIndex: 1000,
    }} onClick={onClose}>
      <div style={{
        background: "#1a1a2e", border: "1px solid #333", borderRadius: 8,
        padding: 20, width: "min(680px, 96vw)", maxHeight: "90vh",
        overflowY: "auto",
      }} onClick={e => e.stopPropagation()}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 14 }}>
          <div style={{ fontWeight: 700, fontSize: 15, color: "#fff" }}>
            CSP Probability — {ticker}
          </div>
          {onClose && (
            <button onClick={onClose} style={{
              background: "transparent", border: "none", color: "#888",
              fontSize: 18, cursor: "pointer", lineHeight: 1,
            }}>✕</button>
          )}
        </div>
        {content}
      </div>
    </div>
  );
}
