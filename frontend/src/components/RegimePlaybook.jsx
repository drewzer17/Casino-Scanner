import React, { useEffect, useState } from "react";

const GRADE_COLORS = { A: "#4caf50", B: "#ffc107", C: "#ff9800", F: "#f44336" };
const VRP_COLORS   = { Rich: "#4caf50", Moderate: "#ffc107", Weak: "#ff9800", Negative: "#f44336" };

function assignColor(pct) {
  if (pct == null) return "#888";
  if (pct < 25) return "#4caf50";
  if (pct < 40) return "#ffc107";
  return "#f44336";
}

function Bar({ pct, maxPct = 60 }) {
  if (pct == null) return <div style={{ color: "#555", fontSize: 11 }}>—</div>;
  const width = Math.min(100, (pct / maxPct) * 100);
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
      <div style={{
        flex: 1,
        height: 8,
        background: "#2a2a3a",
        borderRadius: 4,
        overflow: "hidden",
      }}>
        <div style={{
          width: `${width}%`,
          height: "100%",
          background: assignColor(pct),
          borderRadius: 4,
          transition: "width 0.3s",
        }} />
      </div>
      <span style={{ color: assignColor(pct), fontWeight: 700, fontSize: 12, minWidth: 36, textAlign: "right" }}>
        {pct.toFixed(1)}%
      </span>
    </div>
  );
}

function Section({ title, children }) {
  return (
    <div style={{ marginBottom: 14 }}>
      <div style={{ fontSize: 10, fontWeight: 700, color: "#888", letterSpacing: 1, textTransform: "uppercase", marginBottom: 6 }}>
        {title}
      </div>
      {children}
    </div>
  );
}

function RowItem({ label, labelColor, pct, badge }) {
  return (
    <div style={{ marginBottom: 5 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 2 }}>
        <span style={{ color: labelColor || "#ccc", fontSize: 12, fontWeight: 600 }}>{label}</span>
        {badge && <span style={{ fontSize: 10, color: "#aaa", fontStyle: "italic" }}>{badge}</span>}
      </div>
      <Bar pct={pct} />
    </div>
  );
}

export default function RegimePlaybook({ onApplyFilters, onClose }) {
  const [data, setData]       = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError]     = useState(null);

  useEffect(() => {
    setLoading(true);
    fetch("/api/probability/playbook", { credentials: "include" })
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then(d => { setData(d); setLoading(false); })
      .catch(e => { setError(e.message); setLoading(false); });
  }, []);

  const containerStyle = {
    position: "relative",
    width: 272,
    background: "#12121e",
    border: "1px solid #2a2a3a",
    borderRadius: 8,
    padding: "12px 14px",
    fontSize: 13,
    color: "#ccc",
    overflowY: "auto",
    maxHeight: "calc(100vh - 120px)",
  };

  if (loading) return (
    <div style={containerStyle}>
      <div style={{ color: "#555", textAlign: "center", paddingTop: 40 }}>Loading playbook…</div>
    </div>
  );

  if (error) return (
    <div style={containerStyle}>
      <div style={{ color: "#f44336", fontSize: 12 }}>Error: {error}</div>
    </div>
  );

  if (!data) return null;

  const { regime, by_grade, by_vrp, by_sector, sweet_spot } = data;
  const regimeBg = regime.color === "red" ? "rgba(198,40,40,0.2)"
    : regime.color === "yellow" ? "rgba(245,127,23,0.2)"
    : "rgba(46,125,50,0.2)";
  const regimeDot = regime.color === "red" ? "#ef5350"
    : regime.color === "yellow" ? "#ffa726"
    : "#66bb6a";

  // Sort sectors: safest first, then riskiest
  const sortedSectors = [...by_sector].sort((a, b) => (a.assigned_pct ?? 100) - (b.assigned_pct ?? 100));
  const topSafe    = sortedSectors.slice(0, 5);
  const topRisky   = sortedSectors.slice(-3).reverse();

  // Find max for sector bar scaling
  const maxSectorPct = Math.max(60, ...by_sector.map(s => s.assigned_pct ?? 0));

  return (
    <div style={containerStyle}>
      {/* Close button */}
      {onClose && (
        <button
          onClick={onClose}
          style={{
            position: "absolute",
            top: 8,
            right: 8,
            background: "none",
            border: "none",
            color: "#fff",
            fontSize: 18,
            cursor: "pointer",
            lineHeight: 1,
            padding: "0 2px",
            opacity: 0.85,
          }}
          onMouseEnter={e => e.currentTarget.style.opacity = 0.5}
          onMouseLeave={e => e.currentTarget.style.opacity = 0.85}
          title="Close"
        >
          ✕
        </button>
      )}

      {/* Header */}
      <div style={{ paddingRight: onClose ? 20 : 0 }}>
      <div style={{
        background: regimeBg,
        border: `1px solid ${regimeDot}44`,
        borderRadius: 6,
        padding: "8px 10px",
        marginBottom: 14,
      }}>
        <div style={{ fontWeight: 700, fontSize: 13, color: "#fff", marginBottom: 2 }}>
          <span style={{ color: regimeDot, marginRight: 6 }}>●</span>
          REGIME PLAYBOOK
        </div>
        <div style={{ fontSize: 11, color: "#aaa" }}>
          {regime.name}
        </div>
        <div style={{ fontSize: 11, color: "#888", marginTop: 2 }}>
          {regime.duration}
        </div>
      </div>
      </div>

      {/* By Grade */}
      <Section title="By Path Safety Grade">
        {by_grade.map((row, i) => (
          <RowItem
            key={row.grade}
            label={
              <span>
                <span style={{
                  display: "inline-block",
                  background: GRADE_COLORS[row.grade] || "#888",
                  color: "#000",
                  fontWeight: 800,
                  fontSize: 11,
                  padding: "1px 6px",
                  borderRadius: 3,
                  marginRight: 6,
                }}>
                  {row.grade}
                </span>
                <span style={{ color: "#666", fontSize: 10 }}>n={row.n}</span>
              </span>
            }
            labelColor="#ccc"
            pct={row.assigned_pct != null ? Number(row.assigned_pct) : null}
            badge={i === 0 ? "← safest" : i === by_grade.length - 1 ? "← riskiest" : null}
          />
        ))}
      </Section>

      {/* By VRP */}
      <Section title="By VRP State">
        {by_vrp.map(row => (
          <RowItem
            key={row.vrp}
            label={
              <span>
                <span style={{ color: VRP_COLORS[row.vrp] || "#888", fontWeight: 700 }}>{row.vrp}</span>
                <span style={{ color: "#555", fontSize: 10, marginLeft: 6 }}>n={row.n}</span>
              </span>
            }
            labelColor="#ccc"
            pct={row.assigned_pct != null ? Number(row.assigned_pct) : null}
            badge={Number(row.assigned_pct) > 45 ? "avoid" : null}
          />
        ))}
      </Section>

      {/* By Sector */}
      <Section title="Best Sectors (lowest assign%)">
        {topSafe.map(row => (
          <RowItem
            key={row.sector}
            label={<span style={{ fontSize: 11 }}>{row.sector}</span>}
            labelColor="#ccc"
            pct={row.assigned_pct != null ? Number(row.assigned_pct) : null}
          />
        ))}
      </Section>

      {topRisky.length > 0 && (
        <Section title="Riskiest Sectors">
          {topRisky.map(row => (
            <RowItem
              key={row.sector}
              label={
                <span style={{ fontSize: 11 }}>
                  {Number(row.assigned_pct) > 45 ? "⚠ " : ""}{row.sector}
                </span>
              }
              labelColor="#ccc"
              pct={row.assigned_pct != null ? Number(row.assigned_pct) : null}
            />
          ))}
        </Section>
      )}

      {/* Sweet Spot */}
      {sweet_spot && (
        <Section title="Sweet Spot — Grade A + Rich VRP">
          <div style={{
            background: "rgba(76,175,80,0.1)",
            border: "1px solid #4caf5044",
            borderRadius: 6,
            padding: "8px 10px",
            fontSize: 12,
          }}>
            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
              <span style={{ color: "#888" }}>Assign%</span>
              <span style={{ color: assignColor(sweet_spot.assigned_pct), fontWeight: 700 }}>
                {sweet_spot.assigned_pct != null ? `${sweet_spot.assigned_pct.toFixed(1)}%` : "—"}
              </span>
            </div>
            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
              <span style={{ color: "#888" }}>Avg MAE</span>
              <span style={{
                color: sweet_spot.avg_mae != null && sweet_spot.avg_mae > -5 ? "#4caf50"
                  : sweet_spot.avg_mae != null && sweet_spot.avg_mae > -10 ? "#ffc107"
                  : "#f44336",
                fontWeight: 700,
              }}>
                {sweet_spot.avg_mae != null ? `${sweet_spot.avg_mae.toFixed(2)}%` : "—"}
              </span>
            </div>
            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 6 }}>
              <span style={{ color: "#888" }}>Early Exit%</span>
              <span style={{
                color: sweet_spot.exit_pct != null && sweet_spot.exit_pct > 70 ? "#4caf50"
                  : sweet_spot.exit_pct != null && sweet_spot.exit_pct > 50 ? "#ffc107"
                  : "#f44336",
                fontWeight: 700,
              }}>
                {sweet_spot.exit_pct != null ? `${sweet_spot.exit_pct.toFixed(0)}%` : "—"}
              </span>
            </div>
            <div style={{ fontSize: 10, color: "#555" }}>n={sweet_spot.n} trades</div>
          </div>
        </Section>
      )}

      {/* Apply Filters button */}
      {onApplyFilters && (
        <button
          onClick={() => onApplyFilters({ grades: new Set(["A", "B"]), vrps: new Set(["Rich", "Moderate"]) })}
          style={{
            width: "100%",
            marginTop: 6,
            background: "rgba(46,125,50,0.25)",
            border: "1px solid #4caf50",
            color: "#4caf50",
            padding: "8px 0",
            borderRadius: 5,
            cursor: "pointer",
            fontSize: 12,
            fontWeight: 700,
            letterSpacing: 0.5,
          }}
        >
          ✓ Apply Recommended Filters
        </button>
      )}
    </div>
  );
}
