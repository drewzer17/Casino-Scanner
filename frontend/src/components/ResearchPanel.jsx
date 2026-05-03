import React, { useEffect, useState } from "react";
import { api } from "../api/client.js";

/**
 * ResearchPanel — sliding right panel that shows full sitrep content.
 *
 * Props:
 *   ticker   {string|null}  — ticker to load; null/undefined = panel closed
 *   onClose  {function}     — called when user closes the panel
 *
 * States:
 *   isCompact   — CONTENT toggle: compact shows header + What They Do only;
 *                 full shows all sections. Resets to true on every open.
 *   isExpanded  — WIDTH toggle: normal = min(50vw, 720px); expanded = 100vw.
 *                 Resets to false on every open.
 */

// ── SVG icons for expand / collapse ──────────────────────────────────────────

const ExpandIcon = () => (
  <svg width="15" height="15" viewBox="0 0 15 15" fill="none"
    stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
    <path d="M1.5 5.5V1.5H5.5" />
    <line x1="1.5" y1="1.5" x2="5.8" y2="5.8" />
    <path d="M13.5 9.5V13.5H9.5" />
    <line x1="13.5" y1="13.5" x2="9.2" y2="9.2" />
  </svg>
);

const CollapseIcon = () => (
  <svg width="15" height="15" viewBox="0 0 15 15" fill="none"
    stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
    <path d="M5.5 1.5V5.5H1.5" />
    <line x1="5.5" y1="5.5" x2="1.2" y2="1.2" />
    <path d="M9.5 13.5V9.5H13.5" />
    <line x1="9.5" y1="9.5" x2="13.8" y2="13.8" />
  </svg>
);

// ── Component ─────────────────────────────────────────────────────────────────

export default function ResearchPanel({ ticker, onClose }) {
  const isOpen = !!ticker;
  const [data, setData]           = useState(null);
  const [loading, setLoading]     = useState(false);
  const [error, setError]         = useState(null);
  const [lastTicker, setLastTicker] = useState(null);
  const [isCompact, setIsCompact] = useState(true);
  const [isExpanded, setIsExpanded] = useState(false);

  // Fetch sitrep whenever ticker changes; reset both states on every open
  useEffect(() => {
    if (!ticker) return;
    setLastTicker(ticker);
    setIsCompact(true);    // always compact on open
    setIsExpanded(false);  // always normal width on open
    setLoading(true);
    setError(null);
    setData(null);
    api.sitrep(ticker)
      .then((d) => { setData(d); setLoading(false); })
      .catch((e) => { setError(e.message); setLoading(false); });
  }, [ticker]);

  // Body scroll lock
  useEffect(() => {
    document.body.style.overflow = isOpen ? "hidden" : "";
    return () => { document.body.style.overflow = ""; };
  }, [isOpen]);

  // Close on Escape
  useEffect(() => {
    const handler = (e) => { if (e.key === "Escape" && isOpen) onClose(); };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [isOpen, onClose]);

  const displayTicker = ticker || lastTicker;
  const panelWidth    = isExpanded ? "100vw" : "min(50vw, 720px)";

  // ── Style helpers (light theme) ───────────────────────────────────────────

  const pillStyle = (bg, color, extra = {}) => ({
    background: bg,
    color,
    fontSize: "11px",
    fontWeight: 600,
    padding: "2px 8px",
    borderRadius: "4px",
    whiteSpace: "nowrap",
    cursor: "default",
    ...extra,
  });

  const sectionTitle = (text) => (
    <div style={{
      fontSize: "11px",
      fontWeight: 700,
      letterSpacing: "0.08em",
      textTransform: "uppercase",
      color: "#8b5cf6",
      marginBottom: "8px",
      marginTop: "20px",
      borderBottom: "1px solid rgba(139,92,246,0.3)",
      paddingBottom: "4px",
    }}>
      {text}
    </div>
  );

  // Generic list — handles strings and { label, text, ... } objects.
  // NOT used for winners (dedicated card-grid renderer below).
  const renderList = (items) => {
    if (!items || items.length === 0) return null;
    return (
      <ul style={{ margin: 0, padding: "0 0 0 18px", listStyle: "disc" }}>
        {items.map((item, i) => {
          if (typeof item === "string") {
            return (
              <li key={i} style={{ color: "#111827", fontSize: "13px", lineHeight: 1.55, marginBottom: "4px" }}>
                {item}
              </li>
            );
          }
          const title = item.label || item.title || item.name || item.component || "";
          const body  = item.text  || item.body  || item.detail || item.description || "";
          return (
            <li key={i} style={{ color: "#111827", fontSize: "13px", lineHeight: 1.55, marginBottom: "6px" }}>
              {title && <strong style={{ color: "#000" }}>{title}{body ? " — " : ""}</strong>}
              {body}
            </li>
          );
        })}
      </ul>
    );
  };

  // Winners — items are { ticker, rationale } — 2-column card grid
  const renderWinnersGrid = (winners) => {
    if (!winners || winners.length === 0) return null;
    return (
      <div style={{ display: "grid", gridTemplateColumns: "repeat(2, 1fr)", gap: "8px", marginTop: "2px" }}>
        {winners.map((w, i) => {
          const t = w.ticker || String(w);
          const r = w.rationale || w.text || "";
          return (
            <div key={i} style={{
              background: "rgba(0,0,0,0.04)",
              border: "1px solid rgba(139,92,246,0.3)",
              borderRadius: "6px",
              padding: "10px 12px",
            }}>
              <div style={{
                fontFamily: "'Courier New', Courier, monospace",
                fontSize: "13px",
                fontWeight: 700,
                color: "#7c3aed",
                letterSpacing: "0.04em",
              }}>
                {t}
              </div>
              {r && (
                <div style={{ fontSize: "11px", color: "#1f2937", marginTop: "4px", lineHeight: 1.5 }}>
                  {r}
                </div>
              )}
            </div>
          );
        })}
      </div>
    );
  };

  // Compact/Full toggle button
  const ToggleButton = () => (
    <div style={{ textAlign: "center", marginTop: "24px", marginBottom: "4px" }}>
      <button
        onClick={() => setIsCompact(v => !v)}
        style={{
          background: "#8b5cf6",
          color: "#fff",
          border: "none",
          borderRadius: "6px",
          padding: "10px 20px",
          fontSize: "13px",
          fontWeight: 600,
          cursor: "pointer",
          transition: "background 0.15s",
        }}
        onMouseEnter={(e) => { e.currentTarget.style.background = "#7c3aed"; }}
        onMouseLeave={(e) => { e.currentTarget.style.background = "#8b5cf6"; }}
      >
        {isCompact ? "Show full research ▾" : "Show compact view ▴"}
      </button>
    </div>
  );

  // Header icon button (expand + close share the same base style)
  const iconBtnStyle = {
    background: "none",
    border: "none",
    color: "#374151",
    cursor: "pointer",
    padding: "3px 5px",
    lineHeight: 1,
    display: "inline-flex",
    alignItems: "center",
    justifyContent: "center",
    borderRadius: "4px",
  };

  // ── Render ────────────────────────────────────────────────────────────────

  return (
    <>
      {/* ── Backdrop ── */}
      <div
        onClick={onClose}
        style={{
          position: "fixed",
          inset: 0,
          background: "rgba(0,0,0,0.35)",
          zIndex: 1199,
          opacity: isOpen ? 1 : 0,
          pointerEvents: isOpen ? "auto" : "none",
          transition: "opacity 0.25s ease",
        }}
      />

      {/* ── Panel ── */}
      <div
        style={{
          position: "fixed",
          top: 0,
          right: 0,
          bottom: 0,
          width: panelWidth,
          background: "#e5e7eb",
          borderLeft: "1px solid rgba(0,0,0,0.12)",
          zIndex: 1200,
          display: "flex",
          flexDirection: "column",
          transform: isOpen ? "translateX(0)" : "translateX(100%)",
          transition: "transform 0.25s cubic-bezier(0.4, 0, 0.2, 1), width 0.25s cubic-bezier(0.4, 0, 0.2, 1)",
          boxShadow: "-4px 0 24px rgba(0,0,0,0.15)",
        }}
      >
        {/* ── Header (always visible) ── */}
        <div style={{
          padding: "14px 16px 12px",
          borderBottom: "1px solid rgba(0,0,0,0.1)",
          display: "flex",
          alignItems: "flex-start",
          gap: "10px",
          flexShrink: 0,
          background: "#d1d5db",
        }}>
          {/* Left: ticker + metadata */}
          <div style={{ flex: 1, minWidth: 0 }}>
            {/* Ticker + company name */}
            <div style={{ fontWeight: 800, fontSize: "18px", color: "#111827", lineHeight: 1.2, marginBottom: "6px" }}>
              {displayTicker}
              {data?.company_name && (
                <span style={{ fontWeight: 400, fontSize: "14px", color: "#4b5563", marginLeft: "8px" }}>
                  {data.company_name}
                </span>
              )}
            </div>

            {/* Metadata pills */}
            {data && (data.primary_lens || (data.lenses && data.lenses.length > 0) || data.category) && (
              <div style={{ display: "flex", flexWrap: "wrap", gap: "4px" }}>
                {data.primary_lens && (
                  <span style={pillStyle("#8b5cf6", "#fff", { fontWeight: 700 })}>
                    {data.primary_lens}
                  </span>
                )}
                {(data.lenses || [])
                  .filter(l => l !== data.primary_lens)
                  .map(l => (
                    <span key={l} style={pillStyle("rgba(139,92,246,0.55)", "#fff")}>
                      {l}
                    </span>
                  ))}
                {data.category && (
                  /* Yellow pill needs DARK text for readability */
                  <span style={pillStyle("#fbbf24", "#000")}>
                    {data.category}
                  </span>
                )}
                {data.subcategory && (
                  <span style={pillStyle("#06b6d4", "#fff", { fontSize: "10px" })}>
                    {data.subcategory}
                  </span>
                )}
              </div>
            )}

            {/* Last updated */}
            {data?.last_updated && (
              <div style={{ fontSize: "11px", color: "#6b7280", marginTop: "5px" }}>
                Updated {data.last_updated}
              </div>
            )}
          </div>

          {/* Right: full-page link + expand + close */}
          <div style={{ display: "flex", alignItems: "center", gap: "6px", flexShrink: 0 }}>
            {displayTicker && (
              <a
                href={`/ai-overview/${displayTicker}`}
                target="_blank"
                rel="noreferrer"
                style={{
                  fontSize: "12px",
                  color: "#7c3aed",
                  textDecoration: "none",
                  fontWeight: 600,
                  padding: "4px 8px",
                  border: "1px solid rgba(124,58,237,0.4)",
                  borderRadius: "4px",
                  whiteSpace: "nowrap",
                }}
                onMouseEnter={(e) => {
                  e.currentTarget.style.borderColor = "#7c3aed";
                  e.currentTarget.style.textDecoration = "underline";
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.borderColor = "rgba(124,58,237,0.4)";
                  e.currentTarget.style.textDecoration = "none";
                }}
              >
                Full page ↗
              </a>
            )}

            {/* Expand / collapse panel width */}
            <button
              onClick={() => setIsExpanded(v => !v)}
              title={isExpanded ? "Collapse panel" : "Expand panel"}
              style={iconBtnStyle}
              onMouseEnter={(e) => { e.currentTarget.style.color = "#7c3aed"; }}
              onMouseLeave={(e) => { e.currentTarget.style.color = "#374151"; }}
            >
              {isExpanded ? <CollapseIcon /> : <ExpandIcon />}
            </button>

            {/* Close */}
            <button
              onClick={onClose}
              title="Close"
              style={{ ...iconBtnStyle, fontSize: "17px" }}
              onMouseEnter={(e) => { e.currentTarget.style.color = "#dc2626"; }}
              onMouseLeave={(e) => { e.currentTarget.style.color = "#374151"; }}
            >
              ✕
            </button>
          </div>
        </div>

        {/* ── Body (scrollable) ── */}
        <div style={{ flex: 1, overflowY: "auto", padding: "4px 20px 24px" }}>

          {loading && (
            <div style={{ color: "#6b7280", fontSize: "14px", padding: "32px 0", textAlign: "center" }}>
              Loading…
            </div>
          )}

          {error && (
            <div style={{ color: "#dc2626", fontSize: "13px", padding: "20px 0" }}>
              Error: {error}
            </div>
          )}

          {data && (
            <>
              {/* ── COMPACT: What They Do (always shown) ── */}
              {data.what_they_do && (
                <>
                  {sectionTitle("What They Do")}
                  <p style={{ margin: 0, color: "#111827", fontSize: "13px", lineHeight: 1.65 }}>
                    {data.what_they_do}
                  </p>
                </>
              )}

              {/* ── FULL: remaining sections ── */}
              {!isCompact && (
                <>
                  {data.hidden_angles && data.hidden_angles.length > 0 && (
                    <>
                      {sectionTitle("AI Exposure & Hidden Angles")}
                      {renderList(data.hidden_angles)}
                    </>
                  )}

                  {data.kill_components && data.kill_components.length > 0 && (
                    <>
                      {sectionTitle("What Could Kill This")}
                      {renderList(data.kill_components)}
                    </>
                  )}

                  {data.winners && data.winners.length > 0 && (
                    <>
                      {sectionTitle("Who Wins If This Works")}
                      {renderWinnersGrid(data.winners)}
                    </>
                  )}

                  {data.parse_warnings && data.parse_warnings.length > 0 && (
                    <>
                      {sectionTitle("Parse Notes")}
                      <div style={{ fontSize: "11px", color: "#6b7280" }}>
                        {data.parse_warnings.join(" · ")}
                      </div>
                    </>
                  )}

                  {data.sections_parsed != null && (
                    <div style={{ fontSize: "11px", color: "#9ca3af", marginTop: "24px" }}>
                      {data.sections_parsed} sections parsed
                    </div>
                  )}
                </>
              )}

              {/* ── Compact/Full toggle (always shown once data loads) ── */}
              <ToggleButton />
            </>
          )}
        </div>
      </div>
    </>
  );
}
