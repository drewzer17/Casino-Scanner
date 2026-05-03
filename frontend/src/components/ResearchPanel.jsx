import React, { useEffect, useState } from "react";
import { api } from "../api/client.js";

/**
 * ResearchPanel — sliding right panel that shows full sitrep content.
 *
 * Props:
 *   ticker   {string|null}  — ticker to load; null/undefined = panel closed
 *   onClose  {function}     — called when user closes the panel
 */
export default function ResearchPanel({ ticker, onClose }) {
  const isOpen = !!ticker;
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [lastTicker, setLastTicker] = useState(null);

  // Fetch sitrep whenever ticker changes
  useEffect(() => {
    if (!ticker) return;
    setLastTicker(ticker);
    setLoading(true);
    setError(null);
    setData(null);
    api.sitrep(ticker)
      .then((d) => { setData(d); setLoading(false); })
      .catch((e) => { setError(e.message); setLoading(false); });
  }, [ticker]);

  // Body scroll lock
  useEffect(() => {
    if (isOpen) {
      document.body.style.overflow = "hidden";
    } else {
      document.body.style.overflow = "";
    }
    return () => { document.body.style.overflow = ""; };
  }, [isOpen]);

  // Close on Escape
  useEffect(() => {
    const handler = (e) => { if (e.key === "Escape" && isOpen) onClose(); };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [isOpen, onClose]);

  // Keep showing the last loaded ticker's data while sliding closed
  const displayTicker = ticker || lastTicker;

  const pillStyle = (bg, color = "#fff", extra = {}) => ({
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

  const renderList = (items, emptyText) => {
    if (!items || items.length === 0) return (
      <div style={{ color: "rgba(255,255,255,0.35)", fontSize: "13px" }}>{emptyText || "None"}</div>
    );
    return (
      <ul style={{ margin: 0, padding: "0 0 0 18px", listStyle: "disc" }}>
        {items.map((item, i) => {
          if (typeof item === "string") {
            return (
              <li key={i} style={{ color: "rgba(255,255,255,0.82)", fontSize: "13px", lineHeight: 1.55, marginBottom: "4px" }}>
                {item}
              </li>
            );
          }
          // Object with label/text or title/body
          const title = item.label || item.title || item.name || item.component || "";
          const body = item.text || item.body || item.detail || item.description || "";
          return (
            <li key={i} style={{ color: "rgba(255,255,255,0.82)", fontSize: "13px", lineHeight: 1.55, marginBottom: "6px" }}>
              {title && <strong style={{ color: "#fff" }}>{title}{body ? " — " : ""}</strong>}
              {body}
            </li>
          );
        })}
      </ul>
    );
  };

  return (
    <>
      {/* ── Backdrop ── */}
      <div
        onClick={onClose}
        style={{
          position: "fixed",
          inset: 0,
          background: "rgba(0,0,0,0.45)",
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
          width: "min(540px, 100vw)",
          background: "#13131f",
          borderLeft: "1px solid rgba(255,255,255,0.08)",
          zIndex: 1200,
          display: "flex",
          flexDirection: "column",
          transform: isOpen ? "translateX(0)" : "translateX(100%)",
          transition: "transform 0.25s cubic-bezier(0.4, 0, 0.2, 1)",
          boxShadow: "-6px 0 32px rgba(0,0,0,0.6)",
        }}
      >
        {/* ── Panel header ── */}
        <div style={{
          padding: "14px 18px 12px",
          borderBottom: "1px solid rgba(255,255,255,0.08)",
          display: "flex",
          alignItems: "flex-start",
          gap: "10px",
          flexShrink: 0,
          background: "#0f0f1a",
        }}>
          <div style={{ flex: 1, minWidth: 0 }}>
            {/* Ticker + company name */}
            <div style={{
              fontWeight: 800,
              fontSize: "18px",
              color: "#fff",
              lineHeight: 1.2,
              marginBottom: "6px",
            }}>
              {displayTicker}
              {data?.company_name && (
                <span style={{ fontWeight: 400, fontSize: "14px", color: "rgba(255,255,255,0.55)", marginLeft: "8px" }}>
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
                    <span key={l} style={pillStyle("rgba(139,92,246,0.28)", "rgba(255,255,255,0.7)")}>
                      {l}
                    </span>
                  ))}
                {data.category && (
                  <span style={pillStyle("#fbbf24", "#111")}>
                    {data.category}
                  </span>
                )}
                {data.subcategory && (
                  <span style={pillStyle("#0891b2", "#fff", { fontSize: "10px" })}>
                    {data.subcategory}
                  </span>
                )}
              </div>
            )}

            {/* Last updated */}
            {data?.last_updated && (
              <div style={{ fontSize: "11px", color: "rgba(255,255,255,0.35)", marginTop: "5px" }}>
                Updated {data.last_updated}
              </div>
            )}
          </div>

          {/* Actions: full page link + close */}
          <div style={{ display: "flex", alignItems: "center", gap: "8px", flexShrink: 0 }}>
            {displayTicker && (
              <a
                href={`/ai-overview/${displayTicker}`}
                target="_blank"
                rel="noreferrer"
                style={{
                  fontSize: "12px",
                  color: "#8b5cf6",
                  textDecoration: "none",
                  fontWeight: 600,
                  padding: "4px 8px",
                  border: "1px solid rgba(139,92,246,0.4)",
                  borderRadius: "4px",
                  whiteSpace: "nowrap",
                }}
                onMouseEnter={(e) => { e.currentTarget.style.borderColor = "#8b5cf6"; e.currentTarget.style.color = "#a78bfa"; }}
                onMouseLeave={(e) => { e.currentTarget.style.borderColor = "rgba(139,92,246,0.4)"; e.currentTarget.style.color = "#8b5cf6"; }}
              >
                Full page ↗
              </a>
            )}
            <button
              onClick={onClose}
              style={{
                background: "none",
                border: "none",
                color: "rgba(255,255,255,0.5)",
                fontSize: "18px",
                cursor: "pointer",
                padding: "2px 6px",
                lineHeight: 1,
              }}
              onMouseEnter={(e) => { e.currentTarget.style.color = "#fff"; }}
              onMouseLeave={(e) => { e.currentTarget.style.color = "rgba(255,255,255,0.5)"; }}
            >
              ✕
            </button>
          </div>
        </div>

        {/* ── Panel body ── */}
        <div style={{ flex: 1, overflowY: "auto", padding: "4px 20px 24px" }}>

          {loading && (
            <div style={{ color: "rgba(255,255,255,0.4)", fontSize: "14px", padding: "32px 0", textAlign: "center" }}>
              Loading…
            </div>
          )}

          {error && (
            <div style={{ color: "#f87171", fontSize: "13px", padding: "20px 0" }}>
              Error: {error}
            </div>
          )}

          {data && (
            <>
              {/* What they do */}
              {data.what_they_do && (
                <>
                  {sectionTitle("What They Do")}
                  <p style={{ margin: 0, color: "rgba(255,255,255,0.82)", fontSize: "13px", lineHeight: 1.65 }}>
                    {data.what_they_do}
                  </p>
                </>
              )}

              {/* AI Exposure / Hidden angles */}
              {data.hidden_angles && data.hidden_angles.length > 0 && (
                <>
                  {sectionTitle("AI Exposure & Hidden Angles")}
                  {renderList(data.hidden_angles)}
                </>
              )}

              {/* Kill components */}
              {data.kill_components && data.kill_components.length > 0 && (
                <>
                  {sectionTitle("What Could Kill This")}
                  {renderList(data.kill_components)}
                </>
              )}

              {/* Winners */}
              {data.winners && data.winners.length > 0 && (
                <>
                  {sectionTitle("Who Wins If This Works")}
                  {renderList(data.winners)}
                </>
              )}

              {/* Parse warnings (dev info, show only if any) */}
              {data.parse_warnings && data.parse_warnings.length > 0 && (
                <>
                  {sectionTitle("Parse Notes")}
                  <div style={{ fontSize: "11px", color: "rgba(255,255,255,0.3)" }}>
                    {data.parse_warnings.join(" · ")}
                  </div>
                </>
              )}

              {/* Sections parsed indicator */}
              {data.sections_parsed != null && (
                <div style={{ fontSize: "11px", color: "rgba(255,255,255,0.2)", marginTop: "24px" }}>
                  {data.sections_parsed} sections parsed
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </>
  );
}
