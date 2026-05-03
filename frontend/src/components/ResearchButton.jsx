import React from "react";

/**
 * ResearchButton — purple "Research" link-button for the ticker detail modal.
 * Only renders when hasSitrep is true. Links to /ai-overview/{ticker} same tab.
 */
export default function ResearchButton({ ticker, hasSitrep }) {
  if (!hasSitrep) return null;
  return (
    <a
      href={`/ai-overview/${ticker}`}
      style={{
        display: "inline-flex",
        alignItems: "center",
        padding: "6px 12px",
        background: "#8b5cf6",
        color: "#fff",
        fontWeight: 600,
        fontSize: "13px",
        borderRadius: "6px",
        textDecoration: "none",
        cursor: "pointer",
        whiteSpace: "nowrap",
        lineHeight: 1,
        transition: "background 0.15s",
      }}
      onMouseEnter={(e) => { e.currentTarget.style.background = "#a78bfa"; }}
      onMouseLeave={(e) => { e.currentTarget.style.background = "#8b5cf6"; }}
    >
      Research
    </a>
  );
}
