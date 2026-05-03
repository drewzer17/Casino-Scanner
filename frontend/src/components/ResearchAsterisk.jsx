import React from "react";

/**
 * ResearchAsterisk — small purple * rendered to the left of a ticker symbol.
 * Only renders when hasSitrep is true. Links to /ai-overview/{ticker}.
 * Uses stopPropagation so it doesn't fire parent row click handlers.
 */
export default function ResearchAsterisk({ ticker, hasSitrep }) {
  if (!hasSitrep) return null;
  return (
    <a
      href={`/ai-overview/${ticker}`}
      title="View AI research"
      onClick={(e) => {
        e.stopPropagation();
        sessionStorage.setItem("sitrep_return_to", window.location.href);
      }}
      style={{
        color: "#8b5cf6",
        fontWeight: "bold",
        fontSize: "1.2em",
        marginRight: "6px",
        cursor: "pointer",
        textDecoration: "none",
        lineHeight: 1,
        flexShrink: 0,
      }}
      onMouseEnter={(e) => { e.currentTarget.style.color = "#a78bfa"; }}
      onMouseLeave={(e) => { e.currentTarget.style.color = "#8b5cf6"; }}
    >*</a>
  );
}
