import React from "react";

/**
 * ResearchAsterisk — small purple * rendered to the left of a ticker symbol.
 * Only renders when hasSitrep is true.
 *
 * When onResearch is provided it renders as a <button> that calls onResearch(ticker).
 * When onResearch is NOT provided it falls back to a link to /ai-overview/{ticker}.
 */
export default function ResearchAsterisk({ ticker, hasSitrep, onResearch }) {
  if (!hasSitrep) return null;

  const sharedStyle = {
    color: "#8b5cf6",
    fontWeight: "bold",
    fontSize: "1.2em",
    marginRight: "6px",
    cursor: "pointer",
    lineHeight: 1,
    flexShrink: 0,
    textDecoration: "none",
  };

  const onEnter = (e) => { e.currentTarget.style.color = "#a78bfa"; };
  const onLeave = (e) => { e.currentTarget.style.color = "#8b5cf6"; };

  if (onResearch) {
    return (
      <button
        title="View AI research"
        onClick={(e) => {
          e.stopPropagation();
          onResearch(ticker);
        }}
        style={{
          ...sharedStyle,
          background: "none",
          border: "none",
          padding: 0,
          display: "inline",
        }}
        onMouseEnter={onEnter}
        onMouseLeave={onLeave}
      >*</button>
    );
  }

  // Fallback: navigate to full page
  return (
    <a
      href={`/ai-overview/${ticker}`}
      title="View AI research"
      onClick={(e) => {
        e.stopPropagation();
        sessionStorage.setItem("sitrep_return_to", window.location.href);
      }}
      style={sharedStyle}
      onMouseEnter={onEnter}
      onMouseLeave={onLeave}
    >*</a>
  );
}
