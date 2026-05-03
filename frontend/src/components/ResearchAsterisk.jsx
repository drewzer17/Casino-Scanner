import React from "react";

/**
 * ResearchAsterisk — small purple * rendered to the left of a ticker symbol.
 * Only renders when hasSitrep is true.
 *
 * When onResearch is provided it renders as a <button> that calls onResearch(ticker).
 * When onResearch is NOT provided it falls back to a link to /ai-overview/{ticker}.
 */
export default function ResearchAsterisk({ ticker, hasSitrep, onResearch, isDefense }) {
  if (!hasSitrep) return null;

  // Defense tickers use yellow; all others use purple
  const baseColor  = isDefense ? "#fbbf24" : "#8b5cf6";
  const hoverColor = isDefense ? "#fcd34d" : "#a78bfa";
  const title      = isDefense
    ? "Defense/Aerospace research (not pure AI play)"
    : "View AI research";

  const sharedStyle = {
    color: baseColor,
    fontWeight: "bold",
    fontSize: "1.2em",
    marginRight: "6px",
    cursor: "pointer",
    lineHeight: 1,
    flexShrink: 0,
    textDecoration: "none",
  };

  const onEnter = (e) => { e.currentTarget.style.color = hoverColor; };
  const onLeave = (e) => { e.currentTarget.style.color = baseColor; };

  if (onResearch) {
    return (
      <button
        title={title}
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
      title={title}
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
