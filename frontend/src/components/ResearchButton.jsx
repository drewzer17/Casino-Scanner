import React from "react";

/**
 * ResearchButton — purple "Research" button for the ticker detail modal.
 * Only renders when hasSitrep is true.
 *
 * When onResearch is provided it renders as a <button> that calls onResearch(ticker).
 * When onResearch is NOT provided it falls back to a link to /ai-overview/{ticker}.
 */
export default function ResearchButton({ ticker, hasSitrep, onResearch }) {
  if (!hasSitrep) return null;

  const sharedStyle = {
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
    border: "none",
  };

  const onEnter = (e) => { e.currentTarget.style.background = "#a78bfa"; };
  const onLeave = (e) => { e.currentTarget.style.background = "#8b5cf6"; };

  if (onResearch) {
    return (
      <button
        onClick={() => onResearch(ticker)}
        style={sharedStyle}
        onMouseEnter={onEnter}
        onMouseLeave={onLeave}
      >
        Research
      </button>
    );
  }

  // Fallback: navigate to full page
  return (
    <a
      href={`/ai-overview/${ticker}`}
      onClick={() => {
        sessionStorage.setItem("sitrep_return_to", window.location.href);
      }}
      style={sharedStyle}
      onMouseEnter={onEnter}
      onMouseLeave={onLeave}
    >
      Research
    </a>
  );
}
