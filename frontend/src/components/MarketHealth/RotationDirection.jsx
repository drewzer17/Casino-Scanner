import React from "react";

/**
 * RotationDirection — middle-right panel.
 * Shows a list of rotation targets with directional arrows.
 *
 * Phase 2 will replace PLACEHOLDER_TARGETS with live data from /api/market-health/rotation
 */

const PLACEHOLDER_TARGETS = [
  { ticker: "XLE",  name: "Energy Select",        dir: "up" },
  { ticker: "XOP",  name: "Oil & Gas E&P",         dir: "up" },
  { ticker: "UNG",  name: "Natural Gas",            dir: "neutral" },
  { ticker: "AMLP", name: "MLP Infrastructure",     dir: "up" },
  { ticker: "XLU",  name: "Utilities",              dir: "neutral" },
  { ticker: "XLP",  name: "Consumer Staples",       dir: "up" },
  { ticker: "GLD",  name: "Gold",                   dir: "up" },
  { ticker: "TLT",  name: "Long-Term Treasuries",   dir: "neutral" },
  { ticker: "RSP",  name: "Equal Weight S&P",       dir: "down" },
  { ticker: "IWM",  name: "Russell 2000",           dir: "down" },
  { ticker: "BTC",  name: "Bitcoin",                dir: "down" },
];

const ARROW = {
  up:      { symbol: "↑", label: "Inflow" },
  neutral: { symbol: "→", label: "Neutral" },
  down:    { symbol: "↓", label: "Outflow" },
};

export default function RotationDirection({ targets = PLACEHOLDER_TARGETS }) {
  return (
    <div className="mh-panel" style={{ minHeight: 260 }}>
      <div className="mh-panel-title">Rotation Direction</div>
      <div className="mh-rotation-list">
        {targets.map((t) => {
          const arrow = ARROW[t.dir] || ARROW.neutral;
          return (
            <div key={t.ticker} className="mh-rotation-row">
              <div className="mh-rotation-ticker">{t.ticker}</div>
              <div className="mh-rotation-name">{t.name}</div>
              <div
                className={`mh-rotation-arrow ${t.dir}`}
                title={arrow.label}
              >
                {arrow.symbol}
              </div>
            </div>
          );
        })}
      </div>
      <div className="mh-placeholder-note">— placeholder data — real ingestion in Phase 2 —</div>
    </div>
  );
}
