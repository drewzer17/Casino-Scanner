import React from "react";

/**
 * WheresFear — middle-left panel.
 * Six fear buckets, each with a color indicator + bar + badge.
 *
 * Phase 2 will replace PLACEHOLDER_BUCKETS with live data from /api/market-health/fear
 */

const PLACEHOLDER_BUCKETS = [
  { label: "Credit",       state: "calm",     pct: 18 },
  { label: "Equity",       state: "elevated",  pct: 55 },
  { label: "Bonds",        state: "calm",     pct: 22 },
  { label: "Energy",       state: "stressed", pct: 74 },
  { label: "Growth",       state: "elevated",  pct: 60 },
  { label: "Geopolitical", state: "stressed", pct: 80 },
];

const STATE_LABEL = {
  calm:     "Calm",
  elevated: "Elevated",
  stressed: "Stressed",
};

export default function WheresFear({ buckets = PLACEHOLDER_BUCKETS }) {
  return (
    <div className="mh-panel" style={{ minHeight: 260 }}>
      <div className="mh-panel-title">Where&apos;s the Fear</div>
      {buckets.map((b) => (
        <div key={b.label} className="mh-fear-row">
          <div className="mh-fear-label">{b.label}</div>
          <div className="mh-fear-bar-wrap">
            <div
              className={`mh-fear-bar ${b.state}`}
              style={{ width: `${b.pct}%` }}
            />
          </div>
          <div className={`mh-fear-badge ${b.state}`}>
            {STATE_LABEL[b.state]}
          </div>
        </div>
      ))}
      <div className="mh-placeholder-note">— placeholder data — real ingestion in Phase 2 —</div>
    </div>
  );
}
