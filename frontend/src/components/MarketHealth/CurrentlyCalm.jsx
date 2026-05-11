import React from "react";

/**
 * CurrentlyCalm — bottom-right panel.
 * Lists the fear buckets that are currently green (calm).
 *
 * Phase 2 will derive this from live /api/market-health/fear data
 * rather than a hardcoded list.
 */

const PLACEHOLDER_CALM = [
  { name: "Credit",  detail: "HY spreads tight, IG demand solid" },
  { name: "Bonds",   detail: "TLT holding range, no panic selling" },
];

export default function CurrentlyCalm({ calmBuckets = PLACEHOLDER_CALM }) {
  return (
    <div className="mh-panel" style={{ minHeight: 200 }}>
      <div className="mh-panel-title">Currently Calm</div>
      {calmBuckets.length === 0 ? (
        <div className="mh-placeholder-note" style={{ marginTop: 24 }}>
          No buckets are currently calm.
        </div>
      ) : (
        <div className="mh-calm-list">
          {calmBuckets.map((b) => (
            <div key={b.name} className="mh-calm-item">
              <div className="mh-calm-dot" />
              <div className="mh-calm-name">{b.name}</div>
              {b.detail && <div className="mh-calm-detail">{b.detail}</div>}
            </div>
          ))}
        </div>
      )}
      <div className="mh-placeholder-note">— placeholder data — real ingestion in Phase 2 —</div>
    </div>
  );
}
