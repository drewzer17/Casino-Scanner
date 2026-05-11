import React from "react";

/**
 * BroadMarketState — bottom-left panel.
 * Single classifier: ROTATION / TRANSITIONAL / LIQUIDATION
 *
 * Phase 2 will replace placeholder state with live data from /api/market-health/state
 */

const STATE_META = {
  ROTATION: {
    cls: "rotation",
    desc: "Capital is actively rotating between sectors. Defensive and commodity plays are absorbing flows from growth. Covered calls on rotation targets are well-positioned.",
  },
  TRANSITIONAL: {
    cls: "transitional",
    desc: "Mixed signals across asset classes. No clear directional flow detected. Higher conviction needed before deploying premium strategies.",
  },
  LIQUIDATION: {
    cls: "liquidation",
    desc: "Broad-based selling across asset classes. Cash is king. Avoid new premium positions until conditions stabilize.",
  },
};

export default function BroadMarketState({ state = "ROTATION" }) {
  const meta = STATE_META[state] || STATE_META.ROTATION;

  return (
    <div className="mh-panel" style={{ minHeight: 200 }}>
      <div className="mh-panel-title">Broad Market State</div>
      <div className="mh-state-display">
        <div className={`mh-state-badge ${meta.cls}`}>{state}</div>
        <div className="mh-state-desc">{meta.desc}</div>
      </div>
      <div className="mh-placeholder-note">— placeholder data — real ingestion in Phase 2 —</div>
    </div>
  );
}
