import React from "react";

/**
 * SystemStatus — top full-width panel.
 * Shows the overall market health state as a large color banner + summary sentence.
 *
 * Props (Phase 2 will pass real data):
 *   state: "green" | "yellow" | "red"
 *   label: string  — e.g. "ROTATION", "RISK-OFF", "LIQUIDATION"
 *   text:  string  — one-sentence summary
 */
export default function SystemStatus({ state = "yellow", label = "ROTATION", text }) {
  const defaultText =
    state === "green"
      ? "Markets are broadly healthy. Premium conditions are favorable across most sectors."
      : state === "yellow"
      ? "Markets are in a rotation phase — money is moving from growth to defensives and energy. No systemic risk signals detected."
      : "Risk-off environment detected. Elevated fear across multiple asset classes. Reduce premium exposure.";

  return (
    <div className="mh-panel">
      <div className="mh-panel-title">System Status</div>
      <div className={`mh-status-banner ${state}`}>
        <div className={`mh-status-dot ${state}`} />
        <div>
          <div className={`mh-status-label ${state}`}>{label}</div>
          <div className="mh-status-text">{text || defaultText}</div>
        </div>
      </div>
      <div className="mh-placeholder-note">— placeholder data — real ingestion in Phase 2 —</div>
    </div>
  );
}
