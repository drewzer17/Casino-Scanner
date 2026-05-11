/**
 * MarketHealth — top-level component.
 *
 * Folder structure (all backend/DB items prefixed per spec):
 *   Frontend:  frontend/src/components/MarketHealth/
 *   API:       /api/market-health/*          (Phase 2)
 *   DB tables: mh_*                          (Phase 2)
 *
 * Phase 1: Static skeleton with placeholder data in each panel.
 * Phase 2: Replace placeholder props with live API calls.
 */
import React from "react";
import "./MarketHealth.css";
import SystemStatus       from "./SystemStatus.jsx";
import WheresFear         from "./WheresFear.jsx";
import RotationDirection  from "./RotationDirection.jsx";
import BroadMarketState   from "./BroadMarketState.jsx";
import CurrentlyCalm      from "./CurrentlyCalm.jsx";

export default function MarketHealth() {
  // Phase 2: replace these with useState + useEffect API calls
  const statusState = "yellow";
  const statusLabel = "ROTATION";
  const marketState = "ROTATION";

  return (
    <div className="mh-container">
      {/* Row 1 — full width status banner */}
      <SystemStatus state={statusState} label={statusLabel} />

      {/* Row 2 — fear + rotation side by side */}
      <div className="mh-row">
        <WheresFear />
        <RotationDirection />
      </div>

      {/* Row 3 — broad state + currently calm side by side */}
      <div className="mh-row">
        <BroadMarketState state={marketState} />
        <CurrentlyCalm />
      </div>
    </div>
  );
}
