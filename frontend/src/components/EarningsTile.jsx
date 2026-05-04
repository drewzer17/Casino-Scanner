import React from "react";

/**
 * EarningsTile — small inline badge showing days until next earnings.
 *
 * Props:
 *   days      {number|null}   — days_to_earnings (earnings_days from backend)
 *   inWindow  {boolean}       — earnings_in_window flag from backend (≤14 days)
 *
 * Colours:
 *   Hot  (red bold)  — inWindow === true OR days <= 7
 *   Warn (gray)      — days > 7 and not in window
 *   Unknown (dim)    — days is null/undefined
 */
export default function EarningsTile({ days, inWindow }) {
  if (days == null) {
    return <span className="earn-tile earn-tile-unknown">E ?</span>;
  }
  const isHot = inWindow || days <= 7;
  return (
    <span className={`earn-tile ${isHot ? "earn-tile-hot" : "earn-tile-warn"}`}>
      E {days}
    </span>
  );
}
