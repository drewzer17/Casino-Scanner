import React from "react";

const LABELS = {
  sell_now: "Sell Now",
  buy_sell_later: "Buy Now, Sell Later",
  watchlist: "Watchlist",
  premium_scanner: "Premium Scanner",
};

export default function BucketTabs({ active, counts, onChange }) {
  return (
    <div className="tabs">
      {["sell_now", "buy_sell_later", "watchlist", "premium_scanner"].map((k) => (
        <button
          key={k}
          className={`tab ${active === k ? "active" : ""}`}
          onClick={() => onChange(k)}
        >
          {LABELS[k]}
          {counts[k] != null && <span className="count">{counts[k]}</span>}
        </button>
      ))}
    </div>
  );
}
