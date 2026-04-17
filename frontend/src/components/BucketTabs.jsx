import React from "react";

const LABELS = {
  sell_now: "Sell Now",
  buy_sell_later: "Buy Now, Sell Later",
  watchlist: "Watchlist",
};

export default function BucketTabs({ active, counts, onChange }) {
  return (
    <div className="tabs">
      {["sell_now", "buy_sell_later", "watchlist"].map((k) => (
        <button
          key={k}
          className={`tab ${active === k ? "active" : ""}`}
          onClick={() => onChange(k)}
        >
          {LABELS[k]}
          <span className="count">{counts[k] ?? 0}</span>
        </button>
      ))}
    </div>
  );
}
