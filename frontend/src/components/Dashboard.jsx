import React, { useEffect, useState } from "react";
import { api } from "../api/client.js";
import BucketTabs from "./BucketTabs.jsx";
import ScoreCard from "./ScoreCard.jsx";

// FastAPI returns naive UTC strings without a trailing 'Z'.
// Appending 'Z' ensures the Date constructor treats them as UTC in all browsers.
function toUtcDate(isoString) {
  if (!isoString) return null;
  const s = isoString.endsWith("Z") || isoString.includes("+") ? isoString : isoString + "Z";
  return new Date(s);
}

// "Apr 16, 2026 at 7:36 PM CDT"
function formatCentral(isoString) {
  const d = toUtcDate(isoString);
  if (!d) return "in progress";
  const datePart = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Chicago",
    month: "short",
    day: "numeric",
    year: "numeric",
  }).format(d);
  const timePart = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Chicago",
    hour: "numeric",
    minute: "2-digit",
    timeZoneName: "short",
  }).format(d);
  return `${datePart} at ${timePart}`;
}

// "8 minutes ago", "2 hours ago", etc.
function timeAgo(isoString) {
  const d = toUtcDate(isoString);
  if (!d) return null;
  const mins = Math.floor((Date.now() - d.getTime()) / 60_000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins} minute${mins === 1 ? "" : "s"} ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs} hour${hrs === 1 ? "" : "s"} ago`;
  const days = Math.floor(hrs / 24);
  return `${days} day${days === 1 ? "" : "s"} ago`;
}

export default function Dashboard() {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [active, setActive] = useState("sell_now");

  useEffect(() => {
    let cancelled = false;
    api
      .scanLatest()
      .then((d) => {
        if (!cancelled) setData(d);
      })
      .catch((e) => {
        if (!cancelled) setError(e.message);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  if (error) return <div className="error">Error loading scan: {error}</div>;
  if (!data) return <div className="empty">Loading latest scan…</div>;

  const counts = {
    sell_now: data.sell_now.length,
    buy_sell_later: data.buy_sell_later.length,
    watchlist: data.watchlist.length,
  };
  const rows = data[active] || [];

  return (
    <>
      <div className="header">
        <div>
          <h1>Casino Scanner</h1>
          <div className="subtitle">
            Run #{data.run_id} · {data.tickers_scanned} tickers ·{" "}
            {formatCentral(data.finished_at)}
            {data.finished_at && ` · ${timeAgo(data.finished_at)}`}
          </div>
        </div>
      </div>

      <BucketTabs active={active} counts={counts} onChange={setActive} />

      {rows.length === 0 ? (
        <div className="empty">No tickers in this bucket.</div>
      ) : (
        <div className="grid">
          {rows.map((r) => (
            <ScoreCard key={r.ticker} row={r} />
          ))}
        </div>
      )}
    </>
  );
}
