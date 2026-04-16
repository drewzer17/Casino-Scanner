import React, { useEffect, useState } from "react";
import { api } from "../api/client.js";
import BucketTabs from "./BucketTabs.jsx";
import ScoreCard from "./ScoreCard.jsx";

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
            {data.finished_at ? new Date(data.finished_at).toLocaleString() : "in progress"}
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
