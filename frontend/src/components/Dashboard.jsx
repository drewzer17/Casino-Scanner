import React, { useCallback, useEffect, useRef, useState } from "react";
import { api } from "../api/client.js";
import BucketTabs from "./BucketTabs.jsx";
import ScoreCard from "./ScoreCard.jsx";
import TickerModal from "./TickerModal.jsx";

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

const BUCKET_LABEL = {
  sell_now: "Sell Now",
  buy_sell_later: "Buy Later",
  watchlist: "Watchlist",
};

function TopMovers({ movers }) {
  if (!movers || (movers.gainers.length === 0 && movers.losers.length === 0)) return null;

  const renderList = (list, isGain) =>
    list.map((m) => (
      <div key={m.ticker} className="mover-row">
        <span className="mover-ticker">{m.ticker}</span>
        <span className="mover-scores">
          {m.prev_score != null ? m.prev_score.toFixed(0) : "—"} → {m.score.toFixed(0)}
          {m.prev_bucket && m.prev_bucket !== m.bucket && (
            <> · <span style={{ color: isGain ? "var(--green)" : "var(--red)" }}>
              {BUCKET_LABEL[m.prev_bucket] ?? m.prev_bucket} → {BUCKET_LABEL[m.bucket] ?? m.bucket}
            </span></>
          )}
        </span>
        <span className={`mover-delta ${isGain ? "gain" : "loss"}`}>
          {isGain ? "+" : ""}{m.delta.toFixed(1)}
        </span>
      </div>
    ));

  return (
    <div className="movers-panel">
      <div className="movers-col">
        <div className="movers-col-title up">▲ Top Gainers (7d)</div>
        {movers.gainers.length > 0
          ? renderList(movers.gainers, true)
          : <div style={{ color: "var(--text-muted)", fontSize: 12 }}>Not enough history yet</div>}
      </div>
      <div className="movers-col">
        <div className="movers-col-title dn">▼ Top Losers (7d)</div>
        {movers.losers.length > 0
          ? renderList(movers.losers, false)
          : <div style={{ color: "var(--text-muted)", fontSize: 12 }}>Not enough history yet</div>}
      </div>
    </div>
  );
}

export default function Dashboard() {
  const [data, setData] = useState(null);
  const [movers, setMovers] = useState(null);
  const [error, setError] = useState(null);
  const [active, setActive] = useState("sell_now");

  // Ticker detail modal
  const [selectedRow, setSelectedRow] = useState(null);

  // Price display filter
  const [minPrice, setMinPrice] = useState(10);
  const [maxPrice, setMaxPrice] = useState(300);

  // Scan trigger state
  const [scanning, setScanning] = useState(false);
  const [scanProgress, setScanProgress] = useState(null);
  const pollRef = useRef(null);

  const stopPolling = () => {
    if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
  };

  const refreshData = useCallback(() => {
    api.scanLatest()
      .then(setData)
      .catch((e) => setError(e.message));
    api.movers()
      .then(setMovers)
      .catch(() => {});
  }, []);

  const startPolling = useCallback(() => {
    stopPolling();
    pollRef.current = setInterval(async () => {
      try {
        const status = await api.scanStatus();
        setScanProgress(status);
        if (status.status === "completed" || status.status === "failed") {
          stopPolling();
          setScanning(false);
          setScanProgress(null);
          if (status.status === "completed") {
            refreshData();
          }
        }
      } catch {
        // polling errors are non-fatal
      }
    }, 5000);
  }, [refreshData]);

  // On mount: fetch data, and also check if a scan is already running
  useEffect(() => {
    let cancelled = false;

    api.scanLatest()
      .then((d) => { if (!cancelled) setData(d); })
      .catch((e) => { if (!cancelled) setError(e.message); });
    api.movers()
      .then((m) => { if (!cancelled) setMovers(m); })
      .catch(() => {});

    // Check if a scan is already in progress so the button reflects current state
    api.scanStatus()
      .then((status) => {
        if (!cancelled && status.status === "running") {
          setScanning(true);
          setScanProgress(status);
          startPolling();
        }
      })
      .catch(() => {});

    return () => {
      cancelled = true;
      stopPolling();
    };
  }, [startPolling]);

  const handleRunScan = async () => {
    if (scanning) return;
    setScanning(true);
    setScanProgress(null);
    try {
      await api.triggerScan();
      startPolling();
    } catch (e) {
      setScanning(false);
      setScanProgress(null);
    }
  };

  const scanBtnLabel = () => {
    if (!scanning) return "Run Scan";
    if (scanProgress) {
      const done = scanProgress.tickers_scanned ?? 0;
      const total = scanProgress.tickers_total ?? 500;
      return `Scanning… ${done}/${total}`;
    }
    return "Starting…";
  };

  if (error) return <div className="error">Error loading scan: {error}</div>;
  if (!data) return <div className="empty">Loading latest scan…</div>;

  const priceFilter = (r) =>
    (r.price == null) || (r.price >= minPrice && r.price <= maxPrice);

  const counts = {
    sell_now: data.sell_now.filter(priceFilter).length,
    buy_sell_later: data.buy_sell_later.filter(priceFilter).length,
    watchlist: data.watchlist.filter(priceFilter).length,
  };
  const rows = (data[active] || []).filter(priceFilter);

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
        <div className="header-right">
          <button
            className={`scan-btn${scanning ? " scanning" : ""}`}
            onClick={handleRunScan}
            disabled={scanning}
          >
            {scanBtnLabel()}
          </button>
          <div className="price-filter">
            <label className="price-filter-label">Price</label>
            <span className="price-filter-prefix">$</span>
            <input
              className="price-filter-input"
              type="number"
              min={0}
              max={maxPrice}
              value={minPrice}
              onChange={(e) => setMinPrice(Number(e.target.value))}
              placeholder="Min"
            />
            <span className="price-filter-sep">–</span>
            <span className="price-filter-prefix">$</span>
            <input
              className="price-filter-input"
              type="number"
              min={minPrice}
              value={maxPrice}
              onChange={(e) => setMaxPrice(Number(e.target.value))}
              placeholder="Max"
            />
          </div>
        </div>
      </div>

      <TopMovers movers={movers} />

      <BucketTabs active={active} counts={counts} onChange={setActive} />

      {rows.length === 0 ? (
        <div className="empty">No tickers in this bucket.</div>
      ) : (
        <div className="grid">
          {rows.map((r) => (
            <ScoreCard key={r.ticker} row={r} onClick={() => setSelectedRow(r)} />
          ))}
        </div>
      )}
      {selectedRow && (
        <TickerModal row={selectedRow} onClose={() => setSelectedRow(null)} />
      )}
    </>
  );
}
