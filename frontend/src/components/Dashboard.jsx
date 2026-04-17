import React, { useCallback, useEffect, useRef, useState } from "react";

const SORT_OPTIONS = [
  { key: "score",        label: "Score" },
  { key: "iv_rank",      label: "IV Rank" },
  { key: "premium",      label: "Premium $" },
  { key: "iv_hv",        label: "IV/HV" },
  { key: "chain",        label: "Chain" },
  { key: "risk_reward",  label: "Risk/Reward" },
];

const PREM_TIMEFRAMES = [3, 7, 14, 21, 30];

function getBestPremInWindow(row, maxDte) {
  if (!row.expiry_data?.length) return row.atm_call_premium ?? 0;
  const inWindow = row.expiry_data.filter(e => e.dte <= maxDte);
  if (!inWindow.length) return 0;
  return Math.max(...inWindow.map(e => e.atm_call_prem ?? 0));
}

function sortRows(arr, key, premTimeframe = 30) {
  const s = [...arr];
  if (key === "score")       return s.sort((a, b) => b.score - a.score);
  if (key === "iv_rank")     return s.sort((a, b) => (b.iv_rank ?? 0) - (a.iv_rank ?? 0));
  if (key === "premium")     return s.sort((a, b) => getBestPremInWindow(b, premTimeframe) - getBestPremInWindow(a, premTimeframe));
  if (key === "iv_hv")       return s.sort((a, b) => (b.breakdown?.iv_hv ?? 0) - (a.breakdown?.iv_hv ?? 0));
  if (key === "chain")       return s.sort((a, b) => (b.breakdown?.chain ?? 0) - (a.breakdown?.chain ?? 0));
  if (key === "risk_reward") return s.sort((a, b) => (b.safety_score ?? 0) - (a.safety_score ?? 0));
  return s;
}
import { api } from "../api/client.js";
import BucketTabs from "./BucketTabs.jsx";
import PremiumScanner from "./PremiumScanner.jsx";
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
  const [sort, setSort] = useState("score");
  const [premTimeframe, setPremTimeframe] = useState(30);
  const [showAll, setShowAll] = useState(false);
  const [showPremium, setShowPremium] = useState(false);
  const [scanMode, setScanMode] = useState(null); // "normal" | "extensive" | null
  const [sourceFilter, setSourceFilter] = useState("all");
  const [reloadMsg, setReloadMsg] = useState(null);

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
    setScanMode("normal");
    setScanProgress(null);
    try {
      await api.triggerScan();
      startPolling();
    } catch (e) {
      setScanning(false);
      setScanMode(null);
      setScanProgress(null);
    }
  };

  const handleRunExtensiveScan = async () => {
    if (scanning) return;
    setScanning(true);
    setScanMode("extensive");
    setScanProgress(null);
    try {
      await api.triggerScanExtensive();
      startPolling();
    } catch (e) {
      setScanning(false);
      setScanMode(null);
      setScanProgress(null);
    }
  };

  const handleReloadUniverse = async () => {
    try {
      const res = await api.reloadUniverse();
      setReloadMsg(`✓ ${res.message}`);
      setTimeout(() => setReloadMsg(null), 5000);
    } catch (e) {
      setReloadMsg(`Error: ${e.message}`);
      setTimeout(() => setReloadMsg(null), 5000);
    }
  };

  const scanProgressLabel = () => {
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

  const sourceFilterFn = (r) =>
    sourceFilter === "all" || (r.sources || []).includes(sourceFilter);

  const combinedFilter = (r) => priceFilter(r) && sourceFilterFn(r);

  const counts = {
    sell_now: data.sell_now.filter(combinedFilter).length,
    buy_sell_later: data.buy_sell_later.filter(combinedFilter).length,
    watchlist: data.watchlist.filter(combinedFilter).length,
  };

  const allRows = [
    ...data.sell_now,
    ...data.buy_sell_later,
    ...data.watchlist,
  ];
  const filteredAll = allRows.filter(combinedFilter);
  const premiumRows = allRows.filter(combinedFilter);

  // Source counts for filter buttons (from all scan results regardless of bucket)
  const sourceCounts = {
    sp500:    allRows.filter(r => (r.sources||[]).includes("sp500")).length,
    nasdaq100: allRows.filter(r => (r.sources||[]).includes("nasdaq100")).length,
    ai_sector: allRows.filter(r => (r.sources||[]).includes("ai_sector")).length,
    ai_nuclear: allRows.filter(r => (r.sources||[]).includes("ai_nuclear")).length,
    custom:   allRows.filter(r => (r.sources||[]).includes("custom")).length,
  };

  const rows = showAll
    ? sortRows(filteredAll, "score", premTimeframe)
    : sortRows((data[active] || []).filter(combinedFilter), sort, premTimeframe);

  return (
    <>
      <div className="header">
        <div>
          <h1>Casino Scanner</h1>
          <div className="subtitle">
            Run #{data.run_id} · {data.universe_size || data.tickers_scanned} tickers ·{" "}
            {formatCentral(data.finished_at)}
            {data.finished_at && ` · ${timeAgo(data.finished_at)}`}
          </div>
          <button
            className={`prem-view-btn${showPremium ? " active" : ""}`}
            onClick={() => setShowPremium(v => !v)}
          >
            {showPremium ? "← Cards" : "Premium Scanner"}
          </button>
        </div>
        <div className="header-right">
          <button
            className={`scan-btn scan-btn-extensive${scanning ? " scanning" : ""}`}
            onClick={handleRunExtensiveScan}
            disabled={scanning}
          >
            {scanning && scanMode === "extensive" ? scanProgressLabel() : "Extensive Scan"}
          </button>
          <button
            className={`scan-btn${scanning ? " scanning" : ""}`}
            onClick={handleRunScan}
            disabled={scanning}
          >
            {scanning && scanMode === "normal" ? scanProgressLabel() : "Run Scan"}
          </button>
          <div className="reload-universe-wrap">
            <button className="reload-universe-btn" onClick={handleReloadUniverse}>
              Reload Universe
            </button>
            {reloadMsg && <span className="reload-universe-msg">{reloadMsg}</span>}
          </div>
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

      {!showPremium && (
        <div className="sort-bar">
          <span className="sort-label">Sort</span>
          {SORT_OPTIONS.map(opt => (
            <button
              key={opt.key}
              className={`sort-btn${sort === opt.key ? " active" : ""}`}
              onClick={() => setSort(opt.key)}
            >{opt.label}</button>
          ))}
          {sort === "premium" && (
            <span className="sort-timeframe">
              <span className="sort-label" style={{ marginLeft: 10 }}>within</span>
              {PREM_TIMEFRAMES.map(d => (
                <button
                  key={d}
                  className={`sort-btn${premTimeframe === d ? " active" : ""}`}
                  onClick={() => setPremTimeframe(d)}
                >{d}d</button>
              ))}
            </span>
          )}
        </div>
      )}

      <TopMovers movers={movers} />

      {showPremium ? (
        <PremiumScanner rows={premiumRows} onRowClick={setSelectedRow} />
      ) : (
        <>
          <div className="source-filter-row">
            {[
              { key: "all",        label: "All",       count: allRows.length },
              { key: "sp500",      label: "S&P 500",   count: sourceCounts.sp500 },
              { key: "nasdaq100",  label: "Nasdaq 100",count: sourceCounts.nasdaq100 },
              { key: "ai_sector",  label: "AI Sector", count: sourceCounts.ai_sector },
              { key: "ai_nuclear", label: "Nuclear",   count: sourceCounts.ai_nuclear },
              { key: "custom",     label: "Custom",    count: sourceCounts.custom },
            ].map(({ key, label, count }) => count > 0 || key === "all" ? (
              <button
                key={key}
                className={`source-filter-btn${sourceFilter === key ? " active" : ""}`}
                onClick={() => { setSourceFilter(key); setShowAll(false); }}
              >
                {label}
                <span className="source-filter-count">{count}</span>
              </button>
            ) : null)}
          </div>
          <div className="tabs-row">
            {!showAll && <BucketTabs active={active} counts={counts} onChange={(k) => { setActive(k); setShowAll(false); }} />}
            {showAll && <div className="tabs-spacer" />}
            <button
              className={`show-all-btn${showAll ? " active" : ""}`}
              onClick={() => setShowAll(v => !v)}
            >
              {showAll ? `Show Buckets` : `Show All (${allRows.length})`}
            </button>
          </div>
          {rows.length === 0 ? (
            <div className="empty">No tickers in this bucket.</div>
          ) : (
            <div className="grid">
              {rows.map((r) => (
                <ScoreCard key={r.ticker} row={r} showBucket={showAll} onClick={() => setSelectedRow(r)} />
              ))}
            </div>
          )}
        </>
      )}
      {selectedRow && (
        <TickerModal row={selectedRow} onClose={() => setSelectedRow(null)} />
      )}
    </>
  );
}
