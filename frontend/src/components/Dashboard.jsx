import React, { useCallback, useEffect, useRef, useState } from "react";

const SORT_OPTIONS = [
  { key: "score",        label: "Score" },
  { key: "iv_rank",      label: "IV Rank" },
  { key: "premium",      label: "Premium $" },
  { key: "iv_hv",        label: "IV/HV" },
  { key: "chain",        label: "Chain" },
  { key: "risk_reward",  label: "Risk/Reward" },
  { key: "cc_score",     label: "CC Score" },
  { key: "csp_score",    label: "CSP Score" },
  { key: "iv_ramp",      label: "IV Ramp Score" },
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
  if (key === "cc_score")    return s.sort((a, b) => (b.cc_score ?? 0) - (a.cc_score ?? 0));
  if (key === "csp_score")   return s.sort((a, b) => (b.csp_score ?? 0) - (a.csp_score ?? 0));
  if (key === "iv_ramp")     return s.sort((a, b) => (b.iv_ramp_score ?? 0) - (a.iv_ramp_score ?? 0));
  return s;
}

export function rangeScore(row) {
  if (row.resistance_1 == null || row.support_1 == null || row.price == null) return null;
  const span = row.resistance_1 - row.support_1;
  if (span <= 0) return null;
  return Math.max(0, Math.min(100, ((row.price - row.support_1) / span) * 100));
}

function DualSlider({ min, max, value, onChange, step = 1, fmt = v => String(v) }) {
  const [lo, hi] = value;
  const [editLo, setEditLo] = useState(null);
  const [editHi, setEditHi] = useState(null);
  const pctLo = ((lo - min) / (max - min)) * 100;
  const pctHi = ((hi - min) / (max - min)) * 100;

  const commitLo = (raw) => {
    const v = Number(raw);
    if (!isNaN(v)) onChange([Math.min(Math.max(v, min), hi), hi]);
    setEditLo(null);
  };
  const commitHi = (raw) => {
    const v = Number(raw);
    if (!isNaN(v)) onChange([lo, Math.max(Math.min(v, max), lo)]);
    setEditHi(null);
  };

  return (
    <div className="ds-wrap">
      <div className="ds-vals">
        {editLo !== null ? (
          <input className="ds-edit-input" type="number" value={editLo}
            onChange={e => setEditLo(e.target.value)}
            onBlur={() => commitLo(editLo)}
            onKeyDown={e => { if (e.key === "Enter") commitLo(editLo); if (e.key === "Escape") setEditLo(null); }}
            autoFocus
          />
        ) : (
          <span className="ds-val-clickable" onClick={() => setEditLo(String(lo))}>{fmt(lo)}</span>
        )}
        {editHi !== null ? (
          <input className="ds-edit-input" type="number" value={editHi}
            onChange={e => setEditHi(e.target.value)}
            onBlur={() => commitHi(editHi)}
            onKeyDown={e => { if (e.key === "Enter") commitHi(editHi); if (e.key === "Escape") setEditHi(null); }}
            autoFocus
          />
        ) : (
          <span className="ds-val-clickable" onClick={() => setEditHi(String(hi))}>{fmt(hi)}</span>
        )}
      </div>
      <div className="ds-track">
        <div className="ds-fill" style={{ left: `${pctLo}%`, width: `${pctHi - pctLo}%` }} />
        <input type="range" className="ds-input" min={min} max={max} step={step} value={lo}
          onChange={e => { const v = Number(e.target.value); onChange([Math.min(v, hi), hi]); }}
        />
        <input type="range" className="ds-input" min={min} max={max} step={step} value={hi}
          onChange={e => { const v = Number(e.target.value); onChange([lo, Math.max(v, lo)]); }}
        />
      </div>
    </div>
  );
}

import { api } from "../api/client.js";
import BucketTabs from "./BucketTabs.jsx";
import AsymmetricScanner from "./AsymmetricScanner.jsx";
import { calcAsymmetricFlags } from "../utils/asymmetric.js";
import IvRampScanner from "./IvRampScanner.jsx";
import PremiumScanner from "./PremiumScanner.jsx";
import RangeScanner from "./RangeScanner.jsx";
import ScoreCard from "./ScoreCard.jsx";
import TickerModal from "./TickerModal.jsx";

function toUtcDate(isoString) {
  if (!isoString) return null;
  const s = isoString.endsWith("Z") || isoString.includes("+") ? isoString : isoString + "Z";
  return new Date(s);
}

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
  const [asymOnly, setAsymOnly] = useState(false);
  const [view, setView] = useState("cards"); // "cards" | "premium" | "range" | "ivramp" | "asymmetric"
  const [scanMode, setScanMode] = useState(null); // "normal" | "extensive" | null
  const [sourceFilter, setSourceFilter] = useState("all");
  const [reloadMsg, setReloadMsg] = useState(null);

  // Ticker detail modal
  const [selectedRow, setSelectedRow] = useState(null);

  // Search
  const [searchQuery, setSearchQuery] = useState("");

  // Price display filter
  const [minPrice, setMinPrice] = useState(10);
  const [maxPrice, setMaxPrice] = useState(300);

  // Mode selector
  const [mode, setMode] = useState("all"); // "all" | "cc" | "csp"
  const [filtersOpen, setFiltersOpen] = useState(false);

  // Advanced filters
  const [crossFilter, setCrossFilter] = useState("all");
  const [trendFilter, setTrendFilter] = useState("all");
  const [signalFilter, setSignalFilter] = useState("all");
  const [ivRankRange, setIvRankRange] = useState([0, 100]);
  const [premRange, setPremRange] = useState([0, 50]);
  const [oiRange, setOiRange] = useState([0, 20000]);
  const [safetyRange, setSafetyRange] = useState([0, 5000]);
  const [ccScoreRange, setCcScoreRange] = useState([0, 100]);
  const [cspScoreRange, setCspScoreRange] = useState([0, 100]);
  const [r2DistRange, setR2DistRange] = useState([0, 50]);
  const [r1DistRange, setR1DistRange] = useState([0, 50]);
  const [s1DistRange, setS1DistRange] = useState([0, 50]);
  const [s2DistRange, setS2DistRange] = useState([0, 50]);
  const [spreadRange, setSpreadRange] = useState([0, 50]);
  const [ivRampScoreRange, setIvRampScoreRange] = useState([0, 100]);

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

  useEffect(() => {
    let cancelled = false;

    api.scanLatest()
      .then((d) => { if (!cancelled) setData(d); })
      .catch((e) => { if (!cancelled) setError(e.message); });
    api.movers()
      .then((m) => { if (!cancelled) setMovers(m); })
      .catch(() => {});

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

  // Auto-sort and reset filters when mode changes
  useEffect(() => {
    if (mode === "cc")       setSort("cc_score");
    else if (mode === "csp") setSort("csp_score");
    else                     setSort("score");
    setCrossFilter("all");
    setTrendFilter("all");
    setSignalFilter("all");
    setIvRankRange([0, 100]);
    setPremRange([0, 50]);
    setOiRange([0, 20000]);
    setSafetyRange([0, 5000]);
    setCcScoreRange([0, 100]);
    setCspScoreRange([0, 100]);
    setR2DistRange([0, 50]);
    setR1DistRange([0, 50]);
    setS1DistRange([0, 50]);
    setS2DistRange([0, 50]);
    setSpreadRange([0, 50]);
    setIvRampScoreRange([0, 100]);
  }, [mode]);

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

  // ── Base filters ──────────────────────────────────────────────────
  const priceFilter = (r) =>
    (r.price == null) || (r.price >= minPrice && r.price <= maxPrice);

  const sourceFilterFn = (r) =>
    sourceFilter === "all" || (r.sources || []).includes(sourceFilter);

  const searchFilter = (r) => {
    if (!searchQuery) return true;
    const q = searchQuery.toLowerCase();
    return r.ticker.toLowerCase().includes(q) ||
      (r.company_name || "").toLowerCase().includes(q);
  };

  // ── Advanced toggle filters ───────────────────────────────────────
  const crossFn = (r, opt = crossFilter) => {
    if (opt === "golden") return r.sma_golden_cross === true;
    if (opt === "death")  return r.sma_golden_cross === false;
    return true;
  };

  const trendFn = (r, opt = trendFilter) => {
    if (opt === "all") return true;
    return r.sma_regime === opt;
  };

  const signalFn = (r, opt = signalFilter) => {
    if (opt === "all") return true;
    const rs = rangeScore(r);
    if (opt === "price_discovery")  return r.resistance_1 == null;
    if (opt === "near_support")     return rs !== null && rs <= 30;
    if (opt === "near_resistance")  return rs !== null && rs >= 70;
    return true;
  };

  // ── Slider filters ────────────────────────────────────────────────
  const sliderFn = (r) => {
    if (r.iv_rank != null &&
        (r.iv_rank < ivRankRange[0] || r.iv_rank > ivRankRange[1])) return false;
    if (r.atm_call_premium != null &&
        (r.atm_call_premium < premRange[0] || r.atm_call_premium > premRange[1])) return false;
    if (r.open_interest != null &&
        (r.open_interest < oiRange[0] || r.open_interest > oiRange[1])) return false;
    if (mode === "all" && r.safety_score != null &&
        (r.safety_score < safetyRange[0] || r.safety_score > safetyRange[1])) return false;
    if (mode === "cc"  && (r.cc_score ?? 0) < (r.csp_score ?? 0)) return false;
    if (mode === "csp" && (r.csp_score ?? 0) < (r.cc_score ?? 0)) return false;
    if (mode !== "csp" && r.cc_score != null &&
        (r.cc_score < ccScoreRange[0] || r.cc_score > ccScoreRange[1])) return false;
    if (mode !== "cc" && r.csp_score != null &&
        (r.csp_score < cspScoreRange[0] || r.csp_score > cspScoreRange[1])) return false;
    if (r.resistance_2 != null && r.price != null && r.price > 0) {
      const dist = ((r.resistance_2 - r.price) / r.price) * 100;
      if (dist < r2DistRange[0] || dist > r2DistRange[1]) return false;
    }
    if (r.resistance_1 != null && r.price != null && r.price > 0) {
      const dist = ((r.resistance_1 - r.price) / r.price) * 100;
      if (dist < r1DistRange[0] || dist > r1DistRange[1]) return false;
    }
    if (r.support_1 != null && r.price != null && r.price > 0) {
      const dist = ((r.price - r.support_1) / r.price) * 100;
      if (dist < s1DistRange[0] || dist > s1DistRange[1]) return false;
    }
    if (r.support_2 != null && r.price != null && r.price > 0) {
      const dist = ((r.price - r.support_2) / r.price) * 100;
      if (dist < s2DistRange[0] || dist > s2DistRange[1]) return false;
    }
    if (r.bid_ask_spread_pct != null) {
      const spr = r.bid_ask_spread_pct * 100;
      if (spr < spreadRange[0] || spr > spreadRange[1]) return false;
    }
    if (r.iv_ramp_score != null &&
        (r.iv_ramp_score < ivRampScoreRange[0] || r.iv_ramp_score > ivRampScoreRange[1])) return false;
    return true;
  };

  const baseNoSourceFn = (r) => priceFilter(r) && searchFilter(r) && sliderFn(r);
  const baseFn = (r) => baseNoSourceFn(r) && sourceFilterFn(r);
  const combinedFilter = (r) => baseFn(r) && crossFn(r) && trendFn(r) && signalFn(r);

  // When a search query is active, it overrides ALL other filters
  const isSearching = searchQuery.trim().length > 0;

  const resetFilters = () => {
    setMode("all"); // triggers the mode useEffect which resets everything else
  };

  // ── Signal options per mode ───────────────────────────────────────
  const signalOpts = mode === "cc"
    ? [
        { key: "all",             label: "All" },
        { key: "price_discovery", label: "Price Discovery", cls: "pd" },
        { key: "near_resistance", label: "Near Resistance" },
      ]
    : mode === "csp"
    ? [
        { key: "all",         label: "All" },
        { key: "near_support", label: "Near Support" },
      ]
    : [
        { key: "all",             label: "All" },
        { key: "price_discovery", label: "Price Discovery", cls: "pd" },
        { key: "near_support",    label: "Near Support" },
        { key: "near_resistance", label: "Near Resistance" },
      ];

  // ── Row sets ──────────────────────────────────────────────────────
  const counts = {
    sell_now:      data.sell_now.filter(combinedFilter).length,
    buy_sell_later: data.buy_sell_later.filter(combinedFilter).length,
    watchlist:     data.watchlist.filter(combinedFilter).length,
  };

  const allRows = [
    ...data.sell_now,
    ...data.buy_sell_later,
    ...data.watchlist,
  ].map(r => ({ ...r, ...calcAsymmetricFlags(r) }));
  const filteredAll = allRows.filter(combinedFilter);
  // Search overrides all filters — shows every ticker/company name match regardless of price, sliders, etc.
  const viewRows = isSearching ? allRows.filter(searchFilter) : allRows.filter(combinedFilter);

  // Cascading counts for toggle filters (each group excludes itself)
  const countForCross  = (opt) => allRows.filter(r => baseFn(r) && trendFn(r) && signalFn(r) && crossFn(r, opt)).length;
  const countForTrend  = (opt) => allRows.filter(r => baseFn(r) && crossFn(r) && signalFn(r) && trendFn(r, opt)).length;
  const countForSignal = (opt) => allRows.filter(r => baseFn(r) && crossFn(r) && trendFn(r) && signalFn(r, opt)).length;

  // Cascading source counts (excludes source filter itself)
  const countForSource = (key) => allRows.filter(r =>
    baseNoSourceFn(r) && crossFn(r) && trendFn(r) && signalFn(r) &&
    (key === "all" || (r.sources||[]).includes(key))
  ).length;

  const rows = isSearching
    ? sortRows(allRows.filter(searchFilter), sort, premTimeframe)
    : showAll
      ? sortRows(filteredAll, sort, premTimeframe)
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
          <div className="view-btns">
            <button
              className={`prem-view-btn${view === "premium" ? " active" : ""}`}
              onClick={() => setView(v => v === "premium" ? "cards" : "premium")}
            >
              {view === "premium" ? "← Cards" : "Premium Scanner"}
            </button>
            <button
              className={`range-view-btn${view === "range" ? " active" : ""}`}
              onClick={() => setView(v => v === "range" ? "cards" : "range")}
            >
              {view === "range" ? "← Cards" : "Range Scanner"}
            </button>
            <button
              className={`ivramp-view-btn${view === "ivramp" ? " active" : ""}`}
              onClick={() => setView(v => v === "ivramp" ? "cards" : "ivramp")}
            >
              {view === "ivramp" ? "← Cards" : "IV Ramp ↑"}
            </button>
            <button
              className={`asym-view-btn${view === "asymmetric" ? " active" : ""}`}
              onClick={() => setView(v => v === "asymmetric" ? "cards" : "asymmetric")}
            >
              {view === "asymmetric" ? "← Cards" : "⬟ Asymmetric Setups"}
            </button>
          </div>
          <div className="search-wrap">
            <input
              className="search-input"
              type="text"
              placeholder="Search ticker or company…"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
            />
            {searchQuery && (
              <button className="search-clear" onClick={() => setSearchQuery("")}>✕</button>
            )}
          </div>
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

      {/* ── Mode Selector ── */}
      <div className="mode-selector">
        <div className="mode-btns">
          <button
            className={`mode-btn mode-btn-all${mode === "all" ? " active" : ""}`}
            onClick={() => setMode("all")}
          >ALL</button>
          <button
            className={`mode-btn mode-btn-cc${mode === "cc" ? " active" : ""}`}
            onClick={() => setMode("cc")}
          >CC MODE</button>
          <button
            className={`mode-btn mode-btn-csp${mode === "csp" ? " active" : ""}`}
            onClick={() => setMode("csp")}
          >CSP MODE</button>
        </div>
        <div className="mode-selector-right">
          <button
            className={`filters-collapse-btn${filtersOpen ? " open" : ""}`}
            onClick={() => setFiltersOpen(v => !v)}
          >{filtersOpen ? "▲ Filters" : "▼ Filters"}</button>
          <button className="filter-reset-btn" onClick={resetFilters}>Reset</button>
        </div>
      </div>

      {/* ── Collapsible filter bar ── */}
      {filtersOpen && (
        <div className="filter-bar">
          <div className="filter-toggles">
            {/* CROSS */}
            <div className="filter-group">
              <span className="filter-group-label">CROSS</span>
              {[
                { key: "all",    label: "All" },
                { key: "golden", label: "Golden Cross" },
                { key: "death",  label: "Death Cross" },
              ].map(opt => (
                <button
                  key={opt.key}
                  className={`filter-toggle-btn${crossFilter === opt.key ? " active" : ""}${crossFilter === opt.key && opt.key === "golden" ? " golden" : ""}${crossFilter === opt.key && opt.key === "death" ? " death" : ""}`}
                  onClick={() => setCrossFilter(opt.key)}
                >
                  {opt.label}
                  {opt.key !== "all" && (
                    <span className="filter-count">{countForCross(opt.key)}</span>
                  )}
                </button>
              ))}
            </div>

            {/* TREND */}
            <div className="filter-group">
              <span className="filter-group-label">TREND</span>
              {[
                { key: "all",          label: "All" },
                { key: "UPTREND",      label: "Uptrend" },
                { key: "DOWNTREND",    label: "Downtrend" },
                { key: "TRANSITIONAL", label: "Transitional" },
              ].map(opt => (
                <button
                  key={opt.key}
                  className={`filter-toggle-btn${trendFilter === opt.key ? " active" : ""}`}
                  onClick={() => setTrendFilter(opt.key)}
                >
                  {opt.label}
                  {opt.key !== "all" && (
                    <span className="filter-count">{countForTrend(opt.key)}</span>
                  )}
                </button>
              ))}
            </div>

            {/* SIGNAL */}
            <div className="filter-group">
              <span className="filter-group-label">SIGNAL</span>
              {signalOpts.map(opt => (
                <button
                  key={opt.key}
                  className={`filter-toggle-btn${signalFilter === opt.key ? " active" : ""}${signalFilter === opt.key && opt.cls ? ` ${opt.cls}` : ""}`}
                  onClick={() => setSignalFilter(opt.key)}
                >
                  {opt.label}
                  {opt.key !== "all" && (
                    <span className="filter-count">{countForSignal(opt.key)}</span>
                  )}
                </button>
              ))}
            </div>

            {/* SOURCE */}
            <div className="filter-group">
              <span className="filter-group-label">SOURCE</span>
              {[
                { key: "all",        label: "All" },
                { key: "sp500",      label: "S&P 500" },
                { key: "nasdaq100",  label: "Nasdaq 100" },
                { key: "russell1000",label: "Russell 1000" },
                { key: "russell2000",label: "Russell 2000" },
                { key: "ai_sector",  label: "AI Sector" },
                { key: "ai_nuclear", label: "Nuclear" },
                { key: "etf",        label: "ETF" },
                { key: "custom",     label: "Custom" },
              ].map(opt => (
                <button
                  key={opt.key}
                  className={`filter-toggle-btn${sourceFilter === opt.key ? " active" : ""}`}
                  onClick={() => setSourceFilter(opt.key)}
                >
                  {opt.label}
                  {opt.key !== "all" && (
                    <span className="filter-count">{countForSource(opt.key)}</span>
                  )}
                </button>
              ))}
            </div>
          </div>

          <div className="filter-sliders">
            {mode !== "csp" && (
              <div className="filter-slider-item">
                <span className="filter-slider-label">CC SCORE</span>
                <DualSlider min={0} max={100} value={ccScoreRange} onChange={setCcScoreRange}
                  fmt={v => `${v}`} />
              </div>
            )}
            {mode !== "cc" && (
              <div className="filter-slider-item">
                <span className="filter-slider-label">CSP SCORE</span>
                <DualSlider min={0} max={100} value={cspScoreRange} onChange={setCspScoreRange}
                  fmt={v => `${v}`} />
              </div>
            )}
            <div className="filter-slider-item">
              <span className="filter-slider-label">IV RANK</span>
              <DualSlider min={0} max={100} value={ivRankRange} onChange={setIvRankRange}
                fmt={v => `${v}`} />
            </div>
            <div className="filter-slider-item">
              <span className="filter-slider-label">PREMIUM $</span>
              <DualSlider min={0} max={50} step={0.5} value={premRange} onChange={setPremRange}
                fmt={v => `$${Number(v).toFixed(1)}`} />
            </div>
            <div className="filter-slider-item">
              <span className="filter-slider-label">CHAIN OI</span>
              <DualSlider min={0} max={20000} step={50} value={oiRange} onChange={setOiRange}
                fmt={v => v >= 1000 ? `${(v / 1000).toFixed(0)}K` : `${v}`} />
            </div>
            <div className="filter-slider-item">
              <span className="filter-slider-label">R2 DISTANCE</span>
              <DualSlider min={0} max={50} step={0.5} value={r2DistRange} onChange={setR2DistRange}
                fmt={v => `${Number(v).toFixed(0)}%`} />
            </div>
            <div className="filter-slider-item">
              <span className="filter-slider-label">R1 DISTANCE</span>
              <DualSlider min={0} max={50} step={0.5} value={r1DistRange} onChange={setR1DistRange}
                fmt={v => `${Number(v).toFixed(0)}%`} />
            </div>
            <div className="filter-slider-item">
              <span className="filter-slider-label">S1 DISTANCE</span>
              <DualSlider min={0} max={50} step={0.5} value={s1DistRange} onChange={setS1DistRange}
                fmt={v => `${Number(v).toFixed(0)}%`} />
            </div>
            <div className="filter-slider-item">
              <span className="filter-slider-label">S2 DISTANCE</span>
              <DualSlider min={0} max={50} step={0.5} value={s2DistRange} onChange={setS2DistRange}
                fmt={v => `${Number(v).toFixed(0)}%`} />
            </div>
            <div className="filter-slider-item">
              <span className="filter-slider-label">SPREAD %</span>
              <DualSlider min={0} max={50} step={1} value={spreadRange} onChange={setSpreadRange}
                fmt={v => `${v}%`} />
            </div>
            <div className="filter-slider-item">
              <span className="filter-slider-label">IV RAMP SCORE</span>
              <DualSlider min={0} max={100} value={ivRampScoreRange} onChange={setIvRampScoreRange}
                fmt={v => `${v}`} />
            </div>
            {mode === "all" && (
              <div className="filter-slider-item">
                <span className="filter-slider-label">SAFETY SCORE</span>
                <DualSlider min={0} max={5000} step={10} value={safetyRange} onChange={setSafetyRange}
                  fmt={v => `${v}`} />
              </div>
            )}
          </div>
        </div>
      )}

      {view === "cards" && (
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

      {view === "premium" ? (
        <PremiumScanner rows={viewRows} onRowClick={setSelectedRow} />
      ) : view === "range" ? (
        <RangeScanner rows={viewRows} onRowClick={setSelectedRow} />
      ) : view === "ivramp" ? (
        <IvRampScanner rows={viewRows} onRowClick={setSelectedRow} />
      ) : view === "asymmetric" ? (
        <AsymmetricScanner rows={viewRows} onRowClick={setSelectedRow} />
      ) : (
        <>
          <div className="tabs-row">
            {!showAll && <BucketTabs active={active} counts={counts} onChange={(k) => { setActive(k); setShowAll(false); }} />}
            {showAll && <div className="tabs-spacer" />}
            <button
              className={`asym-only-btn${asymOnly ? " active" : ""}`}
              onClick={() => setAsymOnly(v => !v)}
            >
              {asymOnly ? "Asymmetric Only ✓" : `Asymmetric Only (${allRows.filter(r => r.asymmetric_any_flag).length})`}
            </button>
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
              {rows.filter(r => !asymOnly || r.asymmetric_any_flag).map((r) => (
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
