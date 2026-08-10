import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { api } from "../api/client.js";
import ResearchAsterisk from "./ResearchAsterisk.jsx";
import ResearchPanel from "./ResearchPanel.jsx";

// Synthesized two-tone "ding" via Web Audio API — no external asset or
// pre-recorded file needed, so there's nothing to fabricate or get wrong.
let _audioCtx = null;
function playDing() {
  try {
    const Ctx = window.AudioContext || window.webkitAudioContext;
    if (!Ctx) return;
    if (!_audioCtx) _audioCtx = new Ctx();
    const ctx = _audioCtx;
    const now = ctx.currentTime;
    [880, 1320].forEach((freq, i) => {
      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.type = "sine";
      osc.frequency.value = freq;
      const start = now + i * 0.14;
      gain.gain.setValueAtTime(0, start);
      gain.gain.linearRampToValueAtTime(0.35, start + 0.02);
      gain.gain.exponentialRampToValueAtTime(0.001, start + 0.35);
      osc.connect(gain).connect(ctx.destination);
      osc.start(start);
      osc.stop(start + 0.4);
    });
  } catch (e) {
    // audio is a nice-to-have alert, never let it break the scanner
  }
}

// "edge" is intentionally not in this list — it needs a mode toggle in its
// header and a range-aware cell, so it's rendered explicitly, spliced in
// between stock_price and breakeven (see EDGE_COL_INDEX below).
const COLS = [
  { key: "ticker",              label: "Ticker",     align: "left",   type: "text" },
  { key: "expiration",          label: "Expiration", align: "left",   type: "text" },
  { key: "strike",              label: "Strike",     align: "right",  type: "number" },
  { key: "call_ask",            label: "Call Ask",   align: "right",  type: "number" },
  { key: "call_bid",            label: "Call Bid",   align: "right",  type: "number" },
  { key: "stock_price",         label: "Stock Ask",  align: "right",  type: "number" },
  { key: "breakeven",           label: "Breakeven",  align: "right",  type: "number" },
  { key: "earnings_in_window",  label: "Earnings",   align: "center", type: "bool" },
];
const EDGE_COL_INDEX = 6; // splice point: after stock_price, before breakeven
const COL_TYPE = Object.fromEntries(COLS.map((c) => [c.key, c.type]));
const TOTAL_COLS = COLS.length + 1; // +1 for the spliced-in edge column

const EDGE_MODES = ["ask", "bid", "mid", "range"];

// range mode sorts/floors on the bid (worst case) per spec.
function effectiveEdge(row, mode) {
  if (mode === "ask") return row.edge_ask;
  if (mode === "bid") return row.edge_bid;
  if (mode === "range") return row.edge_bid;
  return row.edge_mid;
}

function compareRows(a, b, key) {
  const type = COL_TYPE[key];
  if (type === "text") {
    return String(a[key] ?? "").localeCompare(String(b[key] ?? ""));
  }
  if (type === "bool") {
    return (a[key] ? 1 : 0) - (b[key] ? 1 : 0);
  }
  const av = a[key];
  const bv = b[key];
  if (av == null && bv == null) return 0;
  if (av == null) return -1;
  if (bv == null) return 1;
  return av - bv;
}

function fmtMoney(v) {
  if (v == null) return "—";
  return `$${Number(v).toFixed(2)}`;
}

function fmtEdge(v) {
  if (v == null) return "—";
  const n = Number(v);
  return `${n < 0 ? "-" : ""}$${Math.abs(n).toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
}

function fmtEdgeCell(row, mode) {
  if (mode === "range") {
    const lo = row.edge_bid;
    const hi = row.edge_ask;
    if (lo == null) return "—";
    if (hi == null) return fmtEdge(lo);
    return `${fmtEdge(lo)}–${fmtEdge(hi)}`;
  }
  return fmtEdge(effectiveEdge(row, mode));
}

function timeAgo(iso) {
  if (!iso) return "never";
  const then = new Date(iso).getTime();
  const diffSec = Math.max(0, Math.floor((Date.now() - then) / 1000));
  if (diffSec < 60) return `${diffSec}s ago`;
  const m = Math.floor(diffSec / 60);
  if (m < 60) return `${m}m ago`;
  return `${Math.floor(m / 60)}h ${m % 60}m ago`;
}

function SortableTh({ c, sort, onSort }) {
  const active = sort.key === c.key;
  return (
    <th
      className="mispriced-sortable-th"
      style={{ textAlign: c.align }}
      onClick={() => onSort(c.key)}
      title={`Sort by ${c.label}`}
    >
      {c.label}
      <span className={`mispriced-sort-arrow${active ? " active" : ""}`}>
        {active ? (sort.dir === "asc" ? " ▲" : " ▼") : ""}
      </span>
    </th>
  );
}

export default function MispricedScanner() {
  const [state, setState] = useState(null);
  const [floorInput, setFloorInput] = useState("500");
  const [loading, setLoading] = useState(false);
  const [flashKeys, setFlashKeys] = useState(new Set());
  const [error, setError] = useState(null);
  const [sort, setSort] = useState({ key: "edge", dir: "desc" });
  const [panelTicker, setPanelTicker] = useState(null);
  // Global edge display mode. Page-session only (plain state, no storage) —
  // recomputes from rows already in hand, never triggers a new sweep.
  const [edgeMode, setEdgeMode] = useState("mid");
  const pollRef = useRef(null);
  const editingFloorRef = useRef(false);

  function handleSort(key) {
    setSort((prev) =>
      prev.key === key
        ? { key, dir: prev.dir === "asc" ? "desc" : "asc" }
        : { key, dir: "asc" }
    );
  }

  const refresh = useCallback(async () => {
    try {
      const s = await api.mispricedState();
      setState(s);
      // Don't clobber an in-progress edit if a poll lands mid-typing.
      if (!editingFloorRef.current) {
        setFloorInput(String(s.floor));
      }
      setError(null);

      // Only ding/flash for rows that are both newly-qualifying AND actually
      // visible under the current mode's floor test — a row that's "new" to
      // the server's superset but doesn't clear the active mode's floor
      // isn't shown, so it shouldn't alert either.
      const visible = (s.rows || []).filter((r) => {
        const v = effectiveEdge(r, edgeMode);
        return v != null && v >= s.floor;
      });
      const newOnes = visible.filter((r) => r.is_new);
      if (newOnes.length > 0) {
        const keys = new Set(newOnes.map((r) => `${r.ticker}|${r.expiration}|${r.strike}`));
        setFlashKeys(keys);
        playDing();
        setTimeout(() => setFlashKeys(new Set()), 6000);
      }
    } catch (e) {
      setError(e.message || String(e));
    }
  }, [edgeMode]);

  useEffect(() => {
    refresh();
    pollRef.current = setInterval(refresh, 10000);
    return () => clearInterval(pollRef.current);
  }, [refresh]);

  async function handleToggle() {
    if (!state) return;
    setLoading(true);
    try {
      const s = await api.mispricedToggle(!state.toggle_on);
      setState(s);
    } catch (e) {
      setError(e.message || String(e));
    } finally {
      setLoading(false);
    }
  }

  async function handleFloorSave() {
    editingFloorRef.current = false;
    const v = parseFloat(floorInput);
    if (Number.isNaN(v) || v < 0) {
      setError("Floor must be a number >= 0");
      return;
    }
    try {
      const s = await api.mispricedSetFloor(v);
      setState(s);
      setError(null);
    } catch (e) {
      setError(e.message || String(e));
    }
  }

  async function handleSweepNow() {
    setLoading(true);
    try {
      const s = await api.mispricedSweepNow();
      setState(s);
      setFloorInput(String(s.floor));
    } catch (e) {
      setError(e.message || String(e));
    } finally {
      setLoading(false);
    }
  }

  const rawRows = state?.rows || [];
  const floorValue = state?.floor ?? 500;

  // Server sends a loose superset (qualifies under the most permissive edge
  // mode) so the toggle never needs a new sweep. The active mode's floor
  // test happens here, purely client-side, from data already in hand.
  const visibleRows = useMemo(() => {
    return rawRows.filter((r) => {
      const v = effectiveEdge(r, edgeMode);
      return v != null && v >= floorValue;
    });
  }, [rawRows, edgeMode, floorValue]);

  // Pure display-order concern — never mutates state.rows.
  const rows = useMemo(() => {
    const copy = [...visibleRows];
    copy.sort((a, b) => {
      let cmp;
      if (sort.key === "edge") {
        const av = effectiveEdge(a, edgeMode);
        const bv = effectiveEdge(b, edgeMode);
        if (av == null && bv == null) cmp = 0;
        else if (av == null) cmp = -1;
        else if (bv == null) cmp = 1;
        else cmp = av - bv;
      } else {
        cmp = compareRows(a, b, sort.key);
      }
      return sort.dir === "asc" ? cmp : -cmp;
    });
    return copy;
  }, [visibleRows, sort, edgeMode]);

  return (
    <div className="mispriced-wrap">
      <div className="mispriced-header">
        <div className="mispriced-title">
          <h2>Mispriced DITM Covered Calls</h2>
          <div className="mispriced-subtitle">
            Standalone — separate universe (ai_sector), separate sweep. Stores nothing.
          </div>
        </div>

        <div className="mispriced-controls">
          <button
            className={`mispriced-toggle${state?.toggle_on ? " on" : " off"}`}
            onClick={handleToggle}
            disabled={loading || !state}
          >
            <span className="mispriced-toggle-dot" />
            {state?.toggle_on ? "SWEEPING: ON" : "SWEEPING: OFF"}
          </button>

          <div className="mispriced-floor">
            <label>Floor $</label>
            <input
              type="number"
              min="0"
              step="50"
              value={floorInput}
              onFocus={() => { editingFloorRef.current = true; }}
              onChange={(e) => setFloorInput(e.target.value)}
              onBlur={handleFloorSave}
              onKeyDown={(e) => e.key === "Enter" && handleFloorSave()}
            />
          </div>

          <button className="mispriced-sweep-btn" onClick={handleSweepNow} disabled={loading}>
            {state?.sweep_in_progress ? "Sweeping…" : "Sweep Now"}
          </button>
        </div>
      </div>

      <div className="mispriced-status">
        {state?.universe_size != null && <span>{state.universe_size} tickers</span>}
        {state?.last_call_count != null && <span>{state.last_call_count} Tradier calls</span>}
        {state?.last_sweep_seconds != null && <span>{state.last_sweep_seconds}s</span>}
        <span>last swept: {timeAgo(state?.last_swept_at)}</span>
        {state?.sweep_in_progress && <span className="mispriced-sweeping-label">● sweeping…</span>}
      </div>

      {error && <div className="mispriced-error">{error}</div>}
      {state?.last_error && <div className="mispriced-error">Last sweep error: {state.last_error}</div>}

      <div className="mispriced-table-wrap">
        <table className="mispriced-table mispriced-table-compact">
          <thead>
            <tr>
              {COLS.slice(0, EDGE_COL_INDEX).map((c) => (
                <SortableTh key={c.key} c={c} sort={sort} onSort={handleSort} />
              ))}
              <th
                className="mispriced-sortable-th mispriced-edge-th"
                style={{ textAlign: "right" }}
                onClick={() => handleSort("edge")}
                title="Sort by Edge"
              >
                <div className="mispriced-edge-toggle" onClick={(e) => e.stopPropagation()}>
                  {EDGE_MODES.map((m) => (
                    <button
                      key={m}
                      className={`mispriced-edge-mode-btn${edgeMode === m ? " active" : ""}`}
                      onClick={() => setEdgeMode(m)}
                      title={
                        m === "range"
                          ? "Show bid–ask range; sort/floor on bid (worst case)"
                          : `Edge from call ${m}`
                      }
                    >
                      {m === "range" ? "ASK–BID" : m.toUpperCase()}
                    </button>
                  ))}
                </div>
                <div>
                  Edge $
                  <span className={`mispriced-sort-arrow${sort.key === "edge" ? " active" : ""}`}>
                    {sort.key === "edge" ? (sort.dir === "asc" ? " ▲" : " ▼") : ""}
                  </span>
                </div>
              </th>
              {COLS.slice(EDGE_COL_INDEX).map((c) => (
                <SortableTh key={c.key} c={c} sort={sort} onSort={handleSort} />
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 ? (
              <tr>
                <td colSpan={TOTAL_COLS} className="mispriced-empty">
                  {state?.sweep_in_progress
                    ? "Sweeping…"
                    : `No strikes clearing the $${floorValue} floor right now (${edgeMode} mode).`}
                </td>
              </tr>
            ) : (
              rows.map((r) => {
                const key = `${r.ticker}|${r.expiration}|${r.strike}`;
                const flashing = flashKeys.has(key);
                return (
                  <tr key={key} className={flashing ? "mispriced-row-new" : ""}>
                    <td>
                      <ResearchAsterisk
                        ticker={r.ticker}
                        hasSitrep={r.has_sitrep}
                        onResearch={setPanelTicker}
                        isDefense={r.is_defense}
                      />
                      {r.ticker}
                      {r.has_sitrep && r.primary_lens && (
                        <span
                          className={`mispriced-lens-pill${r.is_defense ? " defense" : ""}`}
                        >
                          {r.primary_lens}
                        </span>
                      )}
                    </td>
                    <td>{r.expiration}</td>
                    <td style={{ textAlign: "right" }}>{fmtMoney(r.strike)}</td>
                    <td style={{ textAlign: "right" }}>{fmtMoney(r.call_ask)}</td>
                    <td style={{ textAlign: "right" }}>{fmtMoney(r.call_bid)}</td>
                    <td style={{ textAlign: "right" }}>{fmtMoney(r.stock_price)}</td>
                    <td style={{ textAlign: "right" }} className="mispriced-edge">
                      {fmtEdgeCell(r, edgeMode)}
                    </td>
                    <td style={{ textAlign: "right" }}>{fmtMoney(r.breakeven)}</td>
                    <td style={{ textAlign: "center" }}>
                      {r.earnings_in_window ? <span className="mispriced-earn-flag">📅 earnings</span> : "—"}
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>

      <ResearchPanel ticker={panelTicker} onClose={() => setPanelTicker(null)} />
    </div>
  );
}
