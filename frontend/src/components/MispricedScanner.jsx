import React, { useCallback, useEffect, useRef, useState } from "react";
import { api } from "../api/client.js";

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

const COLS = [
  { key: "ticker",              label: "Ticker",     align: "left" },
  { key: "expiration",          label: "Expiration", align: "left" },
  { key: "strike",              label: "Strike",     align: "right" },
  { key: "call_bid",            label: "Call Bid",   align: "right" },
  { key: "stock_price",         label: "Stock (ask)",align: "right" },
  { key: "edge",                label: "Edge $",     align: "right" },
  { key: "breakeven",           label: "Breakeven",  align: "right" },
  { key: "earnings_in_window",  label: "Earnings",   align: "center" },
];

function fmtMoney(v) {
  if (v == null) return "—";
  return `$${Number(v).toFixed(2)}`;
}

function fmtEdge(v) {
  if (v == null) return "—";
  const n = Number(v);
  return `${n < 0 ? "-" : ""}$${Math.abs(n).toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
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

export default function MispricedScanner() {
  const [state, setState] = useState(null);
  const [floorInput, setFloorInput] = useState("500");
  const [loading, setLoading] = useState(false);
  const [flashKeys, setFlashKeys] = useState(new Set());
  const [error, setError] = useState(null);
  const pollRef = useRef(null);
  const editingFloorRef = useRef(false);

  const refresh = useCallback(async () => {
    try {
      const s = await api.mispricedState();
      setState(s);
      // Don't clobber an in-progress edit if a poll lands mid-typing.
      if (!editingFloorRef.current) {
        setFloorInput(String(s.floor));
      }
      setError(null);

      const newOnes = (s.rows || []).filter((r) => r.is_new);
      if (newOnes.length > 0) {
        const keys = new Set(newOnes.map((r) => `${r.ticker}|${r.expiration}|${r.strike}`));
        setFlashKeys(keys);
        playDing();
        setTimeout(() => setFlashKeys(new Set()), 6000);
      }
    } catch (e) {
      setError(e.message || String(e));
    }
  }, []);

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

  const rows = state?.rows || [];

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
        <table className="mispriced-table">
          <thead>
            <tr>
              {COLS.map((c) => (
                <th key={c.key} style={{ textAlign: c.align }}>{c.label}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 ? (
              <tr>
                <td colSpan={COLS.length} className="mispriced-empty">
                  {state?.sweep_in_progress
                    ? "Sweeping…"
                    : `No strikes clearing the $${state?.floor ?? 500} floor right now.`}
                </td>
              </tr>
            ) : (
              rows.map((r) => {
                const key = `${r.ticker}|${r.expiration}|${r.strike}`;
                const flashing = flashKeys.has(key);
                return (
                  <tr key={key} className={flashing ? "mispriced-row-new" : ""}>
                    <td>{r.ticker}</td>
                    <td>{r.expiration}</td>
                    <td style={{ textAlign: "right" }}>{fmtMoney(r.strike)}</td>
                    <td style={{ textAlign: "right" }}>{fmtMoney(r.call_bid)}</td>
                    <td style={{ textAlign: "right" }}>{fmtMoney(r.stock_price)}</td>
                    <td style={{ textAlign: "right" }} className="mispriced-edge">{fmtEdge(r.edge)}</td>
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
    </div>
  );
}
