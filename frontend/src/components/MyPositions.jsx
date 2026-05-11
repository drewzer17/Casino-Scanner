import React, { useCallback, useEffect, useRef, useState } from "react";
import { api } from "../api/client.js";
import { GRADE_COLORS } from "./RiskQualityScanner.jsx";

// ── helpers ───────────────────────────────────────────────────────────────────

function fmt(v, d = 2) {
  if (v == null) return "—";
  return Number(v).toFixed(d);
}

function fmtDollar(v) {
  if (v == null) return "—";
  return `$${Number(v).toFixed(2)}`;
}

function timeAgo(isoString) {
  if (!isoString) return null;
  const s = isoString.endsWith("Z") || isoString.includes("+") ? isoString : isoString + "Z";
  const d = new Date(s);
  const mins = Math.floor((Date.now() - d.getTime()) / 60_000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  return `${Math.floor(hrs / 24)}d ago`;
}

const POSITION_TYPES = ["", "CSP", "CC", "SHARES"];

const VRP_COLORS = { Rich: "#4ade80", Moderate: "#facc15", Weak: "#fb923c", Negative: "#f87171" };

// ── Inline editable cell ──────────────────────────────────────────────────────

function EditableCell({ value, onSave, type = "text", options = null, placeholder = "—" }) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState("");
  const inputRef = useRef(null);

  const startEdit = (e) => {
    e.stopPropagation();
    setDraft(value ?? "");
    setEditing(true);
  };

  const commit = async () => {
    setEditing(false);
    const trimmed = typeof draft === "string" ? draft.trim() : draft;
    const finalVal = trimmed === "" ? null : (type === "number" ? Number(trimmed) : trimmed);
    if (finalVal !== (value ?? null)) {
      await onSave(finalVal);
    }
  };

  useEffect(() => {
    if (editing && inputRef.current) inputRef.current.focus();
  }, [editing]);

  if (editing) {
    if (options) {
      return (
        <select
          ref={inputRef}
          value={draft}
          onChange={e => setDraft(e.target.value)}
          onBlur={commit}
          className="pos-edit-select"
          onClick={e => e.stopPropagation()}
        >
          {options.map(o => <option key={o} value={o}>{o || "—"}</option>)}
        </select>
      );
    }
    return (
      <input
        ref={inputRef}
        className="pos-edit-input"
        type={type}
        value={draft}
        onChange={e => setDraft(e.target.value)}
        onBlur={commit}
        onKeyDown={e => {
          if (e.key === "Enter") commit();
          if (e.key === "Escape") setEditing(false);
        }}
        onClick={e => e.stopPropagation()}
      />
    );
  }

  return (
    <span
      className="pos-editable"
      onClick={startEdit}
      title="Click to edit"
    >
      {value != null && value !== "" ? value : <span className="pos-editable-empty">{placeholder}</span>}
    </span>
  );
}

// ── Scan result row component ─────────────────────────────────────────────────

function ScanResultRow({ row, fullScanGrade, onClick }) {
  const grade = row.risk_grade;
  const gradeColor = grade ? GRADE_COLORS[grade] : "#6b7280";
  const fullGradeColor = fullScanGrade ? GRADE_COLORS[fullScanGrade] : "#6b7280";

  const gradeChanged = fullScanGrade && fullScanGrade !== grade;
  const directionUp = gradeChanged && grade && fullScanGrade &&
    ["A", "B", "C", "F"].indexOf(grade) < ["A", "B", "C", "F"].indexOf(fullScanGrade);

  return (
    <tr className="pos-scan-row" onClick={onClick} style={{ cursor: "pointer" }}>
      <td className="pos-scan-ticker">
        <span className="pos-scan-ticker-text">{row.ticker}</span>
        {row.company_name && <span className="pos-scan-co">{row.company_name}</span>}
      </td>
      <td className="pos-scan-price">{fmtDollar(row.price)}</td>
      <td className="pos-scan-grade" style={{ textAlign: "center" }}>
        <span
          className="pos-scan-grade-pill"
          style={{ background: gradeColor, color: "#000", fontWeight: 700, padding: "2px 8px", borderRadius: 4, fontSize: 12 }}
        >
          {grade ?? "—"}
        </span>
        {gradeChanged && (
          <span
            className="pos-scan-grade-delta"
            style={{ marginLeft: 4, color: directionUp ? "#4ade80" : "#f87171", fontSize: 11 }}
            title={`Full scan grade: ${fullScanGrade}`}
          >
            {directionUp ? "↑" : "↓"}{fullScanGrade}
          </span>
        )}
      </td>
      <td className="pos-scan-vrp" style={{ textAlign: "center" }}>
        {row.vrp_state ? (
          <span
            style={{
              background: VRP_COLORS[row.vrp_state] ?? "#6b7280",
              color: "#000",
              fontWeight: 600,
              padding: "2px 7px",
              borderRadius: 4,
              fontSize: 11,
            }}
          >
            {row.vrp_state}
          </span>
        ) : "—"}
      </td>
      <td className="pos-scan-strategy">{row.strategy_type ?? "—"}</td>
      <td className="pos-scan-ivrank">{row.iv_rank != null ? `${row.iv_rank.toFixed(0)}` : "—"}</td>
      <td className="pos-scan-score" style={{ textAlign: "right" }}>{row.score?.toFixed(1) ?? "—"}</td>
    </tr>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

export default function MyPositions({ allRows, onRowClick }) {
  const [positions, setPositions] = useState([]);
  const [lastScan, setLastScan] = useState(null);        // PositionScanOut
  const [loading, setLoading] = useState(true);
  const [scanning, setScanning] = useState(false);
  const [scanError, setScanError] = useState(null);

  // Bulk add input
  const [bulkInput, setBulkInput] = useState("");
  const [addMsg, setAddMsg] = useState(null);   // { text, ok }

  // Build a grade map from the latest full scan for comparison
  const fullScanGradeMap = {};
  for (const r of (allRows || [])) {
    if (r.risk_grade && !fullScanGradeMap[r.ticker]) {
      fullScanGradeMap[r.ticker] = r.risk_grade;
    }
  }

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const [pos, scan] = await Promise.all([
        api.getPositions(),
        api.scanPositionsLatest(),
      ]);
      setPositions(pos);
      setLastScan(scan);
    } catch {
      // non-fatal
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  const handleRemove = async (id) => {
    try {
      await api.deletePosition(id);
      setPositions(p => p.filter(pos => pos.id !== id));
    } catch {
      // non-fatal
    }
  };

  const handleUpdate = async (id, field, value) => {
    try {
      const updated = await api.updatePosition(id, { [field]: value });
      setPositions(p => p.map(pos => pos.id === id ? { ...pos, ...updated } : pos));
    } catch {
      // non-fatal
    }
  };

  const handleBulkAdd = async () => {
    if (!bulkInput.trim()) return;
    try {
      const created = await api.quickAddPositions(bulkInput);
      if (created.length === 0) {
        setAddMsg({ text: "All tickers already in positions", ok: false });
      } else {
        setAddMsg({ text: `Added: ${created.map(p => p.ticker).join(", ")}`, ok: true });
        setPositions(p => [...p, ...created].sort((a, b) => a.ticker.localeCompare(b.ticker)));
      }
      setBulkInput("");
    } catch (e) {
      setAddMsg({ text: e.message, ok: false });
    } finally {
      setTimeout(() => setAddMsg(null), 4000);
    }
  };

  const handleQuickScan = async () => {
    if (scanning || positions.length === 0) return;
    setScanning(true);
    setScanError(null);
    try {
      const result = await api.scanPositions();
      setLastScan(result);
    } catch (e) {
      setScanError(e.message);
    } finally {
      setScanning(false);
    }
  };

  if (loading) return <div className="empty">Loading positions…</div>;

  const scanResults = lastScan?.results ?? [];

  return (
    <div className="pos-root">
      {/* ── Section A: Positions list ── */}
      <div className="pos-section">
        <div className="pos-section-header">
          <h2 className="pos-section-title">📋 My Positions</h2>
          <div className="pos-actions">
            <div className="pos-bulk-wrap">
              <input
                className="pos-bulk-input"
                type="text"
                placeholder="AAPL, MSFT, CRDO"
                value={bulkInput}
                onChange={e => setBulkInput(e.target.value)}
                onKeyDown={e => e.key === "Enter" && handleBulkAdd()}
              />
              <button className="pos-add-btn" onClick={handleBulkAdd}>
                + Add
              </button>
            </div>
            {addMsg && (
              <span className={`pos-add-msg${addMsg.ok ? " ok" : " err"}`}>
                {addMsg.text}
              </span>
            )}
            <button
              className={`pos-scan-btn${scanning ? " scanning" : ""}`}
              onClick={handleQuickScan}
              disabled={scanning || positions.length === 0}
            >
              {scanning ? "Scanning…" : "⚡ Quick Scan"}
            </button>
            {lastScan?.scanned_at && (
              <span className="pos-last-scan">
                Last scanned: {timeAgo(lastScan.scanned_at)}
              </span>
            )}
            {!lastScan?.scanned_at && positions.length > 0 && (
              <span className="pos-last-scan pos-never">Never scanned</span>
            )}
            {scanError && (
              <span className="pos-scan-error">{scanError}</span>
            )}
          </div>
        </div>

        {positions.length === 0 ? (
          <div className="pos-empty">
            No positions yet. Add tickers above.
          </div>
        ) : (
          <div className="pos-table-wrap">
            <table className="pos-table">
              <thead>
                <tr>
                  <th className="pos-th-ticker">Ticker</th>
                  <th>Type</th>
                  <th>Strike</th>
                  <th>Expiry</th>
                  <th>Contracts</th>
                  <th>Entry $</th>
                  <th>Notes</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {positions.map(pos => (
                  <tr key={pos.id} className="pos-row">
                    <td className="pos-td-ticker">{pos.ticker}</td>
                    <td>
                      <EditableCell
                        value={pos.position_type}
                        onSave={v => handleUpdate(pos.id, "position_type", v)}
                        options={POSITION_TYPES}
                        placeholder="type"
                      />
                    </td>
                    <td>
                      <EditableCell
                        value={pos.strike}
                        onSave={v => handleUpdate(pos.id, "strike", v)}
                        type="number"
                        placeholder="strike"
                      />
                    </td>
                    <td>
                      <EditableCell
                        value={pos.expiry}
                        onSave={v => handleUpdate(pos.id, "expiry", v)}
                        placeholder="yyyy-mm-dd"
                      />
                    </td>
                    <td>
                      <EditableCell
                        value={pos.contracts}
                        onSave={v => handleUpdate(pos.id, "contracts", v)}
                        type="number"
                        placeholder="#"
                      />
                    </td>
                    <td>
                      <EditableCell
                        value={pos.entry_price}
                        onSave={v => handleUpdate(pos.id, "entry_price", v)}
                        type="number"
                        placeholder="$"
                      />
                    </td>
                    <td>
                      <EditableCell
                        value={pos.notes}
                        onSave={v => handleUpdate(pos.id, "notes", v)}
                        placeholder="notes"
                      />
                    </td>
                    <td>
                      <button
                        className="pos-remove-btn"
                        onClick={() => handleRemove(pos.id)}
                        title="Remove position"
                      >
                        ✕
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* ── Section B: Scan results ── */}
      {scanResults.length > 0 && (
        <div className="pos-section pos-results-section">
          <h3 className="pos-results-title">
            Scan Results
            {lastScan?.run_id && (
              <span className="pos-results-runid"> — Run #{lastScan.run_id}</span>
            )}
          </h3>
          <p className="pos-results-hint">
            Grade arrow shows change vs latest full scan.
          </p>
          <div className="pos-scan-table-wrap">
            <table className="pos-scan-table">
              <thead>
                <tr>
                  <th style={{ textAlign: "left" }}>Ticker</th>
                  <th>Price</th>
                  <th>Grade</th>
                  <th>VRP</th>
                  <th>Strategy</th>
                  <th>IV Rank</th>
                  <th style={{ textAlign: "right" }}>Score</th>
                </tr>
              </thead>
              <tbody>
                {scanResults.map(r => (
                  <ScanResultRow
                    key={r.ticker}
                    row={r}
                    fullScanGrade={fullScanGradeMap[r.ticker] ?? null}
                    onClick={() => onRowClick && onRowClick(r)}
                  />
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
