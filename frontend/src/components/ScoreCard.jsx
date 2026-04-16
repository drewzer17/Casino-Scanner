import React from "react";

function pillClass(score) {
  if (score >= 55) return "score-pill hi";
  if (score >= 25) return "score-pill mid";
  return "score-pill lo";
}

function fmtPct(v, digits = 1) {
  if (v === null || v === undefined) return "—";
  return `${(v * 100).toFixed(digits)}%`;
}

function fmt(v, digits = 2) {
  if (v === null || v === undefined) return "—";
  return Number(v).toFixed(digits);
}

export default function ScoreCard({ row }) {
  const b = row.breakdown || {};
  return (
    <div className="card">
      <div className="row">
        <div>
          <div className="ticker">{row.ticker}</div>
          <div className="price">
            ${fmt(row.price, 2)} · IVR {fmt(row.iv_rank, 0)}
          </div>
        </div>
        <div className={pillClass(row.score)}>{fmt(row.score, 0)}</div>
      </div>

      <div className="breakdown">
        <div className="factor">
          <div className="v">{fmt(b.iv_rank, 0)}</div>
          <div className="k">IVR</div>
        </div>
        <div className="factor">
          <div className="v">{fmt(b.premium, 0)}</div>
          <div className="k">PREM</div>
        </div>
        <div className="factor">
          <div className="v">{fmt(b.iv_hv, 0)}</div>
          <div className="k">IV/HV</div>
        </div>
        <div className="factor">
          <div className="v">{fmt(b.catalyst, 0)}</div>
          <div className="k">CAT</div>
        </div>
        <div className="factor">
          <div className="v">{fmt(b.chain, 0)}</div>
          <div className="k">CHAIN</div>
        </div>
      </div>

      <div className="meta">
        <span className="chip">prem {fmtPct(row.premium_pct, 2)}</span>
        <span className="chip">OI {row.open_interest ?? "—"}</span>
        <span className="chip">
          spr {row.bid_ask_spread_pct != null ? fmtPct(row.bid_ask_spread_pct, 1) : "—"}
        </span>
        {row.earnings_days !== null && row.earnings_days !== undefined && (
          <span className="chip">earn {row.earnings_days}d</span>
        )}
        {row.unusual_volume && <span className="chip">unusual vol</span>}
      </div>
    </div>
  );
}
