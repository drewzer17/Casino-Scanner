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

// arrow character + color for each arrow type
const ARROW_STYLE = {
  up_green:    { char: "▲", color: "var(--green)" },
  down_yellow: { char: "▼", color: "var(--yellow)" },
  down_red:    { char: "▼", color: "var(--red)" },
};

function ArrowStrip({ history }) {
  if (!history || history.length === 0) return null;
  return (
    <div className="arrows">
      {history.map((h) => {
        const style = h.arrow ? ARROW_STYLE[h.arrow] : null;
        const color = style ? style.color : "var(--text-muted)";
        const char  = style ? style.char  : "–";
        const tip   = h.delta != null
          ? `${h.days}d ago: ${h.delta > 0 ? "+" : ""}${h.delta.toFixed(1)} pts`
          : `${h.days}d: no data`;
        return (
          <span
            key={h.days}
            className="arrow-tf"
            style={{ color }}
            title={tip}
          >
            {h.days}d{char}
          </span>
        );
      })}
    </div>
  );
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

      <ArrowStrip history={row.history} />

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
