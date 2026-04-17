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

const BUCKET_RANK = { sell_now: 2, buy_sell_later: 1, watchlist: 0 };

// Determine gradient class from score trajectory
function trajectoryClass(history, currentScore, currentBucket) {
  const byDays = {};
  for (const h of (history || [])) byDays[h.days] = h;

  const score7 = byDays[7]?.prev_score ?? null;
  const score3 = byDays[3]?.prev_score ?? null;
  const bucket7 = byDays[7]?.prev_bucket ?? null;

  if (score7 === null || score3 === null) return "traj-neutral";

  const up7 = currentScore > score7;
  const up3 = currentScore > score3;

  if (up7 && up3) return "traj-green";
  if (up7 && !up3) return "traj-green-yellow";
  if (!up7 && up3) return "traj-yellow-green";

  // lower than both 7d and 3d
  const droppedBucket =
    bucket7 !== null &&
    BUCKET_RANK[currentBucket] < (BUCKET_RANK[bucket7] ?? 99);
  return droppedBucket ? "traj-red" : "traj-yellow";
}

function TrajectoryStrip({ history, currentScore, currentBucket }) {
  const byDays = {};
  for (const h of (history || [])) byDays[h.days] = h;

  const slots = [
    { label: "7d", score: byDays[7]?.prev_score ?? null },
    { label: "5d", score: byDays[5]?.prev_score ?? null },
    { label: "4d", score: byDays[4]?.prev_score ?? null },
    { label: "3d", score: byDays[3]?.prev_score ?? null },
    { label: "2d", score: byDays[2]?.prev_score ?? null },
    { label: "1d", score: byDays[1]?.prev_score ?? null },
    { label: "NOW", score: currentScore, now: true },
  ];

  const gradClass = trajectoryClass(history, currentScore, currentBucket);

  return (
    <div className={`traj-strip ${gradClass}`}>
      {slots.map(({ label, score, now }) => (
        <div key={label} className={`traj-slot${now ? " traj-now" : ""}`}>
          <div className="traj-score">
            {score !== null ? Math.round(score) : "–"}
          </div>
          <div className="traj-label">{label}</div>
        </div>
      ))}
    </div>
  );
}

export default function ScoreCard({ row }) {
  const b = row.breakdown || {};
  return (
    <div className="card">
      <div className="card-header">
        <div className="card-ticker-info">
          <div className="ticker">{row.ticker}</div>
          <div className="price">
            ${fmt(row.price, 2)} · IVR {fmt(row.iv_rank, 0)}
          </div>
        </div>
        <TrajectoryStrip
          history={row.history}
          currentScore={row.score}
          currentBucket={row.bucket}
        />
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
