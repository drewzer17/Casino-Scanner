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

function fmtSignedPct(v, digits = 1) {
  if (v === null || v === undefined) return null;
  const sign = v >= 0 ? "+" : "";
  return `${sign}${v.toFixed(digits)}%`;
}

function fmtDollar(v) {
  if (v === null || v === undefined) return "—";
  return `$${Number(v).toFixed(2)}`;
}

function fmtExpiry(exp) {
  if (!exp) return null;
  const [y, m, d] = exp.split("-").map(Number);
  return new Date(y, m - 1, d).toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

const BUCKET_RANK = { sell_now: 2, buy_sell_later: 1, watchlist: 0 };

// Determine gradient class from score trajectory (anchors: 14d and 7d)
function trajectoryClass(history, currentScore, currentBucket) {
  const byDays = {};
  for (const h of (history || [])) byDays[h.days] = h;

  const score14 = byDays[14]?.prev_score ?? null;
  const score7 = byDays[7]?.prev_score ?? null;
  const bucket14 = byDays[14]?.prev_bucket ?? null;

  if (score14 === null || score7 === null) return "traj-neutral";

  const up14 = currentScore > score14;
  const up7 = currentScore > score7;

  if (up14 && up7) return "traj-green";
  if (up14 && !up7) return "traj-green-yellow";
  if (!up14 && up7) return "traj-yellow-green";

  const droppedBucket =
    bucket14 !== null &&
    BUCKET_RANK[currentBucket] < (BUCKET_RANK[bucket14] ?? 99);
  return droppedBucket ? "traj-red" : "traj-yellow";
}

function TrajectoryStrip({ history, currentScore, currentBucket }) {
  const byDays = {};
  for (const h of (history || [])) byDays[h.days] = h;

  const slots = [
    { label: "14d", score: byDays[14]?.prev_score ?? null },
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

const REGIME_CLASS = {
  UPTREND: "regime-up",
  DOWNTREND: "regime-dn",
  TRANSITIONAL: "regime-mid",
};

function SmaPanel({ row }) {
  const hasAny = row.sma_200 || row.sma_50 || row.support_1 || row.resistance_1;
  if (!hasAny) return null;

  const arrow = (pct) => (pct == null ? "" : pct >= 0 ? " ▲" : " ▼");

  return (
    <div className="sma-panel">
      {(row.sma_200 || row.sma_50) && (
        <div className="sma-row">
          {row.sma_200 && (
            <span className="sma-item">
              <span className="sma-label">200d</span>{" "}
              <span className="sma-val">{fmtDollar(row.sma_200)}</span>
              {row.price_vs_sma200_pct != null && (
                <span className={`sma-pct ${row.price_vs_sma200_pct >= 0 ? "up" : "dn"}`}>
                  {fmtSignedPct(row.price_vs_sma200_pct)}{arrow(row.price_vs_sma200_pct)}
                </span>
              )}
            </span>
          )}
          {row.sma_50 && (
            <span className="sma-item">
              <span className="sma-label">50d</span>{" "}
              <span className="sma-val">{fmtDollar(row.sma_50)}</span>
              {row.price_vs_sma50_pct != null && (
                <span className={`sma-pct ${row.price_vs_sma50_pct >= 0 ? "up" : "dn"}`}>
                  {fmtSignedPct(row.price_vs_sma50_pct)}{arrow(row.price_vs_sma50_pct)}
                </span>
              )}
            </span>
          )}
          {row.sma_regime && (
            <span className={`regime-tag ${REGIME_CLASS[row.sma_regime] ?? ""}`}>
              {row.sma_regime}
            </span>
          )}
        </div>
      )}

      {(row.support_1 || row.resistance_1) && row.price && (
        <div className="sr-row">
          {row.support_1 && (
            <span className="sr-item support">
              S1 {fmtDollar(row.support_1)}
              <span className="sr-dist">
                {fmtSignedPct(((row.support_1 - row.price) / row.price) * 100)}
              </span>
            </span>
          )}
          {row.resistance_1 && (
            <span className="sr-item resist">
              R1 {fmtDollar(row.resistance_1)}
              <span className="sr-dist">
                {fmtSignedPct(((row.resistance_1 - row.price) / row.price) * 100)}
              </span>
            </span>
          )}
        </div>
      )}
    </div>
  );
}

export default function ScoreCard({ row, onClick }) {
  const b = row.breakdown || {};
  return (
    <div className="card" onClick={onClick} style={{ cursor: onClick ? "pointer" : "default" }}>
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

      <SmaPanel row={row} />

      <div className="meta">
        <span className="chip">
          {(() => {
            const bestPrem = row.premium_otm2 != null
              ? row.premium_otm2 * 100
              : row.atm_call_premium != null
              ? row.atm_call_premium * 100
              : null;
            const expLabel = fmtExpiry(row.best_expiry);
            if (bestPrem != null && expLabel && row.best_dte != null && row.best_strike != null) {
              return (
                <>
                  best: ${bestPrem.toFixed(2)}{" "}
                  <span style={{ color: "var(--text-muted)" }}>
                    ({expLabel}, {row.best_dte}d, ${row.best_strike} strike)
                  </span>
                </>
              );
            }
            // Fallback: old-style display
            return (
              <>
                prem {fmtPct(row.premium_pct, 2)}
                {row.premium_otm2 != null && (
                  <span style={{ color: "var(--text-muted)" }}>
                    {" "}(${(row.premium_otm2 * 100).toFixed(2)} 2OTM)
                  </span>
                )}
              </>
            );
          })()}
        </span>
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
