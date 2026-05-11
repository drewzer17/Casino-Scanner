"""Phase 2 — Risk Quality Scoring Engine.

Computes risk_grade (A/B/C/F), strategy_type, VRP state, and diagnostic
flags for each scan_result row.  This layer is completely independent of
the existing 0-100 score and bucket system — it adds a new quality signal
on top without touching any existing behaviour.

Public API
----------
compute_risk_quality(row, ticker_source, earnings_date, recent_bars) -> dict
    Pure function. No DB access. Safe to call from any context.
"""
from __future__ import annotations

import json
import logging
import math
from datetime import date, timedelta
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_date(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except (ValueError, TypeError):
        return None


def _to_pct(val: float | None) -> float | None:
    """Convert a stored decimal IV value to percentage points.

    ORATS / Tradier IVs are stored as decimals (0.35 = 35%).
    Spec thresholds (backwardation >15, put_skew >25, etc.) are in
    percentage points.  Values already clearly >1.0 are assumed to
    already be percentages and are returned as-is.
    """
    if val is None:
        return None
    if abs(val) <= 3.0:          # decimal form — convert
        return round(val * 100, 4)
    return round(val, 4)         # already percentage


def _realized_vol_20d(
    recent_bars: list[tuple[str, float]],
    earnings_date: date | None,
) -> float | None:
    """Annualized 20-day realized volatility from bar (date_str, close) pairs.

    recent_bars must be sorted ascending (oldest first), contain at least 16
    entries for 15 usable log returns.

    Post-earnings adjustment: if we are within 10 trading days after
    earnings_date, the return from the last bar before earnings_date
    to the bar ON earnings_date is excluded (conservative approximation
    of the gap return).

    Returns vol as a percentage (e.g. 35.2 means 35.2%).
    Returns None if fewer than 16 bars or fewer than 15 usable returns.
    """
    if len(recent_bars) < 16:
        return None

    # Use the most recent 21 entries (gives up to 20 returns)
    sample = recent_bars[-21:]

    # Identify the earnings gap index to exclude
    skip_idx: int | None = None
    if earnings_date is not None:
        bar_dates = [_parse_date(d) for d, _ in sample]
        # Find first bar on or after earnings_date
        for i, bd in enumerate(bar_dates):
            if bd is None:
                continue
            if bd >= earnings_date:
                # Only exclude if we are within 10 trading days post-earnings
                bars_since = sum(
                    1 for d in bar_dates[i:] if d is not None
                )
                if bars_since <= 10:
                    skip_idx = i   # exclude return from sample[i-1] → sample[i]
                break

    returns: list[float] = []
    for i in range(1, len(sample)):
        prev_close = sample[i - 1][1]
        curr_close = sample[i][1]
        if prev_close <= 0 or curr_close <= 0:
            continue
        if skip_idx is not None and i == skip_idx:
            continue  # exclude earnings gap return
        try:
            returns.append(math.log(curr_close / prev_close))
        except (ValueError, ZeroDivisionError):
            continue

    if len(returns) < 15:
        return None

    n = len(returns)
    mean = sum(returns) / n
    variance = sum((r - mean) ** 2 for r in returns) / (n - 1)
    std_dev = math.sqrt(variance) if variance > 0 else 0.0
    annualized = std_dev * math.sqrt(252)
    return round(annualized * 100, 2)   # percentage


def _vrp_label(spread: float) -> str:
    if spread > 15:
        return "Rich"
    if spread >= 5:
        return "Moderate"
    if spread >= 0:
        return "Weak"
    return "Negative"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compute_risk_quality(
    row: dict[str, Any],
    ticker_source: str,
    earnings_date: date | None,
    recent_bars: list[tuple[str, float]],
) -> dict[str, Any]:
    """Compute Phase 2 Risk Quality fields for one scan_result row.

    Parameters
    ----------
    row:
        scan_result row as a dict.  Expected keys (all optional / nullable):
        price, iv (atm IV as decimal), iv_rank, ema_20, ema_50,
        distribution_days_25, iv_front_month, iv_back_month, put_skew,
        support_1, support_2, resistance_1, resistance_2, range_score,
        iv_ramp_flag, best_strike, best_expiry (YYYY-MM-DD string),
        cc_score, csp_score.
    ticker_source:
        Source tag from ticker_universe (e.g. "ai_sector", "sp500").
    earnings_date:
        Actual next earnings date or None.
    recent_bars:
        List of (date_str, close_price) tuples, sorted oldest-first,
        covering the last ~25 trading days.  Used to compute realized vol.

    Returns
    -------
    dict with all Phase 2 columns.  Safe to merge into the existing row.
    All list fields are returned as Python lists (caller serialises to JSON).
    """
    out: dict[str, Any] = {
        "risk_grade": None,
        "vrp_state": None,
        "vrp_spread": None,
        "realized_vol_20d": None,
        "strategy_type": None,
        "secondary_edge": [],
        "hard_fail_reasons": [],
        "strong_fail_reasons": [],
        "soft_fail_count": 0.0,
        "soft_fail_details": [],
        "event_ramp_eligible": False,
        "technical_location_eligible": False,
        "income_grind_eligible": False,
    }

    try:
        # ── Extract & normalise row fields ────────────────────────────────────

        price         = row.get("price")
        atm_iv_raw    = row.get("iv")          # decimal (e.g. 0.229)
        iv_rank       = row.get("iv_rank")
        ema_20        = row.get("ema_20")
        ema_50        = row.get("ema_50")
        dist_days     = row.get("distribution_days_25")
        iv_front_raw  = row.get("iv_front_month")
        iv_back_raw   = row.get("iv_back_month")
        put_skew_raw  = row.get("put_skew")
        s1            = row.get("support_1")
        s2            = row.get("support_2")
        r1            = row.get("resistance_1")
        r2            = row.get("resistance_2")
        range_score   = row.get("range_score")   # 0-100, None if unavailable
        iv_ramp_flag  = row.get("iv_ramp_flag", False)
        best_strike   = row.get("best_strike")
        best_expiry   = _parse_date(row.get("best_expiry"))
        cc_score      = float(row.get("cc_score") or 0)
        csp_score     = float(row.get("csp_score") or 0)

        # Is this row a CC (call-selling) or CSP (put-selling)?
        is_cc = cc_score > csp_score

        # Convert stored decimals to percentage points for threshold comparisons
        atm_iv_pct   = _to_pct(atm_iv_raw)    # e.g. 22.9
        iv_front_pct = _to_pct(iv_front_raw)  # e.g. 36.2
        iv_back_pct  = _to_pct(iv_back_raw)   # e.g. 45.5
        put_skew_pct = _to_pct(put_skew_raw)  # e.g. 2.2

        # ── Step 3: Realized Vol ─────────────────────────────────────────────

        rv = _realized_vol_20d(recent_bars, earnings_date)
        out["realized_vol_20d"] = rv

        # ── Step 4: VRP State ─────────────────────────────────────────────────

        if atm_iv_pct is not None and rv is not None:
            spread = round(atm_iv_pct - rv, 2)
            out["vrp_spread"] = spread
            out["vrp_state"] = _vrp_label(spread)

        vrp_state_val: str | None = out["vrp_state"]

        # ── Step 5: Strategy Classification ──────────────────────────────────

        # Event Ramp
        event_ramp = bool(
            iv_ramp_flag
            and earnings_date is not None
            and best_expiry is not None
            and best_expiry >= earnings_date
        )

        # Technical Location
        tech_loc = False
        if range_score is not None and price is not None and price > 0:
            range_ok = (
                (not is_cc and range_score <= 30)   # CSP: price near lows
                or (is_cc and range_score >= 70)     # CC: price near highs
            )
            if range_ok:
                levels = []
                if is_cc:
                    if r1 is not None: levels.append(r1)
                    if r2 is not None: levels.append(r2)
                else:
                    if s1 is not None: levels.append(s1)
                    if s2 is not None: levels.append(s2)
                if levels:
                    tech_loc = any(abs(price - lvl) / price <= 0.03 for lvl in levels)

        # Income Grind
        income_grind = vrp_state_val in ("Rich", "Moderate")

        out["event_ramp_eligible"] = event_ramp
        out["technical_location_eligible"] = tech_loc
        out["income_grind_eligible"] = income_grind

        # Priority assignment
        if event_ramp:
            strategy_type = "Event Ramp"
        elif tech_loc:
            strategy_type = "Technical Location"
        else:
            # income_grind True → Income Grind (passes VRP gate)
            # income_grind False → Income Grind (will hard-fail VRP gate)
            strategy_type = "Income Grind"

        out["strategy_type"] = strategy_type

        # Secondary edge
        if strategy_type == "Event Ramp" and tech_loc:
            out["secondary_edge"] = ["TechnicalLocation"]

        # ── Step 6: Risk Grade ────────────────────────────────────────────────

        hard_fail: list[str] = []
        strong_fail: list[str] = []
        soft_count = 0.0
        soft_details: list[str] = []

        # ---- Hard Fails ----

        # Check 1: Earnings Overlap (strategy-conditional)
        if earnings_date is not None and best_expiry is not None:
            if strategy_type == "Income Grind":
                if best_expiry >= earnings_date:
                    hard_fail.append("earnings_overlap")
            elif strategy_type == "Technical Location":
                if best_expiry >= earnings_date:
                    strong_fail.append("earnings_overlap_technical")
            # Event Ramp: skip — earnings in window is a requirement

        # Check 2: Extreme Backwardation
        if iv_front_pct is not None and iv_back_pct is not None:
            backwardation = iv_front_pct - iv_back_pct
            if backwardation > 15:
                hard_fail.append("extreme_backwardation")

        # Check 3: Extreme Put Skew
        if put_skew_pct is not None and put_skew_pct > 25:
            hard_fail.append("extreme_put_skew")

        # Check 4: VRP Gate for Income Grind
        if strategy_type == "Income Grind":
            if vrp_state_val not in ("Rich", "Moderate"):
                hard_fail.append("weak_vrp_income_grind")

        if hard_fail:
            out["hard_fail_reasons"] = hard_fail
            out["strong_fail_reasons"] = strong_fail
            out["soft_fail_count"] = 0.0
            out["soft_fail_details"] = []
            out["risk_grade"] = "F"
            return out

        # ---- Strong Fails ----

        # Check 5: IV Rank Low + VRP state
        if iv_rank is not None and iv_rank < 25:
            if vrp_state_val in ("Weak", "Negative", None):
                strong_fail.append("low_iv_rank_weak_vrp")
            # Rich/Moderate + low iv_rank → becomes Soft 5 below

        # Check 6: Elevated Put Skew (15 < skew <= 25 vol points)
        if put_skew_pct is not None and 15 < put_skew_pct <= 25:
            strong_fail.append("put_skew_elevated")

        # Check 7: earnings_overlap_technical already captured in strong_fail above

        strong_fail_fired = len(strong_fail) > 0

        # ---- Soft Fails ----

        # Soft 1: Trend Filter — CSP only
        if not is_cc:
            if price is not None and ema_20 is not None and ema_50 is not None:
                if price < ema_20 and price < ema_50:
                    soft_count += 1.0
                    soft_details.append("trend_csp")

        # Soft 2: Distribution Days (with cluster rule)
        if dist_days is not None and dist_days > 5:
            if "trend_csp" in soft_details:
                soft_count += 0.5  # correlated cluster — already counted 1.0 for Soft 1
            else:
                soft_count += 1.0
            soft_details.append("distribution_days")

        # Soft 3: Strike Alignment
        if best_strike is not None and price is not None and price > 0:
            if is_cc:
                candidates = [lvl for lvl in [r1, r2] if lvl is not None]
            else:
                candidates = [lvl for lvl in [s1, s2] if lvl is not None]

            if candidates:
                nearest = min(candidates, key=lambda lvl: abs(lvl - best_strike))
                if abs(best_strike - nearest) / price > 0.03:
                    soft_count += 1.0
                    soft_details.append("strike_misaligned")
            # else: no S/R data — skip (Price Discovery)

        # Soft 4: Mild Backwardation (Income Grind only)
        if strategy_type == "Income Grind":
            if iv_front_pct is not None and iv_back_pct is not None:
                backwardation = iv_front_pct - iv_back_pct
                if 5 <= backwardation <= 15:
                    soft_count += 1.0
                    soft_details.append("mild_backwardation")

        # Soft 5: Low IV Rank with Rich/Moderate VRP
        if vrp_state_val in ("Rich", "Moderate"):
            if iv_rank is not None and iv_rank < 25:
                soft_count += 1.0
                soft_details.append("low_iv_rank_rich_vrp")

        out["strong_fail_reasons"] = strong_fail
        out["soft_fail_count"] = round(soft_count, 1)
        out["soft_fail_details"] = soft_details
        out["hard_fail_reasons"] = hard_fail

        # ---- Grade Assignment ----

        grade_count = math.ceil(soft_count)

        if strong_fail_fired:
            risk_grade = "F" if grade_count >= 3 else "C"
        else:
            if grade_count <= 1:
                risk_grade = "A"
            elif grade_count == 2:
                risk_grade = "B"
            elif grade_count <= 4:
                risk_grade = "C"
            else:
                risk_grade = "F"

        out["risk_grade"] = risk_grade

    except Exception as exc:
        logger.warning("compute_risk_quality failed: %s", exc, exc_info=True)
        # Return safe defaults — do not propagate exception into scan loop

    return out
