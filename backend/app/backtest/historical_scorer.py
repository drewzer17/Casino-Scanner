"""
historical_scorer.py — Phase 4 backtesting layer.

Assigns a risk grade to the output of reconstruct_factors(), mirroring the
logic in risk_quality.py but tolerating partial-factor input (missing factors
are skipped entirely, neither pass nor fail).

Primary entry point:
    score_historical(factors: dict) -> dict
"""
from __future__ import annotations

from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def score_historical(factors: dict) -> dict:
    """
    Grade a single reconstruct_factors() result.

    Scoring mirrors risk_quality.py with partial-factor support:
    missing factors are skipped (neither pass nor fail).
    Returns a new dict merging all original fields plus grade fields.
    """
    missing = set(factors.get("missing_factors", []))

    hard_fails:   list[str] = []
    strong_fails: list[str] = []
    soft_fails:   list[str] = []
    factors_used: list[str] = []
    factors_excl: list[str] = list(missing)

    vrp_spread    = factors.get("vrp_spread")
    vrp_state     = factors.get("vrp_state")
    iv_rank       = factors.get("iv_rank")
    ext_ratio     = factors.get("extension_ratio")
    ext_label     = factors.get("extension_label")
    trend         = factors.get("trend")
    dist_days     = factors.get("distribution_days")

    # ── Hard fails ────────────────────────────────────────────────────────────
    # VRP Negative → hard fail for Income Grind
    if "vrp" not in missing and vrp_spread is not None:
        factors_used.append("vrp")
        if vrp_spread < 0:
            hard_fails.append(f"VRP Negative ({vrp_spread:.2f}) — no premium edge")

    # ── Strong fails ──────────────────────────────────────────────────────────
    # Parabolic extension → strong fail, caps at C for CSP
    if "extension" not in missing and ext_ratio is not None:
        factors_used.append("extension")
        if ext_ratio >= 1.50:
            strong_fails.append(f"Extension parabolic ({ext_ratio:.3f}) — caps at C")
        elif ext_ratio >= 1.30:
            soft_fails.append(f"Extension extended ({ext_ratio:.3f})")

    # VRP Weak + low IV rank → strong fail
    if (
        "vrp" not in missing and vrp_state is not None
        and "iv_rank" not in missing and iv_rank is not None
    ):
        if vrp_state == "Weak" and iv_rank < 25:
            strong_fails.append(
                f"VRP Weak ({vrp_spread:.2f}) + low IV Rank ({iv_rank:.1f})"
            )

    # ── Soft fails ────────────────────────────────────────────────────────────
    # Low IV rank alone (without the Weak+low combo already counted above)
    if "iv_rank" not in missing and iv_rank is not None:
        if "iv_rank" not in factors_used:
            factors_used.append("iv_rank")
        already_strong = any("IV Rank" in s for s in strong_fails)
        if iv_rank < 25 and not already_strong:
            soft_fails.append(f"Low IV Rank ({iv_rank:.1f})")

    # Bearish trend (CSP-relevant)
    if "trend" not in missing and trend is not None:
        factors_used.append("trend")
        if trend == "bearish":
            soft_fails.append("Bearish trend (EMA20 < EMA50) — CSP caution")

    # Distribution days > 5
    if "distribution_days" not in missing and dist_days is not None:
        factors_used.append("distribution_days")
        if dist_days > 5:
            soft_fails.append(f"High distribution days ({dist_days})")

    # VRP Weak alone (without the low-IV-rank combo)
    if "vrp" not in missing and vrp_state is not None:
        if vrp_state == "Weak" and not any("IV Rank" in s for s in strong_fails):
            # Only count once — don't double-count if already in strong_fails combo
            soft_fails.append(f"VRP Weak ({vrp_spread:.2f})")

    # Ensure iv_rank is in factors_used if it was available
    if "iv_rank" not in missing and iv_rank is not None and "iv_rank" not in factors_used:
        factors_used.append("iv_rank")

    # ── Grade assignment ──────────────────────────────────────────────────────
    n_soft   = len(soft_fails)
    n_strong = len(strong_fails)
    n_hard   = len(hard_fails)

    if n_hard > 0:
        grade = "F"
    elif n_strong > 0 and n_soft >= 3:
        grade = "F"
    elif n_strong > 0:
        grade = "C"
    elif n_soft >= 5:
        grade = "F"
    elif n_soft >= 3:
        grade = "C"
    elif n_soft == 2:
        grade = "B"
    else:
        grade = "A"

    all_fail_reasons = hard_fails + strong_fails + soft_fails

    result = dict(factors)
    result.update({
        "grade":             grade,
        "soft_fail_count":   n_soft,
        "strong_fail_count": n_strong,
        "hard_fail_count":   n_hard,
        "fail_reasons":      all_fail_reasons,
        "factors_used":      sorted(set(factors_used)),
        "factors_excluded":  factors_excl,
        "grade_basis":       "partial_v1",
    })
    return result


def score_batch(factor_results: list) -> list:
    """Score a list of reconstruct_factors() results."""
    return [score_historical(f) for f in factor_results]


def grade_summary(scored: list) -> dict:
    """
    Aggregate statistics across a scored batch.

    Returns:
        grade_counts        — {A: n, B: n, C: n, F: n}
        vrp_state_counts    — {Rich: n, Moderate: n, Weak: n, Negative: n, None: n}
        avg_iv_rank_by_grade  — {A: float, ...}
        avg_ext_ratio_by_grade — {A: float, ...}
    """
    grade_counts:   dict[str, int]   = {"A": 0, "B": 0, "C": 0, "F": 0}
    vrp_counts:     dict[str, int]   = {}
    iv_by_grade:    dict[str, list]  = {"A": [], "B": [], "C": [], "F": []}
    ext_by_grade:   dict[str, list]  = {"A": [], "B": [], "C": [], "F": []}

    for r in scored:
        g = r.get("grade", "F")
        grade_counts[g] = grade_counts.get(g, 0) + 1

        vrp = r.get("vrp_state") or "None"
        vrp_counts[vrp] = vrp_counts.get(vrp, 0) + 1

        iv = r.get("iv_rank")
        if iv is not None:
            iv_by_grade[g].append(iv)

        ext = r.get("extension_ratio")
        if ext is not None:
            ext_by_grade[g].append(ext)

    def _avg(lst: list) -> Optional[float]:
        return round(sum(lst) / len(lst), 2) if lst else None

    return {
        "grade_counts":           grade_counts,
        "vrp_state_counts":       vrp_counts,
        "avg_iv_rank_by_grade":   {g: _avg(v) for g, v in iv_by_grade.items()},
        "avg_ext_ratio_by_grade": {g: _avg(v) for g, v in ext_by_grade.items()},
    }
