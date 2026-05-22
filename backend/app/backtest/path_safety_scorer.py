"""
path_safety_scorer.py — Phase 4 backtesting layer.

Empirically-derived CSP path safety scorer, trained on 2-date backtest
diagnostics (Jul 2024 neutral + Jan 2025 stress regimes).

Runs ALONGSIDE legacy historical_scorer.py — does NOT replace it.
Both scores are computed and stored for side-by-side comparison.

Key empirical findings driving this design:
  - RV20 is the strongest single assignment predictor (9.3% → 19.6% across buckets)
  - Extension ratio is the second strongest gate
  - Bearish trend is actually safer for CSP (penalty removed)
  - Distribution days > 5 is actually safer (penalty removed)
  - Moderate VRP is the best VRP state; Rich VRP adds path pain
  - IV rank alone has weak predictive power; kept as minor modifier only

Primary entry point:
    score_path_safety(factors: dict) -> dict
"""
from __future__ import annotations

from typing import Optional

# Grade ordering: lower index = better grade
_GRADES = ["A", "B", "C", "F"]
_GRADE_IDX = {g: i for i, g in enumerate(_GRADES)}


def _grade_at(idx: int) -> str:
    return _GRADES[min(max(idx, 0), len(_GRADES) - 1)]


def _drop(ceiling: str, steps: int) -> str:
    return _grade_at(_GRADE_IDX[ceiling] + steps)


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def score_path_safety(factors: dict) -> dict:
    """
    Empirical CSP path safety grade.

    Two hard gates (extension, rv20) establish a ceiling; modifiers
    (VRP state, IV rank) move the final grade within that ceiling.
    Trend and distribution days are neutral in v1 (empirically inverted
    vs. legacy model — removing the false penalties is the key insight).
    """
    missing = set(factors.get("missing_factors", []))
    factors_used:  list[str] = []
    factors_excl:  list[str] = list(missing)

    ext_ratio  = factors.get("extension_ratio")
    rv20       = factors.get("rv20")
    vrp_state  = factors.get("vrp_state")
    iv_rank    = factors.get("iv_rank")

    # ── Gate 1: Extension ratio ───────────────────────────────────────────────
    if "extension" not in missing and ext_ratio is not None:
        factors_used.append("extension")
        if ext_ratio >= 1.30:
            ext_gate    = "extended"
            ext_ceiling = "F"
        elif ext_ratio >= 1.15:
            ext_gate    = "hot"
            ext_ceiling = "C"
        elif ext_ratio >= 1.05:
            ext_gate    = "warm"
            ext_ceiling = "B"
        else:
            ext_gate    = "normal"
            ext_ceiling = "A"
    else:
        factors_excl.append("extension")
        ext_gate    = "unknown"
        ext_ceiling = "A"   # gate skipped — no cap applied

    # ── Gate 2: RV20 absolute cap ─────────────────────────────────────────────
    if "vrp" not in missing and rv20 is not None:
        factors_used.append("rv20")
        if rv20 > 60:
            rv_gate    = ">60%"
            rv_ceiling = "F"
        elif rv20 >= 40:
            rv_gate    = "40-60%"
            rv_ceiling = "C"
        elif rv20 >= 30:
            rv_gate    = "30-40%"
            rv_ceiling = "B"
        else:
            rv_gate    = "<30%"
            rv_ceiling = "A"
    else:
        factors_excl.append("rv20")
        rv_gate    = "unknown"
        rv_ceiling = "A"   # gate skipped — no cap applied

    # Combined ceiling = stricter of the two gates
    ext_idx      = _GRADE_IDX[ext_ceiling]
    rv_idx       = _GRADE_IDX[rv_ceiling]
    gate_ceiling = _GRADES[max(ext_idx, rv_idx)]

    # ── Modifiers ─────────────────────────────────────────────────────────────
    modifier_score = 0.0
    path_pain_flag = False

    # VRP state
    if "vrp" not in missing and vrp_state is not None:
        factors_used.append("vrp")
        if vrp_state == "Moderate":
            modifier_score += 1.0
        elif vrp_state == "Rich":
            modifier_score += 0.0
            path_pain_flag  = True   # high MFE but also high MAE — flag for awareness
        elif vrp_state == "Weak":
            modifier_score += 0.0
        elif vrp_state == "Negative":
            modifier_score -= 2.0

    # IV rank — minor role only
    if "iv_rank" not in missing and iv_rank is not None:
        factors_used.append("iv_rank")
        if iv_rank > 70:
            modifier_score += 0.5
        elif iv_rank < 15:
            modifier_score -= 0.5
        # else: 0 (no adjustment)

    # Trend: NEUTRAL in v1 — empirically bearish is safer, penalty removed
    # Distribution days: NEUTRAL in v1 — empirically >5 is safer, penalty removed

    # ── Grade assignment ──────────────────────────────────────────────────────
    ceiling_idx = _GRADE_IDX[gate_ceiling]

    if modifier_score >= 1.0:
        steps_down = 0
    elif modifier_score <= -2.0:
        steps_down = 2
    else:
        # 0.0 ≤ score < 1.0: drop 1;  -2.0 < score < 0.0: drop 1
        steps_down = 1

    final_grade = _drop(gate_ceiling, steps_down)

    result = dict(factors)
    result.update({
        "path_safety_grade":   final_grade,
        "path_safety_version": "path_safety_v1",
        "threshold_profile":   "rv_30_40_60",
        "extension_gate":      ext_gate,
        "rv20_gate":           rv_gate,
        "modifier_score":      modifier_score,
        "path_pain_flag":      path_pain_flag,
        "gate_ceiling":        gate_ceiling,
        "factors_used":        sorted(set(factors_used)),
        "factors_excluded":    list(set(factors_excl)),
    })
    return result


def score_path_safety_batch(factor_results: list) -> list:
    """Score a list of reconstruct_factors() results with path safety logic."""
    return [score_path_safety(f) for f in factor_results]


def path_safety_summary(scored: list) -> dict:
    """
    Aggregate statistics across a path-safety-scored batch.

    Returns counts per:
      - path_safety_grade
      - gate_ceiling
      - extension_gate
      - rv20_gate
    Plus path_pain_flag count.
    """
    if not scored:
        return {"total": 0}

    grade_counts:    dict[str, int] = {"A": 0, "B": 0, "C": 0, "F": 0}
    ceiling_counts:  dict[str, int] = {}
    ext_gate_counts: dict[str, int] = {}
    rv_gate_counts:  dict[str, int] = {}
    path_pain_count = 0

    for r in scored:
        g = r.get("path_safety_grade", "F")
        grade_counts[g] = grade_counts.get(g, 0) + 1

        c = r.get("gate_ceiling", "?")
        ceiling_counts[c] = ceiling_counts.get(c, 0) + 1

        e = r.get("extension_gate", "?")
        ext_gate_counts[e] = ext_gate_counts.get(e, 0) + 1

        v = r.get("rv20_gate", "?")
        rv_gate_counts[v] = rv_gate_counts.get(v, 0) + 1

        if r.get("path_pain_flag"):
            path_pain_count += 1

    return {
        "total":            len(scored),
        "grade_counts":     grade_counts,
        "ceiling_counts":   ceiling_counts,
        "ext_gate_counts":  ext_gate_counts,
        "rv_gate_counts":   rv_gate_counts,
        "path_pain_count":  path_pain_count,
    }
