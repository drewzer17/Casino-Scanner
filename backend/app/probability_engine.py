"""
probability_engine.py — Empirical CSP probability lookup engine.

Queries backtest_results for observed assignment rates, MAE distributions,
and touch/recovery statistics matching current market regime + ticker characteristics.

Python 3.9 compatible: use Optional[type] not X|None union syntax.

NOTE: This module does NOT import factor_reconstruction or path_safety_scorer
at module level — those are heavy and loaded lazily inside the route handler.
"""
from __future__ import annotations

from typing import Optional

from sqlalchemy import text

# ── Regime cell definitions ───────────────────────────────────────────────────

REGIME_CELLS = {
    ('Low', True): {
        'color': 'red',
        'name': 'Low VIX + Bull',
        'desc': 'Complacent drift — historically worst for CSPs',
        'duration': 'Prefer 5-day max; avoid 21-day',
        'strike': 'Prefer 3% OTM minimum',
    },
    ('Elevated', False): {
        'color': 'red',
        'name': 'Elevated VIX + Bear',
        'desc': 'Grinding bear — no bounce to save you',
        'duration': 'Prefer shorter duration',
        'strike': 'Prefer wider strikes',
    },
    ('Normal', True): {
        'color': 'yellow',
        'name': 'Normal VIX + Bull',
        'desc': 'Regular bull — moderate risk',
        'duration': 'Standard 14-21 day acceptable',
        'strike': 'Standard 2% OTM acceptable',
    },
    ('Elevated', True): {
        'color': 'green',
        'name': 'Elevated VIX + Bull',
        'desc': 'Nervous bull — market worried but holding',
        'duration': 'Standard durations acceptable',
        'strike': 'Standard 2% OTM acceptable',
    },
    ('Normal', False): {
        'color': 'green',
        'name': 'Normal VIX + Bear',
        'desc': 'Post-damage calm — downside largely absorbed',
        'duration': 'Standard durations acceptable',
        'strike': 'Standard strikes acceptable',
    },
    ('High', False): {
        'color': 'green',
        'name': 'High VIX + Bear',
        'desc': 'Peak fear — downside largely priced in',
        'duration': 'Standard durations acceptable',
        'strike': 'Standard strikes acceptable',
    },
}

# Industries where grade separation historically unreliable
FLAGGED_INDUSTRIES = [
    'Biotechnology',
    'Oil & Gas E&P',
    'Oil & Gas Drilling',
    'REIT - Mortgage',
    'REIT - Hotel & Motel',
    'Household & Personal Products',
]


# ── Current regime from live price_history ────────────────────────────────────

def get_current_regime(db_session) -> dict:
    """
    Determine current regime cell from live VIX and SPY data in price_history.
    Returns dict with vix, vix_regime, spy_above_ema50, color, name, desc, etc.
    """
    vix_row = db_session.execute(text(
        "SELECT close FROM price_history WHERE ticker = '^VIX' ORDER BY date DESC LIMIT 1"
    )).fetchone()
    vix = float(vix_row[0]) if vix_row else None

    spy_rows = db_session.execute(text(
        "SELECT close FROM price_history WHERE ticker = 'SPY' ORDER BY date DESC LIMIT 60"
    )).fetchall()

    spy_above_ema50 = True
    if spy_rows and len(spy_rows) >= 50:
        closes = [float(r[0]) for r in spy_rows]
        spy_close = closes[0]
        sma50 = sum(closes[:50]) / 50
        spy_above_ema50 = spy_close > sma50

    if vix is None:
        vix_regime = 'Normal'
    elif vix < 15:
        vix_regime = 'Low'
    elif vix < 20:
        vix_regime = 'Normal'
    elif vix < 30:
        vix_regime = 'Elevated'
    else:
        vix_regime = 'High'

    cell_key = (vix_regime, spy_above_ema50)
    cell_info = REGIME_CELLS.get(cell_key, REGIME_CELLS[('Normal', True)])

    # ── Best Grade+VRP combos for this regime ────────────────────────────────
    best_combos = []
    avoid = None
    try:
        combo_rows = db_session.execute(text("""
            SELECT
                path_safety_grade,
                vrp_state,
                COUNT(*) AS n,
                ROUND((100.0 * SUM(CASE WHEN final_distance_pct < 0 THEN 1 ELSE 0 END)
                       / NULLIF(COUNT(*), 0))::numeric, 1) AS assign_pct,
                ROUND((100.0 * SUM(CASE WHEN mfe_pct >= 4.0 THEN 1 ELSE 0 END)
                       / NULLIF(COUNT(*), 0))::numeric, 1) AS exit_pct
            FROM backtest_results
            WHERE strike_pct = 0.02 AND hold_days = 21
              AND vix_regime = :vix AND spy_above_ema50 = :spy
              AND path_safety_grade IN ('A', 'B') AND vrp_state IS NOT NULL
            GROUP BY path_safety_grade, vrp_state
            HAVING COUNT(*) >= 30
            ORDER BY assign_pct ASC
            LIMIT 3
        """), {"vix": vix_regime, "spy": spy_above_ema50}).fetchall()

        for row in combo_rows:
            best_combos.append({
                "grade":      row[0],
                "vrp":        row[1],
                "n":          int(row[2]),
                "assign_pct": float(row[3]) if row[3] is not None else None,
                "exit_pct":   float(row[4]) if row[4] is not None else None,
            })

        # Worst: highest assignment among A/B + Negative VRP; fallback to worst overall
        avoid_row = db_session.execute(text("""
            SELECT
                path_safety_grade,
                vrp_state,
                COUNT(*) AS n,
                ROUND((100.0 * SUM(CASE WHEN final_distance_pct < 0 THEN 1 ELSE 0 END)
                       / NULLIF(COUNT(*), 0))::numeric, 1) AS assign_pct
            FROM backtest_results
            WHERE strike_pct = 0.02 AND hold_days = 21
              AND vix_regime = :vix AND spy_above_ema50 = :spy
              AND vrp_state = 'Negative'
              AND path_safety_grade IS NOT NULL
            GROUP BY path_safety_grade, vrp_state
            HAVING COUNT(*) >= 30
            ORDER BY assign_pct DESC
            LIMIT 1
        """), {"vix": vix_regime, "spy": spy_above_ema50}).fetchone()

        if avoid_row and avoid_row[3] is not None:
            avoid = {
                "grade":      avoid_row[0],
                "vrp":        avoid_row[1],
                "n":          int(avoid_row[2]),
                "assign_pct": float(avoid_row[3]),
            }
        elif best_combos:
            # No Negative VRP data — use the worst of the best_combos list as avoid signal
            worst = max(best_combos, key=lambda c: c["assign_pct"] or 0)
            if (worst["assign_pct"] or 0) > 40:
                avoid = worst
    except Exception:
        pass  # non-fatal — banner works without combos

    return {
        'vix': vix,
        'vix_regime': vix_regime,
        'spy_above_ema50': spy_above_ema50,
        'cell_key': list(cell_key),
        'best_combos': best_combos,
        'avoid': avoid,
        **cell_info,
    }


# ── Core probability lookup ───────────────────────────────────────────────────

def get_probabilities(
    db_session,
    vix_regime: str,
    spy_above_ema50: bool,
    path_safety_grade: Optional[str],
    strike_pct: float = 0.02,
    hold_days: int = 21,
    sector: Optional[str] = None,
    cohort: str = 'all',
    min_n: int = 30,
) -> Optional[dict]:
    """
    Look up empirical CSP probabilities from backtest_results.
    Falls back to no-sector if n < min_n with sector applied.
    Returns None if no matching rows found.
    """
    conditions = [
        "br.vix_regime = :vix_regime",
        "br.spy_above_ema50 = :spy_above_ema50",
        "br.strike_pct BETWEEN :strike_lo AND :strike_hi",
        "br.hold_days BETWEEN :hold_lo AND :hold_hi",
    ]
    params: dict = {
        'vix_regime':      vix_regime,
        'spy_above_ema50': spy_above_ema50,
        'strike_lo':       strike_pct - 0.005,
        'strike_hi':       strike_pct + 0.005,
        'hold_lo':         hold_days - 3,
        'hold_hi':         hold_days + 3,
    }

    if path_safety_grade:
        conditions.append("br.path_safety_grade = :grade")
        params['grade'] = path_safety_grade

    if cohort in ('foundation_v1', 'ongoing'):
        conditions.append("r.dataset_cohort = :cohort")
        params['cohort'] = cohort

    sector_applied = False
    if sector:
        conditions.append("br.sector = :sector")
        params['sector'] = sector
        sector_applied = True

    def _run(conds, pms):
        where = " AND ".join(conds)
        sql = f"""
            SELECT
                COUNT(*) AS n,
                ROUND((100.0 * SUM(CASE WHEN br.final_distance_pct < 0 THEN 1 ELSE 0 END)
                       / NULLIF(COUNT(*), 0))::numeric, 1) AS assigned_pct,
                ROUND((100.0 * SUM(CASE WHEN br.touched THEN 1 ELSE 0 END)
                       / NULLIF(COUNT(*), 0))::numeric, 1) AS touch_pct,
                ROUND((100.0 * SUM(CASE WHEN br.mfe_pct >= 4.0 THEN 1 ELSE 0 END)
                       / NULLIF(COUNT(*), 0))::numeric, 1) AS runaway_pct,
                ROUND(AVG(br.mae_pct)::numeric, 2)          AS avg_mae,
                ROUND(AVG(br.mfe_pct)::numeric, 2)          AS avg_mfe,
                ROUND(AVG(br.final_distance_pct)::numeric, 2) AS avg_final_dist,
                ROUND((100.0 * SUM(CASE WHEN br.mae_pct < -10 THEN 1 ELSE 0 END)
                       / NULLIF(COUNT(*), 0))::numeric, 1) AS pct_worse_than_10,
                ROUND((100.0 * SUM(CASE WHEN br.mae_pct < -15 THEN 1 ELSE 0 END)
                       / NULLIF(COUNT(*), 0))::numeric, 1) AS pct_worse_than_15,
                ROUND((100.0 * SUM(CASE WHEN br.mae_pct < -20 THEN 1 ELSE 0 END)
                       / NULLIF(COUNT(*), 0))::numeric, 1) AS pct_worse_than_20
            FROM backtest_results br
            JOIN backtest_runs r ON br.run_id = r.id
            WHERE {where}
        """
        return db_session.execute(text(sql), pms).fetchone()

    row = _run(conditions, params)
    n = int(row[0]) if row and row[0] else 0

    # Auto-relax sector
    if n < min_n and sector_applied:
        conditions_ns = [c for c in conditions if 'sector' not in c]
        params_ns = {k: v for k, v in params.items() if k != 'sector'}
        row = _run(conditions_ns, params_ns)
        n = int(row[0]) if row and row[0] else 0
        sector_applied = False

    if n == 0:
        return None

    def _f(v):
        return float(v) if v is not None else None

    if n >= 200:
        confidence = 'high'
    elif n >= 75:
        confidence = 'medium'
    elif n >= min_n:
        confidence = 'low'
    else:
        confidence = 'insufficient'

    assigned_pct = _f(row[1])
    touch_pct    = _f(row[2])
    recovery_rate = (
        round(touch_pct - assigned_pct, 1)
        if touch_pct is not None and assigned_pct is not None
        else None
    )

    return {
        'n':                 n,
        'confidence':        confidence,
        'sector_matched':    sector_applied,
        'assigned_pct':      assigned_pct,
        'touch_pct':         touch_pct,
        'runaway_pct':       _f(row[3]),
        'recovery_rate':     recovery_rate,
        'avg_mae':           _f(row[4]),
        'avg_mfe':           _f(row[5]),
        'avg_final_dist':    _f(row[6]),
        'pct_worse_than_10': _f(row[7]),
        'pct_worse_than_15': _f(row[8]),
        'pct_worse_than_20': _f(row[9]),
    }


# ── Hold window comparison ────────────────────────────────────────────────────

def get_hold_window_comparison(
    db_session,
    vix_regime: str,
    spy_above_ema50: bool,
    path_safety_grade: Optional[str],
    strike_pct: float = 0.02,
    cohort: str = 'all',
) -> list:
    conditions = [
        "br.vix_regime = :vix_regime",
        "br.spy_above_ema50 = :spy_above_ema50",
        "br.strike_pct BETWEEN :strike_lo AND :strike_hi",
    ]
    params: dict = {
        'vix_regime':      vix_regime,
        'spy_above_ema50': spy_above_ema50,
        'strike_lo':       strike_pct - 0.005,
        'strike_hi':       strike_pct + 0.005,
    }
    if path_safety_grade:
        conditions.append("br.path_safety_grade = :grade")
        params['grade'] = path_safety_grade
    if cohort in ('foundation_v1', 'ongoing'):
        conditions.append("r.dataset_cohort = :cohort")
        params['cohort'] = cohort

    where = " AND ".join(conditions)
    sql = f"""
        SELECT
            br.hold_days,
            COUNT(*) AS n,
            ROUND((100.0 * SUM(CASE WHEN br.final_distance_pct < 0 THEN 1 ELSE 0 END)
                   / NULLIF(COUNT(*), 0))::numeric, 1) AS assigned_pct,
            ROUND(AVG(br.mae_pct)::numeric, 2) AS avg_mae
        FROM backtest_results br
        JOIN backtest_runs r ON br.run_id = r.id
        WHERE {where}
        GROUP BY br.hold_days
        HAVING COUNT(*) >= 10
        ORDER BY br.hold_days
    """
    rows = db_session.execute(text(sql), params).fetchall()
    return [
        {
            'hold_days':    int(r[0]),
            'n':            int(r[1]),
            'assigned_pct': float(r[2]) if r[2] is not None else None,
            'avg_mae':      float(r[3]) if r[3] is not None else None,
        }
        for r in rows
    ]


# ── Strike comparison ─────────────────────────────────────────────────────────

def get_strike_comparison(
    db_session,
    vix_regime: str,
    spy_above_ema50: bool,
    path_safety_grade: Optional[str],
    hold_days: int = 21,
    cohort: str = 'all',
) -> list:
    conditions = [
        "br.vix_regime = :vix_regime",
        "br.spy_above_ema50 = :spy_above_ema50",
        "br.hold_days BETWEEN :hold_lo AND :hold_hi",
    ]
    params: dict = {
        'vix_regime':      vix_regime,
        'spy_above_ema50': spy_above_ema50,
        'hold_lo':         hold_days - 3,
        'hold_hi':         hold_days + 3,
    }
    if path_safety_grade:
        conditions.append("br.path_safety_grade = :grade")
        params['grade'] = path_safety_grade
    if cohort in ('foundation_v1', 'ongoing'):
        conditions.append("r.dataset_cohort = :cohort")
        params['cohort'] = cohort

    where = " AND ".join(conditions)
    sql = f"""
        SELECT
            br.strike_pct,
            COUNT(*) AS n,
            ROUND((100.0 * SUM(CASE WHEN br.final_distance_pct < 0 THEN 1 ELSE 0 END)
                   / NULLIF(COUNT(*), 0))::numeric, 1) AS assigned_pct,
            ROUND(AVG(br.mae_pct)::numeric, 2) AS avg_mae
        FROM backtest_results br
        JOIN backtest_runs r ON br.run_id = r.id
        WHERE {where}
        GROUP BY br.strike_pct
        HAVING COUNT(*) >= 10
        ORDER BY br.strike_pct
    """
    rows = db_session.execute(text(sql), params).fetchall()
    return [
        {
            'strike_pct':   float(r[0]),
            'n':            int(r[1]),
            'assigned_pct': float(r[2]) if r[2] is not None else None,
            'avg_mae':      float(r[3]) if r[3] is not None else None,
        }
        for r in rows
    ]


# ── Industry warning ──────────────────────────────────────────────────────────

def check_industry_warning(industry: Optional[str]) -> Optional[str]:
    if not industry:
        return None
    for flagged in FLAGGED_INDUSTRIES:
        if flagged.lower() in industry.lower():
            return (
                f"{industry} — grade separation historically unreliable. "
                "Binary risk events (trials, oil spikes, rate moves) dominate path safety."
            )
    return None


# ── Parabolic stats ───────────────────────────────────────────────────────────

def get_parabolic_stats(
    db_session,
    vix_regime: str,
    spy_above_ema50: bool,
    extension_ratio: float,
) -> dict:
    """
    Query parabolic/extreme-extension stats from backtest_results.
    Only call when extension_ratio >= 1.50.
    Returns dict with all_regimes, current_regime, standard_f_comparison.
    """
    is_extreme = extension_ratio >= 2.00

    def _f(v):
        return float(v) if v is not None else None

    def _confidence(n: int) -> str:
        if n >= 100: return 'high'
        if n >= 50:  return 'moderate'
        if n >= 30:  return 'low'
        return 'very low'

    all_row = db_session.execute(text("""
        SELECT COUNT(*) AS n,
            ROUND((100.0 * SUM(CASE WHEN final_distance_pct < 0 THEN 1 ELSE 0 END)
                   / NULLIF(COUNT(*), 0))::numeric, 1) AS assigned_pct,
            ROUND(AVG(mae_pct)::numeric, 2)              AS avg_mae,
            ROUND(AVG(mfe_pct)::numeric, 2)              AS avg_mfe,
            ROUND((100.0 * SUM(CASE WHEN touched THEN 1 ELSE 0 END)
                   / NULLIF(COUNT(*), 0))::numeric, 1) AS touch_pct,
            ROUND((100.0 * SUM(CASE WHEN mfe_pct >= 4.0 THEN 1 ELSE 0 END)
                   / NULLIF(COUNT(*), 0))::numeric, 1) AS exit_pct
        FROM backtest_results
        WHERE strike_pct = 0.02 AND hold_days = 21 AND extension_ratio >= 1.50
    """)).fetchone()

    regime_row = db_session.execute(text("""
        SELECT COUNT(*) AS n,
            ROUND((100.0 * SUM(CASE WHEN final_distance_pct < 0 THEN 1 ELSE 0 END)
                   / NULLIF(COUNT(*), 0))::numeric, 1) AS assigned_pct,
            ROUND(AVG(mae_pct)::numeric, 2)              AS avg_mae,
            ROUND(AVG(mfe_pct)::numeric, 2)              AS avg_mfe,
            ROUND((100.0 * SUM(CASE WHEN touched THEN 1 ELSE 0 END)
                   / NULLIF(COUNT(*), 0))::numeric, 1) AS touch_pct,
            ROUND((100.0 * SUM(CASE WHEN mfe_pct >= 4.0 THEN 1 ELSE 0 END)
                   / NULLIF(COUNT(*), 0))::numeric, 1) AS exit_pct
        FROM backtest_results
        WHERE strike_pct = 0.02 AND hold_days = 21 AND extension_ratio >= 1.50
          AND vix_regime = :vr AND spy_above_ema50 = :spy
    """), {'vr': vix_regime, 'spy': spy_above_ema50}).fetchone()

    std_f_row = db_session.execute(text("""
        SELECT
            ROUND(AVG(mae_pct)::numeric, 2) AS avg_mae,
            ROUND(AVG(mfe_pct)::numeric, 2) AS avg_mfe
        FROM backtest_results
        WHERE strike_pct = 0.02 AND hold_days = 21
          AND path_safety_grade = 'F'
          AND (extension_ratio IS NULL OR extension_ratio < 1.30)
    """)).fetchone()

    all_n    = int(all_row[0])    if all_row    and all_row[0]    else 0
    regime_n = int(regime_row[0]) if regime_row and regime_row[0] else 0

    return {
        'is_parabolic':    True,
        'is_extreme':      is_extreme,
        'extension_ratio': extension_ratio,
        'all_regimes': {
            'n':            all_n,
            'assigned_pct': _f(all_row[1]) if all_n > 0 else None,
            'avg_mae':      _f(all_row[2]) if all_n > 0 else None,
            'avg_mfe':      _f(all_row[3]) if all_n > 0 else None,
            'touch_pct':    _f(all_row[4]) if all_n > 0 else None,
            'exit_pct':     _f(all_row[5]) if all_n > 0 else None,
        },
        'current_regime': {
            'n':            regime_n,
            'assigned_pct': _f(regime_row[1]) if regime_n > 0 else None,
            'avg_mae':      _f(regime_row[2]) if regime_n > 0 else None,
            'avg_mfe':      _f(regime_row[3]) if regime_n > 0 else None,
            'touch_pct':    _f(regime_row[4]) if regime_n > 0 else None,
            'exit_pct':     _f(regime_row[5]) if regime_n > 0 else None,
            'confidence':   _confidence(regime_n),
        },
        'standard_f_comparison': {
            'avg_mae': _f(std_f_row[0]) if std_f_row else None,
            'avg_mfe': _f(std_f_row[1]) if std_f_row else None,
        },
    }


# ── Earnings proximity check ──────────────────────────────────────────────────

def check_earnings_proximity(db_session, ticker: str, hold_days: int = 21) -> Optional[dict]:
    from datetime import date
    today = date.today()

    row = db_session.execute(text(
        "SELECT next_earnings_date FROM earnings_calendar "
        "WHERE ticker = :ticker AND next_earnings_date >= :today "
        "ORDER BY next_earnings_date ASC LIMIT 1"
    ), {'ticker': ticker, 'today': today}).fetchone()

    if not row or not row[0]:
        return None

    earnings_date = row[0]
    days_until = (earnings_date - today).days

    return {
        'date':       str(earnings_date),
        'days_until': days_until,
        'in_window':  days_until <= hold_days,
        'warning': (
            f"Earnings in {days_until}d ({earnings_date}) — "
            f"within {hold_days}d hold window. Assignment risk elevated."
            if days_until <= hold_days else None
        ),
    }
