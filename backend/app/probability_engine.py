"""
probability_engine.py — Empirical CSP probability lookup engine.

Queries backtest_results for observed assignment rates, MAE distributions,
and touch/recovery statistics matching current market regime + ticker characteristics.

Python 3.9 compatible: use Optional[type] not X|None union syntax.
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

# Industries where grade separation historically unreliable (binary risk / rate proxy)
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
    # Most recent VIX close
    vix_row = db_session.execute(text(
        "SELECT close FROM price_history WHERE ticker = '^VIX' ORDER BY date DESC LIMIT 1"
    )).fetchone()
    vix = float(vix_row[0]) if vix_row else None

    # Last 60 SPY closes for SMA50 calculation (most-recent first)
    spy_rows = db_session.execute(text(
        "SELECT close FROM price_history WHERE ticker = 'SPY' ORDER BY date DESC LIMIT 60"
    )).fetchall()

    spy_above_ema50 = True  # default if data missing
    if spy_rows and len(spy_rows) >= 50:
        closes = [float(r[0]) for r in spy_rows]  # most-recent first
        spy_close = closes[0]
        sma50 = sum(closes[:50]) / 50
        spy_above_ema50 = spy_close > sma50

    # Classify VIX into regime bucket
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

    return {
        'vix': vix,
        'vix_regime': vix_regime,
        'spy_above_ema50': spy_above_ema50,
        'cell_key': list(cell_key),   # JSON-serialisable (tuple → list)
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
    Look up empirical CSP assignment probabilities from backtest_results.

    Filters by regime, grade, strike_pct, hold_days (within ±3 days), and
    optionally by sector. Auto-relaxes sector if n < min_n.

    Returns None if no matching rows found. Falls back to sector=None if n < min_n.
    """
    # ── Build base WHERE conditions ───────────────────────────────────────────
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

    # Cohort filter via backtest_runs join (always join for cohort access)
    cohort_join = "JOIN backtest_runs r ON br.run_id = r.id"
    if cohort in ('foundation_v1', 'ongoing'):
        conditions.append("r.dataset_cohort = :cohort")
        params['cohort'] = cohort
    # 'all' / 'all_with_tails' = no cohort filter

    sector_applied = False
    if sector:
        conditions.append("br.sector = :sector")
        params['sector'] = sector
        sector_applied = True

    where = " AND ".join(conditions)

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
        {cohort_join}
        WHERE {where}
    """

    row = db_session.execute(text(sql), params).fetchone()
    n = int(row[0]) if row and row[0] else 0

    # Auto-relax sector if n < min_n
    if n < min_n and sector_applied:
        conditions_ns = [c for c in conditions if 'sector' not in c]
        params_ns = {k: v for k, v in params.items() if k != 'sector'}
        where_ns = " AND ".join(conditions_ns)
        sql_ns = sql.replace(f"WHERE {where}", f"WHERE {where_ns}")
        row = db_session.execute(text(sql_ns), params_ns).fetchone()
        n = int(row[0]) if row and row[0] else 0
        sector_applied = False

    if n == 0:
        return None

    def _f(v):
        return float(v) if v is not None else None

    # Confidence tier
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
    # Recovery rate = pct that touched but did NOT get assigned (recovered)
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
    """
    Returns assignment rates grouped by hold_days bucket for the current regime + grade.
    """
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
    """
    Returns assignment rates grouped by strike_pct bucket for the current regime + grade.
    """
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
    """Returns a warning string if the industry is in FLAGGED_INDUSTRIES, else None."""
    if not industry:
        return None
    for flagged in FLAGGED_INDUSTRIES:
        if flagged.lower() in industry.lower():
            return (
                f"{industry} — grade separation historically unreliable for this industry. "
                "Binary risk events (trials, oil spikes, rate moves) dominate path safety."
            )
    return None


# ── Earnings proximity check ──────────────────────────────────────────────────

def check_earnings_proximity(
    db_session,
    ticker: str,
    hold_days: int = 21,
) -> Optional[dict]:
    """
    Checks earnings_calendar for an upcoming earnings date within the hold window.
    Returns dict with date and days_until if found, else None.
    """
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

    if days_until <= hold_days:
        return {
            'date':       str(earnings_date),
            'days_until': days_until,
            'in_window':  True,
            'warning':    (
                f"Earnings in {days_until}d ({earnings_date}) — "
                f"within {hold_days}d hold window. Assignment risk elevated."
            ),
        }

    return {
        'date':       str(earnings_date),
        'days_until': days_until,
        'in_window':  False,
        'warning':    None,
    }
