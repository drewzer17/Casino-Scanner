"""
outcome_measurement.py — Phase 4 backtesting layer.

Measures forward outcomes for a synthetic CSP (and CC) given an entry date
and price.  No look-ahead: only queries dates strictly after entry_date.

Primary entry points:
    measure_outcome(ticker, entry_date, entry_close, hold_days, strike_pct, db_session) -> dict
    measure_all_windows(ticker, entry_date, entry_close, db_session) -> list[dict]
"""
from __future__ import annotations

from datetime import date
from typing import Optional

from sqlalchemy import text

HOLD_DAYS_SET  = [5, 10, 15, 21]
STRIKE_PCT_SET = [0.0, 0.01, 0.02, 0.03]


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def measure_outcome(
    ticker: str,
    entry_date: date,
    entry_close: float,
    hold_days: int,
    strike_pct: float,
    db_session,
) -> dict:
    """
    Measure the forward outcome of a synthetic CSP + CC + directional fields.

    Queries price_history for `hold_days` trading days strictly after
    entry_date.  Tracks:
      - CSP: intraday touches, close breaches, MAE, MFE, final distance
      - CC:  upside touches/breaches, overshoot
      - Directional: max up/down moves, threshold flags (5/8/10%)
      - General: stock return
    """
    csp_strike = round(entry_close * (1.0 - strike_pct), 2)
    cc_strike  = round(entry_close * (1.0 + strike_pct), 2)

    rows = db_session.execute(
        text(
            "SELECT date, open, high, low, close FROM price_history "
            "WHERE ticker = :ticker AND date > :entry AND close IS NOT NULL "
            "ORDER BY date ASC "
            "LIMIT :n"
        ),
        {"ticker": ticker, "entry": entry_date, "n": hold_days},
    ).fetchall()

    actual_days = len(rows)
    complete    = actual_days == hold_days

    _empty_directional = {
        # CC
        "upside_touched":        None,
        "upside_breached":       None,
        "time_to_upside_touch":  None,
        "time_to_upside_breach": None,
        "upside_overshoot_pct":  None,
        "stock_return_pct":      None,
        # directional
        "max_up_move_pct":   None,
        "max_down_move_pct": None,
        "hit_5up":   None, "hit_8up":   None, "hit_10up":   None,
        "hit_5down": None, "hit_8down": None, "hit_10down": None,
        "time_to_5up":   None, "time_to_8up":   None, "time_to_10up":   None,
        "time_to_5down": None, "time_to_8down": None, "time_to_10down": None,
    }

    if actual_days == 0:
        result = {
            "ticker":             ticker,
            "entry_date":         entry_date,
            "entry_close":        entry_close,
            "strike":             csp_strike,
            "strike_pct":         strike_pct,
            "hold_days":          hold_days,
            "actual_days":        0,
            "complete":           False,
            "touched":            None,
            "breached":           None,
            "time_to_touch":      None,
            "time_to_breach":     None,
            "mae_pct":            None,
            "mfe_pct":            None,
            "final_close":        None,
            "final_distance_pct": None,
        }
        result.update(_empty_directional)
        return result

    # ── CSP tracking ─────────────────────────────────────────────────────────
    touched:  bool           = False
    breached: bool           = False
    time_to_touch:  Optional[int] = None
    time_to_breach: Optional[int] = None

    worst_low  =  float("inf")   # for MAE (most negative drawdown)
    best_high  = -float("inf")   # for MFE (best gain)

    # ── CC tracking ──────────────────────────────────────────────────────────
    upside_touched:  bool          = False
    upside_breached: bool          = False
    time_to_upside_touch:  Optional[int] = None
    time_to_upside_breach: Optional[int] = None
    max_high = -float("inf")

    # ── Directional threshold tracking ───────────────────────────────────────
    time_to_5up:   Optional[int] = None
    time_to_8up:   Optional[int] = None
    time_to_10up:  Optional[int] = None
    time_to_5down: Optional[int] = None
    time_to_8down: Optional[int] = None
    time_to_10down: Optional[int] = None

    for day_num, row in enumerate(rows, start=1):
        _date, _open, high, low, close = row
        high  = float(high)  if high  is not None else float(close)
        low   = float(low)   if low   is not None else float(close)
        close = float(close)

        if low  < worst_low:  worst_low  = low
        if high > best_high:  best_high  = high
        if high > max_high:   max_high   = high

        # CSP: intraday touch
        if not touched and low <= csp_strike:
            touched       = True
            time_to_touch = day_num

        # CSP: close breach
        if not breached and close < csp_strike:
            breached        = True
            time_to_breach  = day_num

        # CC: upside intraday touch
        if not upside_touched and high >= cc_strike:
            upside_touched       = True
            time_to_upside_touch = day_num

        # CC: upside close breach
        if not upside_breached and close >= cc_strike:
            upside_breached       = True
            time_to_upside_breach = day_num

        # Directional thresholds (based on intraday high/low vs entry_close)
        up_pct   = (high  - entry_close) / entry_close * 100.0
        down_pct = (entry_close - low)   / entry_close * 100.0

        if time_to_5up   is None and up_pct   >= 5.0:  time_to_5up   = day_num
        if time_to_8up   is None and up_pct   >= 8.0:  time_to_8up   = day_num
        if time_to_10up  is None and up_pct   >= 10.0: time_to_10up  = day_num
        if time_to_5down is None and down_pct >= 5.0:  time_to_5down = day_num
        if time_to_8down is None and down_pct >= 8.0:  time_to_8down = day_num
        if time_to_10down is None and down_pct >= 10.0: time_to_10down = day_num

    final_close = float(rows[-1][4])

    # ── Derived metrics ───────────────────────────────────────────────────────
    mae_pct            = round((worst_low  - entry_close) / entry_close * 100.0, 4)
    mfe_pct            = round((best_high  - entry_close) / entry_close * 100.0, 4)
    final_distance_pct = round((final_close - csp_strike) / csp_strike  * 100.0, 4) if csp_strike != 0 else None

    max_up_move_pct   = round((best_high - entry_close) / entry_close * 100.0, 4)
    max_down_move_pct = round((entry_close - worst_low) / entry_close * 100.0, 4)

    stock_return_pct  = round((final_close - entry_close) / entry_close * 100.0, 4)

    upside_overshoot_pct: Optional[float] = None
    if upside_touched and cc_strike != 0:
        upside_overshoot_pct = round((max_high - cc_strike) / cc_strike * 100.0, 4)
    else:
        upside_overshoot_pct = 0.0

    return {
        # ── identity ─────────────────────────────────────────────────────────
        "ticker":             ticker,
        "entry_date":         entry_date,
        "entry_close":        entry_close,
        "strike":             csp_strike,
        "strike_pct":         strike_pct,
        "hold_days":          hold_days,
        "actual_days":        actual_days,
        "complete":           complete,
        # ── CSP ──────────────────────────────────────────────────────────────
        "touched":            touched,
        "breached":           breached,
        "time_to_touch":      time_to_touch,
        "time_to_breach":     time_to_breach,
        "mae_pct":            mae_pct,
        "mfe_pct":            mfe_pct,
        "final_close":        final_close,
        "final_distance_pct": final_distance_pct,
        # ── CC ───────────────────────────────────────────────────────────────
        "upside_touched":        upside_touched,
        "upside_breached":       upside_breached,
        "time_to_upside_touch":  time_to_upside_touch,
        "time_to_upside_breach": time_to_upside_breach,
        "upside_overshoot_pct":  upside_overshoot_pct,
        "stock_return_pct":      stock_return_pct,
        # ── directional ──────────────────────────────────────────────────────
        "max_up_move_pct":   max_up_move_pct,
        "max_down_move_pct": max_down_move_pct,
        "hit_5up":    max_up_move_pct   >= 5.0,
        "hit_8up":    max_up_move_pct   >= 8.0,
        "hit_10up":   max_up_move_pct   >= 10.0,
        "hit_5down":  max_down_move_pct >= 5.0,
        "hit_8down":  max_down_move_pct >= 8.0,
        "hit_10down": max_down_move_pct >= 10.0,
        "time_to_5up":    time_to_5up,
        "time_to_8up":    time_to_8up,
        "time_to_10up":   time_to_10up,
        "time_to_5down":  time_to_5down,
        "time_to_8down":  time_to_8down,
        "time_to_10down": time_to_10down,
    }


def measure_all_windows(
    ticker: str,
    entry_date: date,
    entry_close: float,
    db_session,
) -> list:
    """
    Run measure_outcome for all 16 combinations of hold_days × strike_pct.

    hold_days:  [5, 10, 15, 21]
    strike_pct: [0.00, 0.01, 0.02, 0.03]

    Returns a flat list of 16 outcome dicts ordered by hold_days then strike_pct.
    """
    results = []
    for hd in HOLD_DAYS_SET:
        for sp in STRIKE_PCT_SET:
            results.append(measure_outcome(ticker, entry_date, entry_close, hd, sp, db_session))
    return results
