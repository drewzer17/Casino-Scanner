"""
outcome_measurement.py — Phase 4 backtesting layer.

Measures forward outcomes for a synthetic CSP given an entry date and price.
No look-ahead: only queries dates strictly after entry_date.

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
    Measure the forward outcome of a synthetic CSP.

    Queries price_history for `hold_days` trading days strictly after
    entry_date. Tracks intraday touches, close breaches, MAE, MFE,
    and final distance from the strike.
    """
    strike = round(entry_close * (1.0 - strike_pct), 2)

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

    if actual_days == 0:
        return {
            "ticker":             ticker,
            "entry_date":         entry_date,
            "entry_close":        entry_close,
            "strike":             strike,
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

    touched         = False
    breached        = False
    time_to_touch:  Optional[int] = None
    time_to_breach: Optional[int] = None

    worst_low  =  float("inf")   # for MAE (most negative drawdown)
    best_high  = -float("inf")   # for MFE (best gain)

    for day_num, row in enumerate(rows, start=1):
        _date, _open, high, low, close = row
        high  = float(high)  if high  is not None else float(close)
        low   = float(low)   if low   is not None else float(close)
        close = float(close)

        if low < worst_low:
            worst_low = low
        if high > best_high:
            best_high = high

        # Intraday touch: low at or below strike
        if not touched and low <= strike:
            touched        = True
            time_to_touch  = day_num

        # Close breach: closing price below strike
        if not breached and close < strike:
            breached        = True
            time_to_breach  = day_num

    final_close = float(rows[-1][4])

    mae_pct            = round((worst_low  - entry_close) / entry_close * 100.0, 4)
    mfe_pct            = round((best_high  - entry_close) / entry_close * 100.0, 4)
    final_distance_pct = round((final_close - strike)     / strike      * 100.0, 4) if strike != 0 else None

    return {
        "ticker":             ticker,
        "entry_date":         entry_date,
        "entry_close":        entry_close,
        "strike":             strike,
        "strike_pct":         strike_pct,
        "hold_days":          hold_days,
        "actual_days":        actual_days,
        "complete":           complete,
        "touched":            touched,
        "breached":           breached,
        "time_to_touch":      time_to_touch,
        "time_to_breach":     time_to_breach,
        "mae_pct":            mae_pct,
        "mfe_pct":            mfe_pct,
        "final_close":        final_close,
        "final_distance_pct": final_distance_pct,
    }


def measure_all_windows(
    ticker: str,
    entry_date: date,
    entry_close: float,
    db_session,
) -> list[dict]:
    """
    Run measure_outcome for all 16 combinations of hold_days × strike_pct.

    hold_days:  [5, 10, 15, 21]
    strike_pct: [0.00, 0.01, 0.02, 0.03]

    Returns a flat list of 16 outcome dicts ordered by hold_days then strike_pct.
    """
    results = []
    for hold_days in HOLD_DAYS_SET:
        for strike_pct in STRIKE_PCT_SET:
            results.append(
                measure_outcome(ticker, entry_date, entry_close, hold_days, strike_pct, db_session)
            )
    return results
