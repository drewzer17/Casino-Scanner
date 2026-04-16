"""Scan engine: pulls yfinance data for each ticker, computes metrics, scores, buckets.

Phase 1 keeps this pragmatic. yfinance is network-flaky so every ticker is
wrapped in a try/except — one bad ticker should never kill the run.

IV rank is approximated from 252 days of close-to-close realized vol percentile
because yfinance does not expose historical IV. It is directionally correct for
ranking purposes; a future phase can swap in a real IV-history source.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterable

import numpy as np
import pandas as pd
import yfinance as yf
from sqlalchemy.orm import Session

from .. import models
from ..universe import load_universe
from .buckets import assign_bucket
from .scoring import TickerMetrics, score_ticker

logger = logging.getLogger(__name__)

TRADING_DAYS = 252
HV_WINDOW = 30


@dataclass
class ScanRowResult:
    ticker: str
    metrics: TickerMetrics
    price: float | None
    atm_call_premium: float | None
    score: float
    bucket: str
    breakdown_iv_rank: float
    breakdown_premium: float
    breakdown_iv_hv: float
    breakdown_catalyst: float
    breakdown_chain: float
    notes: str | None = None


def _annualized_vol(returns: pd.Series, window: int = HV_WINDOW) -> float | None:
    if returns is None or len(returns) < window:
        return None
    sample = returns.tail(window).dropna()
    if len(sample) < 5:
        return None
    std = float(sample.std())
    if math.isnan(std) or std <= 0:
        return None
    return std * math.sqrt(TRADING_DAYS)


def _iv_rank_from_history(hist_close: pd.Series, current_iv: float | None) -> float | None:
    """Approximate IV rank from 1-year of realized-vol history.

    We compute a rolling 30-day realized vol for the last 252 trading days and
    find the percentile of ``current_iv`` against that distribution. If current_iv
    is missing, fall back to the latest realized vol percentile against itself
    (always ~100 — so treat as None).
    """
    if hist_close is None or len(hist_close) < 60 or current_iv is None:
        return None
    logret = np.log(hist_close / hist_close.shift(1)).dropna()
    rolling = logret.rolling(HV_WINDOW).std() * math.sqrt(TRADING_DAYS)
    rolling = rolling.dropna().tail(TRADING_DAYS)
    if rolling.empty:
        return None
    lo = float(rolling.min())
    hi = float(rolling.max())
    if hi <= lo:
        return None
    rank = (current_iv - lo) / (hi - lo) * 100
    return max(0.0, min(100.0, float(rank)))


def _nearest_expiry(exp_dates: Iterable[str], min_days: int = 21, max_days: int = 45) -> str | None:
    today = date.today()
    best: tuple[int, str] | None = None
    for exp in exp_dates:
        try:
            d = datetime.strptime(exp, "%Y-%m-%d").date()
        except ValueError:
            continue
        days = (d - today).days
        if days < 7:
            continue
        if min_days <= days <= max_days:
            # prefer the one closest to 30
            score = abs(days - 30)
            if best is None or score < best[0]:
                best = (score, exp)
    if best:
        return best[1]
    # fall back to the first expiry at least 7 days out
    for exp in exp_dates:
        try:
            d = datetime.strptime(exp, "%Y-%m-%d").date()
        except ValueError:
            continue
        if (d - today).days >= 7:
            return exp
    return None


def _pick_atm(calls: pd.DataFrame, price: float) -> pd.Series | None:
    if calls is None or calls.empty or "strike" not in calls.columns:
        return None
    diffs = (calls["strike"] - price).abs()
    idx = diffs.idxmin()
    if idx is None:
        return None
    return calls.loc[idx]


def _days_to_earnings(tkr: yf.Ticker) -> int | None:
    try:
        cal = tkr.calendar
    except Exception:
        return None
    next_dt: datetime | None = None

    # Newer yfinance returns a dict
    if isinstance(cal, dict):
        value = cal.get("Earnings Date")
        if isinstance(value, list) and value:
            value = value[0]
        if isinstance(value, (datetime, date)):
            next_dt = datetime(value.year, value.month, value.day)
    elif isinstance(cal, pd.DataFrame) and not cal.empty:
        try:
            value = cal.loc["Earnings Date"].iloc[0]
            if pd.notna(value):
                next_dt = pd.Timestamp(value).to_pydatetime()
        except Exception:
            next_dt = None

    if next_dt is None:
        return None
    return (next_dt.date() - date.today()).days


def scan_ticker(ticker: str) -> ScanRowResult | None:
    """Scan a single ticker. Returns None if data is too incomplete to score."""
    try:
        tkr = yf.Ticker(ticker)
        hist = tkr.history(period="1y", auto_adjust=False)
        if hist is None or hist.empty:
            return None

        close = hist["Close"].dropna()
        volume = hist["Volume"].dropna()
        if close.empty:
            return None
        price = float(close.iloc[-1])

        logret = np.log(close / close.shift(1)).dropna()
        hv = _annualized_vol(logret, HV_WINDOW)

        exps = tkr.options or []
        exp = _nearest_expiry(exps)
        atm_iv: float | None = None
        atm_premium: float | None = None
        atm_oi: int | None = None
        spread_pct: float | None = None

        if exp:
            try:
                chain = tkr.option_chain(exp)
                atm = _pick_atm(chain.calls, price)
                if atm is not None:
                    iv_raw = atm.get("impliedVolatility")
                    if iv_raw is not None and not pd.isna(iv_raw):
                        atm_iv = float(iv_raw)
                    bid = float(atm.get("bid") or 0)
                    ask = float(atm.get("ask") or 0)
                    last = atm.get("lastPrice")
                    mid = (bid + ask) / 2 if bid > 0 and ask > 0 else (float(last) if last else 0)
                    if mid > 0:
                        atm_premium = mid
                        if ask > 0 and bid > 0:
                            spread_pct = (ask - bid) / mid
                    oi_raw = atm.get("openInterest")
                    if oi_raw is not None and not pd.isna(oi_raw):
                        atm_oi = int(oi_raw)
            except Exception as exc:  # chain fetch can 404
                logger.debug("chain fetch failed for %s %s: %s", ticker, exp, exc)

        iv_rank = _iv_rank_from_history(close, atm_iv)
        premium_pct = (atm_premium / price) if (atm_premium and price) else None

        unusual_vol = False
        if not volume.empty and len(volume) >= 25:
            avg_vol = float(volume.tail(20).mean())
            last_vol = float(volume.iloc[-1])
            if avg_vol > 0 and last_vol > avg_vol * 1.5:
                unusual_vol = True

        earn_days = _days_to_earnings(tkr)

        iv_ramp = False
        if iv_rank is not None and iv_rank >= 70 and earn_days is not None and 0 < earn_days <= 21:
            iv_ramp = True

        metrics = TickerMetrics(
            iv_rank=iv_rank,
            premium_pct=premium_pct,
            iv=atm_iv,
            hv=hv,
            earnings_days=earn_days,
            sector_macro_catalyst=False,  # Phase 1: no macro feed
            iv_ramp=iv_ramp,
            unusual_volume=unusual_vol,
            open_interest=atm_oi,
            bid_ask_spread_pct=spread_pct,
        )
        breakdown = score_ticker(metrics)
        bucket = assign_bucket(breakdown.total, iv_rank, premium_pct, earn_days)

        return ScanRowResult(
            ticker=ticker,
            metrics=metrics,
            price=price,
            atm_call_premium=atm_premium,
            score=round(breakdown.total, 2),
            bucket=bucket,
            breakdown_iv_rank=breakdown.iv_rank,
            breakdown_premium=breakdown.premium,
            breakdown_iv_hv=breakdown.iv_hv,
            breakdown_catalyst=breakdown.catalyst,
            breakdown_chain=breakdown.chain,
        )
    except Exception as exc:
        logger.warning("scan_ticker failed for %s: %s", ticker, exc)
        return None


def run_scan(db: Session, tickers: list[str] | None = None) -> int:
    """Run a full scan and persist results. Returns the ScanRun id."""
    if tickers is None:
        tickers = load_universe()

    run = models.ScanRun(started_at=datetime.utcnow(), status="running")
    db.add(run)
    db.commit()
    db.refresh(run)

    scanned = 0
    errored = 0
    for ticker in tickers:
        result = scan_ticker(ticker)
        if result is None:
            errored += 1
            continue
        scanned += 1
        row = models.ScanResult(
            run_id=run.id,
            ticker=result.ticker,
            price=result.price,
            iv_rank=result.metrics.iv_rank,
            iv=result.metrics.iv,
            hv=result.metrics.hv,
            atm_call_premium=result.atm_call_premium,
            premium_pct=result.metrics.premium_pct,
            open_interest=result.metrics.open_interest,
            bid_ask_spread_pct=result.metrics.bid_ask_spread_pct,
            earnings_days=result.metrics.earnings_days,
            unusual_volume=result.metrics.unusual_volume,
            score=result.score,
            score_iv_rank=result.breakdown_iv_rank,
            score_premium=result.breakdown_premium,
            score_iv_hv=result.breakdown_iv_hv,
            score_catalyst=result.breakdown_catalyst,
            score_chain=result.breakdown_chain,
            bucket=result.bucket,
        )
        db.add(row)
        if scanned % 25 == 0:
            db.commit()

    run.finished_at = datetime.utcnow()
    run.tickers_scanned = scanned
    run.tickers_errored = errored
    run.status = "completed"
    db.commit()
    return run.id


def run_scan_cli() -> None:
    """Entry point for the daily cron / Railway job."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    from ..database import SessionLocal, init_db

    init_db()
    db = SessionLocal()
    try:
        run_id = run_scan(db)
        logger.info("scan complete, run_id=%s", run_id)
    finally:
        db.close()


if __name__ == "__main__":  # pragma: no cover
    run_scan_cli()
