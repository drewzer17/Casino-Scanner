"""Scan engine: pulls yfinance data for each ticker, computes metrics, scores, buckets.

No pandas or numpy are imported here. yfinance returns DataFrames internally but
we convert them to plain Python lists/dicts immediately after each call so none
of the C-extension machinery (libstdc++, glibc) is needed at runtime.

Design goals (Phase 1):
- Crash-resilient: results are committed every BATCH_SIZE tickers.
- Resumable: detects an existing 'running' ScanRun and skips already-done tickers.
- Per-ticker timeout: every ticker runs in a ThreadPoolExecutor with a 10s limit.
- One-bad-ticker isolation: exceptions are caught per-ticker, never kill the run.

IV rank is approximated from 252 days of close-to-close realized vol percentile
because yfinance does not expose historical IV. Directionally correct for ranking;
a future phase can swap in a real IV-history source.
"""
from __future__ import annotations

import logging
import math
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeout
from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable

import yfinance as yf
from sqlalchemy import select
from sqlalchemy.orm import Session

from .. import models
from ..universe import load_universe
from .buckets import assign_bucket
from .scoring import TickerMetrics, score_ticker

logger = logging.getLogger(__name__)

TRADING_DAYS = 252
HV_WINDOW = 30
BATCH_SIZE = 25
TICKER_TIMEOUT = 10  # seconds per ticker before we give up and move on


# ── Pure-Python math helpers (replaces numpy) ─────────────────────────────────

def _is_valid(v: object) -> bool:
    """True if v is a finite, non-None number."""
    try:
        return v is not None and not math.isnan(float(v))
    except (TypeError, ValueError):
        return False


def _stdev(sample: list[float]) -> float:
    """Population-corrected standard deviation (ddof=1). Returns 0 if len<2."""
    n = len(sample)
    if n < 2:
        return 0.0
    mean = sum(sample) / n
    variance = sum((x - mean) ** 2 for x in sample) / (n - 1)
    return math.sqrt(variance) if variance > 0 else 0.0


def _log_returns(prices: list[float]) -> list[float]:
    """Compute log(p[i]/p[i-1]) for a list of prices, skipping non-positive pairs."""
    result: list[float] = []
    for i in range(1, len(prices)):
        if prices[i - 1] > 0 and prices[i] > 0:
            result.append(math.log(prices[i] / prices[i - 1]))
    return result


def _annualized_vol(returns: list[float], window: int = HV_WINDOW) -> float | None:
    """Annualized historical volatility from a list of log returns."""
    if len(returns) < window:
        return None
    sample = returns[-window:]
    std = _stdev(sample)
    if std <= 0:
        return None
    return std * math.sqrt(TRADING_DAYS)


def _rolling_vol(returns: list[float], window: int = HV_WINDOW) -> list[float]:
    """Annualized rolling std over each consecutive window of log returns."""
    result: list[float] = []
    for i in range(window - 1, len(returns)):
        sample = returns[i - window + 1 : i + 1]
        result.append(_stdev(sample) * math.sqrt(TRADING_DAYS))
    return result


def _iv_rank_from_history(closes: list[float], current_iv: float | None) -> float | None:
    """Approximate IV rank from 1-year of realized-vol rolling windows.

    Finds the percentile of current_iv against the distribution of 30-day
    realized vol over the past 252 trading days.
    """
    if len(closes) < 60 or current_iv is None:
        return None
    log_ret = _log_returns(closes)
    rolling = _rolling_vol(log_ret, HV_WINDOW)[-TRADING_DAYS:]
    if not rolling:
        return None
    lo = min(rolling)
    hi = max(rolling)
    if hi <= lo:
        return None
    rank = (current_iv - lo) / (hi - lo) * 100
    return max(0.0, min(100.0, rank))


# ── yfinance data helpers ─────────────────────────────────────────────────────

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
            score = abs(days - 30)
            if best is None or score < best[0]:
                best = (score, exp)
    if best:
        return best[1]
    # fallback: first expiry at least 7 days out
    for exp in exp_dates:
        try:
            d = datetime.strptime(exp, "%Y-%m-%d").date()
        except ValueError:
            continue
        if (d - today).days >= 7:
            return exp
    return None


def _pick_atm(calls: list[dict], price: float) -> dict | None:
    """Return the call row whose strike is closest to price."""
    valid = [c for c in calls if _is_valid(c.get("strike"))]
    if not valid:
        return None
    return min(valid, key=lambda c: abs(float(c["strike"]) - price))


def _days_to_earnings(tkr: yf.Ticker) -> int | None:
    try:
        cal = tkr.calendar
    except Exception:
        return None

    next_dt: datetime | None = None

    if isinstance(cal, dict):
        value = cal.get("Earnings Date")
        if isinstance(value, list) and value:
            value = value[0]
        if isinstance(value, (datetime, date)):
            next_dt = datetime(value.year, value.month, value.day)
    elif cal is not None and hasattr(cal, "loc"):
        # Older yfinance returns a DataFrame — extract without pd.notna / pd.Timestamp
        try:
            value = cal.loc["Earnings Date"].iloc[0]
            if value is not None:
                if hasattr(value, "to_pydatetime"):
                    next_dt = value.to_pydatetime()
                elif hasattr(value, "year"):
                    next_dt = datetime(value.year, value.month, value.day)
        except Exception:
            pass

    if next_dt is None:
        return None
    return (next_dt.date() - date.today()).days


# ── Per-ticker scan ───────────────────────────────────────────────────────────

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


def scan_ticker(ticker: str) -> ScanRowResult | None:
    """Scan a single ticker. Returns None if data is too incomplete to score."""
    try:
        tkr = yf.Ticker(ticker)
        hist = tkr.history(period="1y", auto_adjust=False)

        # Convert DataFrame columns to plain Python lists immediately
        if hist is None or len(hist) == 0:
            return None

        closes: list[float] = [float(v) for v in hist["Close"].tolist() if _is_valid(v)]
        volumes: list[float] = [float(v) for v in hist["Volume"].tolist() if _is_valid(v)]

        if not closes:
            return None
        price = closes[-1]

        log_ret = _log_returns(closes)
        hv = _annualized_vol(log_ret, HV_WINDOW)

        exps = tkr.options or []
        exp = _nearest_expiry(exps)
        atm_iv: float | None = None
        atm_premium: float | None = None
        atm_oi: int | None = None
        spread_pct: float | None = None

        if exp:
            try:
                chain = tkr.option_chain(exp)
                # Convert DataFrame to list-of-dicts right away — no more pandas ops
                calls_df = chain.calls
                calls: list[dict] = (
                    calls_df.to_dict("records") if calls_df is not None and len(calls_df) > 0 else []
                )
                atm = _pick_atm(calls, price)
                if atm is not None:
                    iv_raw = atm.get("impliedVolatility")
                    if _is_valid(iv_raw):
                        atm_iv = float(iv_raw)

                    bid = float(atm.get("bid") or 0)
                    ask = float(atm.get("ask") or 0)
                    last = atm.get("lastPrice")
                    mid = (
                        (bid + ask) / 2
                        if bid > 0 and ask > 0
                        else (float(last) if _is_valid(last) else 0)
                    )
                    if mid > 0:
                        atm_premium = mid
                        if ask > 0 and bid > 0:
                            spread_pct = (ask - bid) / mid

                    oi_raw = atm.get("openInterest")
                    if _is_valid(oi_raw):
                        atm_oi = int(float(oi_raw))
            except Exception as exc:
                logger.debug("chain fetch failed for %s %s: %s", ticker, exp, exc)

        iv_rank = _iv_rank_from_history(closes, atm_iv)
        premium_pct = (atm_premium / price) if (atm_premium and price) else None

        unusual_vol = False
        if len(volumes) >= 25:
            avg_vol = sum(volumes[-20:]) / 20
            if avg_vol > 0 and volumes[-1] > avg_vol * 1.5:
                unusual_vol = True

        earn_days = _days_to_earnings(tkr)

        iv_ramp = (
            iv_rank is not None
            and iv_rank >= 70
            and earn_days is not None
            and 0 < earn_days <= 21
        )

        metrics = TickerMetrics(
            iv_rank=iv_rank,
            premium_pct=premium_pct,
            iv=atm_iv,
            hv=hv,
            earnings_days=earn_days,
            sector_macro_catalyst=False,
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


# ── Timeout wrapper ───────────────────────────────────────────────────────────

def _scan_with_timeout(ticker: str) -> ScanRowResult | None:
    """Run scan_ticker in a thread with a hard timeout.

    yfinance can hang indefinitely on network calls (DNS, SSL, stalled HTTP).
    We isolate each ticker in a 1-worker ThreadPoolExecutor so the timeout is
    enforced without blocking the main scan loop.
    """
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(scan_ticker, ticker)
        try:
            return future.result(timeout=TICKER_TIMEOUT)
        except FuturesTimeout:
            logger.warning("ticker %s timed out after %ds — skipping", ticker, TICKER_TIMEOUT)
            return None
        except Exception as exc:
            logger.warning("ticker %s raised unexpectedly: %s — skipping", ticker, exc)
            return None


# ── DB persistence ────────────────────────────────────────────────────────────

def _persist_result(db: Session, run_id: int, result: ScanRowResult) -> None:
    db.add(models.ScanResult(
        run_id=run_id,
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
    ))


# ── Main scan orchestrator ────────────────────────────────────────────────────

def run_scan(db: Session, tickers: list[str] | None = None, limit: int | None = None) -> int:
    """Run a crash-resilient, resumable scan. Returns the ScanRun id.

    Resume: detects an existing status='running' ScanRun and skips tickers
    that already have DB results for that run.

    Batch commit: commits every BATCH_SIZE tickers — a crash loses at most
    one batch of work.

    Limit: pass limit=N to scan only the first N tickers (smoke testing).
    """
    universe = tickers if tickers is not None else load_universe()
    if limit is not None:
        universe = universe[:limit]

    # ── Find or create a ScanRun ──────────────────────────────────────────────
    existing_run = db.execute(
        select(models.ScanRun)
        .where(models.ScanRun.status == "running")
        .order_by(models.ScanRun.started_at.desc())
        .limit(1)
    ).scalar_one_or_none()

    if existing_run is not None:
        run = existing_run
        logger.info("resuming crashed run_id=%s", run.id)
        done: set[str] = set(
            db.execute(
                select(models.ScanResult.ticker).where(models.ScanResult.run_id == run.id)
            ).scalars().all()
        )
        remaining = [t for t in universe if t not in done]
        scanned = run.tickers_scanned
        errored = run.tickers_errored
        logger.info("resume: %d already done, %d remaining", len(done), len(remaining))
    else:
        run = models.ScanRun(
            started_at=datetime.utcnow(),
            status="running",
            tickers_total=len(universe),
        )
        db.add(run)
        db.commit()
        db.refresh(run)
        remaining = list(universe)
        scanned = 0
        errored = 0
        logger.info("new scan run_id=%s, %d tickers", run.id, len(remaining))

    run.tickers_total = len(universe)
    db.commit()

    # ── Scan in batches ───────────────────────────────────────────────────────
    for batch_start in range(0, len(remaining), BATCH_SIZE):
        batch = remaining[batch_start : batch_start + BATCH_SIZE]
        logger.info(
            "run_id=%s batch %d-%d / %d",
            run.id, batch_start + 1, batch_start + len(batch), len(remaining),
        )
        for ticker in batch:
            result = _scan_with_timeout(ticker)
            if result is None:
                errored += 1
            else:
                scanned += 1
                _persist_result(db, run.id, result)

        run.tickers_scanned = scanned
        run.tickers_errored = errored
        db.commit()
        logger.info("run_id=%s batch done — scanned=%d errored=%d", run.id, scanned, errored)

    run.finished_at = datetime.utcnow()
    run.status = "completed"
    db.commit()
    logger.info("run_id=%s complete — scanned=%d errored=%d", run.id, scanned, errored)
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
