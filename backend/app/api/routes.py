"""Public API endpoints."""
from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .. import models
from ..database import SessionLocal, get_db
from ..schemas import (
    MoverOut,
    MoversOut,
    ScanLatestOut,
    ScanResultOut,
    ScoreBreakdown,
    StrikeSuggestion,
    TimeframeDelta,
    WheelOut,
)

logger = logging.getLogger(__name__)

# Simple lock so two HTTP requests can't kick off concurrent scans
_scan_lock = threading.Lock()

# ── Score history helpers ──────────────────────────────────────────────────────

HISTORY_DAYS = [1, 2, 3, 4, 5, 7]
_BUCKET_RANK = {"sell_now": 2, "buy_sell_later": 1, "watchlist": 0}


def _history_lookup(
    db: Session, current_run_id: int
) -> dict[int, dict[str, tuple[float, str]]]:
    """For each N in HISTORY_DAYS return {ticker: (score, bucket)} from the most
    recent completed run that finished at least N-0.5 days ago.

    Cost: 2 queries per timeframe = 12 queries total, all indexed.
    """
    now = datetime.utcnow()
    result: dict[int, dict[str, tuple[float, str]]] = {}
    for n in HISTORY_DAYS:
        cutoff = now - timedelta(days=n - 0.5)
        hist_run = db.execute(
            select(models.ScanRun)
            .where(
                models.ScanRun.status == "completed",
                models.ScanRun.id != current_run_id,
                models.ScanRun.finished_at <= cutoff,
            )
            .order_by(models.ScanRun.finished_at.desc())
            .limit(1)
        ).scalar_one_or_none()

        if hist_run is None:
            result[n] = {}
            continue

        rows = db.execute(
            select(models.ScanResult.ticker, models.ScanResult.score, models.ScanResult.bucket)
            .where(models.ScanResult.run_id == hist_run.id)
        ).all()
        result[n] = {r.ticker: (r.score, r.bucket) for r in rows}
    return result


def _compute_deltas(
    ticker: str,
    current_score: float,
    current_bucket: str,
    lookup: dict[int, dict[str, tuple[float, str]]],
) -> list[TimeframeDelta]:
    deltas: list[TimeframeDelta] = []
    for n in HISTORY_DAYS:
        entry = lookup.get(n, {}).get(ticker)
        if entry is None:
            deltas.append(TimeframeDelta(days=n, prev_score=None, delta=None, prev_bucket=None, arrow=None))
            continue
        prev_score, prev_bucket = entry
        delta = current_score - prev_score
        if delta >= 0:
            arrow = "up_green"
        elif _BUCKET_RANK.get(current_bucket, 0) < _BUCKET_RANK.get(prev_bucket, 0):
            arrow = "down_red"    # dropped to a lower bucket — opportunity gone
        else:
            arrow = "down_yellow"  # cooling but still in same or higher bucket
        deltas.append(TimeframeDelta(
            days=n,
            prev_score=round(prev_score, 2),
            delta=round(delta, 2),
            prev_bucket=prev_bucket,
            arrow=arrow,
        ))
    return deltas

router = APIRouter(prefix="/api", tags=["scanner"])


def _to_out(
    row: models.ScanResult,
    history: list[TimeframeDelta] | None = None,
) -> ScanResultOut:
    return ScanResultOut(
        ticker=row.ticker,
        price=row.price,
        iv_rank=row.iv_rank,
        iv=row.iv,
        hv=row.hv,
        atm_call_premium=row.atm_call_premium,
        premium_pct=row.premium_pct,
        open_interest=row.open_interest,
        bid_ask_spread_pct=row.bid_ask_spread_pct,
        earnings_days=row.earnings_days,
        unusual_volume=row.unusual_volume,
        score=row.score,
        bucket=row.bucket,
        breakdown=ScoreBreakdown(
            iv_rank=row.score_iv_rank,
            premium=row.score_premium,
            iv_hv=row.score_iv_hv,
            catalyst=row.score_catalyst,
            chain=row.score_chain,
        ),
        history=history or [],
        notes=row.notes,
        created_at=row.created_at,
        # SMA
        sma_200=row.sma_200,
        sma_50=row.sma_50,
        price_vs_sma200_pct=row.price_vs_sma200_pct,
        price_vs_sma50_pct=row.price_vs_sma50_pct,
        sma_regime=row.sma_regime,
        sma_golden_cross=row.sma_golden_cross,
        # S/R
        support_1=row.support_1,
        support_1_strength=row.support_1_strength,
        support_2=row.support_2,
        support_2_strength=row.support_2_strength,
        resistance_1=row.resistance_1,
        resistance_1_strength=row.resistance_1_strength,
        resistance_2=row.resistance_2,
        resistance_2_strength=row.resistance_2_strength,
    )


def _latest_run(db: Session) -> models.ScanRun | None:
    stmt = (
        select(models.ScanRun)
        .where(models.ScanRun.status == "completed")
        .order_by(models.ScanRun.finished_at.desc())
        .limit(1)
    )
    return db.execute(stmt).scalar_one_or_none()


@router.get("/scan/latest", response_model=ScanLatestOut)
def scan_latest(db: Session = Depends(get_db)) -> ScanLatestOut:
    run = _latest_run(db)
    if run is None:
        raise HTTPException(status_code=404, detail="No completed scan runs yet")

    rows = (
        db.execute(
            select(models.ScanResult)
            .where(models.ScanResult.run_id == run.id)
            .order_by(models.ScanResult.score.desc())
        )
        .scalars()
        .all()
    )

    # Compute score history for all timeframes (12 queries total, all indexed)
    lookup = _history_lookup(db, run.id)

    sell_now: list[ScanResultOut] = []
    buy_sell_later: list[ScanResultOut] = []
    watchlist: list[ScanResultOut] = []
    for row in rows:
        hist = _compute_deltas(row.ticker, row.score, row.bucket, lookup)
        out = _to_out(row, history=hist)
        if row.bucket == "sell_now":
            sell_now.append(out)
        elif row.bucket == "buy_sell_later":
            buy_sell_later.append(out)
        else:
            watchlist.append(out)

    return ScanLatestOut(
        run_id=run.id,
        started_at=run.started_at,
        finished_at=run.finished_at,
        tickers_scanned=run.tickers_scanned,
        sell_now=sell_now,
        buy_sell_later=buy_sell_later,
        watchlist=watchlist[:100],  # cap the watchlist payload
    )


@router.get("/ticker/{ticker}", response_model=ScanResultOut)
def ticker_detail(ticker: str, db: Session = Depends(get_db)) -> ScanResultOut:
    ticker = ticker.strip().upper()
    stmt = (
        select(models.ScanResult)
        .where(models.ScanResult.ticker == ticker)
        .order_by(models.ScanResult.created_at.desc())
        .limit(1)
    )
    row = db.execute(stmt).scalar_one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail=f"No scan data for {ticker}")
    return _to_out(row)


@router.get("/movers", response_model=MoversOut)
def movers(db: Session = Depends(get_db), limit: int = 5, days: int = 7) -> MoversOut:
    """Return top gainers and losers over the last `days` days (default 7)."""
    latest = _latest_run(db)
    if latest is None:
        raise HTTPException(status_code=404, detail="No completed scan runs yet")

    # Find the comparison run: most recent completed run at least (days-0.5) days ago
    cutoff = datetime.utcnow() - timedelta(days=days - 0.5)
    prev_stmt = (
        select(models.ScanRun)
        .where(
            models.ScanRun.status == "completed",
            models.ScanRun.id != latest.id,
            models.ScanRun.finished_at <= cutoff,
        )
        .order_by(models.ScanRun.finished_at.desc())
        .limit(1)
    )
    previous = db.execute(prev_stmt).scalar_one_or_none()

    # Fall back to the immediately previous run if nothing is old enough
    if previous is None:
        previous = db.execute(
            select(models.ScanRun)
            .where(models.ScanRun.status == "completed", models.ScanRun.id != latest.id)
            .order_by(models.ScanRun.finished_at.desc())
            .limit(1)
        ).scalar_one_or_none()

    latest_rows = (
        db.execute(select(models.ScanResult).where(models.ScanResult.run_id == latest.id))
        .scalars().all()
    )

    if previous is None:
        top = sorted(latest_rows, key=lambda r: r.score, reverse=True)[:limit]
        gainers = [
            MoverOut(ticker=r.ticker, score=r.score, prev_score=None, delta=r.score,
                     bucket=r.bucket, prev_bucket=None)
            for r in top
        ]
        return MoversOut(gainers=gainers, losers=[])

    prev_rows = (
        db.execute(select(models.ScanResult).where(models.ScanResult.run_id == previous.id))
        .scalars().all()
    )
    prev_by_ticker = {r.ticker: (r.score, r.bucket) for r in prev_rows}

    deltas: list[MoverOut] = []
    for r in latest_rows:
        prev = prev_by_ticker.get(r.ticker)
        prev_score = prev[0] if prev else None
        prev_bucket = prev[1] if prev else None
        delta = r.score - prev_score if prev_score is not None else r.score
        deltas.append(MoverOut(
            ticker=r.ticker,
            score=r.score,
            prev_score=prev_score,
            delta=round(delta, 2),
            bucket=r.bucket,
            prev_bucket=prev_bucket,
        ))

    deltas.sort(key=lambda m: m.delta, reverse=True)
    gainers = [m for m in deltas[:limit] if m.delta > 0]
    losers = [m for m in sorted(deltas, key=lambda m: m.delta)[:limit] if m.delta < 0]
    return MoversOut(gainers=gainers, losers=losers)


@router.get("/scan/run")
def trigger_scan(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    limit: int | None = None,
) -> dict:
    """Kick off a full scan via a plain GET so it works from a browser or bare curl.

    Returns immediately with JSON — the scan runs in a background thread.
    Poll GET /api/scan/status to track progress, or GET /api/scan/latest
    once it finishes (typically 10-15 min for ~500 tickers).

    Returns 409 if a scan is already in progress.
    """
    in_progress = db.execute(
        select(models.ScanRun).where(models.ScanRun.status == "running").limit(1)
    ).scalar_one_or_none()
    if in_progress:
        # Count how many results exist so far for a live progress hint
        scanned = db.execute(
            select(func.count()).select_from(models.ScanResult)
            .where(models.ScanResult.run_id == in_progress.id)
        ).scalar() or 0
        return {
            "status": "already_running",
            "run_id": in_progress.id,
            "tickers_scanned_so_far": scanned,
            "message": f"Scan already in progress. {scanned} tickers done so far. Poll /api/scan/status for live progress.",
        }

    def _background() -> None:
        if not _scan_lock.acquire(blocking=False):
            logger.warning("trigger_scan: lock already held, skipping duplicate run")
            return
        bg_db = SessionLocal()
        try:
            from ..scanner.engine import run_scan
            run_id = run_scan(bg_db, limit=limit)
            logger.info("background scan finished run_id=%s", run_id)
        except Exception as exc:
            logger.exception("background scan failed: %s", exc)
        finally:
            bg_db.close()
            _scan_lock.release()

    background_tasks.add_task(_background)
    ticker_count = f"first {limit}" if limit else "~500"
    return {
        "status": "scan_started",
        "message": f"Scanning {ticker_count} tickers in the background. Results commit every 25 tickers.",
        "poll_progress": "/api/scan/status",
        "poll_results": "/api/scan/latest",
    }


@router.get("/scan/status")
def scan_status(db: Session = Depends(get_db)) -> dict:
    """Live progress for the most recent scan run (running or finished)."""
    run = db.execute(
        select(models.ScanRun).order_by(models.ScanRun.started_at.desc()).limit(1)
    ).scalar_one_or_none()

    if run is None:
        return {"status": "no_scans_yet", "message": "Hit GET /api/scan/run to start the first scan."}

    # ScanResult rows committed so far — the engine commits every 25 tickers
    committed = db.execute(
        select(func.count()).select_from(models.ScanResult)
        .where(models.ScanResult.run_id == run.id)
    ).scalar() or 0

    total = run.tickers_total or 500  # tickers_total added in migration; fall back to 500
    remaining = max(0, total - committed) if run.status == "running" else 0

    return {
        "status": run.status,           # "running" | "completed" | "failed"
        "run_id": run.id,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "tickers_total": total,
        "tickers_scanned": committed,   # live count from DB rows, not the in-memory counter
        "tickers_remaining": remaining,
        "tickers_errored": run.tickers_errored,
        "estimated_minutes_left": round(remaining / 35) if remaining > 0 else 0,
        "batch_size": 25,
    }


@router.get("/scan/test", response_model=None)
def scan_test() -> dict:
    """Scan 5 known-good tickers synchronously and return raw results.

    Runs inline (no background task) so you see results immediately.
    Use this to verify yfinance connectivity and scoring before running
    the full universe.
    """
    from ..scanner.engine import scan_ticker

    TEST_TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
    results = []
    for ticker in TEST_TICKERS:
        try:
            row = scan_ticker(ticker, price=None)
            if row is None:
                results.append({"ticker": ticker, "status": "no_data", "result": None})
            else:
                results.append({
                    "ticker": ticker,
                    "status": "ok",
                    "result": {
                        "price": row.price,
                        "score": row.score,
                        "bucket": row.bucket,
                        "iv_rank": row.metrics.iv_rank,
                        "iv": row.metrics.iv,
                        "hv": row.metrics.hv,
                        "premium_pct": row.metrics.premium_pct,
                        "open_interest": row.metrics.open_interest,
                        "bid_ask_spread_pct": row.metrics.bid_ask_spread_pct,
                        "earnings_days": row.metrics.earnings_days,
                        "unusual_volume": row.metrics.unusual_volume,
                        "breakdown": {
                            "iv_rank": row.breakdown_iv_rank,
                            "premium": row.breakdown_premium,
                            "iv_hv": row.breakdown_iv_hv,
                            "catalyst": row.breakdown_catalyst,
                            "chain": row.breakdown_chain,
                        },
                    },
                })
        except Exception as exc:
            results.append({"ticker": ticker, "status": "error", "error": str(exc)})

    ok_count = sum(1 for r in results if r["status"] == "ok")
    return {
        "summary": f"{ok_count}/{len(TEST_TICKERS)} tickers scanned successfully",
        "tickers": TEST_TICKERS,
        "results": results,
    }


@router.get("/ticker/{ticker}/wheel", response_model=WheelOut)
def ticker_wheel(
    ticker: str,
    db: Session = Depends(get_db),
    support_1: float | None = None,
    resistance_1: float | None = None,
) -> WheelOut:
    """Live wheel math for a ticker.

    Fetches the nearest 30-DTE option chain from Tradier, finds the best
    CSP strike (at/below support_1) and CC strike (at/above resistance_1),
    and returns combined premium + yield estimates.

    Optional query params support_1 and resistance_1 override the DB-stored
    levels so the frontend can recalculate after user edits.
    """
    from ..scanner.engine import (
        fetch_chain,
        fetch_expirations,
        _nearest_expiry,
        _is_valid,
    )
    import math as _math

    ticker = ticker.strip().upper()

    # Latest scan result for this ticker
    row = db.execute(
        select(models.ScanResult)
        .where(models.ScanResult.ticker == ticker)
        .order_by(models.ScanResult.created_at.desc())
        .limit(1)
    ).scalar_one_or_none()

    price = row.price if row else None

    # Use overrides if provided, else fall back to DB values
    s1 = support_1 if support_1 is not None else (row.support_1 if row else None)
    r1 = resistance_1 if resistance_1 is not None else (row.resistance_1 if row else None)

    base = WheelOut(
        ticker=ticker,
        price=price,
        expiration=None,
        support_1=row.support_1 if row else None,
        support_1_strength=row.support_1_strength if row else None,
        support_2=row.support_2 if row else None,
        support_2_strength=row.support_2_strength if row else None,
        resistance_1=row.resistance_1 if row else None,
        resistance_1_strength=row.resistance_1_strength if row else None,
        resistance_2=row.resistance_2 if row else None,
        resistance_2_strength=row.resistance_2_strength if row else None,
        sma_200=row.sma_200 if row else None,
        sma_50=row.sma_50 if row else None,
        price_vs_sma200_pct=row.price_vs_sma200_pct if row else None,
        price_vs_sma50_pct=row.price_vs_sma50_pct if row else None,
        sma_regime=row.sma_regime if row else None,
        sma_golden_cross=row.sma_golden_cross if row else None,
    )

    if price is None:
        return base

    # Fetch live chain
    try:
        exps = fetch_expirations(ticker)
        exp = _nearest_expiry(exps)
        if not exp:
            return base
        chain = fetch_chain(ticker, exp)
        base.expiration = exp
    except Exception as exc:
        logger.warning("wheel chain fetch failed for %s: %s", ticker, exc)
        return base

    def _mid(contract: dict) -> float | None:
        bid = float(contract.get("bid") or 0)
        ask = float(contract.get("ask") or 0)
        if bid > 0 and ask > 0:
            return (bid + ask) / 2
        return None

    puts = [c for c in chain if c.get("option_type") == "put" and _is_valid(c.get("strike"))]
    calls = [c for c in chain if c.get("option_type") == "call" and _is_valid(c.get("strike"))]

    # CSP: highest-strike put at or below s1 (or 5% below price if no s1)
    csp_target = s1 if s1 else price * 0.95
    csp_candidates = [p for p in puts if float(p["strike"]) <= csp_target]
    if csp_candidates:
        best_put = max(csp_candidates, key=lambda p: float(p["strike"]))
        csp_mid = _mid(best_put)
        csp_strike = float(best_put["strike"])
        base.csp = StrikeSuggestion(
            strike=csp_strike,
            premium=round(csp_mid, 2) if csp_mid else None,
            bid=float(best_put.get("bid") or 0) or None,
            ask=float(best_put.get("ask") or 0) or None,
        )
        if csp_mid:
            base.csp_effective_basis = round(csp_strike - csp_mid, 2)

    # CC: lowest-strike call at or above r1 (or 5% above price if no r1)
    cc_target = r1 if r1 else price * 1.05
    cc_candidates = [c for c in calls if float(c["strike"]) >= cc_target]
    if cc_candidates:
        best_call = min(cc_candidates, key=lambda c: float(c["strike"]))
        cc_mid = _mid(best_call)
        cc_strike = float(best_call["strike"])
        base.cc = StrikeSuggestion(
            strike=cc_strike,
            premium=round(cc_mid, 2) if cc_mid else None,
            bid=float(best_call.get("bid") or 0) or None,
            ask=float(best_call.get("ask") or 0) or None,
        )
        if cc_mid:
            base.cc_profit_if_called = round((cc_strike - price + cc_mid) * 100, 2)

    # Combined wheel math
    csp_prem = base.csp.premium if base.csp else None
    cc_prem = base.cc.premium if base.cc else None
    if csp_prem is not None and cc_prem is not None:
        combined = csp_prem + cc_prem
        csp_stk = base.csp.strike if base.csp else price * 0.95
        capital = price * 100 + csp_stk * 100
        monthly_yield = (combined * 100 / capital * 100) if capital > 0 else None
        base.combined_premium_per_share = round(combined, 2)
        base.capital_required = round(capital, 2)
        base.monthly_yield_pct = round(monthly_yield, 2) if monthly_yield else None
        base.annualized_yield_pct = round(monthly_yield * 12, 2) if monthly_yield else None

    return base


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
