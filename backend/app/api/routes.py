"""Public API endpoints."""
from __future__ import annotations

import json
import logging
import math
import threading
from datetime import datetime, timedelta

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .. import models
from ..database import SessionLocal, get_db
from ..schemas import (
    ExpiryRow,
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

HISTORY_DAYS = [1, 2, 3, 4, 5, 7, 14]
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


def _san(val: float | None) -> float | None:
    """Replace NaN/Inf with None so FastAPI can serialize the response."""
    if val is None:
        return None
    try:
        if math.isnan(val) or math.isinf(val):
            return None
    except (TypeError, ValueError):
        return None
    return val


def _parse_expiry_data(raw: str | None) -> list[ExpiryRow]:
    """Deserialize expiry_data JSON, returning [] on any error (handles old format gracefully)."""
    if not raw:
        return []
    try:
        return [ExpiryRow(**e) for e in json.loads(raw)]
    except Exception:
        return []  # old format or corrupt — caller needs a new scan


def _to_out(
    row: models.ScanResult,
    history: list[TimeframeDelta] | None = None,
    sources: list[str] | None = None,
) -> ScanResultOut:
    return ScanResultOut(
        ticker=row.ticker,
        company_name=row.company_name,
        price=_san(row.price),
        iv_rank=_san(row.iv_rank),
        iv=_san(row.iv),
        hv=_san(row.hv),
        atm_call_premium=_san(row.atm_call_premium),
        premium_pct=_san(row.premium_pct),
        premium_otm1=_san(row.premium_otm1),
        premium_otm2=_san(row.premium_otm2),
        open_interest=row.open_interest,
        bid_ask_spread_pct=_san(row.bid_ask_spread_pct),
        earnings_days=row.earnings_days,
        unusual_volume=row.unusual_volume,
        score=_san(row.score) or 0.0,
        bucket=row.bucket,
        breakdown=ScoreBreakdown(
            iv_rank=_san(row.score_iv_rank) or 0.0,
            premium=_san(row.score_premium) or 0.0,
            iv_hv=_san(row.score_iv_hv) or 0.0,
            catalyst=_san(row.score_catalyst) or 0.0,
            chain=_san(row.score_chain) or 0.0,
        ),
        history=history or [],
        notes=row.notes,
        created_at=row.created_at,
        # SMA
        sma_200=_san(row.sma_200),
        sma_50=_san(row.sma_50),
        price_vs_sma200_pct=_san(row.price_vs_sma200_pct),
        price_vs_sma50_pct=_san(row.price_vs_sma50_pct),
        sma_regime=row.sma_regime,
        sma_golden_cross=row.sma_golden_cross,
        # S/R
        support_1=_san(row.support_1),
        support_1_strength=_san(row.support_1_strength),
        support_2=_san(row.support_2),
        support_2_strength=_san(row.support_2_strength),
        resistance_1=_san(row.resistance_1),
        resistance_1_strength=_san(row.resistance_1_strength),
        resistance_2=_san(row.resistance_2),
        resistance_2_strength=_san(row.resistance_2_strength),
        safety_score=_san(row.safety_score),
        # Multi-expiry
        best_expiry=row.best_expiry,
        best_dte=row.best_dte,
        best_strike=_san(row.best_strike),
        expiry_data=_parse_expiry_data(row.expiry_data),
        sources=sources or [],
        # ATM put premium (stored from scan)
        atm_put_premium=_san(row.atm_put_premium),
        best_put_strike=_san(row.best_put_strike),
        best_put_expiry=row.best_put_expiry,
        best_put_dte=row.best_put_dte,
        # CC / CSP scores
        cc_score=row.cc_score,
        csp_score=row.csp_score,
        # Asymmetric setup flags
        asymmetric_cc_flag=row.asymmetric_cc_flag or False,
        asymmetric_csp_flag=row.asymmetric_csp_flag or False,
        asymmetric_ivramp_flag=row.asymmetric_ivramp_flag or False,
        asymmetric_any_flag=row.asymmetric_any_flag or False,
        asymmetric_type=row.asymmetric_type,
        # IV ramp detection
        iv_5d_ago=_san(row.iv_5d_ago),
        iv_10d_ago=_san(row.iv_10d_ago),
        iv_20d_ago=_san(row.iv_20d_ago),
        iv_velocity_5d=_san(row.iv_velocity_5d),
        iv_velocity_10d=_san(row.iv_velocity_10d),
        iv_velocity_20d=_san(row.iv_velocity_20d),
        iv_ramp_score=row.iv_ramp_score or 0,
        iv_ramp_flag=row.iv_ramp_flag or False,
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

    # Bulk-fetch ticker→sources mapping and universe size (2 queries)
    from ..universe import ticker_sources_from_db, universe_size_from_db
    ticker_sources = ticker_sources_from_db(db)
    univ_size = universe_size_from_db(db)

    sell_now: list[ScanResultOut] = []
    buy_sell_later: list[ScanResultOut] = []
    watchlist: list[ScanResultOut] = []
    for row in rows:
        hist = _compute_deltas(row.ticker, row.score, row.bucket, lookup)
        out = _to_out(row, history=hist, sources=ticker_sources.get(row.ticker, []))
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
        universe_size=univ_size,
        sell_now=sell_now,
        buy_sell_later=buy_sell_later,
        watchlist=watchlist,
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


@router.get("/scan/extensive")
def trigger_scan_extensive(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    limit: int | None = None,
) -> dict:
    """Kick off an extensive scan (normal scan + nearest weekly expiry chain per ticker).

    Returns 409 if a scan is already in progress.
    """
    in_progress = db.execute(
        select(models.ScanRun).where(models.ScanRun.status == "running").limit(1)
    ).scalar_one_or_none()
    if in_progress:
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
            logger.warning("trigger_scan_extensive: lock already held, skipping duplicate run")
            return
        bg_db = SessionLocal()
        try:
            from ..scanner.engine import run_scan_extensive
            run_id = run_scan_extensive(bg_db, limit=limit)
            logger.info("background extensive scan finished run_id=%s", run_id)
        except Exception as exc:
            logger.exception("background extensive scan failed: %s", exc)
        finally:
            bg_db.close()
            _scan_lock.release()

    background_tasks.add_task(_background)
    ticker_count = f"first {limit}" if limit else "~500"
    return {
        "status": "extensive_scan_started",
        "message": f"Extensive scan: scanning {ticker_count} tickers with weekly+monthly expiries. Results commit every 25 tickers.",
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

    # Read from the latest completed run (same source as the dashboard card),
    # not just the most recent row by created_at which could be a partial scan.
    _run = _latest_run(db)
    row = None
    if _run:
        row = db.execute(
            select(models.ScanResult)
            .where(
                models.ScanResult.ticker == ticker,
                models.ScanResult.run_id == _run.id,
            )
        ).scalar_one_or_none()
    # Fall back to most-recent row if ticker wasn't in the latest completed run
    if row is None:
        row = db.execute(
            select(models.ScanResult)
            .where(models.ScanResult.ticker == ticker)
            .order_by(models.ScanResult.created_at.desc())
            .limit(1)
        ).scalar_one_or_none()

    price = row.price if row else None

    # Use overrides if provided, else fall back to DB values
    s1 = support_1 if support_1 is not None else (row.support_1 if row else None)
    s2 = row.support_2 if row else None
    r1 = resistance_1 if resistance_1 is not None else (row.resistance_1 if row else None)
    r2 = row.resistance_2 if row else None

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

    # CSP: highest-strike put at or below target.
    # Priority: S1 → S2 → 5% below price (only if no technical level exists).
    def _csp_candidates(target):
        return [p for p in puts if float(p["strike"]) <= target]

    csp_pool = []
    if s1:
        csp_pool = _csp_candidates(s1)
    if not csp_pool and s2:
        csp_pool = _csp_candidates(s2)
    if not csp_pool:
        csp_pool = _csp_candidates(price * 0.95)

    if csp_pool:
        best_put = max(csp_pool, key=lambda p: float(p["strike"]))
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

    # CC: call strike closest to target (above current price).
    # "Closest" beats "strictly above" — R1=$275.17 should pick $275 not $280.
    # Priority: R1 → R2 → 5% above price (only if no technical level exists).
    otm_calls = [c for c in calls if float(c["strike"]) > price]

    def _nearest_cc(target):
        """Strike nearest to target among calls above current price."""
        if not otm_calls:
            return []
        best = min(otm_calls, key=lambda c: abs(float(c["strike"]) - target))
        return [best]

    cc_pool = []
    if r1:
        cc_pool = _nearest_cc(r1)
    if not cc_pool and r2:
        cc_pool = _nearest_cc(r2)
    if not cc_pool:
        cc_pool = _nearest_cc(price * 1.05)

    if cc_pool:
        best_call = min(cc_pool, key=lambda c: float(c["strike"]))
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


@router.get("/debug/ticker/{ticker}", response_model=None)
def debug_ticker(ticker: str) -> dict:
    """Raw Tradier data for any ticker — all expirations, all strikes, bid/ask/mid, IV.

    Useful for diagnosing which expiry was selected, what premiums look like, etc.
    Works for any ticker regardless of scan history.
    """
    from ..scanner.engine import (
        fetch_expirations,
        fetch_chain,
        _nearest_expiry,
        _expirations_for_premium,
        _pick_call_strikes,
        _contract_mid,
        _is_valid,
    )
    from datetime import date as _date, datetime as _dt

    ticker = ticker.strip().upper()

    try:
        exps = fetch_expirations(ticker)
    except Exception as exc:
        return {"ticker": ticker, "error": f"fetch_expirations failed: {exc}"}

    today = _date.today()
    iv_exp = _nearest_expiry(exps)
    prem_exps = _expirations_for_premium(exps)[:5]

    results_by_expiry: list[dict] = []
    for exp in exps:
        try:
            dte = (_dt.strptime(exp, "%Y-%m-%d").date() - today).days
        except ValueError:
            dte = None

        try:
            chain = fetch_chain(ticker, exp)
        except Exception as exc:
            results_by_expiry.append({"expiry": exp, "dte": dte, "error": str(exc)})
            continue

        calls = [c for c in chain if c.get("option_type") == "call"]
        strikes_data: list[dict] = []
        for c in sorted(calls, key=lambda x: float(x.get("strike") or 0)):
            bid = float(c.get("bid") or 0)
            ask = float(c.get("ask") or 0)
            mid = (bid + ask) / 2 if bid > 0 and ask > 0 else None
            greeks = c.get("greeks") or {}
            iv_raw = greeks.get("smv_vol") or greeks.get("mid_iv") or greeks.get("iv")
            strikes_data.append({
                "strike": c.get("strike"),
                "bid": bid or None,
                "ask": ask or None,
                "mid": round(mid, 4) if mid else None,
                "mid_per_contract": round(mid * 100, 2) if mid else None,
                "oi": c.get("open_interest"),
                "iv": float(iv_raw) if _is_valid(iv_raw) else None,
            })

        # Which was selected as ATM?
        atm, otm1, otm2 = _pick_call_strikes(chain, 0)  # will return None without price
        # Re-call with approximate price from chain midpoint if available
        atm_note = "need price to determine ATM; use /api/ticker/{ticker} for scan data"

        results_by_expiry.append({
            "expiry": exp,
            "dte": dte,
            "is_iv_expiry": exp == iv_exp,
            "is_prem_expiry": any(e == exp for _, e in prem_exps),
            "calls_count": len(calls),
            "strikes": strikes_data,
        })

    return {
        "ticker": ticker,
        "all_expirations": exps,
        "iv_expiry": iv_exp,
        "premium_expirations": [{"dte": d, "expiry": e} for d, e in prem_exps],
        "note": "Scores and best-expiry selection require a price; run a scan or see /api/ticker/{ticker}",
        "expirations_detail": results_by_expiry,
    }


@router.get("/debug/errors", response_model=None)
def debug_errors(db: Session = Depends(get_db)) -> dict:
    """Which tickers from the universe didn't make it into the latest scan?

    We don't store per-ticker error messages, so "missing" = universe - scan_results.
    These tickers either timed out, had no options chain, had no price, or raised
    an unhandled exception during scan_ticker().
    """
    from ..universe import load_universe_from_db

    run = _latest_run(db)
    if run is None:
        raise HTTPException(status_code=404, detail="No completed scan runs yet")

    universe = load_universe_from_db(db)
    universe_set = set(universe)

    scanned_tickers: set[str] = set(
        db.execute(
            select(models.ScanResult.ticker).where(models.ScanResult.run_id == run.id)
        ).scalars().all()
    )

    missing = sorted(universe_set - scanned_tickers)
    extra = sorted(scanned_tickers - universe_set)  # in scan but not universe (manual adds, etc.)

    return {
        "run_id": run.id,
        "finished_at": run.finished_at,
        "universe_total": len(universe),
        "scanned_total": len(scanned_tickers),
        "missing_count": len(missing),
        "note": (
            "Missing tickers are in the universe but absent from scan results. "
            "Causes: timeout (most common with multi-expiry fetching), no options chain, "
            "no price from Tradier sandbox, or unhandled exception in scan_ticker(). "
            "Use GET /api/debug/scoring/{ticker} to diagnose a specific ticker live."
        ),
        "missing_tickers": missing,
        "extra_tickers": extra,  # scanned but not in universe (informational)
    }


@router.get("/debug/scoring/{ticker}", response_model=None)
def debug_scoring(ticker: str) -> dict:
    """Live scoring breakdown for any ticker — re-runs the full scan pipeline.

    Returns every intermediate value: raw price, IV, HV, per-expiry premiums,
    chain OI/spread, earnings days, each factor score, SMA regime, and bucket.
    Takes ~10-30s for tickers with many expirations.
    """
    import math as _math
    from ..scanner.engine import (
        fetch_quotes, fetch_bars, fetch_expirations, fetch_chain,
        _nearest_expiry, _expirations_for_premium, _pick_call_strikes,
        _collect_otm_calls, _collect_otm_puts, _contract_mid, _is_valid,
        _log_returns, _annualized_vol, _iv_rank_from_history, _calc_sma,
        _sma_regime, _calc_safety_score, HV_WINDOW,
    )
    from ..scanner.scoring import (
        score_iv_rank, score_premium, score_iv_vs_hv, score_catalyst, score_chain,
    )
    from ..scanner.buckets import assign_bucket
    from ..scanner.engine import build_earnings_lookup
    from datetime import date as _date, datetime as _dt

    ticker = ticker.strip().upper()
    out: dict = {"ticker": ticker, "steps": {}}
    steps = out["steps"]

    def _san_val(v):
        if v is None:
            return None
        try:
            if _math.isnan(v) or _math.isinf(v):
                return None
        except Exception:
            return None
        return round(v, 6) if isinstance(v, float) else v

    # 1. Price
    try:
        quotes = fetch_quotes([ticker])
        q = quotes.get(ticker) or {}
        raw = q.get("last")
        price = float(raw) if _is_valid(raw) else None
        steps["price"] = {"value": price, "raw_quote": {k: q.get(k) for k in ["last", "bid", "ask", "volume", "average_volume"]}}
        if price is None:
            out["error"] = "No price from Tradier — ticker may be delisted or sandbox doesn't carry it"
            return out
    except Exception as exc:
        out["error"] = f"fetch_quotes failed: {exc}"
        return out

    # 2. Historical bars → HV + SMA
    try:
        bars = fetch_bars(ticker)
        closes = [b.close for b in bars]
        log_ret = _log_returns(closes)
        hv = _annualized_vol(log_ret, HV_WINDOW)
        sma_200 = _calc_sma(closes, 200)
        sma_50 = _calc_sma(closes, 50)
        regime = _sma_regime(price, sma_50, sma_200)
        golden_cross = (sma_50 > sma_200) if (sma_50 and sma_200) else None
        steps["history"] = {
            "bars_fetched": len(bars),
            "hv_30d_annualized": _san_val(hv),
            "sma_200": _san_val(sma_200),
            "sma_50": _san_val(sma_50),
            "price_vs_sma200_pct": _san_val((price - sma_200) / sma_200 * 100) if sma_200 else None,
            "price_vs_sma50_pct": _san_val((price - sma_50) / sma_50 * 100) if sma_50 else None,
            "sma_regime": regime,
            "golden_cross": golden_cross,
        }
    except Exception as exc:
        steps["history"] = {"error": str(exc)}
        hv = None; closes = []; bars = []
        sma_200 = sma_50 = regime = golden_cross = None

    # 3. Expirations
    try:
        exps = fetch_expirations(ticker)
        iv_exp = _nearest_expiry(exps)
        prem_exps = _expirations_for_premium(exps)[:5]
        steps["expirations"] = {
            "all": exps,
            "iv_expiry": iv_exp,
            "premium_expirations": [{"dte": d, "expiry": e} for d, e in prem_exps],
        }
        if not exps:
            out["error"] = "No options expirations — not optionable on Tradier sandbox"
            return out
    except Exception as exc:
        out["error"] = f"fetch_expirations failed: {exc}"
        return out

    # 4. IV chain
    atm_iv = None
    atm_oi = None
    spread_pct = None
    chain_cache: dict = {}
    if iv_exp:
        try:
            chain_cache[iv_exp] = fetch_chain(ticker, iv_exp)
            iv_chain = chain_cache[iv_exp]
            atm_c, _, _ = _pick_call_strikes(iv_chain, price)
            if atm_c:
                greeks = atm_c.get("greeks") or {}
                iv_raw = greeks.get("smv_vol") or greeks.get("mid_iv") or greeks.get("iv")
                if _is_valid(iv_raw):
                    atm_iv = float(iv_raw)
                mid = _contract_mid(atm_c)
                if mid and mid > 0:
                    bid = float(atm_c.get("bid") or 0)
                    ask = float(atm_c.get("ask") or 0)
                    if bid > 0 and ask > 0:
                        spread_pct = (ask - bid) / mid
                oi_raw = atm_c.get("open_interest")
                if _is_valid(oi_raw):
                    atm_oi = int(float(oi_raw))
            steps["iv_chain"] = {
                "expiry": iv_exp,
                "atm_strike": float(atm_c["strike"]) if atm_c else None,
                "atm_iv": _san_val(atm_iv),
                "atm_oi": atm_oi,
                "atm_bid": float(atm_c.get("bid") or 0) if atm_c else None,
                "atm_ask": float(atm_c.get("ask") or 0) if atm_c else None,
                "atm_mid": _san_val(_contract_mid(atm_c)) if atm_c else None,
                "spread_pct": _san_val(spread_pct),
                "greeks": atm_c.get("greeks") if atm_c else None,
            }
        except Exception as exc:
            steps["iv_chain"] = {"error": str(exc)}

    # 5. IV rank
    iv_rank = _iv_rank_from_history(closes, atm_iv)
    if iv_rank is None and atm_iv is not None:
        iv_rank = 50.0
    steps["iv_rank"] = {
        "value": _san_val(iv_rank),
        "note": "50.0 = placeholder (need 30+ days of IV history); real value once history accumulates",
    }

    # 6. Premium expirations
    best_atm_premium = None
    best_otm2 = None
    expiry_details = []
    for dte, exp in (prem_exps or []):
        try:
            if exp not in chain_cache:
                chain_cache[exp] = fetch_chain(ticker, exp)
            chain = chain_cache[exp]
            atm_c, _, _ = _pick_call_strikes(chain, price)
            atm_mid = _contract_mid(atm_c)
            atm_strike = float(atm_c["strike"]) if atm_c else None
            otm_calls = _collect_otm_calls(chain, price)
            otm_puts = _collect_otm_puts(chain, price)
            otm2_mid = otm_calls[1]["prem"] if len(otm_calls) > 1 else None
            detail = {
                "expiry": exp,
                "dte": dte,
                "atm_strike": atm_strike,
                "atm_call_mid": _san_val(atm_mid),
                "atm_call_pct": _san_val(atm_mid / price) if atm_mid and price else None,
                "otm_calls": [{"strike": c["strike"], "prem": _san_val(c["prem"])} for c in otm_calls],
                "otm_puts": [{"strike": c["strike"], "prem": _san_val(c["prem"])} for c in otm_puts],
            }
            expiry_details.append(detail)
            if (atm_mid or 0) > (best_atm_premium or 0):
                best_atm_premium = atm_mid
                best_otm2 = otm2_mid
        except Exception as exc:
            expiry_details.append({"expiry": exp, "dte": dte, "error": str(exc)})

    steps["premium_expirations"] = expiry_details

    # 7. Earnings
    try:
        earnings_lookup = build_earnings_lookup()
        earn_days = earnings_lookup.get(ticker)
    except Exception:
        earn_days = None
    steps["earnings"] = {"days_until_earnings": earn_days}

    # 8. Factor scores
    premium_pct = (best_atm_premium / price) if (best_atm_premium and price) else None
    iv_ramp = (iv_rank is not None and iv_rank >= 70 and earn_days is not None and 0 < earn_days <= 21)

    s_ivr = score_iv_rank(iv_rank)
    s_prem = score_premium(premium_pct, best_otm2)
    s_ivhv = score_iv_vs_hv(atm_iv, hv)
    s_cat = score_catalyst(earn_days, False, iv_ramp, False)
    s_chain = score_chain(atm_oi, spread_pct)
    subtotal = s_ivr + s_prem + s_ivhv + s_cat + s_chain

    steps["factor_scores"] = {
        "iv_rank": {
            "raw": _san_val(iv_rank),
            "score": s_ivr,
            "max": 20,
        },
        "premium": {
            "atm_pct": _san_val(premium_pct * 100) if premium_pct else None,
            "otm2_dollar": _san_val(best_otm2 * 100) if best_otm2 else None,
            "score": s_prem,
            "max": 20,
        },
        "iv_vs_hv": {
            "iv": _san_val(atm_iv),
            "hv": _san_val(hv),
            "ratio": _san_val(atm_iv / hv) if (atm_iv and hv and hv > 0) else None,
            "score": s_ivhv,
            "max": 20,
        },
        "catalyst": {
            "earnings_days": earn_days,
            "iv_ramp": iv_ramp,
            "score": s_cat,
            "max": 20,
        },
        "chain": {
            "open_interest": atm_oi,
            "spread_pct": _san_val(spread_pct * 100) if spread_pct else None,
            "score": s_chain,
            "max": 20,
        },
        "subtotal": subtotal,
    }

    # 9. SMA modifier + final score
    try:
        from ..scanner.engine import _sma_score_modifier
        sma_adj = _sma_score_modifier(price, sma_50, sma_200, regime, golden_cross)
    except Exception:
        sma_adj = 0.0

    final_score = round(max(0.0, min(100.0, subtotal + sma_adj)), 2)
    bucket = assign_bucket(final_score, iv_rank, premium_pct, earn_days)

    out["summary"] = {
        "price": price,
        "final_score": final_score,
        "sma_modifier": sma_adj,
        "bucket": bucket,
        "bucket_thresholds": "sell_now: score>=45 AND iv_rank>=45 AND premium_pct>=1.5%",
    }
    out["sell_now_eligible"] = (
        final_score >= 45 and (iv_rank or 0) >= 45 and (premium_pct or 0) * 100 >= 1.5
    )

    return out


@router.get("/ticker/{ticker}/chains", response_model=None)
def ticker_chains(ticker: str, db: Session = Depends(get_db)) -> dict:
    """Live multi-expiry premium table for a ticker (fetched on demand, not during scan).

    Returns all expirations within 7-45 days with ATM + 4 OTM calls + puts.
    Typically takes 5-20s for weekly-options tickers (MSFT, TSLA etc).
    """
    from ..scanner.engine import (
        fetch_expirations, fetch_chain, _expirations_for_premium,
        _pick_call_strikes, _collect_otm_calls, _collect_otm_puts,
        _contract_mid, _is_valid,
    )
    from datetime import date as _date

    ticker = ticker.strip().upper()

    # Get actual stock price from latest completed scan result
    _run = _latest_run(db)
    _row = None
    if _run:
        _row = db.execute(
            select(models.ScanResult)
            .where(models.ScanResult.ticker == ticker, models.ScanResult.run_id == _run.id)
        ).scalar_one_or_none()
    if _row is None:
        _row = db.execute(
            select(models.ScanResult)
            .where(models.ScanResult.ticker == ticker)
            .order_by(models.ScanResult.created_at.desc())
            .limit(1)
        ).scalar_one_or_none()
    actual_price: float | None = _row.price if _row else None

    try:
        exps = fetch_expirations(ticker)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"fetch_expirations failed: {exc}")

    # All expirations 7-45d (wider window than scan to give full picture)
    prem_exps = _expirations_for_premium(exps, min_days=7, max_days=45)
    if not prem_exps:
        return {"ticker": ticker, "expirations": [], "note": "No expirations in 7-45d window"}

    expiry_rows = []
    for dte, exp in prem_exps:
        try:
            chain = fetch_chain(ticker, exp)
            # Need a price — get ATM from the chain itself (lowest spread ATM call)
            calls = sorted(
                [o for o in chain if o.get("option_type") == "call" and _is_valid(o.get("strike"))],
                key=lambda o: float(o["strike"]),
            )
            if not calls:
                continue
            # Use actual stock price; fall back to chain midpoint only if DB has no price
            if actual_price:
                approx_price = actual_price
            else:
                approx_price = float(calls[len(calls) // 2]["strike"])

            atm_c, _, _ = _pick_call_strikes(chain, approx_price)
            atm_strike = float(atm_c["strike"]) if atm_c else None
            atm_call_mid = _contract_mid(atm_c)

            # ATM put
            all_puts = sorted(
                [o for o in chain if o.get("option_type") == "put" and _is_valid(o.get("strike"))],
                key=lambda o: float(o["strike"]),
            )
            atm_put_mid = None
            if all_puts and atm_strike:
                pi = min(range(len(all_puts)), key=lambda i: abs(float(all_puts[i]["strike"]) - atm_strike))
                atm_put_mid = _contract_mid(all_puts[pi])

            otm_calls = _collect_otm_calls(chain, approx_price)
            otm_puts = _collect_otm_puts(chain, approx_price)

            expiry_rows.append({
                "expiry": exp,
                "dte": dte,
                "atm_strike": atm_strike,
                "atm_call_prem": round(atm_call_mid, 4) if atm_call_mid else None,
                "atm_put_prem": round(atm_put_mid, 4) if atm_put_mid else None,
                "calls": [{"strike": c["strike"], "prem": c["prem"]} for c in otm_calls],
                "puts": [{"strike": c["strike"], "prem": c["prem"]} for c in otm_puts],
            })
        except Exception as exc:
            logger.debug("chains: expiry %s failed for %s: %s", exp, ticker, exc)
            expiry_rows.append({"expiry": exp, "dte": dte, "error": str(exc)})

    return {"ticker": ticker, "expirations": expiry_rows}


@router.post("/universe/reload")
def universe_reload(db: Session = Depends(get_db)) -> dict:
    """Re-sync ticker_universe.csv → database.  Safe to call at any time.

    Inserts rows from the CSV that are missing in the DB.  Never deletes rows
    (custom entries added through the UI are preserved).
    """
    from ..universe import sync_universe_from_csv, universe_size_from_db
    inserted = sync_universe_from_csv(db)
    total = universe_size_from_db(db)
    return {
        "status": "ok",
        "inserted": inserted,
        "universe_size": total,
        "message": f"Synced CSV → DB: {inserted} new rows added, {total} distinct active tickers total.",
    }


@router.get("/universe/sources")
def universe_sources(db: Session = Depends(get_db)) -> dict:
    """Return each source tag and its ticker count from the active universe."""
    from ..universe import ticker_sources_from_db, universe_size_from_db
    mapping = ticker_sources_from_db(db)
    source_counts: dict[str, int] = {}
    for sources in mapping.values():
        for s in sources:
            source_counts[s] = source_counts.get(s, 0) + 1
    return {
        "universe_size": universe_size_from_db(db),
        "sources": source_counts,
    }


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
