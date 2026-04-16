"""Public API endpoints."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from .. import models
from ..database import get_db
from ..schemas import (
    MoverOut,
    MoversOut,
    ScanLatestOut,
    ScanResultOut,
    ScoreBreakdown,
)

router = APIRouter(prefix="/api", tags=["scanner"])


def _to_out(row: models.ScanResult) -> ScanResultOut:
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
        notes=row.notes,
        created_at=row.created_at,
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

    sell_now: list[ScanResultOut] = []
    buy_sell_later: list[ScanResultOut] = []
    watchlist: list[ScanResultOut] = []
    for row in rows:
        out = _to_out(row)
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
def movers(db: Session = Depends(get_db), limit: int = 10) -> MoversOut:
    latest = _latest_run(db)
    if latest is None:
        raise HTTPException(status_code=404, detail="No completed scan runs yet")

    prev_stmt = (
        select(models.ScanRun)
        .where(
            models.ScanRun.status == "completed",
            models.ScanRun.id != latest.id,
        )
        .order_by(models.ScanRun.finished_at.desc())
        .limit(1)
    )
    previous = db.execute(prev_stmt).scalar_one_or_none()

    latest_rows = (
        db.execute(
            select(models.ScanResult).where(models.ScanResult.run_id == latest.id)
        )
        .scalars()
        .all()
    )
    if previous is None:
        # no prior run — every current score is its own delta
        top = sorted(latest_rows, key=lambda r: r.score, reverse=True)[:limit]
        gainers = [
            MoverOut(ticker=r.ticker, score=r.score, prev_score=None, delta=r.score, bucket=r.bucket)
            for r in top
        ]
        return MoversOut(gainers=gainers, losers=[])

    prev_rows = (
        db.execute(
            select(models.ScanResult).where(models.ScanResult.run_id == previous.id)
        )
        .scalars()
        .all()
    )
    prev_by_ticker = {r.ticker: r.score for r in prev_rows}

    deltas: list[MoverOut] = []
    for r in latest_rows:
        prev = prev_by_ticker.get(r.ticker)
        delta = r.score - prev if prev is not None else r.score
        deltas.append(
            MoverOut(
                ticker=r.ticker,
                score=r.score,
                prev_score=prev,
                delta=round(delta, 2),
                bucket=r.bucket,
            )
        )
    deltas.sort(key=lambda m: m.delta, reverse=True)
    gainers = deltas[:limit]
    losers = sorted(deltas, key=lambda m: m.delta)[:limit]
    return MoversOut(gainers=gainers, losers=losers)


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
