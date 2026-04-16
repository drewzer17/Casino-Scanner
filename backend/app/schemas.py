"""Pydantic response schemas."""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class ScoreBreakdown(BaseModel):
    iv_rank: float
    premium: float
    iv_hv: float
    catalyst: float
    chain: float


class ScanResultOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    ticker: str
    price: float | None
    iv_rank: float | None
    iv: float | None
    hv: float | None
    atm_call_premium: float | None
    premium_pct: float | None
    open_interest: int | None
    bid_ask_spread_pct: float | None
    earnings_days: int | None
    unusual_volume: bool
    score: float
    bucket: str
    breakdown: ScoreBreakdown
    notes: str | None = None
    created_at: datetime


class ScanLatestOut(BaseModel):
    run_id: int
    started_at: datetime
    finished_at: datetime | None
    tickers_scanned: int
    sell_now: list[ScanResultOut]
    buy_sell_later: list[ScanResultOut]
    watchlist: list[ScanResultOut]


class MoverOut(BaseModel):
    ticker: str
    score: float
    prev_score: float | None
    delta: float
    bucket: str


class MoversOut(BaseModel):
    gainers: list[MoverOut]
    losers: list[MoverOut]
