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


class TimeframeDelta(BaseModel):
    """Score change vs a historical timeframe (1d, 2d, … 7d)."""
    days: int
    prev_score: float | None
    delta: float | None          # positive = improved, negative = declined
    prev_bucket: str | None
    arrow: str | None            # "up_green" | "down_yellow" | "down_red" | None


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
    history: list[TimeframeDelta] = []   # score deltas vs 1d/2d/3d/4d/5d/7d ago
    notes: str | None = None
    created_at: datetime

    # SMA indicators
    sma_200: float | None = None
    sma_50: float | None = None
    price_vs_sma200_pct: float | None = None  # % above/below (positive = above)
    price_vs_sma50_pct: float | None = None
    sma_regime: str | None = None             # UPTREND / DOWNTREND / TRANSITIONAL
    sma_golden_cross: bool | None = None      # True=golden cross, False=death cross

    # Auto-detected support / resistance
    support_1: float | None = None
    support_1_strength: float | None = None
    support_2: float | None = None
    support_2_strength: float | None = None
    resistance_1: float | None = None
    resistance_1_strength: float | None = None
    resistance_2: float | None = None
    resistance_2_strength: float | None = None


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
    prev_bucket: str | None = None


class MoversOut(BaseModel):
    gainers: list[MoverOut]
    losers: list[MoverOut]


# ── Wheel math schemas ────────────────────────────────────────────────────────

class StrikeSuggestion(BaseModel):
    strike: float
    premium: float | None = None   # mid of bid/ask
    bid: float | None = None
    ask: float | None = None


class WheelOut(BaseModel):
    """Response from GET /api/ticker/{ticker}/wheel — live chain + wheel math."""
    ticker: str
    price: float | None
    expiration: str | None          # which expiry was used for premium lookup

    # S/R from last scan (may be None if scan hasn't run yet)
    support_1: float | None = None
    support_1_strength: float | None = None
    support_2: float | None = None
    support_2_strength: float | None = None
    resistance_1: float | None = None
    resistance_1_strength: float | None = None
    resistance_2: float | None = None
    resistance_2_strength: float | None = None

    # SMA snapshot from last scan
    sma_200: float | None = None
    sma_50: float | None = None
    price_vs_sma200_pct: float | None = None
    price_vs_sma50_pct: float | None = None
    sma_regime: float | None = None
    sma_golden_cross: bool | None = None

    # Suggested option legs (nearest standard strikes in live chain)
    csp: StrikeSuggestion | None = None         # put at/below support_1
    cc: StrikeSuggestion | None = None          # call at/above resistance_1

    # Wheel math (all per-contract = 100 shares)
    csp_effective_basis: float | None = None    # csp.strike - csp.premium
    cc_profit_if_called: float | None = None    # (cc.strike - price + cc.premium) * 100
    combined_premium_per_share: float | None = None
    capital_required: float | None = None       # 100*price + csp.strike*100
    monthly_yield_pct: float | None = None      # combined*100 / capital * 100
    annualized_yield_pct: float | None = None   # monthly * 12
