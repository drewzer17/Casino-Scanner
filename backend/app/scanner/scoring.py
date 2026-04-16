"""0-100 scoring algorithm for wheel-strategy readiness.

Five factors, 20 points each:
  1. IV RANK
  2. PREMIUM QUALITY (ATM call premium as % of stock price)
  3. IV vs HV ratio
  4. CATALYST (earnings proximity + modifiers)
  5. CHAIN QUALITY (open interest + bid/ask spread)
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class TickerMetrics:
    iv_rank: float | None = None          # 0-100
    premium_pct: float | None = None      # ATM call premium / stock price, e.g. 0.03 = 3%
    iv: float | None = None               # implied vol (annualized decimal, e.g. 0.45)
    hv: float | None = None               # historical vol (annualized decimal)
    earnings_days: int | None = None      # days until next earnings (None = unknown/no event)
    sector_macro_catalyst: bool = False
    iv_ramp: bool = False
    unusual_volume: bool = False
    open_interest: int | None = None      # ATM option OI
    bid_ask_spread_pct: float | None = None  # spread / mid, e.g. 0.04 = 4%


@dataclass
class ScoreBreakdown:
    iv_rank: float = 0.0
    premium: float = 0.0
    iv_hv: float = 0.0
    catalyst: float = 0.0
    chain: float = 0.0

    @property
    def total(self) -> float:
        return self.iv_rank + self.premium + self.iv_hv + self.catalyst + self.chain


def score_iv_rank(iv_rank: float | None) -> float:
    if iv_rank is None:
        return 0.0
    if iv_rank >= 80:
        return 20
    if iv_rank >= 60:
        return 16
    if iv_rank >= 50:
        return 13
    if iv_rank >= 40:
        return 10
    if iv_rank >= 30:
        return 6
    if iv_rank >= 20:
        return 3
    return 0.0


def score_premium(premium_pct: float | None) -> float:
    if premium_pct is None:
        return 0.0
    pct = premium_pct * 100  # convert 0.03 -> 3.0
    if pct >= 5:
        return 20
    if pct >= 3.5:
        return 17
    if pct >= 2.5:
        return 14
    if pct >= 2:
        return 10
    if pct >= 1.5:
        return 6
    if pct >= 1:
        return 3
    return 0.0


def score_iv_vs_hv(iv: float | None, hv: float | None) -> float:
    if iv is None or hv is None or hv <= 0:
        return 0.0
    ratio = iv / hv
    if ratio > 1.5:
        return 20
    if ratio > 1.3:
        return 16
    if ratio > 1.15:
        return 12
    if ratio > 1.0:
        return 8
    if ratio > 0.9:
        return 4
    return 0.0


def score_catalyst(
    earnings_days: int | None,
    sector_macro: bool = False,
    iv_ramp: bool = False,
    unusual_volume: bool = False,
) -> float:
    """Catalyst score, capped at 20.

    Earnings sweet spot (7-45d) is the base; modifiers stack on top. Earnings inside
    1-6d is a reduced base because short-dated chain risk outweighs premium. "Today"
    is nearly worthless for selling new premium, hence 2.
    """
    if earnings_days is None:
        base = 0.0
    elif 7 <= earnings_days <= 14:
        base = 18.0
    elif 15 <= earnings_days <= 30:
        base = 14.0
    elif 31 <= earnings_days <= 45:
        base = 10.0
    elif 1 <= earnings_days <= 6:
        base = 8.0
    elif earnings_days == 0:
        base = 2.0
    else:
        base = 0.0

    # Modifiers only apply when there's a real sweet-spot earnings window
    if 7 <= (earnings_days or -1) <= 45:
        if sector_macro:
            base += 5
        if iv_ramp:
            base += 5
        if unusual_volume:
            base += 4

    return min(base, 20.0)


def score_chain(open_interest: int | None, spread_pct: float | None) -> float:
    """Chain quality tiers: both conditions in a tier must be true."""
    if open_interest is None or spread_pct is None:
        return 0.0
    oi = open_interest
    spread = spread_pct * 100  # 0.03 -> 3

    if oi > 5000 and spread < 3:
        return 20
    if oi > 2000 and spread < 5:
        return 16
    if oi > 1000 and spread < 8:
        return 12
    if oi > 500 and spread < 10:
        return 8
    if oi > 200 and spread < 15:
        return 4
    return 0.0


def score_ticker(metrics: TickerMetrics) -> ScoreBreakdown:
    return ScoreBreakdown(
        iv_rank=score_iv_rank(metrics.iv_rank),
        premium=score_premium(metrics.premium_pct),
        iv_hv=score_iv_vs_hv(metrics.iv, metrics.hv),
        catalyst=score_catalyst(
            metrics.earnings_days,
            metrics.sector_macro_catalyst,
            metrics.iv_ramp,
            metrics.unusual_volume,
        ),
        chain=score_chain(metrics.open_interest, metrics.bid_ask_spread_pct),
    )
