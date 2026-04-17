"""Bucket assignment rules.

Three buckets:
  - sell_now: score>=45 AND iv_rank>=45 AND premium_pct>=1.5%
  - buy_sell_later: score>=25 AND earnings within 30d, OR score>=30 AND low IV
  - watchlist: everything else
"""
from __future__ import annotations

SELL_NOW = "sell_now"
BUY_SELL_LATER = "buy_sell_later"
WATCHLIST = "watchlist"

LOW_IV_THRESHOLD = 30.0  # iv_rank below this is "low IV"


def assign_bucket(
    score: float,
    iv_rank: float | None,
    premium_pct: float | None,
    earnings_days: int | None,
) -> str:
    premium_pct_val = (premium_pct or 0) * 100  # 0.015 -> 1.5
    iv_rank_val = iv_rank if iv_rank is not None else 0

    if score >= 45 and iv_rank_val >= 45 and premium_pct_val >= 1.5:
        return SELL_NOW

    has_catalyst_30d = earnings_days is not None and 0 < earnings_days <= 30
    low_iv = iv_rank_val < LOW_IV_THRESHOLD

    if score >= 25 and has_catalyst_30d:
        return BUY_SELL_LATER
    if score >= 30 and low_iv:
        return BUY_SELL_LATER

    return WATCHLIST
