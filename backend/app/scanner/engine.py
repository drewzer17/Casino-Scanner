"""Scan engine — data sourced from Tradier API via plain httpx calls.

No pandas, no numpy, no C extensions. All math is pure Python.

Tradier endpoints used per ticker:
  1. GET /v1/markets/quotes          — batch up to 20, pre-fetched per batch
  2. GET /v1/markets/history         — 1 year of daily OHLCV for HV + SMA + S/R
  3. GET /v1/markets/options/expirations  — available expiry dates
  4. GET /v1/markets/options/chains  — full chain for the chosen expiry

IV rank is stored as 50 (neutral) until we accumulate 30+ days of daily
IV snapshots in the database. A future migration will back-fill this.

Design goals (all carried over):
  - Crash-resilient: batch commits every BATCH_SIZE tickers
  - Resumable: skips tickers already written for the current ScanRun
  - Per-ticker timeout: ThreadPoolExecutor with TICKER_TIMEOUT seconds
  - One-bad-ticker isolation: exceptions caught per ticker
"""
from __future__ import annotations

import logging
import math
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeout
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Iterable

import json

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from .. import models
from ..config import settings
from ..universe import load_universe
from .buckets import assign_bucket
from .scoring import TickerMetrics, score_ticker

logger = logging.getLogger(__name__)

TRADING_DAYS = 252
HV_WINDOW = 30
BATCH_SIZE = 25
QUOTE_BATCH = 20      # Tradier max symbols per quotes call
TICKER_TIMEOUT = 60   # seconds — multi-expiry fetches need more time

_sma_sr_debug_count = 0  # log SMA/SR values for first 3 tickers to verify calculation

TRADIER_BASE = "https://sandbox.tradier.com"


# ── Tradier HTTP client ───────────────────────────────────────────────────────

def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {settings.tradier_api_key}",
        "Accept": "application/json",
    }


def _get(path: str, params: dict) -> dict:
    """Single synchronous GET to Tradier. Raises on non-2xx."""
    with httpx.Client(timeout=8.0) as client:
        resp = client.get(TRADIER_BASE + path, params=params, headers=_headers())
        resp.raise_for_status()
        return resp.json()


# ── Tradier data fetchers ─────────────────────────────────────────────────────

def fetch_quotes(symbols: list[str]) -> dict[str, dict]:
    """Return {ticker: quote_dict} for up to QUOTE_BATCH symbols.

    Tradier returns a single dict (not a list) when only one symbol is
    requested — normalise to a list in all cases.
    """
    data = _get("/v1/markets/quotes", {"symbols": ",".join(symbols)})
    quote = (data.get("quotes") or {}).get("quote")
    if not quote:
        return {}
    if isinstance(quote, dict):
        quote = [quote]
    return {q["symbol"]: q for q in quote if q.get("symbol")}


@dataclass
class Bar:
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: float


def fetch_bars(symbol: str, days: int = 365) -> list[Bar]:
    """Return list of daily OHLCV bars, oldest-first."""
    end = date.today()
    start = end - timedelta(days=days)
    data = _get("/v1/markets/history", {
        "symbol": symbol,
        "interval": "daily",
        "start": start.strftime("%Y-%m-%d"),
        "end": end.strftime("%Y-%m-%d"),
    })
    hist = (data.get("history") or {})
    days_data = hist.get("day") or []
    if isinstance(days_data, dict):
        days_data = [days_data]
    result: list[Bar] = []
    for d in days_data:
        try:
            result.append(Bar(
                date=str(d.get("date", "")),
                open=float(d.get("open") or 0),
                high=float(d.get("high") or 0),
                low=float(d.get("low") or 0),
                close=float(d.get("close") or 0),
                volume=float(d.get("volume") or 0),
            ))
        except (TypeError, ValueError):
            continue
    return [b for b in result if b.close > 0]


def fetch_expirations(symbol: str) -> list[str]:
    """Return sorted list of available option expiration date strings."""
    data = _get("/v1/markets/options/expirations", {"symbol": symbol})
    exps = (data.get("expirations") or {})
    dates = exps.get("date") or []
    if isinstance(dates, str):
        dates = [dates]
    return sorted(dates)


def fetch_earnings_calendar(year: int, month: int) -> dict[str, date]:
    """Return {TICKER: earnings_date} for every earnings event in a calendar month.

    Tradier /v1/markets/calendar returns a day-by-day structure; each day may
    have an 'earnings' list with a 'symbol' field.  A single dict is returned
    instead of a list when there is only one event — normalise in all cases.
    """
    data = _get("/v1/markets/calendar", {"month": month, "year": year})
    days = ((data.get("calendar") or {}).get("days") or {}).get("day") or []
    if isinstance(days, dict):
        days = [days]

    # Log a sample to diagnose whether Tradier returns earnings data
    days_with_earnings = [d for d in days if d.get("earnings")]
    logger.info(
        "Calendar %d-%02d: %d days total, %d have earnings entries, sample: %s",
        year, month, len(days), len(days_with_earnings),
        days_with_earnings[0] if days_with_earnings else "(none)",
    )

    result: dict[str, date] = {}
    for day in days:
        date_str = day.get("date")
        if not date_str:
            continue
        try:
            day_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        earnings = day.get("earnings") or []
        if isinstance(earnings, dict):
            earnings = [earnings]
        for entry in earnings:
            sym = (entry.get("symbol") or "").upper()
            if sym and sym not in result:
                result[sym] = day_date  # keep the earliest date if symbol appears twice
    return result


def build_earnings_lookup() -> dict[str, int]:
    """Build {TICKER: days_until_earnings} by fetching the current and next two months.

    Called once per scan run — results are passed into every scan_ticker call so
    we make 3 calendar API calls total instead of one per ticker.

    Only earnings within the next 90 days are kept (beyond that they don't affect
    catalyst scoring).  Past earnings (negative days) are excluded.
    """
    today = date.today()
    year, month = today.year, today.month

    lookup: dict[str, int] = {}
    for offset in range(3):  # current month + next 2
        m = (month - 1 + offset) % 12 + 1
        y = year + (month - 1 + offset) // 12
        try:
            for sym, earn_date in fetch_earnings_calendar(y, m).items():
                days = (earn_date - today).days
                if 0 <= days <= 90:
                    # If a symbol appears in multiple months keep the nearest date
                    if sym not in lookup or days < lookup[sym]:
                        lookup[sym] = days
        except Exception as exc:
            logger.warning("earnings calendar %d-%02d failed: %s", y, m, exc)

    logger.info("earnings lookup: %d tickers with upcoming earnings", len(lookup))
    return lookup


def fetch_chain(symbol: str, expiration: str) -> list[dict]:
    """Return all option contracts for the given expiry."""
    data = _get("/v1/markets/options/chains", {
        "symbol": symbol,
        "expiration": expiration,
        "greeks": "true",
    })
    opts = (data.get("options") or {})
    options = opts.get("option") or []
    if isinstance(options, dict):
        options = [options]
    return options


# ── Pure-Python math helpers ──────────────────────────────────────────────────

def _is_valid(v: object) -> bool:
    try:
        return v is not None and not math.isnan(float(v))
    except (TypeError, ValueError):
        return False


def _stdev(sample: list[float]) -> float:
    n = len(sample)
    if n < 2:
        return 0.0
    mean = sum(sample) / n
    variance = sum((x - mean) ** 2 for x in sample) / (n - 1)
    return math.sqrt(variance) if variance > 0 else 0.0


def _log_returns(prices: list[float]) -> list[float]:
    result: list[float] = []
    for i in range(1, len(prices)):
        if prices[i - 1] > 0 and prices[i] > 0:
            result.append(math.log(prices[i] / prices[i - 1]))
    return result


def _annualized_vol(returns: list[float], window: int = HV_WINDOW) -> float | None:
    if len(returns) < window:
        return None
    std = _stdev(returns[-window:])
    return (std * math.sqrt(TRADING_DAYS)) if std > 0 else None


def _rolling_vol(returns: list[float], window: int = HV_WINDOW) -> list[float]:
    result: list[float] = []
    for i in range(window - 1, len(returns)):
        result.append(_stdev(returns[i - window + 1 : i + 1]) * math.sqrt(TRADING_DAYS))
    return result


def _iv_rank_from_history(closes: list[float], current_iv: float | None) -> float | None:
    if len(closes) < 60 or current_iv is None:
        return None
    rolling = _rolling_vol(_log_returns(closes), HV_WINDOW)[-TRADING_DAYS:]
    if not rolling:
        return None
    lo, hi = min(rolling), max(rolling)
    if hi <= lo:
        return None
    return max(0.0, min(100.0, (current_iv - lo) / (hi - lo) * 100))


# ── SMA helpers ───────────────────────────────────────────────────────────────

def _calc_sma(closes: list[float], period: int) -> float | None:
    if len(closes) < period:
        return None
    return sum(closes[-period:]) / period


def _sma_regime(price: float, sma_50: float | None, sma_200: float | None) -> str | None:
    if sma_50 is None or sma_200 is None:
        return None
    if price > sma_50 and price > sma_200:
        return "UPTREND"
    if price < sma_50 and price < sma_200:
        return "DOWNTREND"
    return "TRANSITIONAL"


def _sma_score_modifier(
    price: float,
    sma_50: float | None,
    sma_200: float | None,
    regime: str | None,
    golden_cross: bool | None,
) -> float:
    """Small ±5 adjustment to the 0-100 score based on SMA position."""
    adj = 0.0
    if sma_200 is not None and price > 0:
        pct_vs_200 = (price - sma_200) / sma_200
        if 0 < pct_vs_200 <= 0.03:   # sitting just above 200 SMA: institutional support
            adj += 3.0
        if pct_vs_200 > 0.20:         # overextended above 200 SMA
            adj -= 2.0
    if sma_50 is not None and price > 0 and regime == "UPTREND":
        pct_vs_50 = (price - sma_50) / sma_50
        if 0 < pct_vs_50 <= 0.03:    # sitting just above 50 SMA in uptrend
            adj += 2.0
    if golden_cross is False:          # death cross
        adj -= 2.0
    return max(-5.0, min(5.0, adj))


# ── Support / Resistance detection ───────────────────────────────────────────

def _find_support_resistance(
    bars: list[Bar],
    price: float,
    window: int = 5,
    cluster_pct: float = 0.02,
    gap_threshold: float = 0.05,
) -> tuple[list[dict], list[dict]]:
    """Detect swing-based S/R zones from OHLCV history.

    Returns (supports, resistances), each a list of dicts:
      {'price': float, 'strength': float, 'touches': int}
    Sorted S1=closest below price, S2=further; R1=closest above, R2=further.
    """
    n = len(bars)
    if n < window * 2 + 5 or price <= 0:
        return [], []

    avg_vol = sum(b.volume for b in bars) / n if n > 0 else 1.0

    # Gap ranges: don't place S/R inside earnings-day gaps (>5% open vs prev close)
    gaps: list[tuple[float, float]] = []
    for i in range(1, n):
        prev_close = bars[i - 1].close
        curr_open = bars[i].open
        if prev_close > 0:
            gap_pct = abs(curr_open - prev_close) / prev_close
            if gap_pct > gap_threshold:
                lo = min(prev_close, curr_open)
                hi = max(prev_close, curr_open)
                gaps.append((lo, hi))

    def in_gap(p: float) -> bool:
        return any(lo <= p <= hi for lo, hi in gaps)

    def recency_weight(idx: int) -> float:
        bars_ago = n - 1 - idx
        if bars_ago <= 63:    # ~3 months
            return 3.0
        if bars_ago <= 126:   # ~6 months
            return 2.0
        return 1.0

    def vol_mult(vol: float) -> float:
        return 2.0 if avg_vol > 0 and vol > avg_vol else 1.0

    support_pts: list[dict] = []
    resistance_pts: list[dict] = []

    for i in range(window, n - window):
        low = bars[i].low
        if low > 0 and not in_gap(low):
            if (all(low <= bars[i - j].low for j in range(1, window + 1)) and
                    all(low <= bars[i + j].low for j in range(1, window + 1))):
                support_pts.append({
                    "price": low,
                    "weight": recency_weight(i) * vol_mult(bars[i].volume),
                })

        high = bars[i].high
        if high > 0 and not in_gap(high):
            if (all(high >= bars[i - j].high for j in range(1, window + 1)) and
                    all(high >= bars[i + j].high for j in range(1, window + 1))):
                resistance_pts.append({
                    "price": high,
                    "weight": recency_weight(i) * vol_mult(bars[i].volume),
                })

    def cluster_and_score(pts: list[dict]) -> list[dict]:
        if not pts:
            return []
        sorted_pts = sorted(pts, key=lambda p: p["price"])
        clusters: list[list[dict]] = []
        current: list[dict] = [sorted_pts[0]]
        for pt in sorted_pts[1:]:
            ref = current[0]["price"]
            if ref > 0 and abs(pt["price"] - ref) / ref <= cluster_pct:
                current.append(pt)
            else:
                clusters.append(current)
                current = [pt]
        clusters.append(current)

        result = []
        for cluster in clusters:
            avg_price = sum(p["price"] for p in cluster) / len(cluster)
            strength = round(sum(p["weight"] for p in cluster), 1)
            result.append({"price": avg_price, "strength": strength, "touches": len(cluster)})
        return result

    all_supports = cluster_and_score(support_pts)
    all_resistances = cluster_and_score(resistance_pts)

    # A broken support (price already below it) is excluded; same for resistances
    valid_supports = [s for s in all_supports if s["price"] < price * 0.98]
    valid_resistances = [r for r in all_resistances if r["price"] > price * 1.02]

    # Proximity-first: take closest 5, then pick 2 strongest among those
    valid_supports.sort(key=lambda s: -s["price"])    # highest (closest) first
    valid_resistances.sort(key=lambda r: r["price"])  # lowest (closest) first

    top2_s = sorted(valid_supports[:5], key=lambda s: -s["strength"])[:2]
    top2_r = sorted(valid_resistances[:5], key=lambda r: -r["strength"])[:2]

    # Final sort: S1=closest below, S2=further below; R1=closest above, R2=further above
    top2_s.sort(key=lambda s: -s["price"])
    top2_r.sort(key=lambda r: r["price"])

    return top2_s, top2_r


# ── Options helpers ───────────────────────────────────────────────────────────

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
    for exp in exp_dates:
        try:
            d = datetime.strptime(exp, "%Y-%m-%d").date()
        except ValueError:
            continue
        if (d - today).days >= 7:
            return exp
    return None


def _expirations_for_premium(
    exp_dates: Iterable[str], min_days: int = 1, max_days: int = 30
) -> list[tuple[int, str]]:
    """Return [(dte, expiry_str)] for all expirations within [min_days, max_days], sorted by DTE."""
    today = date.today()
    result: list[tuple[int, str]] = []
    for exp in exp_dates:
        try:
            d = datetime.strptime(exp, "%Y-%m-%d").date()
        except ValueError:
            continue
        days = (d - today).days
        if min_days <= days <= max_days:
            result.append((days, exp))
    return sorted(result)


def _pick_call_strikes(
    options: list[dict], price: float
) -> tuple[dict | None, dict | None, dict | None]:
    """Return (atm_call, 1_otm_call, 2_otm_call) sorted by strike ascending.

    ATM = call with strike closest to current price.
    1 OTM / 2 OTM = the next 1 and 2 strikes above ATM in the chain.
    """
    calls = sorted(
        [o for o in options
         if o.get("option_type") == "call" and _is_valid(o.get("strike")) and _is_sane_contract(o, price)],
        key=lambda o: float(o["strike"]),
    )
    if not calls:
        return None, None, None
    atm_idx = min(range(len(calls)), key=lambda i: abs(float(calls[i]["strike"]) - price))
    atm  = calls[atm_idx]
    otm1 = calls[atm_idx + 1] if atm_idx + 1 < len(calls) else None
    otm2 = calls[atm_idx + 2] if atm_idx + 2 < len(calls) else None
    return atm, otm1, otm2


def _collect_otm_calls(options: list[dict], price: float, n: int = 4) -> list[dict]:
    """Return up to n OTM call dicts (1 OTM → n OTM above ATM).
    Each: {"strike": float|None, "prem": float|None}
    Skips adjusted/phantom contracts: strike must be strictly above ATM strike,
    and premium must not exceed ATM premium (OTM can't be worth more than ATM).
    """
    calls = sorted(
        [o for o in options
         if o.get("option_type") == "call" and _is_valid(o.get("strike")) and _is_sane_contract(o, price)],
        key=lambda o: float(o["strike"]),
    )
    if not calls:
        return []
    atm_idx = min(range(len(calls)), key=lambda i: abs(float(calls[i]["strike"]) - price))
    atm_strike = float(calls[atm_idx]["strike"])
    atm_mid = _contract_mid(calls[atm_idx])
    result = []
    scan_idx = atm_idx + 1
    while len(result) < n:
        if scan_idx >= len(calls):
            result.append({"strike": None, "prem": None})
        else:
            c = calls[scan_idx]
            c_strike = float(c["strike"])
            mid = _contract_mid(c)
            if c_strike <= atm_strike or (atm_mid is not None and mid is not None and mid > atm_mid):
                # adjusted/phantom contract — skip to next index
                scan_idx += 1
                continue
            result.append({
                "strike": round(c_strike, 2),
                "prem": round(mid, 4) if mid else None,
            })
        scan_idx += 1
    return result


def _collect_otm_puts(options: list[dict], price: float, n: int = 4) -> list[dict]:
    """Return up to n OTM put dicts (1 OTM → n OTM below ATM).
    Each: {"strike": float|None, "prem": float|None}
    Skips adjusted/phantom contracts: strike must be strictly below ATM strike,
    and premium must not exceed ATM premium.
    """
    puts = sorted(
        [o for o in options
         if o.get("option_type") == "put" and _is_valid(o.get("strike")) and _is_sane_contract(o, price)],
        key=lambda o: float(o["strike"]),
    )
    if not puts:
        return []
    atm_idx = min(range(len(puts)), key=lambda i: abs(float(puts[i]["strike"]) - price))
    atm_strike = float(puts[atm_idx]["strike"])
    atm_mid = _contract_mid(puts[atm_idx])
    result = []
    scan_idx = atm_idx - 1
    while len(result) < n:
        if scan_idx < 0:
            result.append({"strike": None, "prem": None})
        else:
            c = puts[scan_idx]
            c_strike = float(c["strike"])
            mid = _contract_mid(c)
            if c_strike >= atm_strike or (atm_mid is not None and mid is not None and mid > atm_mid):
                # adjusted/phantom contract — skip to next index
                scan_idx -= 1
                continue
            result.append({
                "strike": round(c_strike, 2),
                "prem": round(mid, 4) if mid else None,
            })
        scan_idx -= 1
    return result


def _contract_mid(contract: dict | None) -> float | None:
    """Mid price per share from a Tradier option contract dict."""
    if contract is None:
        return None
    bid = float(contract.get("bid") or 0)
    ask = float(contract.get("ask") or 0)
    if bid > 0 and ask > 0:
        return (bid + ask) / 2
    return None


def _is_standard_strike(strike: float) -> bool:
    """Return True if strike is a multiple of $0.50 — all standard option strikes are.

    Adjusted/restructured contracts frequently carry non-standard strikes like
    $60.22 or $47.13 that fail this test.
    """
    return abs(strike * 2 - round(strike * 2)) < 0.01


def _is_sane_contract(contract: dict, price: float) -> bool:
    """Return False if this contract is physically impossible — adjusted/phantom.

    Hard rules (option theory limits):
      - Strike must be a standard $0.50-multiple.
      - Call premium can never exceed the stock price.
      - Call premium can never exceed the strike price (true for ATM/OTM calls).
      - Put premium can never exceed the strike price.
    """
    strike_raw = contract.get("strike")
    if not _is_valid(strike_raw):
        return False
    strike = float(strike_raw)
    if not _is_standard_strike(strike):
        return False
    mid = _contract_mid(contract)
    if mid is None:
        return True  # no bid/ask data — cannot reject on premium alone
    otype = contract.get("option_type")
    if otype == "call":
        if mid > price:
            return False   # call premium > stock price: impossible
        if mid > strike:
            return False   # call premium > strike: impossible for ATM/OTM calls
    elif otype == "put":
        if mid > strike:
            return False   # put premium > strike: impossible
    return True


# ── Safety score ─────────────────────────────────────────────────────────────

def _calc_safety_score(
    premium_otm2: float | None,
    iv: float | None,
    hv: float | None,
    price: float,
    support_1: float | None,
    bars: list[Bar],
    earn_days: int | None,
) -> float | None:
    """Risk-adjusted premium score.

    Premium Safety = (2OTM dollar premium) × (IV/HV ratio) × (support distance %)
                   ÷ (avg daily range % × earnings penalty)

    Higher = more premium for less risk. Comparable across tickers.
    """
    if not premium_otm2 or premium_otm2 <= 0 or price <= 0:
        return None

    dollar_prem = premium_otm2 * 100                    # per-contract $
    iv_hv = (iv / hv) if (iv and hv and hv > 0) else 1.0
    support_dist = (
        (price - support_1) / price * 100
        if support_1 and support_1 < price
        else 5.0
    )

    recent = bars[-30:] if len(bars) >= 30 else bars
    if recent:
        ranges = [(b.high - b.low) / b.close * 100 for b in recent if b.close > 0]
        avg_range = sum(ranges) / len(ranges) if ranges else 5.0
    else:
        avg_range = 5.0

    if earn_days is None or earn_days > 30:
        penalty = 1.0
    elif earn_days >= 14:
        penalty = 1.5
    elif earn_days >= 7:
        penalty = 3.0
    else:
        penalty = 5.0

    if avg_range <= 0:
        return None

    return round((dollar_prem * iv_hv * support_dist) / (avg_range * penalty), 2)


# ── CC / CSP attractiveness scores ───────────────────────────────────────────

def _calc_cc_score(
    price: float | None,
    sma_200: float | None,
    sma_50: float | None,
    sma_golden_cross: bool | None,
    resistance_1: float | None,
    price_vs_sma200_pct: float | None,
    price_vs_sma50_pct: float | None,
    iv_rank: float | None,
    atm_call_premium: float | None,
    iv: float | None,
    hv: float | None,
    premium_pct: float | None,
    open_interest: int | None,
    bid_ask_spread_pct: float | None,
    support_1: float | None,
    sma_regime: str | None,
) -> int:
    s = 0
    iv_hv = (iv / hv) if (iv and hv and hv > 0) else None

    # TREND STRENGTH — 30 pts max
    if price and sma_200 and price > sma_200: s += 8
    if price and sma_50 and price > sma_50:   s += 7
    if sma_golden_cross:                       s += 5
    if resistance_1 is None:                   s += 5
    if price_vs_sma200_pct is not None and price_vs_sma200_pct > 20: s += 3
    if price_vs_sma50_pct  is not None and price_vs_sma50_pct  > 5:  s += 2

    # PREMIUM QUALITY — 30 pts max
    if iv_rank is not None:
        if   iv_rank >= 80: s += 10
        elif iv_rank >= 60: s += 7
        elif iv_rank >= 40: s += 4
        elif iv_rank >= 20: s += 2
    if atm_call_premium is not None:
        if   atm_call_premium >= 10: s += 10
        elif atm_call_premium >= 5:  s += 7
        elif atm_call_premium >= 2:  s += 4
        elif atm_call_premium >= 1:  s += 2
    if iv_hv is not None:
        if   iv_hv >= 1.5: s += 5
        elif iv_hv >= 1.2: s += 3
        elif iv_hv >= 1.0: s += 1
    if premium_pct is not None:
        if   premium_pct >= 0.05:  s += 5
        elif premium_pct >= 0.03:  s += 3
        elif premium_pct >= 0.015: s += 2

    # EXECUTION QUALITY — 20 pts max
    if open_interest is not None:
        if   open_interest >= 5000: s += 10
        elif open_interest >= 1000: s += 7
        elif open_interest >= 500:  s += 5
        elif open_interest >= 100:  s += 3
    if bid_ask_spread_pct is not None:
        if   bid_ask_spread_pct <= 0.03: s += 10
        elif bid_ask_spread_pct <= 0.05: s += 7
        elif bid_ask_spread_pct <= 0.10: s += 4
        elif bid_ask_spread_pct <= 0.15: s += 2

    # SAFETY — 20 pts max
    if support_1 and price and price > 0:
        dist = (price - support_1) / price * 100
        if   dist > 15: s += 8
        elif dist > 10: s += 6
        elif dist > 5:  s += 4
        elif dist > 2:  s += 2
    if   sma_regime == "UPTREND":      s += 7
    elif sma_regime == "TRANSITIONAL": s += 3
    if iv_hv is not None:
        if   iv_hv < 1.5: s += 3  # low: decent but not ideal for CC premium
        elif iv_hv < 2.0: s += 5  # sweet spot: IV well above realized, not extreme

    return min(100, max(0, s))


def _calc_csp_score(
    price: float | None,
    sma_200: float | None,
    sma_50: float | None,
    sma_golden_cross: bool | None,
    sma_regime: str | None,
    support_1: float | None,
    support_1_strength: float | None,
    support_2: float | None,
    iv_rank: float | None,
    atm_put_premium: float | None,
    iv: float | None,
    hv: float | None,
    premium_pct: float | None,
    open_interest: int | None,
    bid_ask_spread_pct: float | None,
) -> int:
    s = 0
    iv_hv = (iv / hv) if (iv and hv and hv > 0) else None

    # SUPPORT QUALITY — 30 pts max
    if support_1 and price and price > 0:
        dist = (price - support_1) / price * 100
        if   dist <= 3:  s += 10
        elif dist <= 5:  s += 8
        elif dist <= 10: s += 5
        elif dist <= 15: s += 3
    if support_1_strength is not None:
        if   support_1_strength >= 15: s += 10
        elif support_1_strength >= 10: s += 7
        elif support_1_strength >= 5:  s += 4
    if support_2 and price and price > 0:
        if abs(price - support_2) / price * 100 <= 20: s += 5
    if support_1 and support_2 and support_1 > 0:
        if (support_1 - support_2) / support_1 * 100 > 10: s += 5

    # TREND CONTEXT — 25 pts max
    if sma_golden_cross:                                   s += 8
    if price and sma_200 and price > sma_200:              s += 7
    if   sma_regime == "UPTREND":      s += 5
    elif sma_regime == "TRANSITIONAL": s += 3
    if price and sma_50 and sma_200 and price < sma_50 and price > sma_200: s += 5

    # PREMIUM QUALITY — 25 pts max
    if iv_rank is not None:
        if   iv_rank >= 80: s += 8
        elif iv_rank >= 60: s += 6
        elif iv_rank >= 40: s += 3
        elif iv_rank >= 20: s += 1
    if atm_put_premium is not None:
        if   atm_put_premium >= 8: s += 8
        elif atm_put_premium >= 4: s += 6
        elif atm_put_premium >= 2: s += 4
        elif atm_put_premium >= 1: s += 2
    if iv_hv is not None:
        if   iv_hv >= 1.5: s += 5
        elif iv_hv >= 1.2: s += 3
        elif iv_hv >= 1.0: s += 1
    put_pct = (atm_put_premium / price) if (atm_put_premium and price) else premium_pct
    if put_pct is not None:
        if   put_pct >= 0.04: s += 4
        elif put_pct >= 0.02: s += 3
        elif put_pct >= 0.01: s += 1

    # EXECUTION QUALITY — 20 pts max
    if open_interest is not None:
        if   open_interest >= 5000: s += 10
        elif open_interest >= 1000: s += 7
        elif open_interest >= 500:  s += 5
        elif open_interest >= 100:  s += 3
    if bid_ask_spread_pct is not None:
        if   bid_ask_spread_pct <= 0.03: s += 10
        elif bid_ask_spread_pct <= 0.05: s += 7
        elif bid_ask_spread_pct <= 0.10: s += 4
        elif bid_ask_spread_pct <= 0.15: s += 2

    return min(100, max(0, s))


# ── Per-ticker scan ───────────────────────────────────────────────────────────

@dataclass
class ScanRowResult:
    ticker: str
    metrics: TickerMetrics
    price: float | None
    atm_call_premium: float | None
    premium_otm1: float | None          # 1 OTM call mid, per share
    premium_otm2: float | None          # 2 OTM call mid, per share
    score: float
    bucket: str
    breakdown_iv_rank: float
    breakdown_premium: float
    breakdown_iv_hv: float
    breakdown_catalyst: float
    breakdown_chain: float
    notes: str | None = None
    safety_score: float | None = None
    iv: float | None = None
    # Multi-expiry premium data
    best_expiry: str | None = None
    best_dte: int | None = None
    best_strike: float | None = None
    expiry_data: str | None = None  # JSON string
    # ATM put premium (same expiry as best_expiry)
    atm_put_premium: float | None = None
    best_put_strike: float | None = None
    best_put_expiry: str | None = None
    best_put_dte: int | None = None
    # Company name from Tradier quote
    company_name: str | None = None
    # SMA
    sma_200: float | None = None
    sma_50: float | None = None
    price_vs_sma200_pct: float | None = None
    price_vs_sma50_pct: float | None = None
    sma_regime: str | None = None
    sma_golden_cross: bool | None = None
    # Support / Resistance
    support_1: float | None = None
    support_1_strength: float | None = None
    support_2: float | None = None
    support_2_strength: float | None = None
    resistance_1: float | None = None
    resistance_1_strength: float | None = None
    resistance_2: float | None = None
    resistance_2_strength: float | None = None
    # CC / CSP attractiveness scores
    cc_score: int | None = None
    csp_score: int | None = None


def scan_ticker(ticker: str, price: float | None = None, earn_days: int | None = None) -> ScanRowResult | None:
    """Scan one ticker via Tradier. price and earn_days may be pre-fetched by the caller."""
    try:
        # 1. Price — fall back to individual quote if not pre-fetched
        if price is None:
            quotes = fetch_quotes([ticker])
            q = quotes.get(ticker) or {}
            raw = q.get("last")
            if not _is_valid(raw):
                logger.debug("%s: no price from Tradier", ticker)
                return None
            price = float(raw)

        # 2. Historical OHLCV bars → HV + SMA + S/R
        bars = fetch_bars(ticker)
        closes = [b.close for b in bars]
        log_ret = _log_returns(closes)
        hv = _annualized_vol(log_ret, HV_WINDOW)

        # SMA calculations
        sma_200 = _calc_sma(closes, 200)
        sma_50 = _calc_sma(closes, 50)
        price_vs_sma200_pct = ((price - sma_200) / sma_200 * 100) if sma_200 else None
        price_vs_sma50_pct = ((price - sma_50) / sma_50 * 100) if sma_50 else None
        regime = _sma_regime(price, sma_50, sma_200)
        golden_cross = (sma_50 > sma_200) if (sma_50 and sma_200) else None

        # Support / Resistance detection
        supports, resistances = _find_support_resistance(bars, price)

        # Diagnostic log for first 3 tickers so we can verify SMA/SR in Railway logs
        global _sma_sr_debug_count
        if _sma_sr_debug_count < 3:
            _sma_sr_debug_count += 1
            logger.info(
                "SMA/SR debug [%d/3] %s: bars=%d "
                "200SMA=%s 50SMA=%s regime=%s "
                "S1=%s S2=%s R1=%s R2=%s",
                _sma_sr_debug_count, ticker, len(closes),
                f"${sma_200:.2f}" if sma_200 else "None",
                f"${sma_50:.2f}" if sma_50 else "None",
                regime or "None",
                f"${supports[0]['price']:.2f}" if supports else "None",
                f"${supports[1]['price']:.2f}" if len(supports) > 1 else "None",
                f"${resistances[0]['price']:.2f}" if resistances else "None",
                f"${resistances[1]['price']:.2f}" if len(resistances) > 1 else "None",
            )

        # 3. Options expirations → Phase A filter 2: must be optionable
        exps = fetch_expirations(ticker)
        if not exps:
            logger.debug("%s: no options chain — skipping", ticker)
            return None

        # IV expiry: 21-45d window, used for IV rank + chain quality metrics
        iv_exp = _nearest_expiry(exps)

        atm_iv: float | None = None
        atm_oi: int | None = None
        spread_pct: float | None = None

        # Chain cache to avoid double-fetching when iv_exp overlaps prem_exps
        chain_cache: dict[str, list[dict]] = {}

        # 4a. Fetch IV expiry chain for IV rank + chain quality
        if iv_exp:
            try:
                chain_cache[iv_exp] = fetch_chain(ticker, iv_exp)
                iv_chain = chain_cache[iv_exp]
                atm_iv_c, _, _ = _pick_call_strikes(iv_chain, price)
                if atm_iv_c:
                    greeks = atm_iv_c.get("greeks") or {}
                    iv_raw = greeks.get("smv_vol") or greeks.get("mid_iv") or greeks.get("iv")
                    if _is_valid(iv_raw):
                        atm_iv = float(iv_raw)
                    atm_mid_iv = _contract_mid(atm_iv_c)
                    if atm_mid_iv and atm_mid_iv > 0:
                        bid = float(atm_iv_c.get("bid") or 0)
                        ask = float(atm_iv_c.get("ask") or 0)
                        if bid > 0 and ask > 0:
                            spread_pct = (ask - bid) / atm_mid_iv
                    oi_raw = atm_iv_c.get("open_interest")
                    if _is_valid(oi_raw):
                        atm_oi = int(float(oi_raw))
            except Exception as exc:
                logger.debug("%s: IV chain fetch failed (%s): %s", ticker, iv_exp, exc)

        # Premium expiry: nearest single expiry in 7-30d window (avoids multi-fetch timeout).
        # Full multi-expiry table is fetched on demand when user opens the detail modal.
        prem_exps = _expirations_for_premium(exps)
        prem_exp_tuple = prem_exps[0] if prem_exps else None
        if prem_exp_tuple is None and iv_exp:
            iv_dte = (datetime.strptime(iv_exp, "%Y-%m-%d").date() - date.today()).days
            prem_exp_tuple = (iv_dte, iv_exp)

        best_atm_premium: float | None = None
        best_otm1: float | None = None
        best_otm2: float | None = None
        best_expiry: str | None = None
        best_dte: int | None = None
        best_strike: float | None = None
        best_put_premium: float | None = None
        best_put_strike: float | None = None
        best_put_expiry: str | None = None
        best_put_dte: int | None = None

        if prem_exp_tuple:
            dte, exp = prem_exp_tuple
            try:
                if exp not in chain_cache:
                    chain_cache[exp] = fetch_chain(ticker, exp)
                chain = chain_cache[exp]
                atm_c, otm1_c, otm2_c = _pick_call_strikes(chain, price)
                best_atm_premium = _contract_mid(atm_c)
                best_otm1 = _contract_mid(otm1_c)
                best_otm2 = _contract_mid(otm2_c)
                best_expiry = exp
                best_dte = dte
                best_strike = round(float(atm_c["strike"]), 2) if atm_c else None
                # ATM put from same chain
                _puts = sorted(
                    [o for o in chain if o.get("option_type") == "put" and _is_valid(o.get("strike")) and _is_sane_contract(o, price)],
                    key=lambda o: float(o["strike"]),
                )
                if _puts:
                    _pi = min(range(len(_puts)), key=lambda i: abs(float(_puts[i]["strike"]) - price))
                    _p = _puts[_pi]
                    best_put_premium = _contract_mid(_p)
                    best_put_strike = round(float(_p["strike"]), 2)
                    best_put_expiry = exp
                    best_put_dte = dte
            except Exception as exc:
                logger.debug("%s: premium chain fetch failed (%s): %s", ticker, exp, exc)

        # Fall back to IV expiry data if premium fetch failed
        if best_atm_premium is None and iv_exp and iv_exp in chain_cache:
            iv_chain = chain_cache[iv_exp]
            atm_c, otm1_c, otm2_c = _pick_call_strikes(iv_chain, price)
            best_atm_premium = _contract_mid(atm_c)
            best_otm1 = _contract_mid(otm1_c)
            best_otm2 = _contract_mid(otm2_c)
            best_expiry = iv_exp
            if iv_exp:
                best_dte = (datetime.strptime(iv_exp, "%Y-%m-%d").date() - date.today()).days
            best_strike = round(float(atm_c["strike"]), 2) if atm_c else None
            # ATM put from fallback IV chain
            _puts = sorted(
                [o for o in iv_chain if o.get("option_type") == "put" and _is_valid(o.get("strike")) and _is_sane_contract(o, price)],
                key=lambda o: float(o["strike"]),
            )
            if _puts:
                _pi = min(range(len(_puts)), key=lambda i: abs(float(_puts[i]["strike"]) - price))
                _p = _puts[_pi]
                best_put_premium = _contract_mid(_p)
                best_put_strike = round(float(_p["strike"]), 2)
                best_put_expiry = iv_exp
                best_put_dte = best_dte

        atm_premium = best_atm_premium
        premium_otm1 = best_otm1
        premium_otm2 = best_otm2

        # IV rank: use rolling-vol approximation if we have closes; else 50 placeholder
        iv_rank = _iv_rank_from_history(closes, atm_iv)
        if iv_rank is None and atm_iv is not None:
            iv_rank = 50.0  # neutral placeholder until we have 30d of IV history

        # Use best ATM premium across all expirations for % scoring
        premium_pct = (atm_premium / price) if (atm_premium and price) else None
        unusual_vol = False

        # earn_days is pre-fetched from the earnings calendar lookup in run_scan
        iv_ramp = (
            iv_rank is not None and iv_rank >= 70
            and earn_days is not None and 0 < earn_days <= 21
        )

        metrics = TickerMetrics(
            iv_rank=iv_rank,
            premium_pct=premium_pct,
            premium_otm2=premium_otm2,
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

        # SMA modifier: small ±5 adjustment baked into the final score
        sma_adj = _sma_score_modifier(price, sma_50, sma_200, regime, golden_cross)
        final_score = round(max(0.0, min(100.0, breakdown.total + sma_adj)), 2)

        bucket = assign_bucket(final_score, iv_rank, premium_pct, earn_days)

        safety_score = _calc_safety_score(
            premium_otm2, atm_iv, hv, price,
            supports[0]["price"] if supports else None,
            bars, earn_days,
        )

        cc_score = _calc_cc_score(
            price=price,
            sma_200=sma_200,
            sma_50=sma_50,
            sma_golden_cross=golden_cross,
            resistance_1=resistances[0]["price"] if resistances else None,
            price_vs_sma200_pct=price_vs_sma200_pct,
            price_vs_sma50_pct=price_vs_sma50_pct,
            iv_rank=iv_rank,
            atm_call_premium=atm_premium,
            iv=atm_iv,
            hv=hv,
            premium_pct=premium_pct,
            open_interest=atm_oi,
            bid_ask_spread_pct=spread_pct,
            support_1=supports[0]["price"] if supports else None,
            sma_regime=regime,
        )

        csp_score = _calc_csp_score(
            price=price,
            sma_200=sma_200,
            sma_50=sma_50,
            sma_golden_cross=golden_cross,
            sma_regime=regime,
            support_1=supports[0]["price"] if supports else None,
            support_1_strength=supports[0]["strength"] if supports else None,
            support_2=supports[1]["price"] if len(supports) > 1 else None,
            iv_rank=iv_rank,
            atm_put_premium=best_put_premium,
            iv=atm_iv,
            hv=hv,
            premium_pct=premium_pct,
            open_interest=atm_oi,
            bid_ask_spread_pct=spread_pct,
        )

        return ScanRowResult(
            ticker=ticker,
            metrics=metrics,
            price=price,
            atm_call_premium=atm_premium,
            premium_otm1=round(premium_otm1, 4) if premium_otm1 else None,
            premium_otm2=round(premium_otm2, 4) if premium_otm2 else None,
            score=final_score,
            bucket=bucket,
            breakdown_iv_rank=breakdown.iv_rank,
            breakdown_premium=breakdown.premium,
            breakdown_iv_hv=breakdown.iv_hv,
            breakdown_catalyst=breakdown.catalyst,
            breakdown_chain=breakdown.chain,
            safety_score=safety_score,
            iv=atm_iv,
            # Multi-expiry
            best_expiry=best_expiry,
            best_dte=best_dte,
            best_strike=best_strike,
            expiry_data=None,
            # ATM put
            atm_put_premium=round(best_put_premium, 4) if best_put_premium else None,
            best_put_strike=best_put_strike,
            best_put_expiry=best_put_expiry,
            best_put_dte=best_put_dte,
            # SMA
            sma_200=round(sma_200, 4) if sma_200 else None,
            sma_50=round(sma_50, 4) if sma_50 else None,
            price_vs_sma200_pct=round(price_vs_sma200_pct, 2) if price_vs_sma200_pct is not None else None,
            price_vs_sma50_pct=round(price_vs_sma50_pct, 2) if price_vs_sma50_pct is not None else None,
            sma_regime=regime,
            sma_golden_cross=golden_cross,
            # S/R
            support_1=round(supports[0]["price"], 2) if len(supports) > 0 else None,
            support_1_strength=supports[0]["strength"] if len(supports) > 0 else None,
            support_2=round(supports[1]["price"], 2) if len(supports) > 1 else None,
            support_2_strength=supports[1]["strength"] if len(supports) > 1 else None,
            resistance_1=round(resistances[0]["price"], 2) if len(resistances) > 0 else None,
            resistance_1_strength=resistances[0]["strength"] if len(resistances) > 0 else None,
            resistance_2=round(resistances[1]["price"], 2) if len(resistances) > 1 else None,
            resistance_2_strength=resistances[1]["strength"] if len(resistances) > 1 else None,
            cc_score=cc_score,
            csp_score=csp_score,
        )
    except Exception as exc:
        logger.warning("scan_ticker failed for %s: %s", ticker, exc)
        return None


# ── Timeout wrapper ───────────────────────────────────────────────────────────

def _scan_with_timeout(ticker: str, price: float | None = None, earn_days: int | None = None) -> ScanRowResult | None:
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(scan_ticker, ticker, price, earn_days)
        try:
            return future.result(timeout=TICKER_TIMEOUT)
        except FuturesTimeout:
            logger.warning("ticker %s timed out after %ds", ticker, TICKER_TIMEOUT)
            return None
        except Exception as exc:
            logger.warning("ticker %s raised unexpectedly: %s", ticker, exc)
            return None


# ── IV Ramp Detection ─────────────────────────────────────────────────────────

def _calc_iv_ramp_metrics(
    db: Session,
    ticker: str,
    current_iv: float | None,
    iv_rank: float | None,
    golden_cross: bool | None,
    sma_regime: str | None,
) -> dict:
    """Query iv_history and compute IV momentum / ramp metrics."""
    empty = {
        "iv_5d_ago": None, "iv_10d_ago": None, "iv_20d_ago": None,
        "iv_velocity_5d": None, "iv_velocity_10d": None, "iv_velocity_20d": None,
        "iv_ramp_score": 0, "iv_ramp_flag": False,
    }
    if current_iv is None or current_iv <= 0:
        return empty
    try:
        from sqlalchemy import text as _text
        rows = db.execute(
            _text("SELECT iv FROM iv_history WHERE ticker = :t ORDER BY recorded_date DESC LIMIT 30"),
            {"t": ticker},
        ).fetchall()
    except Exception:
        return empty

    def _get(idx: int) -> float | None:
        return float(rows[idx].iv) if len(rows) > idx else None

    # rows sorted desc: index 4 = 5d ago, index 9 = 10d ago, index 19 = 20d ago
    iv_5d  = _get(4)
    iv_10d = _get(9)
    iv_20d = _get(19)

    def _vel(prev: float | None) -> float | None:
        if prev is None or prev <= 0:
            return None
        return round((current_iv - prev) / prev * 100, 2)

    v5  = _vel(iv_5d)
    v10 = _vel(iv_10d)
    v20 = _vel(iv_20d)

    result = {
        "iv_5d_ago":       round(iv_5d,  4) if iv_5d  is not None else None,
        "iv_10d_ago":      round(iv_10d, 4) if iv_10d is not None else None,
        "iv_20d_ago":      round(iv_20d, 4) if iv_20d is not None else None,
        "iv_velocity_5d":  v5,
        "iv_velocity_10d": v10,
        "iv_velocity_20d": v20,
        "iv_ramp_score":   0,
        "iv_ramp_flag":    False,
    }

    if len(rows) < 10:
        return result  # not enough history — never fabricate

    s = 0
    # IV Rank component — low rank means premiums still cheap
    if iv_rank is not None:
        if   iv_rank < 40:  s += 25
        elif iv_rank <= 60: s += 10
    # 5d velocity
    if v5 is not None:
        if   v5 > 10: s += 20
        elif v5 > 5:  s += 10
        elif v5 > 0:  s += 5
    # 10d velocity
    if v10 is not None:
        if   v10 > 15: s += 20
        elif v10 > 8:  s += 10
        elif v10 > 0:  s += 5
    # 20d velocity
    if v20 is not None:
        if   v20 > 20: s += 15
        elif v20 > 10: s += 10
        elif v20 > 0:  s += 5
    # Golden cross — healthy stock, not fear-driven IV spike
    if golden_cross: s += 10
    # SMA regime
    if   sma_regime == "UPTREND":      s += 10
    elif sma_regime == "TRANSITIONAL": s += 5

    ramp_score = min(100, max(0, s))
    any_positive = any(v is not None and v > 0 for v in [v5, v10, v20])
    result["iv_ramp_score"] = ramp_score
    result["iv_ramp_flag"] = (
        ramp_score >= 50
        and iv_rank is not None and iv_rank < 50
        and any_positive
    )
    return result


# ── DB persistence ────────────────────────────────────────────────────────────

def _persist_result(db: Session, run_id: int, result: ScanRowResult) -> None:
    iv_ramp = _calc_iv_ramp_metrics(
        db, result.ticker, result.metrics.iv, result.metrics.iv_rank,
        result.sma_golden_cross, result.sma_regime,
    )
    db.add(models.ScanResult(
        run_id=run_id,
        ticker=result.ticker,
        company_name=result.company_name,
        price=result.price,
        iv_rank=result.metrics.iv_rank,
        iv=result.metrics.iv,
        hv=result.metrics.hv,
        atm_call_premium=result.atm_call_premium,
        premium_pct=result.metrics.premium_pct,
        premium_otm1=result.premium_otm1,
        premium_otm2=result.premium_otm2,
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
        # SMA
        sma_200=result.sma_200,
        sma_50=result.sma_50,
        price_vs_sma200_pct=result.price_vs_sma200_pct,
        price_vs_sma50_pct=result.price_vs_sma50_pct,
        sma_regime=result.sma_regime,
        sma_golden_cross=result.sma_golden_cross,
        # S/R
        support_1=result.support_1,
        support_1_strength=result.support_1_strength,
        support_2=result.support_2,
        support_2_strength=result.support_2_strength,
        resistance_1=result.resistance_1,
        resistance_1_strength=result.resistance_1_strength,
        resistance_2=result.resistance_2,
        resistance_2_strength=result.resistance_2_strength,
        safety_score=result.safety_score,
        # Multi-expiry
        best_expiry=result.best_expiry,
        best_dte=result.best_dte,
        best_strike=result.best_strike,
        expiry_data=result.expiry_data,
        # ATM put
        atm_put_premium=result.atm_put_premium,
        best_put_strike=result.best_put_strike,
        best_put_expiry=result.best_put_expiry,
        best_put_dte=result.best_put_dte,
        # CC / CSP scores
        cc_score=result.cc_score,
        csp_score=result.csp_score,
        # IV ramp metrics (computed from iv_history)
        **iv_ramp,
    ))

    # Record IV snapshot for IV rank history (one row per ticker per day)
    if result.metrics.iv is not None:
        try:
            from sqlalchemy import text as _text
            today_str = str(date.today())
            db.execute(
                _text(
                    "INSERT INTO iv_history (ticker, iv, recorded_date) "
                    "VALUES (:ticker, :iv, :date) "
                    "ON CONFLICT (ticker, recorded_date) DO UPDATE SET iv = EXCLUDED.iv"
                ),
                {"ticker": result.ticker, "iv": result.metrics.iv, "date": today_str},
            )
        except Exception as _exc:
            logger.debug("iv_history insert failed for %s: %s", result.ticker, _exc)


# ── Main scan orchestrator ────────────────────────────────────────────────────

def scan_ticker_extensive(ticker: str, price: float | None = None, earn_days: int | None = None) -> ScanRowResult | None:
    """Extensive scan: runs normal scan_ticker then also fetches nearest weekly expiry.

    Weekly = earliest available expiry that is NOT the one already used by the base scan.
    Both entries are stored in expiry_data JSON so the Premium Scanner DTE filter can use them.
    """
    result = scan_ticker(ticker, price, earn_days)
    if result is None:
        return None

    try:
        exps = fetch_expirations(ticker)
        today = date.today()

        # Find weekly = earliest expiry that differs from the one used by the base scan
        used_expiry = result.best_expiry
        weekly_exp: str | None = None
        for exp in exps:
            try:
                d = datetime.strptime(exp, "%Y-%m-%d").date()
            except ValueError:
                continue
            if (d - today).days < 1:
                continue
            if exp != used_expiry:
                weekly_exp = exp
                break

        expiry_rows: list[dict] = []

        if weekly_exp:
            try:
                w_dte = (datetime.strptime(weekly_exp, "%Y-%m-%d").date() - today).days
                w_chain = fetch_chain(ticker, weekly_exp)
                w_price = result.price or 100.0
                w_atm, _, _ = _pick_call_strikes(w_chain, w_price)
                w_atm_prem = _contract_mid(w_atm)
                w_atm_strike = round(float(w_atm["strike"]), 2) if w_atm else None
                w_puts = sorted(
                    [o for o in w_chain if o.get("option_type") == "put" and _is_valid(o.get("strike")) and _is_sane_contract(o, w_price)],
                    key=lambda o: float(o["strike"]),
                )
                w_atm_put_mid: float | None = None
                if w_puts and w_atm_strike:
                    pi = min(range(len(w_puts)), key=lambda i: abs(float(w_puts[i]["strike"]) - w_atm_strike))
                    w_atm_put_mid = _contract_mid(w_puts[pi])
                expiry_rows.append({
                    "expiry": weekly_exp,
                    "dte": w_dte,
                    "atm_strike": w_atm_strike,
                    "atm_call_prem": round(w_atm_prem, 4) if w_atm_prem else None,
                    "atm_put_prem": round(w_atm_put_mid, 4) if w_atm_put_mid else None,
                    "calls": _collect_otm_calls(w_chain, w_price),
                    "puts": _collect_otm_puts(w_chain, w_price),
                })
            except Exception as exc:
                logger.debug("%s: weekly chain fetch failed (%s): %s", ticker, weekly_exp, exc)

        # Add the base (monthly) expiry entry so both are queryable in Premium Scanner
        if result.best_expiry:
            try:
                base_chain = fetch_chain(ticker, result.best_expiry)
                base_price = result.price or 100.0
                base_calls = _collect_otm_calls(base_chain, base_price)
                base_puts  = _collect_otm_puts(base_chain, base_price)
            except Exception:
                base_calls, base_puts = [], []
            expiry_rows.append({
                "expiry": result.best_expiry,
                "dte": result.best_dte,
                "atm_strike": result.best_strike,
                "atm_call_prem": round(result.atm_call_premium, 4) if result.atm_call_premium else None,
                "atm_put_prem": None,
                "calls": base_calls,
                "puts": base_puts,
            })

        if expiry_rows:
            result.expiry_data = json.dumps(expiry_rows)

    except Exception as exc:
        logger.debug("%s: extensive extra fetch failed: %s", ticker, exc)

    return result


def _scan_extensive_with_timeout(ticker: str, price: float | None = None, earn_days: int | None = None) -> ScanRowResult | None:
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(scan_ticker_extensive, ticker, price, earn_days)
        try:
            return future.result(timeout=TICKER_TIMEOUT)
        except FuturesTimeout:
            logger.warning("ticker %s (extensive) timed out after %ds", ticker, TICKER_TIMEOUT)
            return None
        except Exception as exc:
            logger.warning("ticker %s (extensive) raised unexpectedly: %s", ticker, exc)
            return None


def run_scan(db: Session, tickers: list[str] | None = None, limit: int | None = None, scanner_fn=None) -> int:
    """Crash-resilient, resumable scan. Returns ScanRun id.

    scanner_fn: optional override for per-ticker scan function (defaults to _scan_with_timeout).
    Pass _scan_extensive_with_timeout for extensive mode.
    """
    if scanner_fn is None:
        scanner_fn = _scan_with_timeout
    if tickers is not None:
        universe = tickers
    else:
        # Load from DB — the CSV-synced ticker_universe table is the source of truth
        try:
            from ..universe import load_universe_from_db
            universe = load_universe_from_db(db)
            logger.info("loaded %d tickers from ticker_universe table", len(universe))
        except Exception as exc:
            logger.warning("DB universe load failed, falling back to JSON: %s", exc)
            universe = load_universe()
    if limit is not None:
        universe = universe[:limit]

    # Find or create a ScanRun
    existing_run = db.execute(
        select(models.ScanRun)
        .where(models.ScanRun.status == "running")
        .order_by(models.ScanRun.started_at.desc())
        .limit(1)
    ).scalar_one_or_none()

    if existing_run is not None:
        run = existing_run
        done: set[str] = set(
            db.execute(
                select(models.ScanResult.ticker).where(models.ScanResult.run_id == run.id)
            ).scalars().all()
        )
        remaining = [t for t in universe if t not in done]
        scanned, errored = run.tickers_scanned, run.tickers_errored
        logger.info("resuming run_id=%s — %d done, %d remaining", run.id, len(done), len(remaining))
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
        scanned = errored = 0
        logger.info("new scan run_id=%s, %d tickers", run.id, len(remaining))

    run.tickers_total = len(universe)
    db.commit()

    # Build earnings lookup once for the entire scan (3 calendar API calls total)
    earnings_lookup: dict[str, int] = {}
    try:
        earnings_lookup = build_earnings_lookup()
    except Exception as exc:
        logger.warning("earnings lookup failed, catalyst scores will be 0: %s", exc)

    cat_debug_logged = 0  # log earn_days for first 5 tickers to diagnose CAT=0

    for batch_start in range(0, len(remaining), BATCH_SIZE):
        batch = remaining[batch_start : batch_start + BATCH_SIZE]
        logger.info("run_id=%s batch %d-%d / %d", run.id,
                    batch_start + 1, batch_start + len(batch), len(remaining))

        # Pre-fetch quotes for the whole batch (≤20 tickers per Tradier call)
        prices: dict[str, float] = {}
        company_names: dict[str, str] = {}
        for q_start in range(0, len(batch), QUOTE_BATCH):
            q_symbols = batch[q_start : q_start + QUOTE_BATCH]
            try:
                for sym, q in fetch_quotes(q_symbols).items():
                    raw = q.get("last")
                    if _is_valid(raw):
                        prices[sym] = float(raw)
                    desc = (q.get("description") or "").strip()
                    if desc:
                        company_names[sym] = desc
            except Exception as exc:
                logger.warning("quote batch failed (%s): %s", q_symbols, exc)

        # No price filter — scan every ticker in the universe. Price filtering
        # is handled on the frontend display only so data is always available.
        filtered_batch = [t for t in batch if prices.get(t, 0.0) > 0.0]
        skipped_no_price = len(batch) - len(filtered_batch)
        if skipped_no_price:
            logger.info("run_id=%s no price from Tradier: skipped %d tickers",
                        run.id, skipped_no_price)
            errored += skipped_no_price

        for ticker in filtered_batch:
            earn_days_val = earnings_lookup.get(ticker)
            if cat_debug_logged < 5:
                logger.info(
                    "CAT debug [%d/5]: ticker=%s earn_days=%s earnings_in_lookup=%s",
                    cat_debug_logged + 1, ticker, earn_days_val, ticker in earnings_lookup,
                )
                cat_debug_logged += 1
            result = scanner_fn(ticker, price=prices.get(ticker), earn_days=earn_days_val)
            if result is None:
                errored += 1
            else:
                result.company_name = company_names.get(ticker)
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


def run_scan_extensive(db: Session, tickers: list[str] | None = None, limit: int | None = None) -> int:
    """Extensive scan: normal scan + nearest weekly expiry chain per ticker."""
    return run_scan(db, tickers=tickers, limit=limit, scanner_fn=_scan_extensive_with_timeout)


def run_scan_cli() -> None:
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
