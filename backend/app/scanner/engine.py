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
    with httpx.Client(timeout=15.0) as client:
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
    exp_dates: Iterable[str], min_days: int = 7, max_days: int = 30
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
        [o for o in options if o.get("option_type") == "call" and _is_valid(o.get("strike"))],
        key=lambda o: float(o["strike"]),
    )
    if not calls:
        return None, None, None
    atm_idx = min(range(len(calls)), key=lambda i: abs(float(calls[i]["strike"]) - price))
    atm  = calls[atm_idx]
    otm1 = calls[atm_idx + 1] if atm_idx + 1 < len(calls) else None
    otm2 = calls[atm_idx + 2] if atm_idx + 2 < len(calls) else None
    return atm, otm1, otm2


def _contract_mid(contract: dict | None) -> float | None:
    """Mid price per share from a Tradier option contract dict."""
    if contract is None:
        return None
    bid = float(contract.get("bid") or 0)
    ask = float(contract.get("ask") or 0)
    if bid > 0 and ask > 0:
        return (bid + ask) / 2
    return None


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
    # Multi-expiry premium data
    best_expiry: str | None = None
    best_dte: int | None = None
    best_strike: float | None = None
    expiry_data: str | None = None  # JSON string
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

        # Premium expirations: 7-30d, cap at 5 to limit API calls
        prem_exps = _expirations_for_premium(exps)[:5]
        if not prem_exps:
            # No short-term expirations; fall back to whatever we have
            if iv_exp:
                iv_dte = (datetime.strptime(iv_exp, "%Y-%m-%d").date() - date.today()).days
                prem_exps = [(iv_dte, iv_exp)]

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

        # 4b. Fetch each premium expiry and find the best (highest otm2 premium)
        best_atm_premium: float | None = None
        best_premium_pct: float | None = None
        best_otm1: float | None = None
        best_otm2: float | None = None
        best_expiry: str | None = None
        best_dte: int | None = None
        best_strike: float | None = None
        expiry_rows: list[dict] = []

        for dte, exp in prem_exps:
            try:
                if exp not in chain_cache:
                    chain_cache[exp] = fetch_chain(ticker, exp)
                chain = chain_cache[exp]
                atm_c, otm1_c, otm2_c = _pick_call_strikes(chain, price)
                atm_mid_p = _contract_mid(atm_c)
                otm1_mid_p = _contract_mid(otm1_c)
                otm2_mid_p = _contract_mid(otm2_c)
                atm_strike_p = float(atm_c["strike"]) if atm_c else None

                expiry_rows.append({
                    "expiry": exp,
                    "dte": dte,
                    "atm_strike": atm_strike_p,
                    "atm_prem": round(atm_mid_p, 4) if atm_mid_p else None,
                    "otm1_prem": round(otm1_mid_p, 4) if otm1_mid_p else None,
                    "otm2_prem": round(otm2_mid_p, 4) if otm2_mid_p else None,
                })

                # Pick best expiry = highest otm2 premium (fall back to atm if no otm2)
                compare_prem = otm2_mid_p if otm2_mid_p else (atm_mid_p or 0)
                current_best = best_otm2 if best_otm2 else (best_atm_premium or 0)
                if compare_prem > current_best:
                    best_atm_premium = atm_mid_p
                    best_otm1 = otm1_mid_p
                    best_otm2 = otm2_mid_p
                    best_expiry = exp
                    best_dte = dte
                    best_strike = atm_strike_p
            except Exception as exc:
                logger.debug("%s: premium chain fetch failed (%s): %s", ticker, exp, exc)

        # If no premium expirations succeeded, fall back to IV expiry data
        if best_atm_premium is None and iv_exp and iv_exp in chain_cache:
            iv_chain = chain_cache[iv_exp]
            atm_c, otm1_c, otm2_c = _pick_call_strikes(iv_chain, price)
            best_atm_premium = _contract_mid(atm_c)
            best_otm1 = _contract_mid(otm1_c)
            best_otm2 = _contract_mid(otm2_c)
            best_expiry = iv_exp
            if iv_exp:
                best_dte = (datetime.strptime(iv_exp, "%Y-%m-%d").date() - date.today()).days
            best_strike = float(atm_c["strike"]) if atm_c else None

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
            # Multi-expiry
            best_expiry=best_expiry,
            best_dte=best_dte,
            best_strike=round(best_strike, 2) if best_strike else None,
            expiry_data=json.dumps(expiry_rows) if expiry_rows else None,
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
        # Multi-expiry
        best_expiry=result.best_expiry,
        best_dte=result.best_dte,
        best_strike=result.best_strike,
        expiry_data=result.expiry_data,
    ))


# ── Main scan orchestrator ────────────────────────────────────────────────────

def run_scan(db: Session, tickers: list[str] | None = None, limit: int | None = None) -> int:
    """Crash-resilient, resumable scan. Returns ScanRun id."""
    universe = tickers if tickers is not None else load_universe()
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
        for q_start in range(0, len(batch), QUOTE_BATCH):
            q_symbols = batch[q_start : q_start + QUOTE_BATCH]
            try:
                for sym, q in fetch_quotes(q_symbols).items():
                    raw = q.get("last")
                    if _is_valid(raw):
                        prices[sym] = float(raw)
            except Exception as exc:
                logger.warning("quote batch failed (%s): %s", q_symbols, exc)

        # Phase A filter 1: price must be $10–$300 (no volume filter)
        filtered_batch = [t for t in batch if 10.0 <= prices.get(t, 0.0) <= 300.0]
        skipped_price = len(batch) - len(filtered_batch)
        if skipped_price:
            logger.info("run_id=%s price filter: skipped %d tickers outside $10-$300",
                        run.id, skipped_price)
            errored += skipped_price

        for ticker in filtered_batch:
            earn_days_val = earnings_lookup.get(ticker)
            if cat_debug_logged < 5:
                logger.info(
                    "CAT debug [%d/5]: ticker=%s earn_days=%s earnings_in_lookup=%s",
                    cat_debug_logged + 1, ticker, earn_days_val, ticker in earnings_lookup,
                )
                cat_debug_logged += 1
            result = _scan_with_timeout(ticker, price=prices.get(ticker), earn_days=earn_days_val)
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
