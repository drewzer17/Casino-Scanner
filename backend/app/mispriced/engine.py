"""Sweep logic, in-memory state, and the cadence loop for the DITM covered-call
mispricing scanner.

No database writes anywhere. ticker_universe and earnings_calendar are read
only. Everything else — toggle state, floor, latest results — lives in a
module-level dict and is lost on process restart. That's intentional: the
spec is "no storage, no migrations, no new tables."

Not wired to backend/app/scheduler.py's APScheduler cron. The cadence loop
here is a separate background thread, gated entirely by the in-memory toggle:
while off, it does nothing and makes zero Tradier calls.
"""
from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime

import httpx
import pytz
from sqlalchemy import text as _text

from ..config import settings
from ..database import SessionLocal

logger = logging.getLogger(__name__)

TRADIER_BASE = "https://sandbox.tradier.com"
QUOTE_BATCH = 20
STRIKES_BELOW_SPOT = 15
SWEEP_WORKERS = 8  # concurrent tickers per sweep

# Central Time, 24h "HH:MM" strings. Edit this list to retune cadence — no
# other code changes needed. Front-loaded near the open, spreads out midday.
SWEEP_TIMES_CT = [
    "08:32", "08:40", "08:50", "09:00", "09:15", "09:30", "09:50",
    "10:15", "10:45", "11:15", "11:45", "12:30", "13:15", "14:00", "14:45",
]

# Market-hours gate — Mon-Fri only, 8:30 AM-3:00 PM CT. The toggle can stay
# ON overnight/weekends; it just sits idle outside this window and makes no
# Tradier calls until the next valid window arrives.
MARKET_OPEN_CT = "08:30"
MARKET_CLOSE_CT = "15:00"

_CT = pytz.timezone("America/Chicago")


def _within_market_hours(now_ct: datetime) -> bool:
    if now_ct.weekday() >= 5:  # Mon=0 .. Fri=4, Sat=5, Sun=6
        return False
    hhmm = now_ct.strftime("%H:%M")
    return MARKET_OPEN_CT <= hhmm <= MARKET_CLOSE_CT


def is_market_open_now() -> bool:
    return _within_market_hours(datetime.now(_CT))

# ── in-memory state ──────────────────────────────────────────────────────────
_lock = threading.RLock()
_state = {
    "toggle_on": False,
    "floor": 500.0,
    "rows": [],              # last sweep's qualifying rows, sorted desc by edge
    "known_keys": set(),     # (ticker, expiration, strike) that qualified last sweep
    "last_swept_at": None,   # ISO8601 UTC string
    "sweep_in_progress": False,
    "last_error": None,
    "universe_size": None,
    "last_call_count": None,
    "last_sweep_seconds": None,
}


def get_state() -> dict:
    with _lock:
        return {
            "toggle_on": _state["toggle_on"],
            "floor": _state["floor"],
            "rows": _state["rows"],
            "last_swept_at": _state["last_swept_at"],
            "sweep_in_progress": _state["sweep_in_progress"],
            "last_error": _state["last_error"],
            "universe_size": _state["universe_size"],
            "last_call_count": _state["last_call_count"],
            "last_sweep_seconds": _state["last_sweep_seconds"],
            "cadence_times_ct": SWEEP_TIMES_CT,
            "market_open": is_market_open_now(),  # computed live, not stored
        }


def set_toggle(on: bool) -> dict:
    with _lock:
        was_on = _state["toggle_on"]
        _state["toggle_on"] = on
    # Only fire immediately if we're actually inside market hours. Flipping
    # ON overnight/weekend just arms the toggle — it sits idle until the
    # cadence loop's next in-hours slot, no immediate Tradier calls.
    if on and not was_on and is_market_open_now():
        threading.Thread(target=run_sweep, name="mispriced-immediate-sweep", daemon=True).start()
    return get_state()


def set_floor(floor: float) -> dict:
    if floor < 0:
        raise ValueError("floor must be >= 0")
    with _lock:
        _state["floor"] = float(floor)
    return get_state()


# ── minimal Tradier client — deliberately NOT scan_ticker_extensive, which
#    does unrelated scoring work (1yr history, IV history, SMA, ...) that
#    would badly overstate this scanner's real cost ─────────────────────────

def _headers() -> dict:
    return {"Authorization": f"Bearer {settings.tradier_api_key}", "Accept": "application/json"}


def _get(client: httpx.Client, path: str, params: dict, tries: int = 3) -> dict:
    last_exc: Exception | None = None
    for attempt in range(1, tries + 1):
        try:
            resp = client.get(TRADIER_BASE + path, params=params, headers=_headers())
            if resp.status_code == 429:
                time.sleep(1.5 * attempt)
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            last_exc = exc
            time.sleep(0.5 * attempt)
    raise last_exc


def _fetch_quotes_batch(client: httpx.Client, symbols: list[str]) -> dict:
    data = _get(client, "/v1/markets/quotes", {"symbols": ",".join(symbols)})
    q = (data.get("quotes") or {}).get("quote") or []
    if isinstance(q, dict):
        q = [q]
    return {row["symbol"]: row for row in q if "symbol" in row}


def _fetch_expirations(client: httpx.Client, symbol: str) -> list[str]:
    data = _get(client, "/v1/markets/options/expirations", {"symbol": symbol, "includeAllRoots": "true"})
    exps = data.get("expirations") or {}
    dates = exps.get("date") or []
    if isinstance(dates, str):
        dates = [dates]
    return dates


def _fetch_chain(client: httpx.Client, symbol: str, expiration: str) -> list[dict]:
    data = _get(client, "/v1/markets/options/chains",
                {"symbol": symbol, "expiration": expiration, "greeks": "true"})
    opts = data.get("options") or {}
    o = opts.get("option") or []
    if isinstance(o, dict):
        o = [o]
    return o


def _is_valid(v) -> bool:
    return v is not None and v != "" and not (isinstance(v, float) and v != v)


# ── universe + earnings (read only) ─────────────────────────────────────────

def _load_universe() -> list[str]:
    db = SessionLocal()
    try:
        rows = db.execute(_text(
            "SELECT ticker FROM ticker_universe WHERE source = 'ai_sector' AND active = true ORDER BY ticker"
        )).fetchall()
        return [r[0] for r in rows]
    finally:
        db.close()


def _load_earnings(tickers: list[str]) -> dict[str, date]:
    if not tickers:
        return {}
    db = SessionLocal()
    try:
        rows = db.execute(
            _text("SELECT ticker, next_earnings_date FROM earnings_calendar "
                  "WHERE ticker = ANY(:tickers) AND next_earnings_date IS NOT NULL"),
            {"tickers": tickers},
        ).fetchall()
        return {r[0]: r[1] for r in rows}
    finally:
        db.close()


def _load_sitrep_set(tickers: list[str]) -> set[str]:
    if not tickers:
        return set()
    db = SessionLocal()
    try:
        rows = db.execute(
            _text("SELECT ticker FROM sitreps WHERE ticker = ANY(:tickers)"),
            {"tickers": tickers},
        ).fetchall()
        return {r[0] for r in rows}
    finally:
        db.close()


def _load_ai_metadata() -> dict[str, dict]:
    """Same universe/lens metadata (category, lenses, primary_lens) the main
    scanner's purple pills use — reused verbatim, not reimplemented, per the
    same ai_buildout_universe.json / ai_overview_lenses.json source."""
    try:
        from ..api.routes import _build_ai_metadata
        return _build_ai_metadata()
    except Exception as exc:
        logger.warning("mispriced: _build_ai_metadata reuse failed: %s", exc)
        return {}


# ── per-ticker sweep worker ─────────────────────────────────────────────────

def _sweep_ticker(client: httpx.Client, ticker: str, quote: dict | None,
                   earnings_date: date | None) -> tuple[list[dict], int]:
    """Returns (candidate_rows, tradier_calls_made). Never raises — a thin or
    broken ticker contributes fewer calls and zero rows, the sweep continues."""
    calls = 0
    stock_ask = None
    if quote:
        stock_ask = quote.get("ask")
        if not _is_valid(stock_ask):
            stock_ask = quote.get("last")
    if not _is_valid(stock_ask):
        return [], calls
    stock_ask = float(stock_ask)

    try:
        exps = _fetch_expirations(client, ticker)
        calls += 1
    except Exception as exc:
        logger.debug("mispriced: %s expirations failed: %s", ticker, exc)
        return [], calls

    fridays = sorted(d for d in exps if datetime.strptime(d, "%Y-%m-%d").weekday() == 4)
    chosen = fridays[:2]  # 0, 1, or 2 — thin names never error the sweep

    out_rows: list[dict] = []
    today = date.today()
    for exp_str in chosen:
        try:
            chain = _fetch_chain(client, ticker, exp_str)
            calls += 1
        except Exception as exc:
            logger.debug("mispriced: %s chain %s failed: %s", ticker, exp_str, exc)
            continue

        exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date()
        calls_only = [
            o for o in chain
            if o.get("option_type") == "call" and _is_valid(o.get("strike"))
            and float(o["strike"]) <= stock_ask
        ]
        calls_only.sort(key=lambda o: float(o["strike"]), reverse=True)
        deep_itm = calls_only[:STRIKES_BELOW_SPOT]

        earn_in_window = earnings_date is not None and today <= earnings_date <= exp_date

        for idx, o in enumerate(deep_itm):
            call_bid = o.get("bid")
            if not _is_valid(call_bid) or float(call_bid) <= 0:
                continue
            call_bid = float(call_bid)
            strike = float(o["strike"])
            # Position within deep_itm (which is calls_only, already sorted
            # closest-to-spot-first) IS the strike count below spot for this
            # name's actual listed spacing -- idx 0 (closest to/at spot) = 1.
            # Computed from the enumerate() index, not from the post-filter
            # out_rows list, so a skipped zero-bid strike above this one
            # doesn't shift this strike's depth.
            depth_strikes = idx + 1
            depth_dollars = round(stock_ask - strike, 2)

            call_ask_raw = o.get("ask")
            call_ask = float(call_ask_raw) if _is_valid(call_ask_raw) and float(call_ask_raw) > 0 else None

            edge_bid = (strike + call_bid - stock_ask) * 100.0
            edge_ask = (strike + call_ask - stock_ask) * 100.0 if call_ask is not None else None
            edge_mid = (
                (strike + (call_bid + call_ask) / 2.0 - stock_ask) * 100.0
                if call_ask is not None else None
            )

            out_rows.append({
                "ticker": ticker,
                "expiration": exp_str,
                "strike": strike,
                "call_bid": round(call_bid, 2),
                "call_ask": round(call_ask, 2) if call_ask is not None else None,
                "stock_price": round(stock_ask, 2),
                "depth_strikes": depth_strikes,
                "depth_dollars": depth_dollars,
                "edge_bid": round(edge_bid, 2),
                "edge_ask": round(edge_ask, 2) if edge_ask is not None else None,
                "edge_mid": round(edge_mid, 2) if edge_mid is not None else None,
                "breakeven": round(stock_ask - call_bid, 2),
                "earnings_in_window": earn_in_window,
            })

    return out_rows, calls


# ── full sweep ───────────────────────────────────────────────────────────────

def run_sweep() -> dict:
    """Runs one full sweep synchronously. Called from a request handler
    (manual trigger, toggle-on immediate fire) or the cadence loop."""
    with _lock:
        if _state["sweep_in_progress"]:
            return get_state()
        _state["sweep_in_progress"] = True
        _state["last_error"] = None
        floor = _state["floor"]
        prev_known_keys = set(_state["known_keys"])

    t0 = time.time()
    total_calls = 0
    try:
        tickers = _load_universe()
        earnings = _load_earnings(tickers)
        sitrep_set = _load_sitrep_set(tickers)
        ai_meta = _load_ai_metadata()

        with httpx.Client(timeout=15) as client:
            quotes: dict[str, dict] = {}
            for i in range(0, len(tickers), QUOTE_BATCH):
                batch = tickers[i:i + QUOTE_BATCH]
                try:
                    quotes.update(_fetch_quotes_batch(client, batch))
                    total_calls += 1
                except Exception as exc:
                    logger.warning("mispriced: quote batch %s failed: %s", i, exc)

            all_rows: list[dict] = []
            with ThreadPoolExecutor(max_workers=SWEEP_WORKERS) as pool:
                futures = {
                    pool.submit(_sweep_ticker, client, t, quotes.get(t), earnings.get(t)): t
                    for t in tickers
                }
                for fut in as_completed(futures):
                    t = futures[fut]
                    try:
                        rows, calls = fut.result()
                        all_rows.extend(rows)
                        total_calls += calls
                    except Exception as exc:
                        logger.warning("mispriced: sweep failed for %s: %s", t, exc)

        # Loosest bound (edge_ask, the max of the three) so the superset sent
        # to the client covers every row that could qualify under ANY of the
        # frontend's edge-mode toggle positions (ask/bid/mid/range) against
        # the current floor -- the toggle recomputes client-side from data
        # already here, it never triggers a new sweep, so nothing the toggle
        # might need can be pre-filtered away by a single fixed criterion.
        def _loose_edge(r):
            return r["edge_ask"] if r["edge_ask"] is not None else r["edge_bid"]

        qualifying = [r for r in all_rows if _loose_edge(r) >= floor]
        qualifying.sort(key=_loose_edge, reverse=True)

        # Same universe-metadata source the main scanner's purple pills and
        # research asterisk use — reused, not reimplemented.
        for r in qualifying:
            meta = ai_meta.get(r["ticker"]) or {}
            r["has_sitrep"] = r["ticker"] in sitrep_set
            r["primary_lens"] = meta.get("primary_lens")
            r["lenses"] = meta.get("lenses") or []
            r["category"] = meta.get("category")
            r["is_defense"] = meta.get("primary_lens") == "Defense & Aerospace"

        current_keys = {(r["ticker"], r["expiration"], r["strike"]) for r in qualifying}
        for r in qualifying:
            key = (r["ticker"], r["expiration"], r["strike"])
            r["is_new"] = key not in prev_known_keys

        with _lock:
            _state["rows"] = qualifying
            _state["known_keys"] = current_keys
            _state["last_swept_at"] = datetime.utcnow().isoformat() + "Z"
            _state["universe_size"] = len(tickers)
            _state["last_call_count"] = total_calls
            _state["last_sweep_seconds"] = round(time.time() - t0, 1)

        logger.info(
            "mispriced sweep: %d tickers, %d calls, %.1fs, %d qualifying (floor=%.0f)",
            len(tickers), total_calls, time.time() - t0, len(qualifying), floor,
        )
    except Exception as exc:
        logger.exception("mispriced sweep failed: %s", exc)
        with _lock:
            _state["last_error"] = str(exc)
    finally:
        with _lock:
            _state["sweep_in_progress"] = False

    return get_state()


# ── cadence loop — background thread, NOT the Railway/APScheduler cron ─────

_loop_started = False
_loop_lock = threading.Lock()
_fired_today: set[str] = set()
_fired_date: date | None = None


def _cadence_loop() -> None:
    global _fired_date
    while True:
        try:
            now_ct = datetime.now(_CT)
            today = now_ct.date()
            if _fired_date != today:
                _fired_today.clear()
                _fired_date = today

            with _lock:
                on = _state["toggle_on"]
                in_progress = _state["sweep_in_progress"]

            # Market-hours gate: even with the toggle ON, the cadence loop
            # only fires Mon-Fri 8:30 AM-3:00 PM CT. Outside that window this
            # is a no-op every 15s poll — zero Tradier calls, toggle just
            # sits armed until the next in-hours slot.
            if on and not in_progress and _within_market_hours(now_ct):
                hhmm = now_ct.strftime("%H:%M")
                if hhmm in SWEEP_TIMES_CT and hhmm not in _fired_today:
                    _fired_today.add(hhmm)
                    run_sweep()
        except Exception:
            logger.exception("mispriced cadence loop error")
        time.sleep(15)


def start_cadence_loop() -> None:
    """Idempotent, safe to call at app startup. The loop is a no-op while the
    toggle is off (default) — zero Tradier calls, nothing running."""
    global _loop_started
    with _loop_lock:
        if _loop_started:
            return
        _loop_started = True
    threading.Thread(target=_cadence_loop, name="mispriced-cadence", daemon=True).start()
    logger.info("mispriced: cadence loop started (idle — toggle is off by default)")
