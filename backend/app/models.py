"""Database models."""
from __future__ import annotations

import bcrypt
from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime, Float, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .database import Base


class ScanRun(Base):
    __tablename__ = "scan_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    tickers_total: Mapped[int] = mapped_column(Integer, default=0)
    tickers_scanned: Mapped[int] = mapped_column(Integer, default=0)
    tickers_errored: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(32), default="running")
    scan_slot: Mapped[str | None] = mapped_column(String(16), nullable=True)  # "am" | "pm" | "manual"
    scoring_config: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON snapshot of thresholds
    scan_type: Mapped[str | None] = mapped_column(String(20), nullable=True, default="full")  # "full" | "positions"


class ScanResult(Base):
    __tablename__ = "scan_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int] = mapped_column(Integer, index=True)
    ticker: Mapped[str] = mapped_column(String(16), index=True)

    price: Mapped[float | None] = mapped_column(Float, nullable=True)
    iv_rank: Mapped[float | None] = mapped_column(Float, nullable=True)
    iv: Mapped[float | None] = mapped_column(Float, nullable=True)
    hv: Mapped[float | None] = mapped_column(Float, nullable=True)
    atm_call_premium: Mapped[float | None] = mapped_column(Float, nullable=True)
    premium_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    premium_otm1: Mapped[float | None] = mapped_column(Float, nullable=True)  # 1 OTM call mid, per share
    premium_otm2: Mapped[float | None] = mapped_column(Float, nullable=True)  # 2 OTM call mid, per share
    open_interest: Mapped[int | None] = mapped_column(Integer, nullable=True)
    bid_ask_spread_pct: Mapped[float | None] = mapped_column(Float, nullable=True)

    earnings_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    unusual_volume: Mapped[bool] = mapped_column(default=False)

    score: Mapped[float] = mapped_column(Float, default=0.0)
    score_iv_rank: Mapped[float] = mapped_column(Float, default=0.0)
    score_premium: Mapped[float] = mapped_column(Float, default=0.0)
    score_iv_hv: Mapped[float] = mapped_column(Float, default=0.0)
    score_catalyst: Mapped[float] = mapped_column(Float, default=0.0)
    score_chain: Mapped[float] = mapped_column(Float, default=0.0)

    bucket: Mapped[str] = mapped_column(String(32), index=True, default="watchlist")
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    # SMA indicators (computed from 1yr price history)
    sma_200: Mapped[float | None] = mapped_column(Float, nullable=True)
    sma_50: Mapped[float | None] = mapped_column(Float, nullable=True)
    price_vs_sma200_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    price_vs_sma50_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    sma_regime: Mapped[str | None] = mapped_column(String(32), nullable=True)  # UPTREND/DOWNTREND/TRANSITIONAL
    sma_golden_cross: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    # Auto-detected support / resistance zones
    support_1: Mapped[float | None] = mapped_column(Float, nullable=True)
    support_1_strength: Mapped[float | None] = mapped_column(Float, nullable=True)
    support_2: Mapped[float | None] = mapped_column(Float, nullable=True)
    support_2_strength: Mapped[float | None] = mapped_column(Float, nullable=True)
    resistance_1: Mapped[float | None] = mapped_column(Float, nullable=True)
    resistance_1_strength: Mapped[float | None] = mapped_column(Float, nullable=True)
    resistance_2: Mapped[float | None] = mapped_column(Float, nullable=True)
    resistance_2_strength: Mapped[float | None] = mapped_column(Float, nullable=True)

    safety_score: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Multi-expiry premium data
    best_expiry: Mapped[str | None] = mapped_column(Text, nullable=True)   # e.g. "2025-05-01"
    best_dte: Mapped[int | None] = mapped_column(Integer, nullable=True)
    best_strike: Mapped[float | None] = mapped_column(Float, nullable=True)  # ATM strike for best expiry
    expiry_data: Mapped[str | None] = mapped_column(Text, nullable=True)    # JSON list of ExpiryRow

    # Company name from Tradier quote description field
    company_name: Mapped[str | None] = mapped_column(String(128), nullable=True)

    # ATM put premium (stored from normal scan — same expiry as best_expiry)
    atm_put_premium: Mapped[float | None] = mapped_column(Float, nullable=True)
    best_put_strike: Mapped[float | None] = mapped_column(Float, nullable=True)
    best_put_expiry: Mapped[str | None] = mapped_column(Text, nullable=True)
    best_put_dte: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # CC / CSP attractiveness scores (0-100)
    cc_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    csp_score: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # IV ramp detection (computed from iv_history)
    iv_5d_ago: Mapped[float | None] = mapped_column(Float, nullable=True)
    iv_10d_ago: Mapped[float | None] = mapped_column(Float, nullable=True)
    iv_20d_ago: Mapped[float | None] = mapped_column(Float, nullable=True)
    iv_velocity_5d: Mapped[float | None] = mapped_column(Float, nullable=True)
    iv_velocity_10d: Mapped[float | None] = mapped_column(Float, nullable=True)
    iv_velocity_20d: Mapped[float | None] = mapped_column(Float, nullable=True)
    iv_ramp_score: Mapped[int] = mapped_column(Integer, default=0)
    iv_ramp_flag: Mapped[bool] = mapped_column(Boolean, default=False)

    # Phase 1 Risk Quality data layer
    ema_20: Mapped[float | None] = mapped_column(Float, nullable=True)
    ema_50: Mapped[float | None] = mapped_column(Float, nullable=True)
    distribution_days_25: Mapped[int | None] = mapped_column(Integer, nullable=True)
    iv_25d_put: Mapped[float | None] = mapped_column(Float, nullable=True)   # IV at 25-delta put
    iv_25d_call: Mapped[float | None] = mapped_column(Float, nullable=True)  # IV at 25-delta call
    put_skew: Mapped[float | None] = mapped_column(Float, nullable=True)     # iv_25d_put - iv_25d_call
    iv_30d_put: Mapped[float | None] = mapped_column(Float, nullable=True)   # IV at 30-delta put
    iv_30d_call: Mapped[float | None] = mapped_column(Float, nullable=True)  # IV at 30-delta call
    put_skew_30d: Mapped[float | None] = mapped_column(Float, nullable=True) # iv_30d_put - iv_30d_call
    iv_front_month: Mapped[float | None] = mapped_column(Float, nullable=True)  # ATM IV, nearest expiry
    iv_back_month: Mapped[float | None] = mapped_column(Float, nullable=True)   # ATM IV, second expiry
    term_structure: Mapped[float | None] = mapped_column(Float, nullable=True)  # iv_back - iv_front (+= contango)

    # Phase 2 Risk Quality scoring engine
    risk_grade: Mapped[str | None] = mapped_column(String(1), nullable=True)
    vrp_state: Mapped[str | None] = mapped_column(String(10), nullable=True)
    vrp_spread: Mapped[float | None] = mapped_column(Float, nullable=True)
    realized_vol_20d: Mapped[float | None] = mapped_column(Float, nullable=True)
    realized_vol_60d: Mapped[float | None] = mapped_column(Float, nullable=True)  # Phase 3 addition
    strategy_type: Mapped[str | None] = mapped_column(String(20), nullable=True)
    secondary_edge: Mapped[str | None] = mapped_column(Text, nullable=True)       # JSON list
    hard_fail_reasons: Mapped[str | None] = mapped_column(Text, nullable=True)    # JSON list
    strong_fail_reasons: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON list
    soft_fail_count: Mapped[float | None] = mapped_column(Float, nullable=True)
    soft_fail_details: Mapped[str | None] = mapped_column(Text, nullable=True)    # JSON list
    event_ramp_eligible: Mapped[bool] = mapped_column(Boolean, default=False)
    technical_location_eligible: Mapped[bool] = mapped_column(Boolean, default=False)
    income_grind_eligible: Mapped[bool] = mapped_column(Boolean, default=False)
    high_beta_moderate: Mapped[bool] = mapped_column(Boolean, default=False)  # Phase 3 addition
    extension_ratio: Mapped[float | None] = mapped_column(Float, nullable=True)  # Factor 9: price / ema_50

    # Asymmetric setup flags (computed during scan from convergence of multiple criteria)
    asymmetric_cc_flag: Mapped[bool] = mapped_column(Boolean, default=False)
    asymmetric_csp_flag: Mapped[bool] = mapped_column(Boolean, default=False)
    asymmetric_ivramp_flag: Mapped[bool] = mapped_column(Boolean, default=False)
    asymmetric_any_flag: Mapped[bool] = mapped_column(Boolean, default=False)
    asymmetric_type: Mapped[str | None] = mapped_column(String(32), nullable=True)

    # LEAPS flag — True when this row was produced by the on-demand LEAPS scan (180-365 DTE)
    is_leaps: Mapped[bool] = mapped_column(Boolean, default=False)

    # Probability fields — computed during scan from backtest_results
    path_safety_grade: Mapped[str | None] = mapped_column(String(2), nullable=True)
    prob_assign_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    prob_mae: Mapped[float | None] = mapped_column(Float, nullable=True)
    prob_exit_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    prob_regime: Mapped[str | None] = mapped_column(String(50), nullable=True)
    prob_n: Mapped[int | None] = mapped_column(Integer, nullable=True)
    prob_confidence: Mapped[str | None] = mapped_column(String(20), nullable=True)

    # Regime Flag fields
    regime_flag: Mapped[str | None] = mapped_column(String(20), nullable=True)
    regime_backwardated: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    regime_systemic: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    regime_catalyst_clear: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    regime_volume_elevated: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    term_structure_ratio: Mapped[float | None] = mapped_column(Float, nullable=True)
    spy_daily_change: Mapped[float | None] = mapped_column(Float, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


Index("ix_scan_results_run_bucket", ScanResult.run_id, ScanResult.bucket)
Index("ix_scan_results_ticker_created", ScanResult.ticker, ScanResult.created_at)


class TickerUniverse(Base):
    __tablename__ = "ticker_universe"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ticker: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("ticker", "source", name="uq_ticker_universe_ticker_source"),
    )


class EarningsCalendar(Base):
    """One row per ticker — stores the next upcoming earnings date.

    Populated/refreshed by the finnhub_earnings service (≤24h TTL).
    """
    __tablename__ = "earnings_calendar"

    ticker: Mapped[str] = mapped_column(String(16), primary_key=True)
    next_earnings_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class IvHistory(Base):
    __tablename__ = "iv_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ticker: Mapped[str] = mapped_column(String(16), index=True)
    iv: Mapped[float] = mapped_column(Float)
    recorded_date: Mapped[date] = mapped_column(Date, index=True)

    __table_args__ = (UniqueConstraint("ticker", "recorded_date", name="uq_iv_history_ticker_date"),)


class UserPosition(Base):
    """Tracks user-managed positions for the My Positions quick-scan view."""
    __tablename__ = "user_positions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ticker: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    position_type: Mapped[str | None] = mapped_column(String(10), nullable=True)  # CSP / CC / SHARES
    entry_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    entry_date: Mapped[str | None] = mapped_column(String(20), nullable=True)   # ISO date string
    contracts: Mapped[int | None] = mapped_column(Integer, nullable=True)
    strike: Mapped[float | None] = mapped_column(Float, nullable=True)
    expiry: Mapped[str | None] = mapped_column(String(20), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    profit_target_pct: Mapped[float | None] = mapped_column(Float, nullable=True, default=50.0)
    entry_regime: Mapped[str | None] = mapped_column(String(60), nullable=True)
    entry_assign_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    entry_mae: Mapped[float | None] = mapped_column(Float, nullable=True)
    entry_exit_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    entry_grade: Mapped[str | None] = mapped_column(String(4), nullable=True)
    entry_underlying_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    last_alert_type: Mapped[str | None] = mapped_column(String(30), nullable=True)
    last_alert_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    manual_status_override_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    current_mark: Mapped[float | None] = mapped_column(Float, nullable=True)
    owner: Mapped[str] = mapped_column(String(10), nullable=False, default="drew", server_default="drew")


class OptionSnapshot(Base):
    """Daily snapshot of option chain data captured during each scan.

    Only puts within 5% of the current price are stored, for the nearest
    2-3 expiration dates.  One snapshot set per ticker per day (dedup enforced
    at write time in the scanner).
    """
    __tablename__ = "option_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    scan_date: Mapped[date] = mapped_column(Date, index=True)
    scan_timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    ticker: Mapped[str] = mapped_column(String(10), index=True)
    expiration: Mapped[date] = mapped_column(Date)
    strike: Mapped[float] = mapped_column(Float)
    option_type: Mapped[str] = mapped_column(String(4))  # "put" or "call"
    bid: Mapped[float | None] = mapped_column(Float, nullable=True)
    ask: Mapped[float | None] = mapped_column(Float, nullable=True)
    mark: Mapped[float | None] = mapped_column(Float, nullable=True)
    iv: Mapped[float | None] = mapped_column(Float, nullable=True)
    delta: Mapped[float | None] = mapped_column(Float, nullable=True)
    theta: Mapped[float | None] = mapped_column(Float, nullable=True)
    gamma: Mapped[float | None] = mapped_column(Float, nullable=True)
    volume: Mapped[int | None] = mapped_column(Integer, nullable=True)
    open_interest: Mapped[int | None] = mapped_column(Integer, nullable=True)

    __table_args__ = (
        Index("ix_option_snapshots_ticker_date", "ticker", "scan_date"),
    )


class User(Base):
    """Application user — used for session-based authentication."""
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    username: Mapped[str] = mapped_column(String(80), unique=True, nullable=False, index=True)
    password_hash: Mapped[str] = mapped_column(String(128), nullable=False)

    def set_password(self, password: str) -> None:
        """Hash and store password using bcrypt."""
        hashed = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt())
        self.password_hash = hashed.decode("utf-8")

    def check_password(self, password: str) -> bool:
        """Verify a plaintext password against the stored bcrypt hash."""
        return bcrypt.checkpw(password.encode("utf-8"), self.password_hash.encode("utf-8"))
