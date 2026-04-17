"""Database models."""
from __future__ import annotations

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


class IvHistory(Base):
    __tablename__ = "iv_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ticker: Mapped[str] = mapped_column(String(16), index=True)
    iv: Mapped[float] = mapped_column(Float)
    recorded_date: Mapped[date] = mapped_column(Date, index=True)

    __table_args__ = (UniqueConstraint("ticker", "recorded_date", name="uq_iv_history_ticker_date"),)
