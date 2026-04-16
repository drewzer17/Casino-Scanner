"""Database models."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Float, Index, Integer, String, Text
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

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


Index("ix_scan_results_run_bucket", ScanResult.run_id, ScanResult.bucket)
Index("ix_scan_results_ticker_created", ScanResult.ticker, ScanResult.created_at)
