"""SQLAlchemy engine + session factory."""
from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from .config import settings


connect_args: dict = {}
if settings.database_url.startswith("sqlite"):
    connect_args = {"check_same_thread": False}

engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,
    future=True,
    connect_args=connect_args,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


class Base(DeclarativeBase):
    pass


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db() -> None:
    from . import models  # noqa: F401  ensure models are imported
    from sqlalchemy import text

    Base.metadata.create_all(bind=engine)

    # Idempotent migrations: add columns introduced after initial deploy.
    # create_all() only creates missing *tables*, not missing *columns*, so we
    # ALTER TABLE manually and swallow the error if the column already exists.
    _add_column_if_missing(
        "ALTER TABLE scan_runs ADD COLUMN tickers_total INTEGER DEFAULT 0"
    )
    # OTM premium columns added in v3
    for _ddl in [
        "ALTER TABLE scan_results ADD COLUMN premium_otm1 FLOAT",
        "ALTER TABLE scan_results ADD COLUMN premium_otm2 FLOAT",
    ]:
        _add_column_if_missing(_ddl)
    _add_column_if_missing("ALTER TABLE scan_results ADD COLUMN safety_score FLOAT")
    # Multi-expiry columns added in v4
    for _ddl in [
        "ALTER TABLE scan_results ADD COLUMN best_expiry TEXT",
        "ALTER TABLE scan_results ADD COLUMN best_dte INTEGER",
        "ALTER TABLE scan_results ADD COLUMN best_strike FLOAT",
        "ALTER TABLE scan_results ADD COLUMN expiry_data TEXT",
    ]:
        _add_column_if_missing(_ddl)
    # iv_history table for real IV rank calculation (one row per ticker per day)
    _add_column_if_missing(
        "CREATE TABLE IF NOT EXISTS iv_history ("
        "id SERIAL PRIMARY KEY, "
        "ticker VARCHAR(16) NOT NULL, "
        "iv FLOAT NOT NULL, "
        "recorded_date DATE NOT NULL, "
        "CONSTRAINT uq_iv_history_ticker_date UNIQUE (ticker, recorded_date)"
        ")"
    )
    # ticker_universe table — CSV-driven source-of-truth for the scan universe
    _add_column_if_missing(
        "CREATE TABLE IF NOT EXISTS ticker_universe ("
        "id SERIAL PRIMARY KEY, "
        "ticker VARCHAR(16) NOT NULL, "
        "source VARCHAR(32) NOT NULL, "
        "active BOOLEAN NOT NULL DEFAULT TRUE, "
        "created_at TIMESTAMP DEFAULT NOW(), "
        "CONSTRAINT uq_ticker_universe_ticker_source UNIQUE (ticker, source)"
        ")"
    )

    # ATM put premium columns added in v5
    for _ddl in [
        "ALTER TABLE scan_results ADD COLUMN atm_put_premium FLOAT",
        "ALTER TABLE scan_results ADD COLUMN best_put_strike FLOAT",
        "ALTER TABLE scan_results ADD COLUMN best_put_expiry TEXT",
        "ALTER TABLE scan_results ADD COLUMN best_put_dte INTEGER",
    ]:
        _add_column_if_missing(_ddl)

    # SMA + S/R columns added in v2
    for _ddl in [
        "ALTER TABLE scan_results ADD COLUMN sma_200 FLOAT",
        "ALTER TABLE scan_results ADD COLUMN sma_50 FLOAT",
        "ALTER TABLE scan_results ADD COLUMN price_vs_sma200_pct FLOAT",
        "ALTER TABLE scan_results ADD COLUMN price_vs_sma50_pct FLOAT",
        "ALTER TABLE scan_results ADD COLUMN sma_regime VARCHAR(32)",
        "ALTER TABLE scan_results ADD COLUMN sma_golden_cross BOOLEAN",
        "ALTER TABLE scan_results ADD COLUMN support_1 FLOAT",
        "ALTER TABLE scan_results ADD COLUMN support_1_strength FLOAT",
        "ALTER TABLE scan_results ADD COLUMN support_2 FLOAT",
        "ALTER TABLE scan_results ADD COLUMN support_2_strength FLOAT",
        "ALTER TABLE scan_results ADD COLUMN resistance_1 FLOAT",
        "ALTER TABLE scan_results ADD COLUMN resistance_1_strength FLOAT",
        "ALTER TABLE scan_results ADD COLUMN resistance_2 FLOAT",
        "ALTER TABLE scan_results ADD COLUMN resistance_2_strength FLOAT",
    ]:
        _add_column_if_missing(_ddl)


def _add_column_if_missing(ddl: str) -> None:
    from sqlalchemy import text

    with engine.connect() as conn:
        try:
            conn.execute(text(ddl))
            conn.commit()
        except Exception:
            # Column already exists — safe to ignore
            conn.rollback()
