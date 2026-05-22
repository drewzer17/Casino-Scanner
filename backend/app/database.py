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

    # Company name column added in v6
    _add_column_if_missing("ALTER TABLE scan_results ADD COLUMN company_name VARCHAR(128)")

    # ATM put premium columns added in v5
    for _ddl in [
        "ALTER TABLE scan_results ADD COLUMN atm_put_premium FLOAT",
        "ALTER TABLE scan_results ADD COLUMN best_put_strike FLOAT",
        "ALTER TABLE scan_results ADD COLUMN best_put_expiry TEXT",
        "ALTER TABLE scan_results ADD COLUMN best_put_dte INTEGER",
    ]:
        _add_column_if_missing(_ddl)

    # CC / CSP attractiveness scores added in v7
    for _ddl in [
        "ALTER TABLE scan_results ADD COLUMN cc_score INTEGER",
        "ALTER TABLE scan_results ADD COLUMN csp_score INTEGER",
    ]:
        _add_column_if_missing(_ddl)

    # IV ramp detection columns added in v8
    for _ddl in [
        "ALTER TABLE scan_results ADD COLUMN iv_5d_ago FLOAT",
        "ALTER TABLE scan_results ADD COLUMN iv_10d_ago FLOAT",
        "ALTER TABLE scan_results ADD COLUMN iv_20d_ago FLOAT",
        "ALTER TABLE scan_results ADD COLUMN iv_velocity_5d FLOAT",
        "ALTER TABLE scan_results ADD COLUMN iv_velocity_10d FLOAT",
        "ALTER TABLE scan_results ADD COLUMN iv_velocity_20d FLOAT",
        "ALTER TABLE scan_results ADD COLUMN iv_ramp_score INTEGER DEFAULT 0",
        "ALTER TABLE scan_results ADD COLUMN iv_ramp_flag BOOLEAN DEFAULT FALSE",
    ]:
        _add_column_if_missing(_ddl)

    # scan_slot column added in v10 (tracks which scheduled slot triggered the run)
    _add_column_if_missing("ALTER TABLE scan_runs ADD COLUMN scan_slot VARCHAR(16)")

    # Phase 1 Risk Quality data layer — v11
    # EMA indicators
    for _ddl in [
        "ALTER TABLE scan_results ADD COLUMN ema_20 FLOAT",
        "ALTER TABLE scan_results ADD COLUMN ema_50 FLOAT",
    ]:
        _add_column_if_missing(_ddl)
    # Distribution day count
    _add_column_if_missing("ALTER TABLE scan_results ADD COLUMN distribution_days_25 INTEGER")
    # 25-delta IV and put skew
    for _ddl in [
        "ALTER TABLE scan_results ADD COLUMN iv_25d_put FLOAT",
        "ALTER TABLE scan_results ADD COLUMN iv_25d_call FLOAT",
        "ALTER TABLE scan_results ADD COLUMN put_skew FLOAT",
    ]:
        _add_column_if_missing(_ddl)
    # Term structure (front vs back month ATM IV)
    for _ddl in [
        "ALTER TABLE scan_results ADD COLUMN iv_front_month FLOAT",
        "ALTER TABLE scan_results ADD COLUMN iv_back_month FLOAT",
        "ALTER TABLE scan_results ADD COLUMN term_structure FLOAT",
    ]:
        _add_column_if_missing(_ddl)

    # Asymmetric setup flags added in v9
    for _ddl in [
        "ALTER TABLE scan_results ADD COLUMN asymmetric_cc_flag BOOLEAN DEFAULT FALSE",
        "ALTER TABLE scan_results ADD COLUMN asymmetric_csp_flag BOOLEAN DEFAULT FALSE",
        "ALTER TABLE scan_results ADD COLUMN asymmetric_ivramp_flag BOOLEAN DEFAULT FALSE",
        "ALTER TABLE scan_results ADD COLUMN asymmetric_any_flag BOOLEAN DEFAULT FALSE",
        "ALTER TABLE scan_results ADD COLUMN asymmetric_type VARCHAR(32)",
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

    # Phase 2 Risk Quality scoring engine — v12
    for _ddl in [
        "ALTER TABLE scan_results ADD COLUMN risk_grade VARCHAR(1)",
        "ALTER TABLE scan_results ADD COLUMN vrp_state VARCHAR(10)",
        "ALTER TABLE scan_results ADD COLUMN vrp_spread FLOAT",
        "ALTER TABLE scan_results ADD COLUMN realized_vol_20d FLOAT",
        "ALTER TABLE scan_results ADD COLUMN strategy_type VARCHAR(20)",
        "ALTER TABLE scan_results ADD COLUMN secondary_edge TEXT",
        "ALTER TABLE scan_results ADD COLUMN hard_fail_reasons TEXT",
        "ALTER TABLE scan_results ADD COLUMN strong_fail_reasons TEXT",
        "ALTER TABLE scan_results ADD COLUMN soft_fail_count FLOAT",
        "ALTER TABLE scan_results ADD COLUMN soft_fail_details TEXT",
        "ALTER TABLE scan_results ADD COLUMN event_ramp_eligible BOOLEAN DEFAULT FALSE",
        "ALTER TABLE scan_results ADD COLUMN technical_location_eligible BOOLEAN DEFAULT FALSE",
        "ALTER TABLE scan_results ADD COLUMN income_grind_eligible BOOLEAN DEFAULT FALSE",
    ]:
        _add_column_if_missing(_ddl)

    # Phase 3 additions — v13
    # realized_vol_60d: 60-day RV for backtesting analysis (not used in grading)
    # high_beta_moderate: informational flag (Moderate VRP + rv20 > 35%)
    # scoring_config: JSON snapshot of thresholds used in each scan run
    for _ddl in [
        "ALTER TABLE scan_results ADD COLUMN realized_vol_60d FLOAT",
        "ALTER TABLE scan_results ADD COLUMN high_beta_moderate BOOLEAN DEFAULT FALSE",
        "ALTER TABLE scan_runs ADD COLUMN scoring_config TEXT",
    ]:
        _add_column_if_missing(_ddl)

    # My Positions + Quick Scan — v14
    # user_positions: user-managed positions for the My Positions view
    # scan_runs.scan_type: distinguishes full scans from positions quick scans
    _add_column_if_missing(
        "CREATE TABLE IF NOT EXISTS user_positions ("
        "id SERIAL PRIMARY KEY, "
        "ticker VARCHAR(16) NOT NULL, "
        "position_type VARCHAR(10), "
        "entry_price FLOAT, "
        "entry_date VARCHAR(20), "
        "contracts INTEGER, "
        "strike FLOAT, "
        "expiry VARCHAR(20), "
        "notes TEXT, "
        "active BOOLEAN NOT NULL DEFAULT TRUE, "
        "created_at TIMESTAMP DEFAULT NOW()"
        ")"
    )
    _add_column_if_missing("ALTER TABLE scan_runs ADD COLUMN scan_type VARCHAR(20) DEFAULT 'full'")

    # LEAPS flag — v15
    # is_leaps = TRUE for rows produced by the on-demand LEAPS scan (180-365 DTE)
    _add_column_if_missing("ALTER TABLE scan_results ADD COLUMN is_leaps BOOLEAN DEFAULT FALSE")

    # Factor 9: CSP Path Risk v1 — v16
    # extension_ratio = price / ema_50 (how far price is above its EMA-50)
    _add_column_if_missing("ALTER TABLE scan_results ADD COLUMN extension_ratio FLOAT")

    # Phase 4: Price history table — v17
    # Daily OHLCV from yfinance backfill; used for technical analysis, backtesting
    _add_column_if_missing(
        "CREATE TABLE IF NOT EXISTS price_history ("
        "id SERIAL PRIMARY KEY, "
        "ticker VARCHAR(20) NOT NULL, "
        "date DATE NOT NULL, "
        "open DOUBLE PRECISION, "
        "high DOUBLE PRECISION, "
        "low DOUBLE PRECISION, "
        "close DOUBLE PRECISION, "
        "adj_close DOUBLE PRECISION, "
        "volume BIGINT, "
        "source VARCHAR(20) DEFAULT 'yfinance', "
        "created_at TIMESTAMP DEFAULT NOW(), "
        "UNIQUE(ticker, date)"
        ")"
    )
    _add_column_if_missing(
        "CREATE INDEX IF NOT EXISTS idx_price_history_ticker_date ON price_history(ticker, date)"
    )
    _add_column_if_missing(
        "CREATE INDEX IF NOT EXISTS idx_price_history_date ON price_history(date)"
    )

    # Phase 4: Backtest tables — v18
    # backtest_runs: one row per replay run (date + parameters)
    # backtest_results: one row per ticker × hold_days × strike_pct
    _add_column_if_missing(
        "CREATE TABLE IF NOT EXISTS backtest_runs ("
        "id SERIAL PRIMARY KEY, "
        "test_date DATE NOT NULL, "
        "hold_days INTEGER[] NOT NULL, "
        "strike_pcts DOUBLE PRECISION[] NOT NULL, "
        "total_tickers INTEGER, "
        "graded_tickers INTEGER, "
        "grade_a INTEGER, "
        "grade_b INTEGER, "
        "grade_c INTEGER, "
        "grade_f INTEGER, "
        "factors_available TEXT DEFAULT 'partial_v1', "
        "parameters JSONB, "
        "created_at TIMESTAMP DEFAULT NOW()"
        ")"
    )
    _add_column_if_missing(
        "CREATE TABLE IF NOT EXISTS backtest_results ("
        "id SERIAL PRIMARY KEY, "
        "run_id INTEGER REFERENCES backtest_runs(id), "
        "ticker VARCHAR(20) NOT NULL, "
        "test_date DATE NOT NULL, "
        "grade VARCHAR(1), "
        "vrp_state VARCHAR(20), "
        "iv_rank DOUBLE PRECISION, "
        "vrp_spread DOUBLE PRECISION, "
        "extension_ratio DOUBLE PRECISION, "
        "distribution_days INTEGER, "
        "trend VARCHAR(10), "
        "available_factors INTEGER, "
        "missing_factors JSONB, "
        "fail_reasons JSONB, "
        "strike DOUBLE PRECISION, "
        "strike_pct DOUBLE PRECISION, "
        "hold_days INTEGER, "
        "touched BOOLEAN, "
        "breached BOOLEAN, "
        "time_to_touch INTEGER, "
        "time_to_breach INTEGER, "
        "mae_pct DOUBLE PRECISION, "
        "mfe_pct DOUBLE PRECISION, "
        "final_close DOUBLE PRECISION, "
        "final_distance_pct DOUBLE PRECISION, "
        "complete BOOLEAN"
        ")"
    )
    _add_column_if_missing(
        "CREATE INDEX IF NOT EXISTS idx_backtest_results_run ON backtest_results(run_id)"
    )
    _add_column_if_missing(
        "CREATE INDEX IF NOT EXISTS idx_backtest_results_grade ON backtest_results(grade)"
    )
    _add_column_if_missing(
        "CREATE INDEX IF NOT EXISTS idx_backtest_results_ticker ON backtest_results(ticker, test_date)"
    )


def _add_column_if_missing(ddl: str) -> None:
    from sqlalchemy import text

    with engine.connect() as conn:
        try:
            conn.execute(text(ddl))
            conn.commit()
        except Exception:
            # Column already exists — safe to ignore
            conn.rollback()
