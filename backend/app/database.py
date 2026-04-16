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


def _add_column_if_missing(ddl: str) -> None:
    from sqlalchemy import text

    with engine.connect() as conn:
        try:
            conn.execute(text(ddl))
            conn.commit()
        except Exception:
            # Column already exists — safe to ignore
            conn.rollback()
