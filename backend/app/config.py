"""Environment config loader.

Railway sometimes passes env values with leading '=' or surrounding whitespace,
so every variable is cleaned with lstrip('=').strip() before use. The Postgres
URL is also normalized from the legacy ``postgres://`` scheme to ``postgresql://``
so SQLAlchemy 2.x accepts it.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _clean(value: str | None) -> str:
    if value is None:
        return ""
    return value.lstrip("=").strip()


def _normalize_db_url(url: str) -> str:
    if not url:
        return "sqlite:///./casino_scanner.db"
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://") :]
    return url


@dataclass(frozen=True)
class Settings:
    database_url: str
    alert_email: str
    alert_sms: str
    environment: str
    cors_origins: list[str]
    tradier_api_key: str


def load_settings() -> Settings:
    raw_db = _clean(os.environ.get("DATABASE_URL"))
    database_url = _normalize_db_url(raw_db)

    alert_email = _clean(os.environ.get("ALERT_EMAIL")) or "drewreb17@gmail.com"
    alert_sms = _clean(os.environ.get("ALERT_SMS")) or "8062395470@txt.att.net"
    environment = _clean(os.environ.get("ENVIRONMENT")) or "development"

    origins_raw = _clean(os.environ.get("CORS_ORIGINS"))
    if origins_raw:
        cors_origins = [o.strip() for o in origins_raw.split(",") if o.strip()]
    else:
        cors_origins = ["*"]

    tradier_api_key = _clean(os.environ.get("TRADIER_API_KEY"))

    return Settings(
        database_url=database_url,
        alert_email=alert_email,
        alert_sms=alert_sms,
        environment=environment,
        cors_origins=cors_origins,
        tradier_api_key=tradier_api_key,
    )


settings = load_settings()
