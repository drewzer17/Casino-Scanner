"""FastAPI entry point.

Serves the React SPA for all non-API routes.
  - /assets/*  → frontend/dist/assets/  (JS/CSS bundles from Vite)
  - /api/*      → API routers (registered first, take priority)
  - /*          → frontend/dist/index.html  (SPA catch-all)
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from .api import router as api_router
from .auth import require_login_page
from .auth_routes import auth_router
from .config import settings
from .database import init_db
from .mispriced.routes import router as mispriced_router
from .scheduler import start_scheduler, stop_scheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Repo root is three levels up from this file (backend/app/main.py → backend/app → backend → root)
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DIST = _REPO_ROOT / "frontend" / "dist"

app = FastAPI(title="Casino Scanner", version="0.1.0", docs_url="/api/docs", redoc_url="/api/redoc")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(
    SessionMiddleware,
    secret_key=settings.secret_key,
    session_cookie="cs_session",
    https_only=settings.environment == "production",
    same_site="lax",
)

# ── Auth routes (login/logout — public, registered before API router) ─────────
app.include_router(auth_router)

# ── API routes (registered before static mounts so /api/* is never swallowed) ──
app.include_router(api_router)

# ── DITM mispriced-options scanner — separate, standalone, stores nothing ──────
app.include_router(mispriced_router)


@app.on_event("startup")
def _on_startup() -> None:
    init_db()
    # Sync ticker universe CSV → DB (idempotent, safe to run on every boot)
    from .database import SessionLocal
    from .universe import sync_universe_from_csv, check_ai_universe_drift
    _db = SessionLocal()
    try:
        sync_universe_from_csv(_db)
    except Exception as _exc:
        import logging as _logging
        _logging.getLogger(__name__).warning("Universe CSV sync failed on startup: %s", _exc)
    finally:
        _db.close()
    # Warn if ai_buildout_universe.json and ticker_universe.csv (ai_sector) have drifted
    try:
        check_ai_universe_drift()
    except Exception as _exc:
        import logging as _logging
        _logging.getLogger(__name__).warning("AI universe drift check failed: %s", _exc)
    # Main scan scheduler registers first, unconditionally, so nothing below
    # it can ever prevent the 8:35/3:30/4:00 jobs from being registered.
    start_scheduler()
    # DITM mispriced-options scanner — idle background thread, gated by an
    # in-memory toggle (default off). NOT the APScheduler cron above. Wrapped
    # so a DITM startup failure logs and continues instead of propagating.
    try:
        from .mispriced.engine import start_cadence_loop
        start_cadence_loop()
    except Exception as _exc:
        import logging as _logging
        _logging.getLogger(__name__).warning("DITM cadence loop failed to start: %s", _exc)


@app.on_event("shutdown")
def _on_shutdown() -> None:
    stop_scheduler()


# ── Static asset bundles produced by `vite build` ──────────────────────────────
# Mount only if the dist folder exists (skipped in bare dev without a build).
# Hashed filenames (e.g. index-abc123.js) are immutable — cache forever.
_assets_dir = DIST / "assets"
if _assets_dir.exists():
    app.mount("/assets", StaticFiles(directory=str(_assets_dir)), name="assets")


# ── AI Overview standalone page ────────────────────────────────────────────────
# Must be registered BEFORE the catch-all so /{full_path:path} doesn't swallow it.
_TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"


@app.get("/ai-overview", include_in_schema=False, response_model=None)
async def serve_ai_overview(request: Request) -> FileResponse | dict:
    redirect = require_login_page(request)
    if redirect:
        return redirect
    html = _TEMPLATES_DIR / "ai_overview.html"
    if html.exists():
        return FileResponse(
            str(html),
            media_type="text/html",
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )
    return {"error": "AI overview template not found"}


# ── AI Overview ticker detail pages ───────────────────────────────────────────
# Registered BEFORE the catch-all.  Decides detail vs missing vs 404.
_AI_UNIVERSE_PATH = _REPO_ROOT / "backend" / "data" / "ai_buildout_universe.json"


@app.get("/ai-overview/{ticker}", include_in_schema=False, response_model=None)
async def serve_ai_sitrep(ticker: str, request: Request) -> FileResponse:
    redirect = require_login_page(request)
    if redirect:
        return redirect
    ticker = ticker.upper()

    # Load universe to validate the ticker is known
    if not _AI_UNIVERSE_PATH.exists():
        raise HTTPException(status_code=404, detail="Universe data not found")
    with open(_AI_UNIVERSE_PATH) as f:
        universe = json.load(f)
    known = {t["ticker"] for t in universe.get("tickers", [])}
    if ticker not in known:
        raise HTTPException(status_code=404, detail=f"Ticker {ticker} not in AI universe")

    # Check whether a sitrep exists in the DB
    from .database import SessionLocal
    from sqlalchemy import text as _text
    db = SessionLocal()
    try:
        row = db.execute(_text("SELECT 1 FROM sitreps WHERE ticker = :t"), {"t": ticker}).fetchone()
        has_sitrep = row is not None
    except Exception:
        has_sitrep = False
    finally:
        db.close()

    template = "ai_sitrep_detail.html" if has_sitrep else "ai_sitrep_missing.html"
    html = _TEMPLATES_DIR / template
    if not html.exists():
        raise HTTPException(status_code=500, detail=f"Template {template} not found")
    return FileResponse(
        str(html),
        media_type="text/html",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


# ── SPA catch-all ──────────────────────────────────────────────────────────────
# Any path that didn't match /api/* or /assets/* returns index.html so that
# React Router can handle client-side navigation.
# index.html must never be cached — it references hashed asset filenames that
# change on every deploy, so browsers must always fetch a fresh copy.
@app.get("/{full_path:path}", include_in_schema=False, response_model=None)
async def serve_spa(full_path: str, request: Request) -> FileResponse | dict:
    redirect = require_login_page(request)
    if redirect:
        return redirect
    index = DIST / "index.html"
    if index.exists():
        return FileResponse(
            str(index),
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )
    # Fallback when running backend without a frontend build (local dev / CI)
    return {"app": "Casino Scanner", "status": "frontend not built", "env": settings.environment}
