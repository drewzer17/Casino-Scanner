"""FastAPI entry point.

Serves the React SPA for all non-API routes.
  - /assets/*  → frontend/dist/assets/  (JS/CSS bundles from Vite)
  - /api/*      → API routers (registered first, take priority)
  - /*          → frontend/dist/index.html  (SPA catch-all)
"""
from __future__ import annotations

import logging
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .api import router as api_router
from .config import settings
from .database import init_db
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

# ── API routes (registered before static mounts so /api/* is never swallowed) ──
app.include_router(api_router)


@app.on_event("startup")
def _on_startup() -> None:
    init_db()
    # Sync ticker universe CSV → DB (idempotent, safe to run on every boot)
    from .database import SessionLocal
    from .universe import sync_universe_from_csv
    _db = SessionLocal()
    try:
        sync_universe_from_csv(_db)
    except Exception as _exc:
        import logging as _logging
        _logging.getLogger(__name__).warning("Universe CSV sync failed on startup: %s", _exc)
    finally:
        _db.close()
    start_scheduler()


@app.on_event("shutdown")
def _on_shutdown() -> None:
    stop_scheduler()


# ── Static asset bundles produced by `vite build` ──────────────────────────────
# Mount only if the dist folder exists (skipped in bare dev without a build).
# Hashed filenames (e.g. index-abc123.js) are immutable — cache forever.
_assets_dir = DIST / "assets"
if _assets_dir.exists():
    app.mount("/assets", StaticFiles(directory=str(_assets_dir)), name="assets")


# ── SPA catch-all ──────────────────────────────────────────────────────────────
# Any path that didn't match /api/* or /assets/* returns index.html so that
# React Router can handle client-side navigation.
# index.html must never be cached — it references hashed asset filenames that
# change on every deploy, so browsers must always fetch a fresh copy.
@app.get("/{full_path:path}", include_in_schema=False, response_model=None)
async def serve_spa(full_path: str) -> FileResponse | dict:
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
