"""HTTP surface for the DITM mispricing scanner. Fully separate from
backend/app/api/routes.py (the main scan API) — its own router, its own prefix.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from ..auth import require_login
from . import engine

router = APIRouter(prefix="/api/mispriced", tags=["mispriced"], dependencies=[Depends(require_login)])


@router.get("/state")
def get_state() -> dict:
    return engine.get_state()


@router.post("/toggle")
def toggle(body: dict) -> dict:
    on = body.get("on")
    if not isinstance(on, bool):
        raise HTTPException(status_code=422, detail="body must include boolean 'on'")
    return engine.set_toggle(on)


@router.post("/floor")
def set_floor(body: dict) -> dict:
    floor = body.get("floor")
    if not isinstance(floor, (int, float)):
        raise HTTPException(status_code=422, detail="body must include numeric 'floor'")
    try:
        return engine.set_floor(float(floor))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


@router.post("/sweep")
def sweep_now() -> dict:
    """Manual trigger — runs one sweep synchronously and returns the result.
    Independent of the toggle; useful for testing without waiting on cadence."""
    return engine.run_sweep()
