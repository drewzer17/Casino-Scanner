"""
auth_routes.py — Login / logout routes for Casino Scanner.

  GET  /login  → serve login page
  POST /login  → validate credentials, set session
  GET  /logout → clear session, redirect to /login
"""
from __future__ import annotations

import logging
from pathlib import Path

from fastapi import APIRouter, Form, Request, status
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from sqlalchemy import select

from .database import SessionLocal
from .models import User

logger = logging.getLogger(__name__)

auth_router = APIRouter(tags=["auth"])

_TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
_LOGIN_HTML = _TEMPLATES_DIR / "login.html"


@auth_router.get("/login", include_in_schema=False, response_model=None)
async def login_page(request: Request):
    """Serve the login page. If already authenticated, redirect to home."""
    if request.session.get("user_id"):
        return RedirectResponse(url="/", status_code=302)
    return FileResponse(str(_LOGIN_HTML), media_type="text/html")


@auth_router.post("/login", include_in_schema=False, response_model=None)
async def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
):
    """Validate credentials and create a session."""
    db = SessionLocal()
    try:
        user = db.execute(
            select(User).where(User.username == username.strip().lower())
        ).scalar_one_or_none()
    finally:
        db.close()

    if user is None or not user.check_password(password):
        # Re-serve login page with error message embedded in query param
        return RedirectResponse(url="/login?error=1", status_code=302)

    request.session["user_id"] = str(user.id)
    request.session["username"] = user.username
    logger.info("User %s logged in", user.username)
    return RedirectResponse(url="/", status_code=302)


@auth_router.get("/logout", include_in_schema=False, response_model=None)
async def logout(request: Request):
    """Clear session and redirect to login page."""
    username = request.session.get("username", "unknown")
    request.session.clear()
    logger.info("User %s logged out", username)
    return RedirectResponse(url="/login", status_code=302)
