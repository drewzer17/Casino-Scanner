"""
auth.py — Session-based authentication helpers for Casino Scanner.

Uses Starlette's SessionMiddleware (cookie-backed, signed with SECRET_KEY).
Equivalent to Flask-Login's @login_required / current_user pattern, but
implemented as FastAPI Depends() dependencies.

Two variants:
  require_login       — for /api/* routes: raises HTTP 401 (JSON)
  require_login_page  — for HTML page routes: redirects to /login
"""
from __future__ import annotations

from fastapi import Depends, HTTPException, Request, status
from fastapi.responses import RedirectResponse


def require_login(request: Request) -> int:
    """
    FastAPI dependency for JSON API routes.

    Raises HTTP 401 if the session has no authenticated user_id.
    The React app intercepts 401 and redirects to /login.
    """
    user_id = request.session.get("user_id")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
        )
    return int(user_id)


def require_login_page(request: Request):
    """
    FastAPI dependency for HTML page routes.

    Returns a redirect to /login if not authenticated.
    Use as: response = require_login_page(request); if response: return response
    """
    user_id = request.session.get("user_id")
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)
    return None


def get_user_id(request: Request) -> int | None:
    """Return the current user_id from session, or None."""
    uid = request.session.get("user_id")
    return int(uid) if uid else None
