"""
seed_user.py — Create or update a Casino Scanner user.

Usage:
    cd backend && python -m app.scripts.seed_user <username> <password>

Example:
    cd backend && python -m app.scripts.seed_user drew mysecretpassword

Passwords are NEVER stored in plaintext — bcrypt hash only.
Safe to re-run: updates the password if the user already exists.
"""
from __future__ import annotations

import sys
from pathlib import Path

_BACKEND_DIR = Path(__file__).resolve().parent.parent.parent
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.config import settings
from app.database import Base
from app.models import User


def seed_user(username: str, password: str) -> None:
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    # Ensure the users table exists
    Base.metadata.create_all(bind=engine, tables=[User.__table__])

    Session = sessionmaker(bind=engine)
    db = Session()
    try:
        username = username.strip().lower()
        existing = db.execute(
            select(User).where(User.username == username)
        ).scalar_one_or_none()

        if existing:
            existing.set_password(password)
            db.commit()
            print(f"Updated password for user '{username}'.")
        else:
            user = User(username=username)
            user.set_password(password)
            db.add(user)
            db.commit()
            print(f"Created user '{username}' (id={user.id}).")
    finally:
        db.close()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python -m app.scripts.seed_user <username> <password>")
        sys.exit(1)

    _, uname, pwd = sys.argv

    if len(pwd) < 8:
        print("Error: password must be at least 8 characters.")
        sys.exit(1)

    seed_user(uname, pwd)
