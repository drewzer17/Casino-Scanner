"""
seed_user.py — Create or update a Casino Scanner user.

Uses psycopg2 directly to avoid importing models.py (which uses
Python 3.10+ X | None union syntax incompatible with local Python 3.9).

Usage:
    cd backend && python -m app.scripts.seed_user <username> <password>

Example:
    cd backend && python -m app.scripts.seed_user drew mysecretpassword

Passwords are NEVER stored in plaintext — bcrypt hash only.
Safe to re-run: updates the password if the user already exists.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

_BACKEND_DIR = Path(__file__).resolve().parent.parent.parent
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))

import bcrypt
import psycopg2

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:uIcMzUUNlqmhekvgoKBcQxRIQOoajQyu@nozomi.proxy.rlwy.net:46336/railway",
)


def _ensure_users_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id            SERIAL PRIMARY KEY,
                username      VARCHAR(80) UNIQUE NOT NULL,
                password_hash VARCHAR(128) NOT NULL
            )
        """)
    conn.commit()


def seed_user(username: str, password: str) -> None:
    username = username.strip().lower()
    hashed = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

    conn = psycopg2.connect(DATABASE_URL)
    try:
        _ensure_users_table(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM users WHERE username = %s", (username,))
            row = cur.fetchone()
            if row:
                cur.execute(
                    "UPDATE users SET password_hash = %s WHERE username = %s",
                    (hashed, username),
                )
                conn.commit()
                print(f"Updated password for user '{username}'.")
            else:
                cur.execute(
                    "INSERT INTO users (username, password_hash) VALUES (%s, %s) RETURNING id",
                    (username, hashed),
                )
                user_id = cur.fetchone()[0]
                conn.commit()
                print(f"Created user '{username}' (id={user_id}).")
    finally:
        conn.close()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python -m app.scripts.seed_user <username> <password>")
        sys.exit(1)

    _, uname, pwd = sys.argv

    if len(pwd) < 8:
        print("Error: password must be at least 8 characters.")
        sys.exit(1)

    seed_user(uname, pwd)
