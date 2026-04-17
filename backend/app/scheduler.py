"""APScheduler background job: daily auto-scan at 4:30 PM CT, weekdays only."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import select

from .database import SessionLocal
from . import models

logger = logging.getLogger(__name__)

_CT = pytz.timezone("America/Chicago")

scheduler = BackgroundScheduler(timezone=_CT)


def _already_scanned_today() -> bool:
    """Return True if a scan already completed today in Central Time."""
    db = SessionLocal()
    try:
        now_ct = datetime.now(_CT)
        today_start_ct = now_ct.replace(hour=0, minute=0, second=0, microsecond=0)
        today_start_utc = today_start_ct.astimezone(pytz.utc).replace(tzinfo=None)

        run = db.execute(
            select(models.ScanRun)
            .where(
                models.ScanRun.status == "completed",
                models.ScanRun.finished_at >= today_start_utc,
            )
            .limit(1)
        ).scalar_one_or_none()
        return run is not None
    finally:
        db.close()


def _daily_scan() -> None:
    """Scheduled job: skip if already scanned today, otherwise run full scan."""
    if _already_scanned_today():
        logger.info("scheduler: scan already completed today — skipping")
        return

    logger.info("scheduler: starting daily auto-scan at 4:30 PM CT")
    db = SessionLocal()
    try:
        from .scanner.engine import run_scan
        run_id = run_scan(db)
        logger.info("scheduler: daily scan finished run_id=%s", run_id)
    except Exception as exc:
        logger.exception("scheduler: daily scan failed: %s", exc)
    finally:
        db.close()


def start_scheduler() -> None:
    """Register the cron job and start the scheduler. Called from main.py startup."""
    scheduler.add_job(
        _daily_scan,
        trigger="cron",
        day_of_week="mon-fri",
        hour=16,
        minute=30,
        timezone=_CT,
        id="daily_scan",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("scheduler: started — daily scan job registered for 4:30 PM CT Mon-Fri")


def stop_scheduler() -> None:
    """Gracefully shut down the scheduler. Called from main.py shutdown."""
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("scheduler: stopped")
