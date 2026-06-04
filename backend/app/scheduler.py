"""APScheduler background jobs: Extensive Scan at 8:35 AM and 3:30 PM CT, weekdays."""
from __future__ import annotations

import logging
from datetime import datetime

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import select

from .database import SessionLocal
from . import models

logger = logging.getLogger(__name__)

_CT = pytz.timezone("America/Chicago")

scheduler = BackgroundScheduler(timezone=_CT)


def _already_scanned_in_slot(slot: str) -> bool:
    """Return True if a scan with the given slot already completed today (CT)."""
    db = SessionLocal()
    try:
        now_ct = datetime.now(_CT)
        today_start_ct = now_ct.replace(hour=0, minute=0, second=0, microsecond=0)
        today_start_utc = today_start_ct.astimezone(pytz.utc).replace(tzinfo=None)

        run = db.execute(
            select(models.ScanRun)
            .where(
                models.ScanRun.status == "completed",
                models.ScanRun.scan_slot == slot,
                models.ScanRun.finished_at >= today_start_utc,
            )
            .limit(1)
        ).scalar_one_or_none()
        return run is not None
    finally:
        db.close()


def _run_extensive_scan(slot: str) -> None:
    """Core logic shared by both scheduled jobs."""
    if _already_scanned_in_slot(slot):
        logger.info("scheduler: slot=%s already completed today — skipping", slot)
        return

    logger.info("scheduler: starting extensive scan slot=%s", slot)
    db = SessionLocal()
    try:
        from .scanner.engine import run_scan_extensive
        run_id = run_scan_extensive(db, scan_slot=slot)
        logger.info("scheduler: extensive scan finished slot=%s run_id=%s", slot, run_id)
    except Exception as exc:
        logger.exception("scheduler: extensive scan failed slot=%s: %s", slot, exc)
    finally:
        db.close()


def _morning_extensive_scan() -> None:
    """8:35 AM CT job."""
    _run_extensive_scan("am")


def _afternoon_extensive_scan() -> None:
    """3:30 PM CT job."""
    _run_extensive_scan("pm")


def start_scheduler() -> None:
    """Register cron jobs and start the scheduler. Called from main.py startup."""
    scheduler.add_job(
        _morning_extensive_scan,
        trigger="cron",
        day_of_week="mon-fri",
        hour=8,
        minute=35,
        timezone=_CT,
        id="morning_extensive_scan",
        replace_existing=True,
    )
    scheduler.add_job(
        _afternoon_extensive_scan,
        trigger="cron",
        day_of_week="mon-fri",
        hour=15,
        minute=30,
        timezone=_CT,
        id="afternoon_extensive_scan",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(
        "scheduler: started — extensive scan jobs registered for 8:35 AM and 3:30 PM CT Mon-Fri"
    )


def stop_scheduler() -> None:
    """Gracefully shut down the scheduler. Called from main.py shutdown."""
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("scheduler: stopped")
