from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.config import Settings

logger = logging.getLogger("vegaplex.scheduler")

_scheduler = None


def start_scheduler(settings: "Settings") -> None:
    global _scheduler
    from apscheduler.schedulers.background import BackgroundScheduler  # noqa: PLC0415
    from apscheduler.triggers.cron import CronTrigger  # noqa: PLC0415

    _scheduler = BackgroundScheduler(timezone=settings.scheduler_timezone)

    # Daily Polygon seeder job
    if settings.polygon_api_key:
        _scheduler.add_job(
            _seed_polygon_job,
            trigger=CronTrigger.from_crontab(settings.polygon_seed_cron),
            id="polygon_seeder",
            name="Daily Polygon skew seed",
            kwargs={"api_key": settings.polygon_api_key},
            replace_existing=True,
        )
        logger.info("Polygon seeder job scheduled: %s", settings.polygon_seed_cron)
    else:
        logger.info("No POLYGON_API_KEY — seeder job not scheduled")

    _scheduler.start()
    logger.info("APScheduler started")


def stop_scheduler() -> None:
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("APScheduler stopped")


def get_scheduler():
    return _scheduler


def _seed_polygon_job(api_key: str) -> None:
    """Runs inside the APScheduler thread pool."""
    try:
        from seed_polygon import SkewHistorySeeder  # noqa: PLC0415
        seeder = SkewHistorySeeder(api_key=api_key)
        seeder.seed_iv_history(date_mode="today")
        logger.info("Polygon seeder job completed")
    except Exception as exc:
        logger.error("Polygon seeder job failed: %s", exc, exc_info=True)


def list_jobs() -> list[dict]:
    if _scheduler is None:
        return []
    jobs = []
    for job in _scheduler.get_jobs():
        jobs.append({
            "id": job.id,
            "name": job.name,
            "next_run": str(job.next_run_time) if job.next_run_time else None,
            "trigger": str(job.trigger),
            "paused": job.next_run_time is None,
        })
    return jobs


def pause_job(job_id: str) -> bool:
    if _scheduler is None:
        return False
    try:
        _scheduler.pause_job(job_id)
        return True
    except Exception:
        return False


def resume_job(job_id: str) -> bool:
    if _scheduler is None:
        return False
    try:
        _scheduler.resume_job(job_id)
        return True
    except Exception:
        return False


def trigger_job_now(job_id: str) -> bool:
    if _scheduler is None:
        return False
    try:
        _scheduler.get_job(job_id).modify(next_run_time=None)
        _scheduler.get_job(job_id).resume()
        return True
    except Exception:
        return False
