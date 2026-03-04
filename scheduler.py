"""
Scheduler – runs the pipeline once per day at 08:30 America/Sao_Paulo.

Uses APScheduler (BlockingScheduler) with the pytz timezone.

Usage
-----
    python scheduler.py          # start the daemon (blocks)

The process should be managed by systemd (see docs/systemd.service).
"""

from __future__ import annotations

import signal
import sys

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from config import SCHEDULE_HOUR, SCHEDULE_MINUTE, SCHEDULE_TIMEZONE
from logger_setup import get_logger, setup_logging
from pipeline import run_pipeline

log = get_logger(__name__)


def _scheduled_job() -> None:
    """Wrapper called by APScheduler – catches and logs all exceptions."""
    try:
        run_pipeline()
    except Exception as exc:
        log.exception("Pipeline run failed: %s", exc)


def _graceful_shutdown(signum, frame) -> None:
    log.info("Received signal %s – shutting down scheduler", signum)
    sys.exit(0)


def start_scheduler() -> None:
    setup_logging()
    log.info(
        "Starting Balancer Monitor scheduler  "
        "(runs daily at %02d:%02d %s)",
        SCHEDULE_HOUR, SCHEDULE_MINUTE, SCHEDULE_TIMEZONE,
    )

    signal.signal(signal.SIGTERM, _graceful_shutdown)
    signal.signal(signal.SIGINT,  _graceful_shutdown)

    scheduler = BlockingScheduler(timezone=SCHEDULE_TIMEZONE)
    scheduler.add_job(
        _scheduled_job,
        trigger=CronTrigger(
            hour=SCHEDULE_HOUR,
            minute=SCHEDULE_MINUTE,
            timezone=SCHEDULE_TIMEZONE,
        ),
        id="balancer_monitor",
        name="Balancer Dune Monitor",
        max_instances=1,           # prevent overlapping runs
        coalesce=True,             # skip missed runs instead of back-filling
        misfire_grace_time=600,    # allow up to 10 min late start
    )

    log.info("Scheduler started. Next run: %s", scheduler.get_jobs()[0].next_run_time)
    scheduler.start()


if __name__ == "__main__":
    start_scheduler()
