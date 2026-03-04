"""
Core pipeline – orchestrates one full monitoring run.

Steps
-----
1. Initialise DB
2. Create Dune client
3. Extract snapshots from Dune API
4. Persist snapshots to DB
5. Detect ATH events
6. Detect milestone events
7. Classify events
8. Generate JSON report
9. Print summary

Designed to be called both from the scheduler and from the CLI.
"""

from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

from ath_engine import detect_aths
from classifier import classify_events
from database import get_db, init_db, upsert_snapshot
from extractor import Snapshot, extract_all as dune_extract_all
from dune_client import DuneClient
from milestone_engine import detect_milestones
from reporter import generate_report, print_report_summary
from logger_setup import get_logger

log = get_logger(__name__)


def run_pipeline(today: date | None = None) -> Path:
    """
    Execute one full monitoring cycle.

    Parameters
    ----------
    today: Override today's date (useful for back-testing / replays).
           Defaults to date.today().

    Returns
    -------
    Path to the generated JSON report file.
    """
    if today is None:
        today = date.today()

    log.info("=== Pipeline START  date=%s ===", today)

    # 1 – Initialise database
    init_db()

    # 2 – Create Dune API client
    log.info("Connecting to Dune API...")
    client = DuneClient()

    # 3 – Extract snapshots from Dune API
    log.info("Extracting data from Dune API...")
    snapshots: list[Snapshot] = dune_extract_all(client)
    log.info("Extracted %d snapshots total", len(snapshots))

    if not snapshots:
        log.warning("No snapshots extracted – aborting pipeline")
        raise RuntimeError("No data extracted from Dune API.")

    # 4 – Persist + detect inside a single transaction
    with get_db() as conn:
        _persist_snapshots(conn, snapshots)

        # 5 – ATH detection
        ath_events = detect_aths(conn, snapshots, today)

        # 6 – Milestone detection
        milestone_events = detect_milestones(conn, snapshots, today)

    # 7 – Classify
    entries = classify_events(ath_events, milestone_events)

    # 8 – Generate report
    report_path = generate_report(entries, today)

    # 9 – Print summary
    print_report_summary(entries, today)

    log.info(
        "=== Pipeline DONE  events=%d  report=%s ===",
        len(entries), report_path,
    )
    return report_path


# ── Helpers ───────────────────────────────────────────────────────────────────

def _persist_snapshots(conn: sqlite3.Connection, snapshots: list[Snapshot]) -> None:
    persisted = 0
    for snap in snapshots:
        try:
            upsert_snapshot(
                conn,
                snap.snap_date,
                snap.metric_name,
                snap.scope_type,
                snap.scope_value,
                snap.value,
            )
            persisted += 1
        except Exception as exc:
            log.error("Failed to persist snapshot %s: %s", snap, exc)

    log.info("Persisted %d / %d snapshots to DB", persisted, len(snapshots))
