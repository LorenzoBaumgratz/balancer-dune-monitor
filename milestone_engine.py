"""
Milestone Detection Engine.

Definition
----------
A milestone is crossed when a cumulative metric passes a boundary defined
by the step function in config.milestone_step().

The milestone_value is computed as:
    milestone_value = floor(current / step) * step

A milestone is emitted only if:
  1. floor(prev / step) < floor(current / step)   (a new tier was crossed)
  2. The milestone has NOT already been recorded in reported_milestones.

Cumulative metric identification
---------------------------------
A metric is cumulative if its name starts with "cumulative_" or "total_",
or is listed in CUMULATIVE_METRICS.

Output
------
Returns a list of `MilestoneEvent` objects.
"""

from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from datetime import date
from typing import Optional

from config import milestone_step
from database import (
    get_previous_snapshot,
    milestone_already_reported,
    record_milestone,
)
from extractor import Snapshot
from logger_setup import get_logger

log = get_logger(__name__)

CUMULATIVE_METRICS: frozenset[str] = frozenset(
    {
        "cumulative_volume",
        "cumulative_swaps",
        "cumulative_fees",
        "cumulative_volume_by_chain",
        "cumulative_volume_by_pool_type",
        "cumulative_pools_created",
    }
)


def _is_cumulative(metric_name: str) -> bool:
    if metric_name in CUMULATIVE_METRICS:
        return True
    return metric_name.startswith("cumulative") or metric_name.startswith("total")


# ── Domain object ─────────────────────────────────────────────────────────────

@dataclass
class MilestoneEvent:
    metric_name:     str
    scope_type:      str
    scope_value:     str
    snap_date:       date
    current_value:   float
    previous_value:  Optional[float]
    milestone_value: float


# ── Helper ────────────────────────────────────────────────────────────────────

def _crossed_milestones(
    prev: float,
    current: float,
) -> list[float]:
    """
    Return sorted list of milestone values crossed when moving from prev → current.
    Handles the case where multiple milestones were crossed in one period.
    """
    crossed: list[float] = []
    # Walk through the value range in steps
    # Start from the first milestone above prev
    step = milestone_step(prev)
    candidate = (math.floor(prev / step) + 1) * step

    while candidate <= current:
        crossed.append(candidate)
        step = milestone_step(candidate)
        candidate = (math.floor(candidate / step) + 1) * step

    return crossed


# ── Engine ────────────────────────────────────────────────────────────────────

def detect_milestones(
    conn: sqlite3.Connection,
    snapshots: list[Snapshot],
    today: date,
) -> list[MilestoneEvent]:
    """
    Inspect today's cumulative snapshots for milestone crossings.

    Side-effects
    ------------
    * Records new milestones in reported_milestones table.

    Parameters
    ----------
    conn:      Open SQLite connection (caller must commit).
    snapshots: All Snapshot objects collected today.
    today:     The date these snapshots represent.

    Returns
    -------
    List of MilestoneEvent objects (one per crossed milestone per metric).
    """
    events: list[MilestoneEvent] = []

    cumulative_snaps = [s for s in snapshots if _is_cumulative(s.metric_name)]
    log.info(
        "Milestone engine: evaluating %d cumulative snapshots", len(cumulative_snaps)
    )

    for snap in cumulative_snaps:
        if snap.value <= 0:
            continue

        prev_result = get_previous_snapshot(
            conn,
            before_date=snap.snap_date,
            metric_name=snap.metric_name,
            scope_type=snap.scope_type,
            scope_value=snap.scope_value,
        )

        if prev_result is None:
            # No previous data – can't determine crossings; skip
            log.debug(
                "No previous data for %s/%s/%s – skipping milestone check",
                snap.metric_name, snap.scope_type, snap.scope_value,
            )
            continue

        _prev_date, prev_value = prev_result

        if snap.value <= prev_value:
            # Cumulative went down (data anomaly) – skip
            continue

        crossings = _crossed_milestones(prev_value, snap.value)
        if not crossings:
            continue

        for milestone_val in crossings:
            # Deduplication guard
            if milestone_already_reported(
                conn,
                snap.metric_name,
                snap.scope_type,
                snap.scope_value,
                milestone_val,
            ):
                log.debug(
                    "Milestone %.0f for %s already reported – skipping",
                    milestone_val, snap.metric_name,
                )
                continue

            log.info(
                "MILESTONE! %s [%s=%s]: %.0f crossed (prev=%.0f, cur=%.0f)",
                snap.metric_name, snap.scope_type, snap.scope_value,
                milestone_val, prev_value, snap.value,
            )

            record_milestone(
                conn,
                snap.metric_name,
                snap.scope_type,
                snap.scope_value,
                milestone_val,
                snap.snap_date,
            )

            events.append(
                MilestoneEvent(
                    metric_name=snap.metric_name,
                    scope_type=snap.scope_type,
                    scope_value=snap.scope_value,
                    snap_date=snap.snap_date,
                    current_value=snap.value,
                    previous_value=prev_value,
                    milestone_value=milestone_val,
                )
            )

    log.info("Milestone engine: detected %d new milestone events", len(events))
    return events
