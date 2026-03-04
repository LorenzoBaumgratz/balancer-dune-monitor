"""
ATH (All-Time-High) Detection Engine.

Definition
----------
An ATH event is triggered when today's *daily* metric value is strictly
greater than the highest value ever previously recorded for that
(metric_name, scope_type, scope_value) tuple.

Only daily metrics are eligible for ATH detection.  Cumulative metrics
monotonically increase by definition and would always be "ATH", making
the signal meaningless.

Daily metric identification
---------------------------
A metric is classified as daily if its name starts with "daily_" or
belongs to the DAILY_METRICS set defined below.

Output
------
Returns a list of `ATHEvent` dataclass instances that the classification
engine will later convert to report entries.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date
from typing import Optional

from database import get_ath, upsert_ath
from extractor import Snapshot
from logger_setup import get_logger

log = get_logger(__name__)

# Metrics explicitly classified as "daily" even if not prefixed with "daily_"
DAILY_METRICS: frozenset[str] = frozenset(
    {
        "tvl",
        "tvl_by_chain",
        "tvl_by_pool_type",
        "pools_by_type",
    }
)

# Metrics explicitly classified as "cumulative" (excluded from ATH)
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


def _is_daily(metric_name: str) -> bool:
    if metric_name in CUMULATIVE_METRICS:
        return False
    if metric_name.startswith("cumulative") or metric_name.startswith("total"):
        return False
    return metric_name.startswith("daily") or metric_name in DAILY_METRICS


# ── Domain object ─────────────────────────────────────────────────────────────

@dataclass
class ATHEvent:
    metric_name:   str
    scope_type:    str
    scope_value:   str
    snap_date:     date
    current_value: float
    previous_ath:  Optional[float]   # None → first ever record
    previous_ath_date: Optional[date]


# ── Engine ────────────────────────────────────────────────────────────────────

def detect_aths(
    conn: sqlite3.Connection,
    snapshots: list[Snapshot],
    today: date,
) -> list[ATHEvent]:
    """
    Inspect today's snapshots for ATH conditions.

    Side-effects
    ------------
    * Updates ath_records table for every new ATH found.

    Parameters
    ----------
    conn:      Open SQLite connection (caller must commit).
    snapshots: All Snapshot objects collected today.
    today:     The date these snapshots represent (usually date.today()).

    Returns
    -------
    List of ATHEvent objects (one per broken ATH).
    """
    events: list[ATHEvent] = []

    daily_snaps = [s for s in snapshots if _is_daily(s.metric_name)]
    log.info("ATH engine: evaluating %d daily snapshots", len(daily_snaps))

    for snap in daily_snaps:
        if snap.value <= 0:
            # Skip zero / negative values – not meaningful ATHs
            continue

        existing = get_ath(conn, snap.metric_name, snap.scope_type, snap.scope_value)

        if existing is None:
            # First ever record – store as ATH but don't emit an event
            upsert_ath(
                conn,
                snap.metric_name,
                snap.scope_type,
                snap.scope_value,
                snap.value,
                snap.snap_date,
            )
            log.debug(
                "First ATH record for %s/%s/%s = %.4f",
                snap.metric_name, snap.scope_type, snap.scope_value, snap.value,
            )
            continue

        prev_ath_value, prev_ath_date = existing

        if snap.value > prev_ath_value:
            log.info(
                "ATH! %s [%s=%s]: %.4f > %.4f (prev ATH on %s)",
                snap.metric_name, snap.scope_type, snap.scope_value,
                snap.value, prev_ath_value, prev_ath_date,
            )
            upsert_ath(
                conn,
                snap.metric_name,
                snap.scope_type,
                snap.scope_value,
                snap.value,
                snap.snap_date,
            )
            events.append(
                ATHEvent(
                    metric_name=snap.metric_name,
                    scope_type=snap.scope_type,
                    scope_value=snap.scope_value,
                    snap_date=snap.snap_date,
                    current_value=snap.value,
                    previous_ath=prev_ath_value,
                    previous_ath_date=prev_ath_date,
                )
            )

    log.info("ATH engine: detected %d new ATH events", len(events))
    return events


# ── Historical ATH rebuild ────────────────────────────────────────────────────

def rebuild_aths_from_snapshots(conn: sqlite3.Connection) -> int:
    """
    Varre TODOS os daily_snapshots salvos no banco e recalcula ath_records do zero.
    Usado após seed histórico para garantir que os ATHs reflitam o máximo real.

    Retorna o número de registros de ATH atualizados.
    """
    rows = conn.execute(
        """
        SELECT metric_name, scope_type, scope_value, snap_date, value
        FROM daily_snapshots
        WHERE (metric_name LIKE 'daily_%' OR metric_name IN ('tvl', 'tvl_eth', 'pools_by_type'))
          AND value > 0
        ORDER BY metric_name, scope_type, scope_value, snap_date
        """
    ).fetchall()

    # Agrupar por (metric, scope_type, scope_value) e achar o máximo
    bests: dict[tuple, tuple[float, str]] = {}
    for r in rows:
        key = (r["metric_name"], r["scope_type"], r["scope_value"])
        val = float(r["value"])
        if key not in bests or val > bests[key][0]:
            bests[key] = (val, r["snap_date"])

    # Upsert em ath_records com o verdadeiro máximo histórico
    for (metric, scope_type, scope_value), (ath_val, ath_date_str) in bests.items():
        upsert_ath(conn, metric, scope_type, scope_value, ath_val, date.fromisoformat(ath_date_str))
        log.debug("ATH rebuilt: %s [%s/%s] = %.2f on %s", metric, scope_type, scope_value, ath_val, ath_date_str)

    log.info("Rebuilt %d ATH records from historical snapshots", len(bests))
    return len(bests)
