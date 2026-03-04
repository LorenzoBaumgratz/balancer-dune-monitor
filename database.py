"""
Database layer – SQLite via sqlite3 standard library.

Tables
------
daily_snapshots     – one row per (date, metric, scope_type, scope_value)
ath_records         – current ATH per (metric, scope_type, scope_value)
reported_milestones – milestone crossings already reported (dedup guard)
"""

import sqlite3
from contextlib import contextmanager
from datetime import date, datetime
from typing import Generator, Optional

from config import DB_PATH
from logger_setup import get_logger

log = get_logger(__name__)

# ── DDL ───────────────────────────────────────────────────────────────────────

_DDL = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS daily_snapshots (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    snap_date   TEXT    NOT NULL,          -- ISO-8601 YYYY-MM-DD
    metric_name TEXT    NOT NULL,
    scope_type  TEXT    NOT NULL,          -- global | v3 | chain | pool_type
    scope_value TEXT    NOT NULL DEFAULT '',
    value       REAL    NOT NULL,
    inserted_at TEXT    NOT NULL DEFAULT (datetime('now')),
    UNIQUE (snap_date, metric_name, scope_type, scope_value)
);

CREATE INDEX IF NOT EXISTS ix_ds_metric
    ON daily_snapshots (metric_name, scope_type, scope_value, snap_date);

CREATE TABLE IF NOT EXISTS ath_records (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    metric_name TEXT    NOT NULL,
    scope_type  TEXT    NOT NULL,
    scope_value TEXT    NOT NULL DEFAULT '',
    ath_value   REAL    NOT NULL,
    ath_date    TEXT    NOT NULL,
    updated_at  TEXT    NOT NULL DEFAULT (datetime('now')),
    UNIQUE (metric_name, scope_type, scope_value)
);

CREATE TABLE IF NOT EXISTS reported_milestones (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    metric_name    TEXT    NOT NULL,
    scope_type     TEXT    NOT NULL,
    scope_value    TEXT    NOT NULL DEFAULT '',
    milestone_value REAL   NOT NULL,
    reported_date  TEXT    NOT NULL,
    inserted_at    TEXT    NOT NULL DEFAULT (datetime('now')),
    UNIQUE (metric_name, scope_type, scope_value, milestone_value)
);
"""


# ── Connection factory ────────────────────────────────────────────────────────

def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def get_db() -> Generator[sqlite3.Connection, None, None]:
    """Yield an open, auto-committing connection; roll back on error."""
    conn = _connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    """Create tables if they don't exist yet."""
    with get_db() as conn:
        conn.executescript(_DDL)
    log.info("Database initialised at %s", DB_PATH)


# ── daily_snapshots helpers ───────────────────────────────────────────────────

def upsert_snapshot(
    conn: sqlite3.Connection,
    snap_date: date,
    metric_name: str,
    scope_type: str,
    scope_value: str,
    value: float,
) -> None:
    conn.execute(
        """
        INSERT INTO daily_snapshots
               (snap_date, metric_name, scope_type, scope_value, value)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(snap_date, metric_name, scope_type, scope_value)
        DO UPDATE SET value = excluded.value,
                      inserted_at = datetime('now')
        """,
        (snap_date.isoformat(), metric_name, scope_type, scope_value, value),
    )


def get_snapshot(
    conn: sqlite3.Connection,
    snap_date: date,
    metric_name: str,
    scope_type: str,
    scope_value: str,
) -> Optional[float]:
    row = conn.execute(
        """
        SELECT value FROM daily_snapshots
        WHERE snap_date = ? AND metric_name = ?
          AND scope_type = ? AND scope_value = ?
        """,
        (snap_date.isoformat(), metric_name, scope_type, scope_value),
    ).fetchone()
    return float(row["value"]) if row else None


def get_previous_snapshot(
    conn: sqlite3.Connection,
    before_date: date,
    metric_name: str,
    scope_type: str,
    scope_value: str,
) -> Optional[tuple[date, float]]:
    """Return (date, value) of the most recent snapshot strictly before *before_date*."""
    row = conn.execute(
        """
        SELECT snap_date, value FROM daily_snapshots
        WHERE snap_date < ? AND metric_name = ?
          AND scope_type = ? AND scope_value = ?
        ORDER BY snap_date DESC
        LIMIT 1
        """,
        (before_date.isoformat(), metric_name, scope_type, scope_value),
    ).fetchone()
    if row:
        return date.fromisoformat(row["snap_date"]), float(row["value"])
    return None


# ── ath_records helpers ───────────────────────────────────────────────────────

def get_ath(
    conn: sqlite3.Connection,
    metric_name: str,
    scope_type: str,
    scope_value: str,
) -> Optional[tuple[float, date]]:
    row = conn.execute(
        """
        SELECT ath_value, ath_date FROM ath_records
        WHERE metric_name = ? AND scope_type = ? AND scope_value = ?
        """,
        (metric_name, scope_type, scope_value),
    ).fetchone()
    if row:
        return float(row["ath_value"]), date.fromisoformat(row["ath_date"])
    return None


def upsert_ath(
    conn: sqlite3.Connection,
    metric_name: str,
    scope_type: str,
    scope_value: str,
    ath_value: float,
    ath_date: date,
) -> None:
    conn.execute(
        """
        INSERT INTO ath_records
               (metric_name, scope_type, scope_value, ath_value, ath_date)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(metric_name, scope_type, scope_value)
        DO UPDATE SET ath_value  = excluded.ath_value,
                      ath_date   = excluded.ath_date,
                      updated_at = datetime('now')
        """,
        (metric_name, scope_type, scope_value, ath_value, ath_date.isoformat()),
    )


# ── reported_milestones helpers ───────────────────────────────────────────────

def milestone_already_reported(
    conn: sqlite3.Connection,
    metric_name: str,
    scope_type: str,
    scope_value: str,
    milestone_value: float,
) -> bool:
    row = conn.execute(
        """
        SELECT 1 FROM reported_milestones
        WHERE metric_name = ? AND scope_type = ? AND scope_value = ?
          AND milestone_value = ?
        """,
        (metric_name, scope_type, scope_value, milestone_value),
    ).fetchone()
    return row is not None


def record_milestone(
    conn: sqlite3.Connection,
    metric_name: str,
    scope_type: str,
    scope_value: str,
    milestone_value: float,
    reported_date: date,
) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO reported_milestones
               (metric_name, scope_type, scope_value, milestone_value, reported_date)
        VALUES (?, ?, ?, ?, ?)
        """,
        (metric_name, scope_type, scope_value, milestone_value, reported_date.isoformat()),
    )


def get_all_snapshots(conn: sqlite3.Connection, limit: int = 100) -> list[dict]:
    """Retorna os snapshots mais recentes do banco."""
    cur = conn.execute(
        """
        SELECT snap_date, metric_name, scope_type, scope_value, value
        FROM daily_snapshots
        ORDER BY snap_date DESC, metric_name
        LIMIT ?
        """,
        (limit,),
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_all_aths(conn: sqlite3.Connection) -> list[dict]:
    """Retorna todos os ATHs registrados."""
    cur = conn.execute(
        """
        SELECT metric_name, scope_type, scope_value, ath_value, ath_date
        FROM ath_records
        ORDER BY metric_name
        """
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_all_snapshots_for_metric(
    conn: sqlite3.Connection,
    metric_name: str,
    scope_type: str,
    scope_value: str,
) -> list[tuple[date, float]]:
    rows = conn.execute(
        """
        SELECT snap_date, value FROM daily_snapshots
        WHERE metric_name = ? AND scope_type = ? AND scope_value = ?
        ORDER BY snap_date ASC
        """,
        (metric_name, scope_type, scope_value),
    ).fetchall()
    return [(date.fromisoformat(r["snap_date"]), float(r["value"])) for r in rows]
