"""
Report Generation.

Takes the list of ReportEntry objects produced by the classifier and
serialises them into a JSON file.

Output format
-------------
{
  "generated_at": "2025-01-15T08:32:07-03:00",
  "report_date":  "2025-01-15",
  "total_events": 4,
  "events_by_type": {
    "ath_daily": 1,
    "milestone_cumulative": 2,
    "v3_specific_milestone": 1,
    "chain_specific_ath": 0,
    "pool_type_ath": 0
  },
  "events": [
    {
      "event_type":     "ath_daily",
      "metric":         "daily_swap_volume",
      "scope_type":     "global",
      "scope_value":    "",
      "current_value":  12345678.90,
      "previous_value": 11234567.00,
      "snap_date":      "2025-01-15",
      "explanation":    "New all-time high …",
      "priority_score": 0.82
    },
    …
  ]
}
"""

from __future__ import annotations

import json
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Any

import pytz

from classifier import ReportEntry
from config import REPORT_DIR, SCHEDULE_TIMEZONE
from logger_setup import get_logger

log = get_logger(__name__)

_EVENT_TYPES = [
    "ath_daily",
    "milestone_cumulative",
    "v3_specific_milestone",
    "chain_specific_ath",
    "pool_type_ath",
]


def generate_report(
    entries: list[ReportEntry],
    report_date: date,
) -> Path:
    """
    Serialise *entries* to a dated JSON file under REPORT_DIR.

    Returns the path to the written file.
    """
    tz = pytz.timezone(SCHEDULE_TIMEZONE)
    now_local = datetime.now(tz=tz)

    counts: dict[str, int] = {t: 0 for t in _EVENT_TYPES}
    for entry in entries:
        key = entry.event_type if entry.event_type in counts else "ath_daily"
        counts[key] += 1

    payload: dict[str, Any] = {
        "generated_at": now_local.isoformat(),
        "report_date":  report_date.isoformat(),
        "total_events": len(entries),
        "events_by_type": counts,
        "events": [e.to_dict() for e in entries],
    }

    filename = REPORT_DIR / f"report_{report_date.isoformat()}.json"
    filename.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    log.info("Report written → %s  (%d events)", filename, len(entries))
    return filename


def print_report_summary(entries: list[ReportEntry], report_date: date) -> None:
    """Print a human-readable summary to stdout."""
    print(f"\n{'='*60}")
    print(f"  Balancer Monitor Report — {report_date}")
    print(f"  Total events: {len(entries)}")
    print(f"{'='*60}")

    if not entries:
        print("  No notable events detected today.\n")
        return

    for i, e in enumerate(entries, 1):
        print(f"\n  [{i}] {e.event_type.upper()}  priority={e.priority_score:.3f}")
        print(f"      Metric : {e.metric}")
        print(f"      Scope  : {e.scope_type}/{e.scope_value or '-'}")
        print(f"      Value  : {e.current_value:,.2f}")
        print(f"      Explain: {e.explanation}")

    print(f"\n{'='*60}\n")
