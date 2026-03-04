"""
Classification Engine.

Converts raw ATHEvent / MilestoneEvent objects into structured, human-readable
report entries with:
  * event_type  (one of five canonical strings)
  * explanation (natural-language string)
  * priority_score (0.0 – 1.0)

Priority scoring rationale
---------------------------
Priority is a measure of *magnitude relative to history*.

For ATH events:
  priority = (current - previous_ath) / previous_ath
  → capped at 1.0; first-ever ATHs get 0.5 by convention

For milestone events:
  priority = log10(milestone_value) / log10(MAX_REFERENCE)
  where MAX_REFERENCE = 10_000_000_000 (10 billion)
  → higher milestones = higher priority

All five event_type categories have equal strategic weight, so no category
multiplier is applied.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any

from ath_engine import ATHEvent
from milestone_engine import MilestoneEvent
from logger_setup import get_logger

log = get_logger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

_MAX_REFERENCE   = 10_000_000_000   # 10 billion – upper bound for priority normalisation
_FIRST_ATH_SCORE = 0.5              # priority for first-ever ATH records


# ── Event type routing ────────────────────────────────────────────────────────

def _classify_ath_type(event: ATHEvent) -> str:
    if event.scope_type == "chain":
        return "chain_specific_ath"
    if event.scope_type == "pool_type":
        return "pool_type_ath"
    if event.scope_type == "v3":
        return "v3_specific_milestone"  # V3 ATHs treated as v3_specific
    return "ath_daily"


def _classify_milestone_type(event: MilestoneEvent) -> str:
    if event.scope_type == "v3":
        return "v3_specific_milestone"
    return "milestone_cumulative"


# ── Human-readable formatters ─────────────────────────────────────────────────

def _fmt_value(value: float, metric_name: str) -> str:
    """Return a compact, human-readable string for a numeric value."""
    if "swap" in metric_name and "volume" not in metric_name:
        # counts – integer format
        return f"{value:,.0f}"
    if value >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"
    if value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"${value / 1_000:.2f}K"
    return f"${value:.2f}"


def _metric_label(metric_name: str) -> str:
    return metric_name.replace("_", " ").title()


def _scope_label(scope_type: str, scope_value: str) -> str:
    if scope_value:
        return f"{scope_value} ({scope_type})"
    return scope_type


def _explain_ath(event: ATHEvent) -> str:
    metric = _metric_label(event.metric_name)
    scope  = _scope_label(event.scope_type, event.scope_value)
    cur    = _fmt_value(event.current_value, event.metric_name)

    if event.previous_ath is None:
        return (
            f"First ever record for {metric} on {scope}: {cur} on {event.snap_date}. "
            f"This establishes the baseline ATH."
        )

    prev = _fmt_value(event.previous_ath, event.metric_name)
    pct  = (event.current_value - event.previous_ath) / event.previous_ath * 100
    return (
        f"New all-time high for {metric} on {scope}: {cur} "
        f"(+{pct:.1f}% vs previous ATH of {prev} set on {event.previous_ath_date}). "
        f"This is the strongest single-day performance ever recorded."
    )


def _explain_milestone(event: MilestoneEvent) -> str:
    metric    = _metric_label(event.metric_name)
    scope     = _scope_label(event.scope_type, event.scope_value)
    milestone = _fmt_value(event.milestone_value, event.metric_name)
    current   = _fmt_value(event.current_value, event.metric_name)
    prev_str  = (
        _fmt_value(event.previous_value, event.metric_name)
        if event.previous_value is not None
        else "unknown"
    )
    return (
        f"Cumulative {metric} on {scope} crossed the {milestone} milestone. "
        f"Current value: {current} (up from {prev_str}). "
        f"This marks a significant growth checkpoint."
    )


# ── Priority calculators ──────────────────────────────────────────────────────

def _priority_ath(event: ATHEvent) -> float:
    if event.previous_ath is None or event.previous_ath == 0:
        return _FIRST_ATH_SCORE
    ratio = (event.current_value - event.previous_ath) / event.previous_ath
    # Scale: 1% improvement → ~0.01, 100% → ~0.5, 1000% → ~0.91 (logarithmic)
    score = math.log1p(ratio) / math.log1p(10)   # saturates at ~10× improvement
    return min(round(score, 4), 1.0)


def _priority_milestone(event: MilestoneEvent) -> float:
    if event.milestone_value <= 0:
        return 0.0
    score = math.log10(event.milestone_value) / math.log10(_MAX_REFERENCE)
    return min(round(score, 4), 1.0)


# ── Report entry ──────────────────────────────────────────────────────────────

@dataclass
class ReportEntry:
    event_type:     str
    metric:         str
    scope_type:     str
    scope_value:    str
    current_value:  float
    previous_value: float | None    # previous ATH or previous cumulative
    snap_date:      str             # ISO-8601
    explanation:    str
    priority_score: float

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        return d


# ── Public classifier ─────────────────────────────────────────────────────────

def classify_events(
    ath_events:       list[ATHEvent],
    milestone_events: list[MilestoneEvent],
) -> list[ReportEntry]:
    """
    Classify all events and return a list of ReportEntry objects,
    sorted by priority_score descending.
    """
    entries: list[ReportEntry] = []

    for ev in ath_events:
        entries.append(
            ReportEntry(
                event_type=_classify_ath_type(ev),
                metric=ev.metric_name,
                scope_type=ev.scope_type,
                scope_value=ev.scope_value,
                current_value=round(ev.current_value, 6),
                previous_value=round(ev.previous_ath, 6) if ev.previous_ath is not None else None,
                snap_date=ev.snap_date.isoformat(),
                explanation=_explain_ath(ev),
                priority_score=_priority_ath(ev),
            )
        )

    for ev in milestone_events:
        entries.append(
            ReportEntry(
                event_type=_classify_milestone_type(ev),
                metric=ev.metric_name,
                scope_type=ev.scope_type,
                scope_value=ev.scope_value,
                current_value=round(ev.current_value, 6),
                previous_value=round(ev.previous_value, 6) if ev.previous_value is not None else None,
                snap_date=ev.snap_date.isoformat(),
                explanation=_explain_milestone(ev),
                priority_score=_priority_milestone(ev),
            )
        )

    entries.sort(key=lambda e: e.priority_score, reverse=True)
    log.info("Classifier: produced %d report entries", len(entries))
    return entries
