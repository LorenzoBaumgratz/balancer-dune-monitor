"""
Data extraction layer.

Mapeamento real das colunas das queries Dune:

v3_summary (4373453):
  day, Volume, Swaps, All-Time Volume, Avg. Volume per Swap,
  30d Vol SMA, 50d Vol SMA, 100d Vol SMA, 200d Vol SMA, avg_volume

v3_volume_by_chain (4373470):
  blockchain, volume, week

v3_volume_by_pool_type (4373461):
  pool_type, volume, week

v3_pools_created (4373500):  <- na verdade retorna TVL
  day, tvl_eth, tvl_usd

v3_pools_by_type (4353295):
  block_date, version, daily_volume_usd, daily_fees_usd,
  daily_tvl_usd, daily_tvl_eth, total_volume_usd, total_fees_usd,
  total_tvl_usd, total_tvl_eth, volume_dominance_pct,
  fees_dominance_pct, tvl_dominance_pct, tvl_eth_dominance_pct

global_pools_created (2617646):
  blockchain, pools_registered, week

global_volume_by_version (22261) [cached]:
  week, version, volume
  Retorna uma linha por (semana, versao) — V1, V2 e V3.
  Deste dado calculamos:
    weekly_volume_by_version  (scope_type="version", scope_value="v1"/"v2"/"v3")
    cumulative_volume         (scope_type="version") — soma historica por versao
    cumulative_volume         (scope_type="global",  scope_value="") — soma total de todas versoes

v3_volume_by_chain (4373470) [cached — historico diario completo]:
  week (ou day), blockchain, volume
  Uma linha por (dia, blockchain). Deste dado calculamos:
    weekly_volume_by_chain  (scope_type="chain") — semana ISO corrente
    monthly_swap_volume     (scope_type="chain") — mes corrente
    cumulative_volume       (scope_type="chain") — all-time
    weekly_swap_volume      (scope_type="v3")    — soma de todas chains, semana corrente
    monthly_swap_volume     (scope_type="v3")    — soma de todas chains, mes corrente

v3_volume_by_pool_type (4373461) [cached — historico diario completo]:
  week (ou day), pool_type, volume
  Uma linha por (dia, pool_type). Deste dado calculamos:
    weekly_volume_by_pool_type (scope_type="pool_type") — semana ISO corrente
    monthly_swap_volume        (scope_type="pool_type") — mes corrente
    cumulative_volume          (scope_type="pool_type") — all-time

v3_tvl_by_chain (4373530) [cached]:
  day, blockchain, tvl_usd  (possivelmente tvl_eth)
  Retorna ultima linha por blockchain:
    tvl  (scope_type="chain") — TVL atual por chain V3

v3_fees_daily (4373539) [cached — historico diario completo]:
  day, fees_usd  (colunas exatas a confirmar)
  Deste dado calculamos:
    daily_fees      (scope_type="v3") — valor do dia mais recente
    weekly_fees     (scope_type="v3") — semana ISO corrente
    monthly_fees    (scope_type="v3") — mes corrente
    cumulative_fees (scope_type="v3") — all-time
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Callable

from config import DUNE_QUERIES, DUNE_QUERIES_CACHED
from dune_client import DuneClient
from logger_setup import get_logger

log = get_logger(__name__)


# ── Domain object ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Snapshot:
    snap_date:   date
    metric_name: str
    scope_type:  str   # global | v3 | chain | pool_type
    scope_value: str   # '' for global/v3; chain name or pool-type name otherwise
    value:       float


# ── Helpers ───────────────────────────────────────────────────────────────────

def _fval(row: dict, *keys: str) -> float:
    """Return the first non-None numeric value found among *keys*."""
    for k in keys:
        v = row.get(k)
        if v is not None:
            try:
                f = float(v)
                if not math.isnan(f) and not math.isinf(f):
                    return f
            except (TypeError, ValueError):
                continue
    raise KeyError(f"None of {keys!r} found / valid in row: {row!r}")


def _sval(row: dict, *keys: str) -> str:
    for k in keys:
        v = row.get(k)
        if v is not None:
            return str(v).strip()
    raise KeyError(f"None of {keys!r} found in row: {row!r}")


def _normalize_name(s: str) -> str:
    """Strip emoji / non-ASCII chars from a Dune dimension value (chain name, pool type, etc.).

    Example: 'ethereum 🇬🇧' → 'ethereum',  'gnosis🏴' → 'gnosis'
    Falls back to the original string if stripping removes everything.
    """
    cleaned = re.sub(r"[^\x00-\x7F]+", "", s).strip()
    return cleaned if cleaned else s


def _parse_row_date(row: dict) -> date:
    """Extract the date from a row, defaulting to today if not found."""
    DATE_COLS = ("day", "date", "block_date", "week", "snapshot_date", "period")
    for col in DATE_COLS:
        v = row.get(col)
        if v is None:
            continue
        if isinstance(v, date) and not isinstance(v, datetime):
            return v
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, str):
            # handles "2026-02-19 00:00:00.000 UTC" and "2026-02-19"
            v_clean = v[:10]
            try:
                return datetime.strptime(v_clean, "%Y-%m-%d").date()
            except ValueError:
                continue
    return date.today()


def _most_recent_row(rows: list[dict]) -> dict | None:
    """Pick the row with the most recent date."""
    DATE_COLS = ("day", "date", "block_date", "week", "snapshot_date", "period")

    best_row: dict | None = None
    best_date: date | None = None

    for row in rows:
        for col in DATE_COLS:
            if col in row:
                d = _parse_row_date(row)
                if d and (best_date is None or d > best_date):
                    best_date = d
                    best_row = row
                break

    return best_row if best_row is not None else (rows[-1] if rows else None)


def _most_recent_rows_per_group(rows: list[dict], group_col: str) -> list[dict]:
    """
    For each unique value of group_col, return the row with the most recent date.
    Used for per-chain and per-pool-type queries.
    """
    best: dict[str, tuple[date, dict]] = {}
    for row in rows:
        key = str(row.get(group_col, "")).strip()
        d = _parse_row_date(row)
        if key not in best or d > best[key][0]:
            best[key] = (d, row)
    return [v[1] for v in best.values()]


def _aggregate_historical(
    rows: list[dict],
    group_col: str | None,
    *value_keys: str,
) -> tuple[dict, dict]:
    """
    Single-pass aggregation of ALL historical rows.

    Args:
        rows       : all rows returned by the query
        group_col  : column to group by (e.g. "blockchain", "pool_type");
                     pass None to treat all rows as one group
        value_keys : candidate column names for the numeric value (tried in order)

    Returns:
        by_group – {group_value: {
                      "all_time"     : float,  # sum of all rows
                      "current_week" : float,  # sum of rows in current ISO week
                      "current_month": float,  # sum of rows in current calendar month
                      "latest_date"  : date | None,
                      "latest_val"   : float,  # value of the most-recent row
                   }}
        global_  – same structure but summed across ALL groups (no group key)
    """
    today = date.today()
    cur_yw = today.isocalendar()[:2]       # (iso_year, iso_week)
    cur_ym = (today.year, today.month)

    by_group: dict[str, dict] = {}
    global_: dict = {
        "all_time": 0.0, "current_week": 0.0, "current_month": 0.0,
        "latest_date": None, "latest_val": 0.0,
    }

    for row in rows:
        if group_col is not None:
            # Grouped mode: skip rows where the dimension is null/missing
            raw_grp = row.get(group_col)
            if raw_grp is None:
                continue
            grp = str(raw_grp).strip()
            if not grp or grp.lower() == "none":
                continue
        else:
            grp = ""  # no grouping — all rows contribute to one global bucket
        try:
            val = _fval(row, *value_keys)
        except KeyError:
            continue
        d = _parse_row_date(row)

        if grp not in by_group:
            by_group[grp] = {
                "all_time": 0.0, "current_week": 0.0, "current_month": 0.0,
                "latest_date": None, "latest_val": 0.0,
            }
        entry = by_group[grp]
        entry["all_time"] += val
        if d.isocalendar()[:2] == cur_yw:
            entry["current_week"] += val
        if (d.year, d.month) == cur_ym:
            entry["current_month"] += val
        if entry["latest_date"] is None or d > entry["latest_date"]:
            entry["latest_date"] = d
            entry["latest_val"] = val

        global_["all_time"] += val
        if d.isocalendar()[:2] == cur_yw:
            global_["current_week"] += val
        if (d.year, d.month) == cur_ym:
            global_["current_month"] += val
        if global_["latest_date"] is None or d > global_["latest_date"]:
            global_["latest_date"] = d
            global_["latest_val"] = val

    return by_group, global_


# ── Per-query parsers ─────────────────────────────────────────────────────────

def _parse_v3_summary(rows: list[dict], scope: str) -> list[Snapshot]:
    """
    Query 4373453 – v3_summary
    Columns: day, Volume, Swaps, All-Time Volume, avg_volume, ...SMA fields

    Produces from most recent row:
      - daily_swap_volume, daily_swaps, cumulative_volume

    Produces from ALL historical rows:
      - monthly_swap_volume  (scope v3) — volume acumulado no mês corrente
      - monthly_swaps        (scope v3) — swaps acumulados no mês corrente
      - cumulative_swaps     (scope v3) — total histórico de swaps
    """
    if not rows:
        return []

    snap_date = _parse_row_date(_most_recent_row(rows) or rows[-1])
    snaps: list[Snapshot] = []
    row = _most_recent_row(rows)

    # ── From most recent row ──────────────────────────────────────────────────
    if row is not None:
        try:
            val = _fval(row, "Volume", "volume")
            snaps.append(Snapshot(snap_date, "daily_swap_volume", scope, "", val))
        except KeyError:
            log.debug("daily_swap_volume not found in v3_summary")

        try:
            val = _fval(row, "Swaps", "swaps")
            snaps.append(Snapshot(snap_date, "daily_swaps", scope, "", val))
        except KeyError:
            log.debug("daily_swaps not found in v3_summary")

        try:
            val = _fval(row, "All-Time Volume", "all_time_volume")
            snaps.append(Snapshot(snap_date, "cumulative_volume", scope, "", val))
        except KeyError:
            log.debug("cumulative_volume not found in v3_summary")

    # ── From ALL historical rows (monthly aggregates + cumulative swaps) ──────
    _, vol_g = _aggregate_historical(rows, None, "Volume", "volume")
    _, swp_g = _aggregate_historical(rows, None, "Swaps", "swaps")

    ld = vol_g["latest_date"] or snap_date

    if vol_g["current_month"] > 0:
        snaps.append(Snapshot(ld, "monthly_swap_volume", scope, "", vol_g["current_month"]))

    if swp_g["current_month"] > 0:
        snaps.append(Snapshot(ld, "monthly_swaps", scope, "", swp_g["current_month"]))

    if swp_g["all_time"] > 0:
        snaps.append(Snapshot(ld, "cumulative_swaps", scope, "", swp_g["all_time"]))

    return snaps


def _parse_v3_volume_by_chain(rows: list[dict], scope: str) -> list[Snapshot]:
    """
    Query 4373470 – v3_volume_by_chain  [CACHED — full history]
    Columns: blockchain, volume, week/day

    Produces per chain (V3):
      weekly_volume_by_chain  — semana ISO corrente
      monthly_swap_volume     — mês corrente
      cumulative_volume       — all-time

    Produces global V3 (sum all chains):
      weekly_swap_volume      — semana ISO corrente
      monthly_swap_volume     — mês corrente
    """
    snaps: list[Snapshot] = []
    by_chain, global_ = _aggregate_historical(rows, "blockchain", "volume", "Volume")
    ld = global_["latest_date"] or date.today()

    for raw_chain, t in by_chain.items():
        chain = _normalize_name(raw_chain)
        if not chain:
            continue
        d = t["latest_date"] or ld
        if t["current_week"] > 0:
            snaps.append(Snapshot(d, "weekly_volume_by_chain", "chain", chain, t["current_week"]))
        if t["current_month"] > 0:
            snaps.append(Snapshot(d, "monthly_swap_volume",    "chain", chain, t["current_month"]))
        if t["all_time"] > 0:
            snaps.append(Snapshot(d, "cumulative_volume",      "chain", chain, t["all_time"]))

    # V3 totals (sum of all chains)
    if global_["current_week"] > 0:
        snaps.append(Snapshot(ld, "weekly_swap_volume",  scope, "", global_["current_week"]))
    if global_["current_month"] > 0:
        snaps.append(Snapshot(ld, "monthly_swap_volume", scope, "", global_["current_month"]))

    return snaps


def _parse_v3_volume_by_pool_type(rows: list[dict], scope: str) -> list[Snapshot]:
    """
    Query 4373461 – v3_volume_by_pool_type  [CACHED — full history]
    Columns: pool_type, volume, week/day

    Produces per pool_type (V3):
      weekly_volume_by_pool_type — semana ISO corrente
      monthly_swap_volume        — mês corrente
      cumulative_volume          — all-time
    """
    snaps: list[Snapshot] = []
    by_pt, _ = _aggregate_historical(rows, "pool_type", "volume", "Volume")

    for ptype, t in by_pt.items():
        ptype = _normalize_name(ptype)
        if not ptype:
            continue
        d = t["latest_date"] or date.today()
        if t["current_week"] > 0:
            snaps.append(Snapshot(d, "weekly_volume_by_pool_type", "pool_type", ptype, t["current_week"]))
        if t["current_month"] > 0:
            snaps.append(Snapshot(d, "monthly_swap_volume",        "pool_type", ptype, t["current_month"]))
        if t["all_time"] > 0:
            snaps.append(Snapshot(d, "cumulative_volume",          "pool_type", ptype, t["all_time"]))

    return snaps


def _parse_v3_tvl(rows: list[dict], scope: str) -> list[Snapshot]:
    """
    Query 4373500 – v3_pools_created (na verdade retorna TVL diário)
    Columns: day, tvl_eth, tvl_usd
    """
    row = _most_recent_row(rows)
    if row is None:
        return []

    snap_date = _parse_row_date(row)
    snaps: list[Snapshot] = []

    try:
        val = _fval(row, "tvl_usd")
        snaps.append(Snapshot(snap_date, "tvl", scope, "", val))
    except KeyError:
        log.debug("tvl_usd not found")

    try:
        val = _fval(row, "tvl_eth")
        snaps.append(Snapshot(snap_date, "tvl_eth", scope, "", val))
    except KeyError:
        log.debug("tvl_eth not found")

    return snaps


def _parse_v3_pools_by_type(rows: list[dict], scope: str) -> list[Snapshot]:
    """
    Query 4353295 – v3_pools_by_type
    Columns: block_date, version, daily_volume_usd, daily_fees_usd,
             daily_tvl_usd, total_volume_usd, total_fees_usd, total_tvl_usd, ...
    Returns most recent date per version.
    """
    snaps: list[Snapshot] = []
    recent_rows = _most_recent_rows_per_group(rows, "version")

    for row in recent_rows:
        version = str(row.get("version", "")).strip()
        snap_date = _parse_row_date(row)

        for metric, col in [
            ("daily_swap_volume",  "daily_volume_usd"),
            ("daily_fees",         "daily_fees_usd"),
            ("tvl",                "daily_tvl_usd"),
            ("cumulative_volume",  "total_volume_usd"),
            ("cumulative_fees",    "total_fees_usd"),
        ]:
            try:
                val = _fval(row, col)
                snaps.append(Snapshot(snap_date, metric, "pool_type", f"v{version}", val))
            except KeyError:
                pass

    return snaps


def _parse_v3_tvl_by_chain(rows: list[dict], scope: str) -> list[Snapshot]:
    """
    Query 4373530 – v3_tvl_by_chain  [CACHED — daily history]
    Expected cols: day, blockchain, tvl_usd  (possibly tvl_eth)

    Produces per chain (V3):
      tvl — TVL atual (most recent row per chain)
    ATH detected automatically by the ATH engine.
    """
    snaps: list[Snapshot] = []
    recent_rows = _most_recent_rows_per_group(rows, "blockchain")

    for row in recent_rows:
        raw_chain = row.get("blockchain")
        if raw_chain is None:
            continue
        chain = str(raw_chain).strip()
        if not chain:
            continue
        d = _parse_row_date(row)
        try:
            snaps.append(Snapshot(d, "tvl", "chain", chain, _fval(row, "tvl_usd", "tvl")))
        except KeyError:
            pass

    return snaps


def _parse_v3_fees_daily(rows: list[dict], scope: str) -> list[Snapshot]:
    """
    Query 4373539 – v3_fees_daily  [CACHED — daily history since launch]
    Cols: date, fee_type, fees_usd
    Multiple rows per day (one per fee_type: swap_fee, yield_fee, etc.)

    Sums all fee_types per day before aggregating. Produces (V3 global):
      daily_fees      — total fees do dia mais recente (soma dos fee_types)
      weekly_fees     — semana ISO corrente
      monthly_fees    — mês corrente
      cumulative_fees — all-time
    """
    snaps: list[Snapshot] = []
    if not rows:
        return snaps

    fee_keys = ("fees_usd", "fees", "fee_amount", "total_fees_usd")

    # Collapse multiple fee_types per day → one total per day
    daily_totals: dict[date, float] = {}
    for row in rows:
        d = _parse_row_date(row)
        try:
            val = _fval(row, *fee_keys)
        except KeyError:
            continue
        daily_totals[d] = daily_totals.get(d, 0.0) + val

    if not daily_totals:
        log.warning("v3_fees_daily: no valid rows — check column names. Sample: %s", rows[0] if rows else "(empty)")
        return snaps

    today   = date.today()
    cur_yw  = today.isocalendar()[:2]
    cur_ym  = (today.year, today.month)
    ld      = max(daily_totals)

    all_time     = sum(daily_totals.values())
    current_week = sum(v for d, v in daily_totals.items() if d.isocalendar()[:2] == cur_yw)
    current_month= sum(v for d, v in daily_totals.items() if (d.year, d.month) == cur_ym)

    # Latest daily total (sum of all fee_types on the most recent date)
    snaps.append(Snapshot(ld, "daily_fees", scope, "", daily_totals[ld]))

    if current_week > 0:
        snaps.append(Snapshot(ld, "weekly_fees",  scope, "", current_week))
    if current_month > 0:
        snaps.append(Snapshot(ld, "monthly_fees", scope, "", current_month))
    if all_time > 0:
        snaps.append(Snapshot(ld, "cumulative_fees", scope, "", all_time))

    return snaps


def _parse_global_pools_created(rows: list[dict], scope: str) -> list[Snapshot]:
    """
    Query 2617646 – global_pools_created
    Columns: blockchain, pools_registered, week
    Sums all pools_registered across all chains and weeks as cumulative total.
    Also returns per-chain totals.
    """
    snaps: list[Snapshot] = []

    # Total acumulado por blockchain
    totals: dict[str, float] = {}
    for row in rows:
        chain = row.get("blockchain", "").strip()
        # Remove emojis/unicode extras: keep only the word part
        chain_clean = chain.split()[0] if chain else chain
        try:
            val = _fval(row, "pools_registered")
            totals[chain_clean] = totals.get(chain_clean, 0) + val
        except KeyError:
            pass

    if not totals:
        return []

    snap_date = _parse_row_date(_most_recent_row(rows) or rows[0])

    # Snapshot por chain
    for chain, total in totals.items():
        if chain:
            snaps.append(Snapshot(snap_date, "cumulative_pools_created", "chain", chain, total))

    # Snapshot global (soma de todas as chains)
    grand_total = sum(totals.values())
    snaps.append(Snapshot(snap_date, "cumulative_pools_created", "global", "", grand_total))

    return snaps


def _parse_global_volume_by_version(rows: list[dict], scope: str) -> list[Snapshot]:
    """
    Query 22261 – Volume (USD) by Version, Weekly  (V1 + V2 + V3)
    Expected cols: week, version, volume (or volume_usd / Volume)

    Produces three kinds of snapshots:
      - weekly_volume_by_version (scope_type="version")  – most recent week per version
      - cumulative_volume        (scope_type="version")  – sum of ALL historical weeks per version
      - cumulative_volume        (scope_type="global", scope_value="") – grand total all versions
    """
    snaps: list[Snapshot] = []

    # ── Pass 1: accumulate totals across ALL historical weeks ─────────────────
    version_totals: dict[str, float] = {}
    version_latest_date: dict[str, date] = {}
    global_total = 0.0
    global_latest_date: date | None = None

    for row in rows:
        raw_version = row.get("version")
        if raw_version is None:
            continue
        version = str(raw_version).strip().lower()
        if not version:
            continue
        try:
            val = _fval(row, "volume", "volume_usd", "Volume")
        except KeyError:
            continue
        snap_date = _parse_row_date(row)

        version_totals[version] = version_totals.get(version, 0.0) + val
        if version not in version_latest_date or snap_date > version_latest_date[version]:
            version_latest_date[version] = snap_date

        global_total += val
        if global_latest_date is None or snap_date > global_latest_date:
            global_latest_date = snap_date

    if not version_totals:
        return snaps

    # ── Weekly snapshots: most recent week per version ────────────────────────
    for row in _most_recent_rows_per_group(rows, "version"):
        version = str(row.get("version", "")).strip().lower()
        if not version:
            continue
        snap_date = _parse_row_date(row)
        try:
            val = _fval(row, "volume", "volume_usd", "Volume")
            snaps.append(Snapshot(snap_date, "weekly_volume_by_version", "version", version, val))
        except KeyError:
            pass

    # ── Cumulative per version (sum of all historical weeks) ──────────────────
    for version, total in version_totals.items():
        snaps.append(Snapshot(version_latest_date[version], "cumulative_volume", "version", version, total))

    # ── Cumulative global: ALL versions + ALL weeks summed ────────────────────
    if global_latest_date is not None and global_total > 0:
        snaps.append(Snapshot(global_latest_date, "cumulative_volume", "global", "", global_total))

    return snaps


# ── Parser registry ───────────────────────────────────────────────────────────

PARSERS: dict[str, tuple[Callable[[list[dict], str], list[Snapshot]], str]] = {
    # Global (all versions)
    "global_pools_created":      (_parse_global_pools_created,      "global"),
    "global_volume_by_version":  (_parse_global_volume_by_version,  "global"),

    # V3
    "v3_summary":                (_parse_v3_summary,                "v3"),
    "v3_volume_by_chain":        (_parse_v3_volume_by_chain,        "v3"),
    "v3_volume_by_pool_type":    (_parse_v3_volume_by_pool_type,    "v3"),
    "v3_pools_created":          (_parse_v3_tvl,                    "v3"),
    "v3_pools_by_type":          (_parse_v3_pools_by_type,          "v3"),
    "v3_tvl_by_chain":           (_parse_v3_tvl_by_chain,           "v3"),
    "v3_fees_daily":             (_parse_v3_fees_daily,             "v3"),
}


# ── Main extraction function ──────────────────────────────────────────────────

def extract_all(client: DuneClient) -> list[Snapshot]:
    """
    Run every query in DUNE_QUERIES (fresh execution) and DUNE_QUERIES_CACHED
    (latest cached result, no execution cost), parse results, return all Snapshots.
    """
    all_snapshots: list[Snapshot] = []

    # ── Fresh executions ──────────────────────────────────────────────────────
    for logical_name, query_id in DUNE_QUERIES.items():
        entry = PARSERS.get(logical_name)
        if entry is None:
            log.warning("No parser registered for query '%s' – skipping", logical_name)
            continue

        parser_fn, scope = entry

        try:
            rows = client.run_query(query_id)
        except Exception as exc:
            log.error("Failed to fetch query '%s' (id=%d): %s", logical_name, query_id, exc)
            continue

        try:
            snaps = parser_fn(rows, scope)
        except Exception as exc:
            log.error("Failed to parse query '%s': %s", logical_name, exc, exc_info=True)
            continue

        log.info(
            "Query '%s': %d rows → %d snapshots", logical_name, len(rows), len(snaps)
        )
        all_snapshots.extend(snaps)

    # ── Cached results (no new execution) ─────────────────────────────────────
    for logical_name, query_id in DUNE_QUERIES_CACHED.items():
        entry = PARSERS.get(logical_name)
        if entry is None:
            log.warning("No parser registered for cached query '%s' – skipping", logical_name)
            continue

        parser_fn, scope = entry

        try:
            rows = client.get_latest_result(query_id)
        except Exception as exc:
            log.error(
                "Failed to fetch cached query '%s' (id=%d): %s", logical_name, query_id, exc
            )
            continue

        try:
            snaps = parser_fn(rows, scope)
        except Exception as exc:
            log.error("Failed to parse cached query '%s': %s", logical_name, exc, exc_info=True)
            continue

        log.info(
            "Cached query '%s': %d rows → %d snapshots", logical_name, len(rows), len(snaps)
        )
        all_snapshots.extend(snaps)

    return all_snapshots


# ── Historical extraction (seed) ──────────────────────────────────────────────

def extract_all_history(client: DuneClient) -> list[Snapshot]:
    """
    Busca TODO o histórico das queries e popula o DB com dados históricos.
    Usado no seed inicial. Diferente de extract_all() que opera no presente.

    Estratégia por query:
      - Queries diárias (v3_summary, v3_tvl, v3_pools_by_type, v3_fees_daily):
        emite um Snapshot por linha/dia para histórico completo de ATH detection.
      - Queries cacheadas com parser de agregados (v3_volume_by_chain, v3_volume_by_pool_type,
        v3_tvl_by_chain): chama o parser diretamente (retorna agregados do período atual).
      - global_volume_by_version (22261): emite um Snapshot por (semana, versão) + cumulativos.
    """
    all_snapshots: list[Snapshot] = []

    # ── v3_summary (4373453): uma linha por dia ───────────────────────────────
    log.info("Seeding v3_summary history (query 4373453)...")
    try:
        rows = client.run_query(4373453)
        for row in rows:
            snap_date = _parse_row_date(row)
            try:
                val = _fval(row, "Volume", "volume")
                all_snapshots.append(Snapshot(snap_date, "daily_swap_volume", "v3", "", val))
            except KeyError:
                pass
            try:
                val = _fval(row, "Swaps", "swaps")
                all_snapshots.append(Snapshot(snap_date, "daily_swaps", "v3", "", val))
            except KeyError:
                pass
        log.info("v3_summary: %d rows", len(rows))
    except Exception as exc:
        log.error("Failed to seed v3_summary: %s", exc)

    # ── v3 TVL (4373500): uma linha por dia ───────────────────────────────────
    log.info("Seeding v3 TVL history (query 4373500)...")
    try:
        rows = client.run_query(4373500)
        for row in rows:
            snap_date = _parse_row_date(row)
            try:
                all_snapshots.append(Snapshot(snap_date, "tvl",     "v3", "", _fval(row, "tvl_usd")))
            except KeyError:
                pass
            try:
                all_snapshots.append(Snapshot(snap_date, "tvl_eth", "v3", "", _fval(row, "tvl_eth")))
            except KeyError:
                pass
        log.info("v3 TVL: %d rows", len(rows))
    except Exception as exc:
        log.error("Failed to seed v3 TVL: %s", exc)

    # ── v3_pools_by_type (4353295): uma linha por (data, version) ─────────────
    log.info("Seeding v3_pools_by_type history (query 4353295)...")
    try:
        rows = client.run_query(4353295)
        for row in rows:
            snap_date = _parse_row_date(row)
            version   = str(row.get("version", "")).strip()
            scope_val = f"v{version}"
            for metric, col in [
                ("daily_swap_volume", "daily_volume_usd"),
                ("daily_fees",        "daily_fees_usd"),
                ("tvl",               "daily_tvl_usd"),
            ]:
                try:
                    all_snapshots.append(Snapshot(snap_date, metric, "pool_type", scope_val, _fval(row, col)))
                except KeyError:
                    pass
        log.info("v3_pools_by_type: %d rows", len(rows))
    except Exception as exc:
        log.error("Failed to seed v3_pools_by_type: %s", exc)

    # ── global_volume_by_version (22261): uma linha por (semana, versao) ──────
    log.info("Seeding global_volume_by_version history (query 22261, cached)...")
    try:
        rows = client.get_latest_result(22261)
        for row in rows:
            snap_date = _parse_row_date(row)
            version   = str(row.get("version", "")).strip().lower()
            if not version:
                continue
            try:
                val = _fval(row, "volume", "volume_usd", "Volume")
                all_snapshots.append(Snapshot(snap_date, "weekly_volume_by_version", "version", version, val))
            except KeyError:
                pass
        # Cumulative totals (per version + global)
        cumulative_snaps = [
            s for s in _parse_global_volume_by_version(rows, "global")
            if s.metric_name == "cumulative_volume"
        ]
        all_snapshots.extend(cumulative_snaps)
        log.info("global_volume_by_version: %d rows → %d weekly + %d cumulative", len(rows), len(rows), len(cumulative_snaps))
    except Exception as exc:
        log.error("Failed to seed global_volume_by_version: %s", exc)

    # ── v3_fees_daily (4373539): cols date, fee_type, fees_usd ───────────────
    # Multiple rows per day (one per fee_type). Sum per day for ATH tracking.
    log.info("Seeding v3_fees_daily history (query 4373539, cached)...")
    try:
        rows = client.get_latest_result(4373539)
        fee_keys = ("fees_usd", "fees", "fee_amount", "total_fees_usd")
        # Collapse fee_types per day → one daily_fees snapshot per day
        daily_totals: dict[date, float] = {}
        for row in rows:
            d = _parse_row_date(row)
            try:
                daily_totals[d] = daily_totals.get(d, 0.0) + _fval(row, *fee_keys)
            except KeyError:
                pass
        for d, total in daily_totals.items():
            all_snapshots.append(Snapshot(d, "daily_fees", "v3", "", total))
        # Period aggregates (weekly, monthly, cumulative)
        period_snaps = [s for s in _parse_v3_fees_daily(rows, "v3") if s.metric_name != "daily_fees"]
        all_snapshots.extend(period_snaps)
        log.info("v3_fees_daily: %d rows → %d daily + %d period snapshots", len(rows), len(daily_totals), len(period_snaps))
    except Exception as exc:
        log.error("Failed to seed v3_fees_daily: %s", exc)

    # ── v3_volume_by_chain (4373470): parser computes current aggregates ───────
    log.info("Seeding v3_volume_by_chain aggregates (query 4373470, cached)...")
    try:
        rows = client.get_latest_result(4373470)
        snaps = _parse_v3_volume_by_chain(rows, "v3")
        all_snapshots.extend(snaps)
        log.info("v3_volume_by_chain: %d rows → %d snapshots", len(rows), len(snaps))
    except Exception as exc:
        log.error("Failed to seed v3_volume_by_chain: %s", exc)

    # ── v3_volume_by_pool_type (4373461): parser computes current aggregates ───
    log.info("Seeding v3_volume_by_pool_type aggregates (query 4373461, cached)...")
    try:
        rows = client.get_latest_result(4373461)
        snaps = _parse_v3_volume_by_pool_type(rows, "v3")
        all_snapshots.extend(snaps)
        log.info("v3_volume_by_pool_type: %d rows → %d snapshots", len(rows), len(snaps))
    except Exception as exc:
        log.error("Failed to seed v3_volume_by_pool_type: %s", exc)

    # ── v3_tvl_by_chain (4373530): latest TVL per chain ───────────────────────
    log.info("Seeding v3_tvl_by_chain (query 4373530, cached)...")
    try:
        rows = client.get_latest_result(4373530)
        snaps = _parse_v3_tvl_by_chain(rows, "v3")
        all_snapshots.extend(snaps)
        log.info("v3_tvl_by_chain: %d rows → %d snapshots", len(rows), len(snaps))
    except Exception as exc:
        log.error("Failed to seed v3_tvl_by_chain: %s", exc)

    log.info("Historical seed total: %d snapshots", len(all_snapshots))
    return all_snapshots
