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
"""

from __future__ import annotations

import math
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


# ── Per-query parsers ─────────────────────────────────────────────────────────

def _parse_v3_summary(rows: list[dict], scope: str) -> list[Snapshot]:
    """
    Query 4373453 – v3_summary
    Columns: day, Volume, Swaps, All-Time Volume, avg_volume, ...SMA fields
    """
    row = _most_recent_row(rows)
    if row is None:
        return []

    snap_date = _parse_row_date(row)
    snaps: list[Snapshot] = []

    # Daily volume
    try:
        val = _fval(row, "Volume", "volume")
        snaps.append(Snapshot(snap_date, "daily_swap_volume", scope, "", val))
    except KeyError:
        log.debug("daily_swap_volume not found in v3_summary")

    # Daily swaps
    try:
        val = _fval(row, "Swaps", "swaps")
        snaps.append(Snapshot(snap_date, "daily_swaps", scope, "", val))
    except KeyError:
        log.debug("daily_swaps not found in v3_summary")

    # Cumulative / all-time volume
    try:
        val = _fval(row, "All-Time Volume", "all_time_volume")
        snaps.append(Snapshot(snap_date, "cumulative_volume", scope, "", val))
    except KeyError:
        log.debug("cumulative_volume not found in v3_summary")

    return snaps


def _parse_v3_volume_by_chain(rows: list[dict], scope: str) -> list[Snapshot]:
    """
    Query 4373470 – v3_volume_by_chain
    Columns: blockchain, volume, week
    Returns most recent week per blockchain.
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
        snap_date = _parse_row_date(row)
        try:
            val = _fval(row, "volume", "Volume")
            snaps.append(Snapshot(snap_date, "weekly_volume_by_chain", "chain", chain, val))
        except KeyError:
            pass

    return snaps


def _parse_v3_volume_by_pool_type(rows: list[dict], scope: str) -> list[Snapshot]:
    """
    Query 4373461 – v3_volume_by_pool_type
    Columns: pool_type, volume, week
    Returns most recent week per pool_type.
    """
    snaps: list[Snapshot] = []
    recent_rows = _most_recent_rows_per_group(rows, "pool_type")

    for row in recent_rows:
        raw_ptype = row.get("pool_type")
        if raw_ptype is None:
            continue
        ptype = str(raw_ptype).strip()
        if not ptype:
            continue
        snap_date = _parse_row_date(row)
        try:
            val = _fval(row, "volume", "Volume")
            snaps.append(Snapshot(snap_date, "weekly_volume_by_pool_type", "pool_type", ptype, val))
        except KeyError:
            pass

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
    Returns the most recent week per version.
    """
    snaps: list[Snapshot] = []
    recent_rows = _most_recent_rows_per_group(rows, "version")

    for row in recent_rows:
        version = str(row.get("version", "")).strip().lower()
        if not version:
            continue
        snap_date = _parse_row_date(row)
        try:
            val = _fval(row, "volume", "volume_usd", "Volume")
            snaps.append(
                Snapshot(snap_date, "weekly_volume_by_version", "version", version, val)
            )
        except KeyError:
            pass

    return snaps


# ── Parser registry ───────────────────────────────────────────────────────────

PARSERS: dict[str, tuple[Callable[[list[dict], str], list[Snapshot]], str]] = {
    "global_pools_created":      (_parse_global_pools_created,      "global"),
    "global_volume_by_version":  (_parse_global_volume_by_version,  "global"),
    "v3_summary":                (_parse_v3_summary,                "v3"),
    "v3_volume_by_chain":        (_parse_v3_volume_by_chain,        "v3"),
    "v3_volume_by_pool_type":    (_parse_v3_volume_by_pool_type,    "v3"),
    "v3_pools_created":          (_parse_v3_tvl,                    "v3"),
    "v3_pools_by_type":          (_parse_v3_pools_by_type,          "v3"),
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
    Busca TODO o histórico das queries e retorna um Snapshot por linha/dia.
    Usado para seed inicial do banco com dados históricos corretos.
    Diferente de extract_all() que só pega o dado mais recente.
    """
    all_snapshots: list[Snapshot] = []

    # v3_summary (4373453): uma linha por dia → daily_swap_volume, daily_swaps
    log.info("Fetching historical v3_summary (query 4373453)...")
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
        log.info("v3_summary: %d rows processed", len(rows))
    except Exception as exc:
        log.error("Failed to fetch v3_summary history: %s", exc)

    # v3 TVL (4373500): uma linha por dia → tvl, tvl_eth
    log.info("Fetching historical TVL (query 4373500)...")
    try:
        rows = client.run_query(4373500)
        for row in rows:
            snap_date = _parse_row_date(row)
            try:
                val = _fval(row, "tvl_usd")
                all_snapshots.append(Snapshot(snap_date, "tvl", "v3", "", val))
            except KeyError:
                pass
            try:
                val = _fval(row, "tvl_eth")
                all_snapshots.append(Snapshot(snap_date, "tvl_eth", "v3", "", val))
            except KeyError:
                pass
        log.info("TVL history: %d rows processed", len(rows))
    except Exception as exc:
        log.error("Failed to fetch TVL history: %s", exc)

    # v3_pools_by_type (4353295): uma linha por (data, version) → daily_swap_volume, daily_fees, tvl
    log.info("Fetching historical pools_by_type (query 4353295)...")
    try:
        rows = client.run_query(4353295)
        for row in rows:
            snap_date = _parse_row_date(row)
            version = str(row.get("version", "")).strip()
            scope_val = f"v{version}"
            for metric, col in [
                ("daily_swap_volume", "daily_volume_usd"),
                ("daily_fees",        "daily_fees_usd"),
                ("tvl",               "daily_tvl_usd"),
            ]:
                try:
                    val = _fval(row, col)
                    all_snapshots.append(Snapshot(snap_date, metric, "pool_type", scope_val, val))
                except KeyError:
                    pass
        log.info("pools_by_type history: %d rows processed", len(rows))
    except Exception as exc:
        log.error("Failed to fetch pools_by_type history: %s", exc)

    # Volume by version, all time (22261): one row per (week, version)
    log.info("Fetching historical volume by version (query 22261, cached)...")
    try:
        rows = client.get_latest_result(22261)
        for row in rows:
            snap_date = _parse_row_date(row)
            version = str(row.get("version", "")).strip().lower()
            if not version:
                continue
            try:
                val = _fval(row, "volume", "volume_usd", "Volume")
                all_snapshots.append(
                    Snapshot(snap_date, "weekly_volume_by_version", "version", version, val)
                )
            except KeyError:
                pass
        log.info("volume_by_version history: %d rows processed", len(rows))
    except Exception as exc:
        log.error("Failed to fetch volume_by_version history: %s", exc)

    log.info("Historical extraction total: %d snapshots", len(all_snapshots))
    return all_snapshots
