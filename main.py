"""
Balancer Dune Monitor – main entry point.

Usage
-----
  Run the pipeline once (now):
      python main.py run

  Run the pipeline for a specific date (replay / back-fill):
      python main.py run --date 2025-01-10

  Start the scheduler daemon (blocks):
      python main.py schedule

  Show database stats:
      python main.py stats

  List recent reports:
      python main.py reports
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

from config import DB_PATH, REPORT_DIR
from database import get_db, init_db
from logger_setup import get_logger, setup_logging
from pipeline import run_pipeline

log = get_logger(__name__)


# ── Sub-commands ──────────────────────────────────────────────────────────────

def cmd_run(args: argparse.Namespace) -> int:
    """Execute one pipeline run."""
    target_date: date | None = None
    if args.date:
        try:
            target_date = date.fromisoformat(args.date)
        except ValueError:
            print(f"ERROR: invalid date format '{args.date}'. Use YYYY-MM-DD.", file=sys.stderr)
            return 1

    try:
        report_path = run_pipeline(today=target_date)
        print(f"\nReport saved to: {report_path}")
        return 0
    except Exception as exc:
        log.exception("Pipeline failed: %s", exc)
        return 1


def cmd_schedule(_args: argparse.Namespace) -> int:
    """Start the daily scheduler daemon."""
    from scheduler import start_scheduler
    start_scheduler()
    return 0  # never reached unless scheduler exits cleanly


def cmd_stats(_args: argparse.Namespace) -> int:
    """Print database statistics."""
    init_db()
    with get_db() as conn:
        snap_count = conn.execute("SELECT COUNT(*) FROM daily_snapshots").fetchone()[0]
        ath_count  = conn.execute("SELECT COUNT(*) FROM ath_records").fetchone()[0]
        ms_count   = conn.execute("SELECT COUNT(*) FROM reported_milestones").fetchone()[0]

        metrics = conn.execute(
            "SELECT metric_name, scope_type, COUNT(*) as c "
            "FROM daily_snapshots GROUP BY metric_name, scope_type "
            "ORDER BY metric_name"
        ).fetchall()

        earliest = conn.execute(
            "SELECT MIN(snap_date) FROM daily_snapshots"
        ).fetchone()[0]
        latest = conn.execute(
            "SELECT MAX(snap_date) FROM daily_snapshots"
        ).fetchone()[0]

    print(f"\n{'='*50}")
    print(f"  Database: {DB_PATH}")
    print(f"{'='*50}")
    print(f"  Snapshots:  {snap_count}")
    print(f"  ATH records:{ath_count}")
    print(f"  Milestones: {ms_count}")
    print(f"  Date range: {earliest} → {latest}")
    print(f"\n  Metrics tracked:")
    for row in metrics:
        print(f"    {row[0]:40s} [{row[1]:10s}]  {row[2]} days")
    print()
    return 0


def cmd_inspect(_args: argparse.Namespace) -> int:
    """Mostra todos os dados extraídos e ATHs para verificação manual."""
    import sys as _sys
    if hasattr(_sys.stdout, "reconfigure"):
        _sys.stdout.reconfigure(errors="replace")
    from database import get_all_snapshots, get_all_aths
    init_db()
    with get_db() as conn:
        snaps = get_all_snapshots(conn)
        aths  = get_all_aths(conn)

    # ── Snapshots ──────────────────────────────────────────────────────────
    print(f"\n{'='*90}")
    print(f"  SNAPSHOTS EXTRAÍDOS  ({len(snaps)} registros)")
    print(f"{'='*90}")
    print(f"  {'Data':<12} {'Métrica':<38} {'Scope':<20} {'Valor':>16}")
    print(f"  {'-'*12} {'-'*38} {'-'*20} {'-'*16}")
    for s in snaps:
        scope = f"{s['scope_type']}:{s['scope_value']}" if s['scope_value'] else s['scope_type']
        val   = f"{s['value']:>16,.2f}"
        print(f"  {str(s['snap_date']):<12} {s['metric_name']:<38} {scope:<20} {val}")

    # ── ATHs ───────────────────────────────────────────────────────────────
    print(f"\n{'='*90}")
    print(f"  ATHs REGISTRADOS  ({len(aths)} registros)")
    print(f"{'='*90}")
    print(f"  {'Métrica':<38} {'Scope':<20} {'ATH Valor':>16} {'Data ATH':<12}")
    print(f"  {'-'*38} {'-'*20} {'-'*16} {'-'*12}")
    for a in aths:
        scope = f"{a['scope_type']}:{a['scope_value']}" if a['scope_value'] else a['scope_type']
        val   = f"{a['ath_value']:>16,.2f}"
        print(f"  {a['metric_name']:<38} {scope:<20} {val} {str(a['ath_date']):<12}")

    print(f"\n  Compare esses valores com https://dune.com/balancer/v3\n")
    return 0


def cmd_seed(_args: argparse.Namespace) -> int:
    """Baixa TODO o histórico do Dune e popula o banco + recalcula ATHs corretamente."""
    from dune_client import DuneClient
    from extractor import extract_all_history
    from ath_engine import rebuild_aths_from_snapshots
    from database import upsert_snapshot

    init_db()
    client = DuneClient()

    print("\nBaixando histórico completo do Dune (pode demorar 2-3 min)...")
    snapshots = extract_all_history(client)
    print(f"Extraídos {len(snapshots)} snapshots históricos")

    with get_db() as conn:
        saved = 0
        for snap in snapshots:
            try:
                upsert_snapshot(
                    conn, snap.snap_date, snap.metric_name,
                    snap.scope_type, snap.scope_value, snap.value,
                )
                saved += 1
            except Exception as exc:
                log.error("Failed to save snapshot %s: %s", snap, exc)

        print(f"Salvos {saved} snapshots no banco")

        n = rebuild_aths_from_snapshots(conn)
        print(f"ATHs recalculados: {n} registros")

    print("\nPronto! Rode 'python main.py inspect' para verificar os ATHs corretos.\n")
    return 0


def cmd_reports(_args: argparse.Namespace) -> int:
    """List the 10 most recent report files."""
    reports = sorted(REPORT_DIR.glob("report_*.json"), reverse=True)[:10]
    if not reports:
        print("No reports found.")
        return 0

    print(f"\n{'='*50}")
    print("  Recent Reports")
    print(f"{'='*50}")
    for rp in reports:
        try:
            data = json.loads(rp.read_text(encoding="utf-8"))
            print(
                f"  {rp.name}  –  "
                f"{data.get('total_events', '?')} events  "
                f"(generated {data.get('generated_at', '?')[:19]})"
            )
        except Exception:
            print(f"  {rp.name}  [unreadable]")
    print()
    return 0


# ── Argument parser ───────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="balancer-monitor",
        description="Balancer Dune Analytics Monitor",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # run
    p_run = sub.add_parser("run", help="Execute one pipeline run immediately")
    p_run.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Override today's date (for back-fill / testing)",
    )
    p_run.set_defaults(func=cmd_run)

    # schedule
    p_sched = sub.add_parser("schedule", help="Start the daily scheduler daemon")
    p_sched.set_defaults(func=cmd_schedule)

    # stats
    p_stats = sub.add_parser("stats", help="Show database statistics")
    p_stats.set_defaults(func=cmd_stats)

    # reports
    p_rep = sub.add_parser("reports", help="List recent report files")
    p_rep.set_defaults(func=cmd_reports)

    # inspect
    p_ins = sub.add_parser("inspect", help="Mostrar dados extraídos e ATHs para verificação")
    p_ins.set_defaults(func=cmd_inspect)

    # seed
    p_seed = sub.add_parser("seed", help="Baixar histórico completo do Dune e recalcular ATHs")
    p_seed.set_defaults(func=cmd_seed)

    return parser


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    setup_logging()
    parser = build_parser()
    args = parser.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
