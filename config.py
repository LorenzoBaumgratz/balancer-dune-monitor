"""
Central configuration for the Balancer Dune Monitor.
All tuneable parameters live here.
"""

import os
from pathlib import Path

# ── Carrega .env se existir (sem dependências externas) ──────────────────
env_file = Path(__file__).parent / ".env"
if env_file.exists():
    for line in env_file.read_text().strip().split("\n"):
        if "=" in line and not line.startswith("#"):
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())


# ── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent
DATA_DIR   = BASE_DIR / "data"
DB_PATH    = DATA_DIR / "balancer_monitor.db"
REPORT_DIR = DATA_DIR / "reports"
LOG_DIR    = BASE_DIR / "logs"

for _d in (DATA_DIR, REPORT_DIR, LOG_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ── Claude Vision API (Anthropic) ─────────────────────────────────────────
CLAUDE_API_KEY: str = os.environ.get("CLAUDE_API_KEY", "")
CLAUDE_MODEL = "claude-3-5-sonnet-20241022"  # Vision-capable model (free tier)

# ── Dashboard URLs to monitor ──────────────────────────────────────────────────
DASHBOARDS = {
    "balancer_v3": "https://dune.com/balancer/v3",
    "balancer_pools": "https://dune.com/balancer/pools",
    "balancer_pool_analysis": "https://dune.com/balancer/pool-analysis",
}

SCREENSHOTS_DIR = DATA_DIR / "screenshots"
SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)

# ── Dune API ─────────────────────────────────────────────────────────────────

DUNE_API_KEY: str = os.environ.get("DUNE_API_KEY", "")


# Base URL for Dune Analytics API v1
DUNE_API_BASE = "https://api.dune.com/api/v1"

# How long to wait (seconds) between polling a query execution
DUNE_POLL_INTERVAL = 5
# Maximum seconds to wait for a single query before giving up
DUNE_TIMEOUT = 300

# ── Scheduler ────────────────────────────────────────────────────────────────
SCHEDULE_TIMEZONE = "America/Sao_Paulo"
SCHEDULE_HOUR     = 8
SCHEDULE_MINUTE   = 30

# ── Milestones ───────────────────────────────────────────────────────────────
import math as _math

def milestone_step(value: float) -> float:
    """
    Return the milestone step for *value*: always a multiple of 5 at the
    relevant order of magnitude.

    Examples:
      $17.8B  → $5B   (milestones at $15B, $20B, $25B …)
      $68.9M  → $50M  (milestones at $50M, $100M, $150M …)
      $1.4M   → $500K (milestones at $500K, $1M, $1.5M …)
      2 637   → 500   (milestones at 500, 1000, 1500, 2000 …)
    """
    if value <= 0:
        return 1.0
    return 5 * (10 ** _math.floor(_math.log10(value / 5)))

# ── Dune Query IDs ───────────────────────────────────────────────────────────
# These query IDs map to saved Dune queries that return the required metrics.
# You MUST replace the placeholder values with your own saved-query IDs
# (or use the Dune API to run SQL on-the-fly via query.run()).
#
# Format:
#   DUNE_QUERIES["<logical_name>"] = <query_id: int>
#
# Logical names consumed by extractor.py:

DUNE_QUERIES: dict[str, int] = {
    # GLOBAL
#   "global_summary":             ENCONTRAR,    # precisa achar
#    "global_volume_by_chain":     ENCONTRAR,
#    "global_volume_by_pool_type": ENCONTRAR,
    "global_pools_created":       2617646,      # ← Balancer Pools Created, by Blockchain
#    "global_pools_by_type":       ENCONTRAR,

    # V3
    "v3_summary":                 4373453,
    "v3_volume_by_chain":         4373470,
    "v3_volume_by_pool_type":     4373461,
    "v3_pools_created":           4373500,
    "v3_pools_by_type":           4353295,
}

# Queries fetched via get_latest_result() — no new execution triggered,
# reads the latest cached result. Use for public third-party queries
# (e.g. official Balancer Dune queries) to save API credits.
DUNE_QUERIES_CACHED: dict[str, int] = {
    "global_volume_by_version":   22261,        # ← Volume (USD) by Version, Weekly (V1+V2+V3)
    # "global_fees_by_version":   TOFIND,       # ← pending: cumulative fees all versions
    # "global_tvl_by_chain":      TOFIND,       # ← pending: TVL by chain all versions
    # "global_volume_by_chain":   TOFIND,       # ← pending: volume by chain all versions
}



# ── Logging ──────────────────────────────────────────────────────────────────
LOG_LEVEL  = "INFO"
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
LOG_FILE   = LOG_DIR / "monitor.log"
