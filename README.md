# Balancer Dune Monitor

A production-ready Python system that monitors Balancer V3 dashboards on Dune
Analytics, detects daily All-Time-Highs and cumulative milestones, and outputs
structured JSON reports.

---

## Project structure

```
bot dune balancer/
├── main.py              ← CLI entry point  (python main.py <command>)
├── scheduler.py         ← APScheduler daemon (08:30 America/Sao_Paulo)
├── pipeline.py          ← Orchestrates one full run
├── config.py            ← All tuneable settings (query IDs, paths, schedule)
├── logger_setup.py      ← Logging configuration
│
├── dune_client.py       ← Dune Analytics REST API v1 client
├── extractor.py         ← Query → Snapshot parser layer
│
├── database.py          ← SQLite schema + CRUD helpers
├── ath_engine.py        ← ATH detection logic
├── milestone_engine.py  ← Milestone crossing detection logic
├── classifier.py        ← Event classification + priority scoring
├── reporter.py          ← JSON report generation
│
├── .env.example         ← Template — copy to .env and add your API key
├── requirements.txt
├── run_bot.bat          ← Windows Task Scheduler helper
├── data/
│   ├── balancer_monitor.db   ← SQLite database (auto-created)
│   └── reports/              ← JSON reports (one file per run)
├── logs/
│   └── monitor.log           ← Rotating log file
└── docs/
    └── balancer-monitor.service  ← systemd unit file (Linux)
```

---

## Quick start

### 1. Requirements

- Python 3.11+
- A **Dune Analytics API key** — free tier works fine

### 2. Install

```bash
git clone https://github.com/<your-user>/balancer-dune-monitor.git
cd balancer-dune-monitor
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Configure

Copy the example env file and fill in your Dune API key:

```bash
# Linux / macOS
cp .env.example .env

# Windows (PowerShell)
Copy-Item .env.example .env
```

Then edit `.env`:

```
DUNE_API_KEY=your_dune_api_key_here
```

> Get your free Dune API key at https://dune.com/settings/api

> **⚠️ Never commit `.env`** — it is listed in `.gitignore` and must stay local.

### 4. Seed historical data (first run only)

On first install, download the full query history so ATH records start from the
correct baseline (not just today's values):

```bash
python main.py seed
```

This takes ~2–3 minutes and stores ~2 000+ historical snapshots.
You only need to run this once.

### 5. Run once (test)

```bash
python main.py run
```

Executes the full pipeline immediately and writes a report to
`data/reports/report_YYYY-MM-DD.json`.

### 6. Automate daily runs

**Windows — Task Scheduler:**

```powershell
schtasks /create /tn "BalancerMonitor" /tr "\"C:\path\to\run_bot.bat\"" /sc daily /st 08:30 /f
```

Edit `run_bot.bat` to match your project path, then verify in Task Scheduler.

**Linux — systemd** (see `docs/balancer-monitor.service`):

```bash
sudo cp docs/balancer-monitor.service /etc/systemd/system/
sudo systemctl enable --now balancer-monitor
```

---

## CLI commands

| Command | Description |
|---------|-------------|
| `python main.py run` | Run pipeline once (right now) |
| `python main.py run --date 2025-01-10` | Run for a specific date (back-fill) |
| `python main.py seed` | Download full history + recalculate ATHs *(run once after install)* |
| `python main.py inspect` | Print all stored snapshots and ATH records for manual verification |
| `python main.py schedule` | Start the daily daemon (blocks until stopped) |
| `python main.py stats` | Show DB statistics |
| `python main.py reports` | List recent report files |

---

## Dune queries used

These public Balancer V3 queries are already configured in `config.py` — no
changes needed for the default setup:

| Logical name | Query ID | Returns |
|---|---|---|
| `v3_summary` | [4373453](https://dune.com/queries/4373453) | Daily volume, swaps |
| `v3_volume_by_chain` | [4373470](https://dune.com/queries/4373470) | Weekly volume per chain |
| `v3_volume_by_pool_type` | [4373461](https://dune.com/queries/4373461) | Weekly volume per pool type |
| `v3_pools_created` | [4373500](https://dune.com/queries/4373500) | Daily TVL (USD + ETH) |
| `v3_pools_by_type` | [4353295](https://dune.com/queries/4353295) | Daily metrics per pool type |
| `global_pools_created` | [2617646](https://dune.com/queries/2617646) | Cumulative pools per chain |

---

## Database schema

```sql
-- One row per (date, metric, scope_type, scope_value)
daily_snapshots (
    snap_date   TEXT,     -- YYYY-MM-DD
    metric_name TEXT,
    scope_type  TEXT,     -- global | v3 | chain | pool_type
    scope_value TEXT,     -- '' | 'ethereum' | 'weighted' etc.
    value       REAL
)

-- Current ATH per (metric, scope_type, scope_value)
ath_records (
    metric_name TEXT,
    scope_type  TEXT,
    scope_value TEXT,
    ath_value   REAL,
    ath_date    TEXT
)

-- Deduplication guard for reported milestones
reported_milestones (
    metric_name     TEXT,
    scope_type      TEXT,
    scope_value     TEXT,
    milestone_value REAL,
    reported_date   TEXT
)
```

---

## Report format

```json
{
  "generated_at": "2025-01-15T08:32:07-03:00",
  "report_date":  "2025-01-15",
  "total_events": 3,
  "events_by_type": {
    "ath_daily":             1,
    "milestone_cumulative":  1,
    "v3_specific_milestone": 1,
    "chain_specific_ath":    0,
    "pool_type_ath":         0
  },
  "events": [
    {
      "event_type":     "ath_daily",
      "metric":         "daily_swap_volume",
      "scope_type":     "v3",
      "scope_value":    "",
      "current_value":  12345678.9,
      "previous_value": 11234567.0,
      "snap_date":      "2025-01-15",
      "explanation":    "New all-time high for Daily Swap Volume: $12.35M (+9.9% vs previous ATH of $11.23M set on 2024-12-01).",
      "priority_score": 0.432
    }
  ]
}
```

### Event types

| event_type | When emitted |
|------------|-------------|
| `ath_daily` | Daily metric breaks its all-time high (global or v3) |
| `milestone_cumulative` | Global cumulative metric crosses a step boundary |
| `v3_specific_milestone` | V3-specific cumulative or ATH event |
| `chain_specific_ath` | Per-chain daily metric breaks its ATH |
| `pool_type_ath` | Per-pool-type daily metric breaks its ATH |

### Priority scoring

- **ATH events**: `log(1 + Δ%) / log(11)`, capped at 1.0 (first-ever ATH → 0.5)
- **Milestones**: `log10(milestone_value) / log10(10_000_000_000)`, capped at 1.0

---

## Adding new metrics

1. Create a saved Dune query that returns the required columns.
2. Add its ID to `config.DUNE_QUERIES` with a logical name.
3. Write a `_parse_<name>(rows, scope)` function in `extractor.py`.
4. Register it in `extractor.PARSERS`.
5. If the metric is daily → it will be auto-evaluated for ATH.
6. If cumulative → it will be auto-evaluated for milestones.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `DUNE_API_KEY is not set` | Missing `.env` file | Copy `.env.example` → `.env` and add your key |
| `DuneTimeoutError` | Query takes >5 min | Increase `DUNE_TIMEOUT` in config or optimise query |
| `DuneAPIError: HTTP 429` | Rate limit exceeded | Reduce run frequency or upgrade Dune plan |
| `No snapshots extracted` | Wrong query IDs | Check `config.DUNE_QUERIES` against your Dune account |
| `KeyError` in parser | Column name mismatch | Add the actual column name to the parser's accepted keys |
| Report has 0 events | No history in DB | Run `python main.py seed` first to load historical data |
| ATH looks wrong (too low) | Seed not run yet | Run `python main.py seed`, then `python main.py inspect` to verify |
