"""
Catálogo de todas as métricas armazenadas pelo Balancer Dune Monitor.

Cada entrada documenta:
  - description      : o que a métrica representa
  - scope_types      : quais scope_types essa métrica pode ter no DB
  - update_frequency : com que frequência o valor é atualizado
                       "daily"   → atualizado a cada execução diária
                       "weekly"  → representa a semana corrente, recalculado diariamente
                       "monthly" → representa o mês corrente, recalculado diariamente
                       "static"  → calculado uma vez por seed/history run
  - tracks_ath       : se a engine de ATH monitora esta métrica
  - tracks_milestone : se a engine de milestones monitora esta métrica
  - source_queries   : query_catalog keys que alimentam esta métrica

Nota sobre ATHs:
  Os registros de ATH ficam na tabela `ath_records` (não em daily_snapshots).
  Eles são atualizados APENAS quando o valor atual supera o ATH anterior.
  Métricas com tracks_ath=True são monitoradas automaticamente.

Nota sobre métricas mensais/semanais:
  São recalculadas a cada execução diária usando TODOS os dados históricos
  disponíveis na query. O valor representa o total acumulado no período
  corrente (semana ISO / mês calendário). Isso significa que no início de
  um período o valor começa baixo e cresce ao longo do período.
"""

METRICS_CATALOG: dict[str, dict] = {

    # ── VOLUME ────────────────────────────────────────────────────────────────

    "daily_swap_volume": {
        "description":      "Volume de swap num único dia",
        "scope_types":      ["v3", "pool_type"],
        "update_frequency": "daily",
        "tracks_ath":       True,
        "tracks_milestone": False,
        "source_queries":   ["v3_summary", "v3_pools_by_type"],
    },

    "weekly_swap_volume": {
        "description":      "Volume de swap acumulado na semana ISO corrente (soma de chains V3)",
        "scope_types":      ["v3"],
        "update_frequency": "weekly",
        "tracks_ath":       True,
        "tracks_milestone": False,
        "source_queries":   ["v3_volume_by_chain"],
    },

    "monthly_swap_volume": {
        "description":      "Volume de swap acumulado no mês calendário corrente",
        "scope_types":      ["v3", "chain", "pool_type"],
        "update_frequency": "monthly",
        "tracks_ath":       True,
        "tracks_milestone": False,
        "source_queries":   ["v3_summary", "v3_volume_by_chain", "v3_volume_by_pool_type"],
    },

    "cumulative_volume": {
        "description":      "Volume de swap acumulado all-time desde o lançamento",
        "scope_types":      ["v3", "version", "global", "chain", "pool_type"],
        "update_frequency": "daily",
        "tracks_ath":       False,   # cumulativo só cresce — milestone é mais relevante
        "tracks_milestone": True,
        "source_queries":   [
            "v3_summary",              # scope v3
            "global_volume_by_version",# scope version + global
            "v3_volume_by_chain",      # scope chain (V3 only)
            "v3_pools_by_type",        # scope pool_type
        ],
    },

    "weekly_volume_by_chain": {
        "description":      "Volume de swap da semana corrente por blockchain (V3)",
        "scope_types":      ["chain"],
        "update_frequency": "weekly",
        "tracks_ath":       True,
        "tracks_milestone": False,
        "source_queries":   ["v3_volume_by_chain"],
    },

    "weekly_volume_by_pool_type": {
        "description":      "Volume de swap da semana corrente por tipo de pool (V3)",
        "scope_types":      ["pool_type"],
        "update_frequency": "weekly",
        "tracks_ath":       True,
        "tracks_milestone": False,
        "source_queries":   ["v3_volume_by_pool_type"],
    },

    "weekly_volume_by_version": {
        "description":      "Volume de swap da semana mais recente por versão (V1/V2/V3)",
        "scope_types":      ["version"],
        "update_frequency": "weekly",
        "tracks_ath":       True,
        "tracks_milestone": False,
        "source_queries":   ["global_volume_by_version"],
    },

    "monthly_volume_by_version": {
        "description":      "Volume de swap acumulado no mês calendário corrente por versão (V1/V2/V3)",
        "scope_types":      ["version"],
        "update_frequency": "monthly",
        "tracks_ath":       True,
        "tracks_milestone": False,
        "source_queries":   ["global_volume_by_version"],
    },

    # ── SWAPS (contagem) ──────────────────────────────────────────────────────

    "daily_swaps": {
        "description":      "Número de swaps realizados num único dia (V3)",
        "scope_types":      ["v3"],
        "update_frequency": "daily",
        "tracks_ath":       True,
        "tracks_milestone": False,
        "source_queries":   ["v3_summary"],
    },

    "monthly_swaps": {
        "description":      "Número de swaps acumulado no mês calendário corrente (V3)",
        "scope_types":      ["v3"],
        "update_frequency": "monthly",
        "tracks_ath":       True,
        "tracks_milestone": False,
        "source_queries":   ["v3_summary"],
    },

    "cumulative_swaps": {
        "description":      "Total histórico de swaps desde o lançamento da V3",
        "scope_types":      ["v3"],
        "update_frequency": "daily",
        "tracks_ath":       False,
        "tracks_milestone": True,
        "source_queries":   ["v3_summary"],
    },

    # ── TVL ───────────────────────────────────────────────────────────────────

    "tvl": {
        "description":      "Total Value Locked (TVL) em USD — snapshot do dia",
        "scope_types":      ["v3", "chain", "pool_type"],
        "update_frequency": "daily",
        "tracks_ath":       True,
        "tracks_milestone": False,
        "source_queries":   ["v3_tvl", "v3_tvl_by_chain", "v3_pools_by_type"],
    },

    "tvl_eth": {
        "description":      "Total Value Locked em ETH — snapshot do dia (V3)",
        "scope_types":      ["v3"],
        "update_frequency": "daily",
        "tracks_ath":       True,
        "tracks_milestone": False,
        "source_queries":   ["v3_tvl"],
    },

    "tvl_total": {
        "description":      "TVL total Balancer (V1+V2+V3) em USD por chain e global",
        "scope_types":      ["chain", "global"],
        "update_frequency": "daily",
        "tracks_ath":       True,
        "tracks_milestone": False,
        "source_queries":   ["global_tvl_by_chain"],
    },

    # ── FEES ─────────────────────────────────────────────────────────────────

    "daily_fees": {
        "description":      "Fees coletadas num único dia",
        "scope_types":      ["v3", "pool_type"],
        "update_frequency": "daily",
        "tracks_ath":       True,
        "tracks_milestone": False,
        "source_queries":   ["v3_fees_daily", "v3_pools_by_type"],
    },

    "weekly_fees": {
        "description":      "Fees acumuladas na semana ISO corrente (V3 global)",
        "scope_types":      ["v3"],
        "update_frequency": "weekly",
        "tracks_ath":       True,
        "tracks_milestone": False,
        "source_queries":   ["v3_fees_daily"],
    },

    "monthly_fees": {
        "description":      "Fees acumuladas no mês calendário corrente (V3 global)",
        "scope_types":      ["v3"],
        "update_frequency": "monthly",
        "tracks_ath":       True,
        "tracks_milestone": False,
        "source_queries":   ["v3_fees_daily"],
    },

    "cumulative_fees": {
        "description":      "Fees acumuladas all-time desde o lançamento",
        "scope_types":      ["v3", "pool_type"],
        "update_frequency": "daily",
        "tracks_ath":       False,
        "tracks_milestone": True,
        "source_queries":   ["v3_fees_daily", "v3_pools_by_type"],
    },

    # ── POOLS ─────────────────────────────────────────────────────────────────

    "cumulative_pools_created": {
        "description":      "Total de pools criadas acumulado desde o início (todas as versões)",
        "scope_types":      ["global", "chain"],
        "update_frequency": "daily",
        "tracks_ath":       False,
        "tracks_milestone": True,
        "source_queries":   ["global_pools_created"],
    },
}
