"""
Catálogo de todas as queries Dune usadas pelo Balancer Dune Monitor.

Para cada query está documentado:
  - id          : ID numérico da query no Dune Analytics
  - fetch       : "run_query" (execução fresca, consome créditos) ou
                  "get_latest_result" (resultado cacheado, sem custo)
  - scope       : "v3" | "global" | "all_versions"
  - granularity : granularidade temporal dos dados retornados
  - description : o que a query retorna em linguagem natural
  - columns     : colunas conhecidas retornadas pela query
  - used_for    : métricas (metric_name) que extraímos desta query
  - status      : "active" | "pending" (ID ainda não encontrado)

Queries com status "pending" estão comentadas — adicione o ID quando encontrar.
"""

QUERY_CATALOG: dict[str, dict] = {

    # ── V3 ONLY ───────────────────────────────────────────────────────────────

    "v3_summary": {
        "id":          4373453,
        "fetch":       "run_query",
        "scope":       "v3",
        "granularity": "daily",
        "description": (
            "Resumo diário da V3: volume de swap, número de swaps, volume acumulado all-time, "
            "médias móveis de volume (30d/50d/100d/200d)."
        ),
        "columns": [
            "day", "Volume", "Swaps", "All-Time Volume",
            "Avg. Volume per Swap", "30d Vol SMA", "50d Vol SMA",
            "100d Vol SMA", "200d Vol SMA", "avg_volume",
        ],
        "used_for": [
            "daily_swap_volume",    # volume de swap do dia (scope v3)
            "daily_swaps",          # número de swaps do dia (scope v3)
            "cumulative_volume",    # all-time volume V3 (scope v3)
            "monthly_swap_volume",  # volume acumulado no mês corrente (scope v3)
            "monthly_swaps",        # swaps acumulados no mês corrente (scope v3)
            "cumulative_swaps",     # total histórico de swaps V3 (scope v3)
        ],
        "status": "active",
    },

    "v3_volume_by_chain": {
        "id":          4373470,
        "fetch":       "get_latest_result",
        "scope":       "v3",
        "granularity": "daily",
        "description": (
            "Volume de swap diário por blockchain na V3, desde o lançamento. "
            "Uma linha por (dia, blockchain)."
        ),
        "columns": ["week", "blockchain", "volume"],
        "used_for": [
            "weekly_volume_by_chain",  # volume da semana corrente por chain (scope chain, V3)
            "monthly_swap_volume",     # volume do mês corrente por chain (scope chain, V3)
            "cumulative_volume",       # volume all-time por chain (scope chain, V3)
            "weekly_swap_volume",      # volume semanal total V3 (soma de chains, scope v3)
            "monthly_swap_volume",     # volume mensal total V3 (soma de chains, scope v3)
        ],
        "status": "active",
    },

    "v3_volume_by_pool_type": {
        "id":          4373461,
        "fetch":       "get_latest_result",
        "scope":       "v3",
        "granularity": "daily",
        "description": (
            "Volume de swap diário por tipo de pool na V3, desde o lançamento. "
            "Uma linha por (dia, pool_type). "
            "Tipos: weighted, stable, ECLP, LBP, reClAMM, etc."
        ),
        "columns": ["week", "pool_type", "volume"],
        "used_for": [
            "weekly_volume_by_pool_type",  # volume semanal por tipo de pool (scope pool_type)
            "monthly_swap_volume",         # volume mensal por tipo de pool (scope pool_type)
            "cumulative_volume",           # volume all-time por tipo de pool (scope pool_type)
        ],
        "status": "active",
    },

    "v3_tvl": {
        "id":          4373500,
        "fetch":       "run_query",
        "scope":       "v3",
        "granularity": "daily",
        "description": "TVL diário total da V3 em USD e ETH.",
        "columns":     ["day", "tvl_usd", "tvl_eth"],
        "used_for": [
            "tvl",      # TVL atual V3 em USD (scope v3)
            "tvl_eth",  # TVL atual V3 em ETH (scope v3)
        ],
        "status": "active",
    },

    "v3_tvl_by_chain": {
        "id":          4373530,
        "fetch":       "get_latest_result",
        "scope":       "v3",
        "granularity": "daily",
        "description": (
            "TVL diário por blockchain na V3, desde o lançamento. "
            "Uma linha por (dia, blockchain)."
        ),
        "columns": ["day", "blockchain", "tvl_usd"],   # tvl_eth pode existir também
        "used_for": [
            "tvl",  # TVL atual por chain V3 (scope chain) — ATH detectado automaticamente
        ],
        "status": "active",
    },

    "v3_fees_daily": {
        "id":          4373539,
        "fetch":       "get_latest_result",
        "scope":       "v3",
        "granularity": "daily",
        "description": (
            "Fees diárias da V3 desde o lançamento. "
            "Colunas exatas a confirmar no primeiro fetch."
        ),
        "columns": ["date", "fee_type", "fees_usd"],   # confirmado: múltiplas linhas por dia (swap_fee, yield_fee, etc.)
        "used_for": [
            "daily_fees",       # fees do dia (scope v3)
            "weekly_fees",      # fees da semana corrente (scope v3)
            "monthly_fees",     # fees do mês corrente (scope v3)
            "cumulative_fees",  # fees acumuladas all-time V3 (scope v3)
        ],
        "status": "active",
    },

    "v3_pools_by_type": {
        "id":          4353295,
        "fetch":       "run_query",
        "scope":       "v3",
        "granularity": "daily",
        "description": (
            "Métricas diárias por versão de pool na V3: volume, fees, TVL — diário e acumulado. "
            "Inclui dominância relativa entre versões."
        ),
        "columns": [
            "block_date", "version",
            "daily_volume_usd", "daily_fees_usd", "daily_tvl_usd", "daily_tvl_eth",
            "total_volume_usd", "total_fees_usd", "total_tvl_usd", "total_tvl_eth",
            "volume_dominance_pct", "fees_dominance_pct", "tvl_dominance_pct",
        ],
        "used_for": [
            "daily_swap_volume",  # volume diário por versão de pool (scope pool_type)
            "daily_fees",         # fees diárias por versão de pool (scope pool_type)
            "tvl",                # TVL por versão de pool (scope pool_type)
            "cumulative_volume",  # volume acumulado por versão de pool (scope pool_type)
            "cumulative_fees",    # fees acumuladas por versão de pool (scope pool_type)
        ],
        "status": "active",
    },

    # ── GLOBAL (todas as versões) ─────────────────────────────────────────────

    "global_pools_created": {
        "id":          2617646,
        "fetch":       "run_query",
        "scope":       "all_versions",
        "granularity": "weekly",
        "description": (
            "Número de pools criadas por blockchain, todas as versões. "
            "Uma linha por (semana, blockchain)."
        ),
        "columns": ["blockchain", "pools_registered", "week"],
        "used_for": [
            "cumulative_pools_created",  # total acumulado por chain (scope chain)
            "cumulative_pools_created",  # total global acumulado (scope global)
        ],
        "status": "active",
    },

    "global_volume_by_version": {
        "id":          22261,
        "fetch":       "get_latest_result",
        "scope":       "all_versions",
        "granularity": "weekly",
        "description": (
            "Volume de swap semanal por versão (V1, V2, V3). "
            "Uma linha por (semana, version). Cobre toda a história da Balancer."
        ),
        "columns": ["week", "version", "volume"],
        "used_for": [
            "weekly_volume_by_version",  # volume semanal por versão (scope version)
            "cumulative_volume",         # volume acumulado por versão (scope version)
            "cumulative_volume",         # volume acumulado total global (scope global)
        ],
        "status": "active",
    },

    # ── PENDING — queries ainda não encontradas ───────────────────────────────

    "global_fees_by_version": {
        "id":          None,   # TOFIND
        "fetch":       "get_latest_result",
        "scope":       "all_versions",
        "granularity": "weekly",
        "description": (
            "Fees acumuladas por versão (V1+V2+V3). "
            "Esperado: uma linha por (semana, version) com coluna fees ou fees_usd."
        ),
        "columns": ["week", "version", "fees"],   # colunas esperadas
        "used_for": [
            "cumulative_fees",  # fees acumuladas por versão (scope version)
            "cumulative_fees",  # fees acumuladas total global (scope global)
        ],
        "status": "pending",
    },

    "global_volume_by_chain": {
        "id":          None,   # TOFIND
        "fetch":       "get_latest_result",
        "scope":       "all_versions",
        "granularity": "weekly",
        "description": (
            "Volume de swap por blockchain, TODAS as versões somadas (V1+V2+V3). "
            "Diferente do v3_volume_by_chain que é só V3."
        ),
        "columns": ["week", "blockchain", "volume"],   # colunas esperadas
        "used_for": [
            "weekly_volume_by_chain",  # volume semanal por chain, todas versões (scope chain)
            "cumulative_volume",       # volume acumulado por chain, todas versões (scope chain)
        ],
        "status": "pending",
    },

    "global_tvl_by_chain": {
        "id":          None,   # TOFIND
        "fetch":       "get_latest_result",
        "scope":       "all_versions",
        "granularity": "daily",
        "description": (
            "TVL por blockchain, TODAS as versões (V1+V2+V3). "
            "Diferente do v3_tvl_by_chain que é só V3."
        ),
        "columns": ["day", "blockchain", "tvl_usd"],   # colunas esperadas
        "used_for": [
            "tvl",  # TVL atual por chain, todas versões (scope chain)
            "tvl",  # TVL total global, todas versões (scope global)
        ],
        "status": "pending",
    },
}
