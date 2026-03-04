"""
Extração de dados via Ollama Vision (LOCAL - 100% GRÁTIS).

Usa Ollama + LLaVA (roda no seu PC, sem APIs externas).
Retorna dados estruturados (Snapshot-compatible).
"""

import base64
import json
import re
from pathlib import Path
from datetime import date

import requests

from extractor import Snapshot
from logger_setup import get_logger

log = get_logger(__name__)

# Ollama roda localmente em http://localhost:11434
OLLAMA_API = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "llava"


def extract_from_image(image_path: Path, dashboard_name: str) -> list[Snapshot]:
    """
    Envia imagem pro Ollama LLaVA (local), extrai números e métricas.

    Retorna lista de Snapshot objects.
    """
    if not image_path.exists():
        log.error("Image file not found: %s", image_path)
        return []

    log.info("Extracting data from image with Ollama: %s", image_path)

    try:
        # Lê imagem e codifica em base64
        with open(image_path, "rb") as f:
            image_data = base64.standard_b64encode(f.read()).decode("utf-8")

        # Prompt simples e direto para maximizar chance de JSON válido
        prompt = (
            'Look at this Balancer dashboard image and extract all numeric metrics you can see. '
            'Reply with ONLY a JSON object, no markdown, no explanation. '
            'Use exactly this format: '
            '{"metrics": [{"name": "Daily Volume", "value": 1234567.89, "unit": "USD", "date": "'
            + date.today().isoformat() + '"}, '
            '{"name": "TVL", "value": 9876543.21, "unit": "USD", "date": "'
            + date.today().isoformat() + '"}]} '
            'Extract metrics like: Volume, TVL, Fees, Swaps count, Pools count. '
            'If you cannot read any numbers, reply with: {"metrics": []}'
        )

        # Chama Ollama
        response = requests.post(
            OLLAMA_API,
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "images": [image_data],
                "stream": False,
            },
            timeout=300,
        )

        if not response.ok:
            log.error("Ollama returned error: %s", response.text)
            return []

        response_data = response.json()
        response_text = response_data.get("response", "").strip()

        if not response_text:
            log.warning("Ollama returned empty response for %s", image_path)
            return []

        log.debug("Ollama raw response: %s", response_text[:500])

        # Tenta extrair JSON mesmo que venha com texto ao redor
        data = _parse_json_response(response_text)
        if data is None:
            log.error("Could not parse JSON from Ollama response for %s", image_path)
            return []

        snapshots = []
        for metric in data.get("metrics", []):
            try:
                snap = Snapshot(
                    snap_date=date.fromisoformat(
                        metric.get("date", date.today().isoformat())
                    ),
                    metric_name=_normalize_metric_name(metric["name"]),
                    scope_type=_detect_scope(metric["name"], dashboard_name),
                    scope_value=_detect_scope_value(metric["name"]),
                    value=float(metric.get("value", 0)),
                )
                snapshots.append(snap)
                log.debug(
                    "Extracted metric: %s = %.2f", snap.metric_name, snap.value
                )
            except Exception as e:
                log.warning("Failed to parse metric %s: %s", metric, e)
                continue

        log.info("Extracted %d metrics from %s", len(snapshots), image_path)
        return snapshots

    except requests.exceptions.ConnectionError:
        log.error(
            "Cannot connect to Ollama on localhost:11434. Is Ollama running? "
            "Run: ollama serve"
        )
        return []
    except Exception as e:
        log.error("Failed to extract from image %s: %s", image_path, e)
        return []


def _parse_json_response(text: str) -> dict | None:
    """
    Tenta extrair JSON válido do texto, mesmo que venha com markdown ou texto extra.
    """
    # Tentativa 1: parse direto
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Tentativa 2: encontrar bloco JSON entre { }
    match = re.search(r'\{.*\}', text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass

    # Tentativa 3: remover markdown ```json ... ```
    clean = re.sub(r'```(?:json)?', '', text).strip('`').strip()
    try:
        return json.loads(clean)
    except json.JSONDecodeError:
        pass

    return None


def extract_all(image_paths: list[tuple[str, Path, str]]) -> list[Snapshot]:
    """
    Processa todas as imagens e retorna lista consolidada de Snapshots.

    Args:
        image_paths: Lista de (dashboard_key, image_path, dashboard_name)

    Returns:
        Lista de Snapshot objects
    """
    all_snaps = []

    for dashboard_key, image_path, dashboard_name in image_paths:
        snaps = extract_from_image(image_path, dashboard_name)
        all_snaps.extend(snaps)

    log.info("Total snapshots extracted: %d", len(all_snaps))
    return all_snaps


# ── Helpers ───────────────────────────────────────────────────────────────────


def _normalize_metric_name(raw: str) -> str:
    """Converte nomes de Ollama → nomes canônicos do projeto."""
    raw_lower = raw.lower()

    if "daily" in raw_lower and "volume" in raw_lower:
        return "daily_swap_volume"
    if "cumulative" in raw_lower and "volume" in raw_lower:
        return "cumulative_volume"
    if "total" in raw_lower and "volume" in raw_lower:
        return "cumulative_volume"
    if "tvl" in raw_lower or "liquidity" in raw_lower:
        return "tvl"
    if "fees" in raw_lower and "cumulative" in raw_lower:
        return "cumulative_fees"
    if "fees" in raw_lower:
        return "daily_fees"
    if "swap" in raw_lower and ("count" in raw_lower or "number" in raw_lower):
        return "cumulative_swaps"
    if "pool" in raw_lower and "created" in raw_lower:
        return "cumulative_pools_created"
    if "pool" in raw_lower and ("count" in raw_lower or "number" in raw_lower):
        return "cumulative_pools_created"

    # Fallback: slugify
    return raw.replace(" ", "_").lower()


def _detect_scope(metric_name: str, dashboard_name: str) -> str:
    """
    Detecta scope (global vs v3 vs chain vs pool_type) baseado no nome.
    """
    metric_lower = metric_name.lower()
    dashboard_lower = dashboard_name.lower()

    # Se o dashboard é V3, tudo é V3
    if "v3" in dashboard_lower:
        return "v3"

    # Se a métrica menciona chain/blockchain
    if "chain" in metric_lower or "blockchain" in metric_lower:
        return "chain"

    # Se a métrica menciona pool type
    if "pool type" in metric_lower or "pool_type" in metric_lower:
        return "pool_type"

    # Default: global
    return "global"


def _detect_scope_value(metric_name: str) -> str:
    """
    Extrai scope_value (ethereum, arbitrum, weighted, etc).
    """
    metric_lower = metric_name.lower()

    # Chains
    if "ethereum" in metric_lower:
        return "ethereum"
    if "arbitrum" in metric_lower:
        return "arbitrum"
    if "polygon" in metric_lower:
        return "polygon"
    if "optimism" in metric_lower:
        return "optimism"
    if "mainnet" in metric_lower:
        return "ethereum"

    # Pool types
    if "weighted" in metric_lower:
        return "weighted"
    if "stable" in metric_lower:
        return "stable"
    if "concentrated" in metric_lower:
        return "concentrated"

    return ""
