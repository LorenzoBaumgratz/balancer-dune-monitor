"""
Dune Analytics API v1 client.

Responsibilities
----------------
* Execute saved queries by ID (POST /query/{id}/execute → GET /execution/{id}/results)
* Execute ad-hoc SQL via the query engine (POST /query/execute with inline SQL)
* Poll until results are ready with exponential back-off
* Return results as list[dict] (one dict per row)

Design
------
* No third-party `dune-client` package dependency – uses `requests` only.
* Thread-safe (no shared mutable state beyond the session object).
* Raises `DuneAPIError` on unrecoverable failures; callers must handle.
"""

from __future__ import annotations

import time
from typing import Any

import requests

from config import DUNE_API_BASE, DUNE_API_KEY, DUNE_POLL_INTERVAL, DUNE_TIMEOUT
from logger_setup import get_logger

log = get_logger(__name__)


# ── Exceptions ────────────────────────────────────────────────────────────────

class DuneAPIError(RuntimeError):
    """Raised when the Dune API returns an unrecoverable error."""


class DuneTimeoutError(DuneAPIError):
    """Raised when a query execution exceeds DUNE_TIMEOUT seconds."""


# ── Client ────────────────────────────────────────────────────────────────────

class DuneClient:
    """Thin wrapper around the Dune Analytics REST API v1."""

    def __init__(self, api_key: str = DUNE_API_KEY) -> None:
        if not api_key:
            raise ValueError(
                "DUNE_API_KEY is not set. "
                "Export it as an environment variable before running."
            )
        self._session = requests.Session()
        self._session.headers.update(
            {
                "X-DUNE-API-KEY": api_key,
                "Content-Type": "application/json",
            }
        )

    # ── Public methods ────────────────────────────────────────────────────────

    def run_query(
        self,
        query_id: int,
        parameters: dict[str, Any] | None = None,
    ) -> list[dict]:
        """
        Execute a saved Dune query and return its result rows.

        Parameters
        ----------
        query_id:   Dune query ID (integer shown in the URL).
        parameters: Optional dict of query parameters to override defaults.
        """
        log.info("Executing Dune query %d …", query_id)
        execution_id = self._trigger_execution(query_id, parameters or {})
        return self._wait_for_results(execution_id)

    def run_sql(self, sql: str) -> list[dict]:
        """
        Execute arbitrary SQL via the Dune query engine.
        Returns result rows as a list of dicts.
        NOTE: This uses the 'query' endpoint – your API plan must support it.
        """
        log.info("Running ad-hoc SQL (first 80 chars): %s …", sql[:80])
        resp = self._post(
            "/query/execute",
            json={"query": sql, "parameters": []},
        )
        execution_id = resp["execution_id"]
        return self._wait_for_results(execution_id)

    # ── Private helpers ───────────────────────────────────────────────────────

    def _trigger_execution(
        self,
        query_id: int,
        parameters: dict[str, Any],
    ) -> str:
        body: dict[str, Any] = {}
        if parameters:
            body["query_parameters"] = [
                {"key": k, "value": v} for k, v in parameters.items()
            ]
        resp = self._post(f"/query/{query_id}/execute", json=body)
        execution_id: str = resp["execution_id"]
        log.debug("Execution ID for query %d: %s", query_id, execution_id)
        return execution_id

    def _wait_for_results(self, execution_id: str) -> list[dict]:
        """Poll until state is QUERY_STATE_COMPLETED, then fetch results."""
        deadline = time.monotonic() + DUNE_TIMEOUT
        poll_interval = DUNE_POLL_INTERVAL

        while True:
            status_resp = self._get(f"/execution/{execution_id}/status")
            state: str = status_resp.get("state", "")
            log.debug("Execution %s state: %s", execution_id, state)

            if state == "QUERY_STATE_COMPLETED":
                break
            elif state in ("QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED"):
                error = status_resp.get("error", {})
                raise DuneAPIError(
                    f"Execution {execution_id} ended with state '{state}': {error}"
                )

            if time.monotonic() > deadline:
                raise DuneTimeoutError(
                    f"Execution {execution_id} did not complete within "
                    f"{DUNE_TIMEOUT}s. Last state: {state}"
                )

            time.sleep(poll_interval)
            # mild exponential back-off, capped at 30 s
            poll_interval = min(poll_interval * 1.5, 30)

        return self._fetch_all_results(execution_id)

    def _fetch_all_results(self, execution_id: str) -> list[dict]:
        """Fetch paginated results, concatenating all pages."""
        all_rows: list[dict] = []
        next_uri: str | None = None

        while True:
            if next_uri:
                resp = self._get_url(next_uri)
            else:
                resp = self._get(f"/execution/{execution_id}/results")

            result = resp.get("result", {})
            rows: list[dict] = result.get("rows", [])
            all_rows.extend(rows)

            next_uri = resp.get("next_uri")
            if not next_uri:
                break

        log.info("Fetched %d rows from execution %s", len(all_rows), execution_id)
        return all_rows

    # ── HTTP helpers ──────────────────────────────────────────────────────────

    def _post(self, path: str, **kwargs) -> dict:
        url = f"{DUNE_API_BASE}{path}"
        resp = self._session.post(url, **kwargs)
        return self._check(resp)

    def _get(self, path: str, **kwargs) -> dict:
        url = f"{DUNE_API_BASE}{path}"
        resp = self._session.get(url, **kwargs)
        return self._check(resp)

    def _get_url(self, url: str, **kwargs) -> dict:
        resp = self._session.get(url, **kwargs)
        return self._check(resp)

    @staticmethod
    def _check(resp: requests.Response) -> dict:
        try:
            data = resp.json()
        except ValueError:
            data = {}

        if not resp.ok:
            raise DuneAPIError(
                f"HTTP {resp.status_code} from {resp.url}: "
                f"{data.get('error', resp.text[:200])}"
            )
        return data
