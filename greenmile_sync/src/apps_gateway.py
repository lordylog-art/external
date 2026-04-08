"""
HTTP client for the Apps Script Web App gateway.
Implements pull_pending_route_keys and push_route_snapshots actions.
"""
import datetime
import logging
from typing import Any

from config import Config
from http_client import post_json

logger = logging.getLogger(__name__)


class AppsGateway:
    """Communicates with the Apps Script doPost gateway."""

    def __init__(self, config: Config):
        self._config = config

    # ── Public API ─────────────────────────────────────────────────────────────

    def pull_pending_route_keys(self) -> list[str]:
        """
        Calls greenmile_sync_pull_pending_route_keys on Apps Script.
        Returns list of pending route.keys (non-cancelled, non-finished rows).
        """
        result = self.pull_pending_context()
        route_keys = result.get('routeKeys', [])
        logger.info("Pulled %d pending route.keys from Apps Script.", len(route_keys))
        return list(route_keys)

    def pull_pending_context(self) -> dict:
        """
        Calls greenmile_sync_pull_pending_route_keys and returns the full result payload.
        This may include metadata such as existingNotasByRouteKey for NF reuse.
        """
        response = self._post('greenmile_sync_pull_pending_route_keys', {})
        self._assert_ok(response)
        result = response.get('result')
        if result is None:
            raise RuntimeError("Apps Script returned ok=True but no 'result' key.")
        return result

    def push_route_snapshots(self, snapshots: dict) -> dict:
        """
        Calls greenmile_sync_push_route_snapshots on Apps Script with the snapshot map.
        Splits into chunks of config.chunk_size to avoid payload limits.
        Returns aggregated push summary.
        """
        if not snapshots:
            logger.info("No snapshots to push — skipping.")
            return {'processedRows': 0, 'updatedRows': 0, 'routeKeys': 0}

        keys = list(snapshots.keys())
        chunk_size = self._config.chunk_size
        chunks = [keys[i:i + chunk_size] for i in range(0, len(keys), chunk_size)]

        total_processed = 0
        total_updated = 0
        total_keys = 0
        last_result: dict = {}

        for idx, chunk_keys in enumerate(chunks):
            chunk_snapshots = {k: snapshots[k] for k in chunk_keys}
            logger.info(
                "Pushing chunk %d/%d (%d snapshots)...", idx + 1, len(chunks), len(chunk_snapshots)
            )
            response = self._post('greenmile_sync_push_route_snapshots', {'snapshots': chunk_snapshots})
            self._assert_ok(response)
            result = response.get('result') or {}
            total_processed += result.get('processedRows', 0)
            total_updated += result.get('updatedRows', 0)
            total_keys += result.get('routeKeys', 0)
            last_result = result

        return {
            'processedRows': total_processed,
            'updatedRows': total_updated,
            'routeKeys': total_keys,
            **{k: v for k, v in last_result.items() if k not in ('processedRows', 'updatedRows', 'routeKeys')},
        }

    # ── Internal ───────────────────────────────────────────────────────────────

    def _post(self, action: str, payload: dict) -> dict:
        """
        POST a gateway request to Apps Script.
        Uses the shared resilient HTTP client with timeout/retry configuration.
        """
        body = {
            'token': self._config.apps_script_token,
            'action': action,
            'payload': payload,
            'requestedAt': datetime.datetime.utcnow().isoformat() + 'Z',
        }
        try:
            return post_json(
                self._config.apps_script_url,
                body=body,
                headers={'Content-Type': 'application/json'},
                timeout=self._config.request_timeout,
                max_retries=self._config.max_retries,
            )
        except Exception as e:
            raise RuntimeError(f"HTTP error calling Apps Script ({action}): {e}") from e

    @staticmethod
    def _assert_ok(response: dict) -> None:
        if not response.get('ok'):
            error = response.get('error', 'Unknown error')
            raise RuntimeError(f"Apps Script error: {error}")
