"""
End-to-end orchestration: pull route.keys -> fetch GreenMile -> build snapshots -> push.
The Python worker does NOT implement any Viagens business logic; that stays in Apps Script.
"""
import copy
import datetime
import logging
import time
from typing import Any

from config import Config
from apps_gateway import AppsGateway
from greenmile_client import GreenmileClient
from snapshot_mapper import build_snapshots_from_responses

logger = logging.getLogger(__name__)


class SyncRunner:
    """Orchestrates one full sync cycle."""

    def __init__(
        self,
        config: Config,
        apps_gateway: AppsGateway | None = None,
        greenmile_client: GreenmileClient | None = None,
    ):
        self._config = config
        self._gw = apps_gateway or AppsGateway(config)
        self._gm = greenmile_client or GreenmileClient(config)
        self._last_pending_route_keys: list[str] = []
        self._last_snapshots_by_route_key: dict[str, dict[str, Any]] = {}
        self._last_snapshot_cache_at: float = 0.0

    def run(self) -> dict[str, Any]:
        """
        Execute one sync cycle.
        Returns a summary dict with keys: route_keys_found, push_result, started_at, finished_at.
        """
        started_at = datetime.datetime.utcnow().isoformat() + 'Z'
        logger.info("=== greenmile_sync: starting run at %s ===", started_at)

        pending_context = _pull_pending_context(self._gw)
        route_keys = _normalize_route_keys(pending_context.get('routeKeys') or [])
        existing_notas_by_route_key = pending_context.get('existingNotasByRouteKey') or {}
        logger.info("Found %d pending route.key(s).", len(route_keys))

        if not route_keys:
            self._last_pending_route_keys = []
            self._last_snapshots_by_route_key = {}
            self._last_snapshot_cache_at = 0.0
            finished_at = datetime.datetime.utcnow().isoformat() + 'Z'
            logger.info("No pending route.keys - nothing to sync.")
            return {
                'route_keys_found': 0,
                'skipped': True,
                'reason': 'no_pending_route_keys',
                'started_at': started_at,
                'finished_at': finished_at,
            }

        reuse_cached_snapshots = _can_reuse_last_snapshots(
            route_keys,
            self._last_pending_route_keys,
            self._last_snapshots_by_route_key,
            self._last_snapshot_cache_at,
            time.time(),
            getattr(self._config, 'snapshot_reuse_ttl_seconds', 600),
        )

        if reuse_cached_snapshots:
            logger.info(
                "Pending route.keys unchanged; skipping GreenMile and refreshing only Apps Script times."
            )
            snapshots = _clone_snapshots_for_route_keys(self._last_snapshots_by_route_key, route_keys)
            sync_mode = 'apps_script_refresh_only'
        else:
            logger.info("Fetching stop views from GreenMile for %d keys...", len(route_keys))
            skip_nf_route_keys = {
                str(route_key)
                for route_key, notas in existing_notas_by_route_key.items()
                if notas
            }
            stop_views = self._gm.fetch_stop_views(
                route_keys,
                skip_order_numbers_for_route_keys=skip_nf_route_keys,
            )

            logger.info("Fetching route summaries from GreenMile for %d keys...", len(route_keys))
            summaries = self._gm.fetch_route_summaries(route_keys)

            logger.info("Building snapshots...")
            snapshots = build_snapshots_from_responses(route_keys, stop_views, summaries)
            logger.info("Built %d snapshot(s).", len(snapshots))

            sync_mode = 'greenmile_refresh'

        _merge_existing_notas_into_snapshots(snapshots, existing_notas_by_route_key)

        if sync_mode == 'greenmile_refresh':
            self._last_pending_route_keys = list(route_keys)
            self._last_snapshots_by_route_key = _clone_snapshots_for_route_keys(snapshots, route_keys)
            self._last_snapshot_cache_at = time.time()

        nf_unavailable = _detect_nf_unavailable_route_keys(route_keys, snapshots)
        if nf_unavailable:
            logger.warning("NF indisponivel para %d rota(s): %s", len(nf_unavailable), nf_unavailable)

        logger.info("Pushing snapshots to Apps Script...")
        push_result = self._gw.push_route_snapshots(snapshots)
        logger.info("Push result: %s", push_result)
        last_post_succeeded_at = datetime.datetime.utcnow().isoformat() + 'Z'

        finished_at = datetime.datetime.utcnow().isoformat() + 'Z'
        logger.info("=== greenmile_sync: completed at %s ===", finished_at)

        return {
            'route_keys_found': len(route_keys),
            'snapshots_built': len(snapshots),
            'pending_rows': int(pending_context.get('pendingRows', len(route_keys))),
            'moved_rows': int((push_result or {}).get('updatedRows', 0)),
            'last_post_succeeded_at': last_post_succeeded_at,
            'push_result': push_result,
            'nf_unavailable_route_keys': nf_unavailable,
            'sync_mode': sync_mode,
            'started_at': started_at,
            'finished_at': finished_at,
        }


def _pull_pending_context(apps_gateway: AppsGateway) -> dict[str, Any]:
    if hasattr(apps_gateway, 'pull_pending_context'):
        result = apps_gateway.pull_pending_context()
        if isinstance(result, dict):
            return result
    route_keys = apps_gateway.pull_pending_route_keys()
    return {'routeKeys': route_keys, 'existingNotasByRouteKey': {}}


def _normalize_route_keys(route_keys: list[Any]) -> list[str]:
    normalized = []
    for key in route_keys:
        text = str(key or '').strip()
        if text:
            normalized.append(text)
    return normalized


def _merge_existing_notas_into_snapshots(
    snapshots: dict[str, dict],
    existing_notas_by_route_key: dict[str, list[str]],
) -> None:
    for route_key, notas in (existing_notas_by_route_key or {}).items():
        snapshot = snapshots.get(route_key)
        if not isinstance(snapshot, dict):
            continue
        current_order_numbers = snapshot.get('orderNumbers')
        if current_order_numbers:
            continue
        normalized = []
        seen = set()
        for number in notas or []:
            text = str(number or '').strip()
            if not text or text == '-' or text in seen:
                continue
            seen.add(text)
            normalized.append(text)
        if normalized:
            snapshot['orderNumbers'] = normalized


def _can_reuse_last_snapshots(
    route_keys: list[str],
    last_route_keys: list[str],
    last_snapshots_by_route_key: dict[str, dict[str, Any]],
    cache_created_at: float,
    now_ts: float,
    snapshot_reuse_ttl_seconds: int,
) -> bool:
    if not route_keys:
        return False
    if snapshot_reuse_ttl_seconds <= 0:
        return False
    if not cache_created_at or (now_ts - cache_created_at) > snapshot_reuse_ttl_seconds:
        return False
    if set(route_keys) != set(last_route_keys or []):
        return False
    return all(_is_reusable_snapshot(last_snapshots_by_route_key.get(key)) for key in route_keys)


def _is_reusable_snapshot(snapshot: dict[str, Any] | None) -> bool:
    if not isinstance(snapshot, dict):
        return False
    if snapshot.get('routeResolved') is False:
        return False
    fingerprint = str(snapshot.get('fingerprint') or '').strip()
    if not fingerprint:
        return False
    return True


def _clone_snapshots_for_route_keys(
    snapshots_by_route_key: dict[str, dict[str, Any]],
    route_keys: list[str],
) -> dict[str, dict[str, Any]]:
    cloned = {}
    for key in route_keys:
        snapshot = snapshots_by_route_key.get(key)
        if isinstance(snapshot, dict):
            cloned[key] = copy.deepcopy(snapshot)
    return cloned


def _detect_nf_unavailable_route_keys(
    route_keys: list[str],
    snapshots: dict[str, dict],
) -> list[str]:
    """
    Retorna as route.keys onde o snapshot final segue sem orderNumbers.
    Isso evita falso positivo quando a NF foi reaproveitada da planilha.
    """
    unavailable = []
    for key in route_keys:
        snapshot = snapshots.get(key)
        if snapshot is None:
            unavailable.append(key)
            continue
        order_numbers = snapshot.get('orderNumbers') if isinstance(snapshot, dict) else None
        if not order_numbers:
            unavailable.append(key)
    return unavailable
