"""
GreenMile API client aligned with the validated local Node reference:
- POST /login with form credentials
- Cookie + Bearer token on subsequent requests
- POST /RouteView/Summary?criteria=...
- POST /StopView/restrictions?criteria=...
"""
import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from config import Config

logger = logging.getLogger(__name__)

_BATCH_SIZE = 20
_DEFAULT_MODULE = 'LIVE'
_DEFAULT_BUILD = '1705315'
_DEFAULT_VERSION = '26.0130'
_DEFAULT_ACCEPT = 'application/json, text/plain, */*'


class GreenmileClient:
    """Fetches route data from GreenMile using the same contract as the validated Node tool."""

    def __init__(self, config: Config):
        self._config = config
        self._auth: dict[str, Any] | None = None
        self._summary_cache: dict[str, dict] = {}
        logger.info(
            "GreenMile client initialized | baseUrl=%s | batchSize=%s | timeout=%ss | maxRetries=%s",
            self._config.greenmile_url,
            _BATCH_SIZE,
            self._config.request_timeout,
            self._config.max_retries,
        )

    def login(self) -> dict[str, Any]:
        """Authenticate against GreenMile and cache the session cookie + bearer token."""
        payload = urllib.parse.urlencode(
            {
                'j_username': self._config.greenmile_username,
                'j_password': self._config.greenmile_password,
            }
        ).encode('utf-8')
        response = self._request_raw(
            f"{self._config.greenmile_url}/login",
            method='POST',
            headers={
                'Accept': 'application/json, text/html, */*',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Greenmile-Module': _DEFAULT_MODULE,
            },
            payload=payload,
            use_auth=False,
        )
        parsed = _parse_json_response(response['text'], 'GreenMile login')
        self._auth = {
            'cookie': _normalize_set_cookie(_get_header_case_insensitive(response['headers'], 'set-cookie')),
            'token': (
                ((parsed.get('analyticsToken') or {}).get('access_token') or '')
                if isinstance(parsed, dict)
                else ''
            ),
            'jsessionid': parsed.get('jsessionid') if isinstance(parsed, dict) else '',
            'raw': parsed,
        }
        logger.info(
            "GreenMile login OK | hasCookie=%s | hasBearer=%s",
            bool(self._auth['cookie']),
            bool(self._auth['token']),
        )
        return self._auth

    def ensure_auth(self) -> dict[str, Any]:
        if self._auth and self._auth.get('cookie'):
            return self._auth
        return self.login()

    def fetch_route_summaries(self, route_keys: list[str]) -> dict[str, dict]:
        if not route_keys:
            logger.info("GreenMile RouteView/Summary | no route keys to fetch")
            return {}

        result: dict[str, dict] = {}
        pending = [key for key in route_keys if key not in self._summary_cache]
        batches = list(_batch(pending, _BATCH_SIZE))
        logger.info(
            "GreenMile RouteView/Summary | totalRouteKeys=%s | uncached=%s | batches=%s",
            len(route_keys),
            len(pending),
            len(batches),
        )
        for index, batch in enumerate(batches, start=1):
            logger.info(
                "GreenMile RouteView/Summary | batch=%s/%s | routeKeysInBatch=%s | firstRouteKey=%s",
                index,
                len(batches),
                len(batch),
                batch[0] if batch else '',
            )
            for summary in self._fetch_route_summaries_batch(batch):
                route = summary.get('route') or {}
                route_key = route.get('key')
                if route_key:
                    self._summary_cache[str(route_key)] = summary

        for route_key in route_keys:
            summary = self._summary_cache.get(route_key)
            if summary:
                result[route_key] = summary

        logger.info(
            "GreenMile RouteView/Summary | completed | resolvedRouteKeys=%s/%s",
            len(result),
            len(route_keys),
        )
        return result

    def fetch_stop_views(
        self,
        route_keys: list[str],
        skip_order_numbers_for_route_keys: set[str] | None = None,
    ) -> dict[str, dict]:
        if not route_keys:
            logger.info("GreenMile StopView | no route keys to fetch")
            return {}

        result: dict[str, dict] = {}
        skip_order_numbers_for_route_keys = set(skip_order_numbers_for_route_keys or set())
        summaries = self.fetch_route_summaries(route_keys)
        batches = list(_batch(route_keys, _BATCH_SIZE))
        logger.info(
            "GreenMile StopView | totalRouteKeys=%s | batches=%s",
            len(route_keys),
            len(batches),
        )
        for batch_index, batch in enumerate(batches, start=1):
            logger.info(
                "GreenMile StopView | batch=%s/%s | routeKeysInBatch=%s | firstRouteKey=%s",
                batch_index,
                len(batches),
                len(batch),
                batch[0] if batch else '',
            )
            route_ids_by_key: dict[str, str] = {}
            route_key_by_id: dict[str, str] = {}
            for route_key in batch:
                summary = summaries.get(route_key) or {}
                route = summary.get('route') or {}
                route_id = route.get('id') or summary.get('id')
                if not route_id:
                    logger.warning(
                        "GreenMile StopView | skipping routeKey=%s because route.id was not resolved",
                        route_key,
                    )
                    continue
                route_id_str = str(route_id)
                route_ids_by_key[route_key] = route_id_str
                route_key_by_id[route_id_str] = route_key

            for route_key, stop_payload in self._fetch_stop_views_batch(
                route_ids_by_key,
                route_key_by_id,
                skip_order_numbers_for_route_keys,
            ).items():
                result[route_key] = stop_payload

        logger.info(
            "GreenMile StopView | completed | resolvedRouteKeys=%s/%s",
            len(result),
            len(route_keys),
        )
        return result

    def _fetch_route_summaries_batch(self, route_keys: list[str]) -> list[dict[str, Any]]:
        if not route_keys:
            return []
        criteria = {
            'filters': _route_summary_filters(),
            'firstResult': 0,
            'maxResults': len(route_keys),
        }
        payload = {
            'sort': [{'attr': 'route.date', 'type': 'DESC'}],
            **_build_multi_filter_criteria('route.key', route_keys, include_match_mode=True),
        }
        rows = self._request_json(
            pathname='/RouteView/Summary',
            criteria=criteria,
            method='POST',
            payload=payload,
        )
        return _extract_rows(rows)

    def _fetch_stop_views_batch(
        self,
        route_ids_by_key: dict[str, str],
        route_key_by_id: dict[str, str],
        skip_order_numbers_for_route_keys: set[str] | None = None,
    ) -> dict[str, dict[str, Any]]:
        route_ids = list(route_ids_by_key.values())
        if not route_ids:
            return {}
        skip_order_numbers_for_route_keys = set(skip_order_numbers_for_route_keys or set())
        criteria = {
            'filters': _stop_view_restrictions_filters(),
            'including': ['geofence'],
        }
        payload = {
            'sort': [{'attr': 'stop.plannedSequenceNum', 'type': 'ASC'}],
            **_build_multi_filter_criteria('route.id', route_ids, include_match_mode=False),
        }
        rows = self._request_json(
            pathname='/StopView/restrictions',
            criteria=criteria,
            method='POST',
            payload=payload,
        )
        extracted = _extract_rows(rows)
        stop_rows_for_nf = []
        for item in extracted:
            route = item.get('route') if isinstance(item, dict) else {}
            route_id = str((route or {}).get('id') or '')
            route_key = route_key_by_id.get(route_id)
            if route_key and route_key in skip_order_numbers_for_route_keys:
                continue
            stop_rows_for_nf.append(item)
        order_numbers_by_stop_id = {}
        if stop_rows_for_nf:
            try:
                order_numbers_by_stop_id = self._fetch_order_numbers_by_stop_ids(stop_rows_for_nf)
            except Exception as exc:
                logger.warning(
                    "GreenMile Order/restrictions unavailable | continuing without order numbers | err=%s",
                    exc,
                )
                order_numbers_by_stop_id = {}
        grouped: dict[str, dict[str, Any]] = {}
        for route_key, route_id in route_ids_by_key.items():
            grouped[route_key] = {
                'routeKey': route_key,
                'routeId': int(route_id) if str(route_id).isdigit() else route_id,
                'stops': [],
            }

        for item in extracted:
            route = item.get('route') if isinstance(item, dict) else {}
            route_id = str((route or {}).get('id') or '')
            route_key = route_key_by_id.get(route_id)
            if not route_key:
                continue
            grouped[route_key]['stops'].append(_normalize_stop_row(item, order_numbers_by_stop_id))

        for route_key, payload in grouped.items():
            logger.info(
                "GreenMile StopView | routeKey=%s | routeId=%s | stops=%s",
                route_key,
                payload['routeId'],
                len(payload['stops']),
            )
        return grouped

    def _fetch_order_numbers_by_stop_ids(self, stop_rows: list[dict[str, Any]]) -> dict[str, list[str]]:
        stop_ids: list[str] = []
        for item in stop_rows:
            stop = item.get('stop') if isinstance(item, dict) else {}
            stop_id = first_non_empty(stop.get('id') if isinstance(stop, dict) else None, item.get('stopId') if isinstance(item, dict) else None)
            if stop_id is None:
                continue
            stop_ids.append(str(stop_id))

        if not stop_ids:
            return {}

        numbers_by_stop_id: dict[str, list[str]] = {}
        for stop_id in stop_ids:
            criteria = {
                'filters': _order_restrictions_filters(),
            }
            payload = {
                'sort': [],
                **_build_single_filter_criteria('stop.id', stop_id, include_match_mode=False),
            }
            rows = self._request_json(
                pathname='/Order/restrictions',
                criteria=criteria,
                method='POST',
                payload=payload,
            )
            order_numbers: list[str] = []
            seen: set[str] = set()
            for order in _extract_rows(rows):
                number = str((order or {}).get('number') or '').strip()
                if not number or number in seen:
                    continue
                seen.add(number)
                order_numbers.append(number)
            numbers_by_stop_id[stop_id] = order_numbers
        return numbers_by_stop_id

    def _request_json(
        self,
        pathname: str,
        *,
        criteria: dict[str, Any] | None = None,
        method: str = 'GET',
        payload: dict[str, Any] | None = None,
    ) -> Any:
        query = ''
        if criteria is not None:
            query = '?criteria=' + urllib.parse.quote(json.dumps(criteria, separators=(',', ':')))
        response = self._request_raw(
            f"{self._config.greenmile_url}{pathname}{query}",
            method=method,
            headers={'Content-Type': 'application/json;charset=UTF-8'},
            payload=json.dumps(payload).encode('utf-8') if payload is not None else None,
            use_auth=True,
        )
        return _parse_json_response(response['text'], pathname)

    def _request_raw(
        self,
        url: str,
        *,
        method: str,
        headers: dict[str, str] | None = None,
        payload: bytes | None = None,
        use_auth: bool,
    ) -> dict[str, Any]:
        retries = self._config.max_retries
        last_error: Exception | None = None
        for attempt in range(retries):
            request_headers = {}
            if use_auth:
                auth = self.ensure_auth()
                request_headers.update(
                    {
                        'Accept': _DEFAULT_ACCEPT,
                        'Greenmile-Module': _DEFAULT_MODULE,
                        'Greenmile-Build': _DEFAULT_BUILD,
                        'Greenmile-Version': _DEFAULT_VERSION,
                        'Cookie': auth.get('cookie') or '',
                    }
                )
                if auth.get('token'):
                    request_headers['Authorization'] = f"Bearer {auth['token']}"
            if headers:
                request_headers.update(headers)

            logger.info(
                "GreenMile request start | attempt=%s/%s | method=%s | timeout=%ss | url=%s",
                attempt + 1,
                retries,
                method,
                self._config.request_timeout,
                _sanitize_url(url),
            )
            try:
                req = urllib.request.Request(
                    url,
                    headers=request_headers,
                    data=payload,
                    method=method,
                )
                with urllib.request.urlopen(req, timeout=self._config.request_timeout) as resp:
                    raw = resp.read().decode('utf-8', errors='replace')
                    response_headers = dict(getattr(resp, 'headers', {}) or {})
                logger.info(
                    "GreenMile request success | attempt=%s/%s | bytes=%s | url=%s",
                    attempt + 1,
                    retries,
                    len(raw),
                    _sanitize_url(url),
                )
                return {'text': raw, 'headers': response_headers}
            except urllib.error.HTTPError as exc:
                response_body = _read_error_body(exc)
                if exc.code == 401 and use_auth and attempt == 0:
                    logger.warning(
                        "GreenMile auth expired | attempt=%s/%s | refreshing session | url=%s",
                        attempt + 1,
                        retries,
                        _sanitize_url(url),
                    )
                    self._auth = None
                    last_error = exc
                    continue
                if exc.code < 500:
                    logger.error(
                        "GreenMile HTTP fatal | attempt=%s/%s | status=%s | url=%s | body=%s",
                        attempt + 1,
                        retries,
                        exc.code,
                        _sanitize_url(url),
                        _truncate(response_body),
                    )
                    raise RuntimeError(f"GreenMile HTTP {exc.code}: {_sanitize_url(url)}") from exc
                last_error = exc
                logger.warning(
                    "GreenMile HTTP retryable | attempt=%s/%s | status=%s | url=%s | body=%s",
                    attempt + 1,
                    retries,
                    exc.code,
                    _sanitize_url(url),
                    _truncate(response_body),
                )
            except (urllib.error.URLError, OSError, TimeoutError) as exc:
                last_error = exc
                logger.warning(
                    "GreenMile connection retryable | attempt=%s/%s | err=%s | url=%s",
                    attempt + 1,
                    retries,
                    exc,
                    _sanitize_url(url),
                )

            if attempt < retries - 1:
                sleep_seconds = 2 ** attempt
                logger.info(
                    "GreenMile request backoff | nextAttempt=%s/%s | sleep=%ss | url=%s",
                    attempt + 2,
                    retries,
                    sleep_seconds,
                    _sanitize_url(url),
                )
                time.sleep(sleep_seconds)

        logger.error(
            "GreenMile request failed | attempts=%s | url=%s | lastError=%s",
            retries,
            _sanitize_url(url),
            last_error,
        )
        raise ConnectionError(f"GreenMile fetch failed after {retries} attempts: {last_error}") from last_error


def _batch(items: list[str], size: int):
    for index in range(0, len(items), size):
        yield items[index:index + size]


def _normalize_set_cookie(set_cookie: str | None) -> str:
    if not set_cookie:
        return ''
    return '; '.join(
        part
        for part in (
            segment.split(';', 1)[0].strip()
            for segment in str(set_cookie).split(',')
        )
        if part
    )


def _build_single_filter_criteria(
    attr: str,
    value: str,
    *,
    include_match_mode: bool,
) -> dict[str, Any]:
    filter_item = {
        'attr': str(attr),
        'eq': str(value),
    }
    if include_match_mode:
        filter_item['matchMode'] = 'EXACT'
    return {
        'criteriaChain': [
            {
                'and': [filter_item],
            }
        ]
    }


def _build_multi_filter_criteria(
    attr: str,
    values: list[str],
    *,
    include_match_mode: bool,
) -> dict[str, Any]:
    filters = []
    for value in values:
        filter_item = {
            'attr': str(attr),
            'eq': str(value),
        }
        if include_match_mode:
            filter_item['matchMode'] = 'EXACT'
        filters.append(filter_item)
    return {
        'criteriaChain': [
            {
                'or': filters,
            }
        ]
    }


def _route_summary_filters() -> list[str]:
    return [
        'id',
        'route.driverAssignments.*',
        'route.driverAssignments.driver.id',
        'route.driverAssignments.driver.name',
        'route.driverAssignments.driver.key',
        'primaryAssignments.equipment.id',
        'primaryAssignments.equipment.key',
        'route.baseLineDeparture',
        'route.plannedDistance',
        'route.baselineSize1',
        'route.actualDistance',
        'route.plannedSize1',
        'route.actualSize1',
        'route.key',
        'route.description',
        'route.origin.id',
        'route.origin.description',
        'route.destination.id',
        'route.destination.description',
        'route.baseLineArrival',
        'route.plannedDeparture',
        'route.plannedArrival',
        'route.projectedDeparture',
        'route.projectedArrival',
        'route.actualDeparture',
        'route.actualArrival',
        'route.baseLineComplete',
        'route.plannedComplete',
        'route.projectedComplete',
        'route.actualComplete',
        'route.actualDistanceDataQuality',
        'route.actualCompleteDataQuality',
        'route.actualDepartureDataQuality',
        'route.plannedStart',
        'route.actualCost',
        'route.actualStart',
        'route.plannedCost',
        'route.baseLineCost',
        'route.id',
        'route.date',
        'route.totalStops',
        'route.canceledStops',
        'route.redeliveredStops',
        'route.actualDepartures',
        'route.organization.description',
        'route.status',
        'route.lastModificationDate',
        'routePercentage',
        'stopView',
        'route.undeliveredStops',
        'totalStopsInProgress',
        'lastModificationDate',
    ]


def _stop_view_restrictions_filters() -> list[str]:
    return [
        'id',
        '*',
        'stop.*',
        'stop.location.*',
        'stop.location.locationType.*',
        'stop.stopType.*',
        'stop.cancelCode.*',
        'stop.redeliveryStop.*',
        'stop.redeliveryStop.location.key*',
        'stop.undeliverableCode.*',
        'route.origLatitude',
        'route.origLongitude',
        'route.destLatitude',
        'route.destLongitude',
        'route.origin.*',
        'route.destination.*',
        'route.organization.id',
        'route.proactiveRouteOptConfig',
    ]


def _order_restrictions_filters() -> list[str]:
    return [
        'id',
        'number',
        'lineItems.sku.id',
        'lineItems.sku.description',
        'lineItems.plannedSize1',
        'lineItems.plannedSize2',
        'lineItems.plannedSize3',
        'lineItems.actualSize1',
        'lineItems.actualSize2',
        'lineItems.actualSize3',
        'lineItems.plannedPickupSize1',
        'lineItems.plannedPickupSize2',
        'lineItems.plannedPickupSize3',
        'lineItems.actualPickupSize1',
        'lineItems.actualPickupSize2',
        'lineItems.actualPickupSize3',
        'lineItems.damagedSize1',
        'lineItems.damagedSize2',
        'lineItems.damagedSize3',
        'lineItems.deliveryReasonCode.id',
        'lineItems.deliveryReasonCode.description',
        'lineItems.overReasonCode.id',
        'lineItems.overReasonCode.description',
        'lineItems.shortReasonCode.id',
        'lineItems.shortReasonCode.description',
        'lineItems.damagedReasonCode.id',
        'lineItems.damagedReasonCode.description',
        'lineItems.pickupReasonCode.id',
        'lineItems.pickupReasonCode.description',
        'lineItems.lineItemID',
    ]


def _extract_rows(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        rows = data.get('content') or data.get('rows') or data.get('items') or []
        return rows if isinstance(rows, list) else []
    return []


def _normalize_stop_row(item: dict[str, Any], order_numbers_by_stop_id: dict[str, list[str]] | None = None) -> dict[str, Any]:
    stop = item.get('stop') if isinstance(item, dict) else None
    stop_data = stop if isinstance(stop, dict) else (item if isinstance(item, dict) else {})
    location = stop_data.get('location') or {}
    stop_id = str(first_non_empty(stop_data.get('id'), item.get('stopId') if isinstance(item, dict) else '') or '').strip()
    normalized = dict(stop_data)
    normalized['locationName'] = location.get('description') or stop_data.get('description') or ''
    normalized['orderNumbers'] = list((order_numbers_by_stop_id or {}).get(stop_id, []))
    return normalized


def first_non_empty(*values):
    for value in values:
        if value is not None and value != '':
            return value
    return None


def _parse_json_response(text: str, label: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{label} returned invalid JSON.") from exc


def _read_error_body(exc: urllib.error.HTTPError) -> str:
    try:
        return exc.read().decode('utf-8', errors='replace')
    except Exception:
        return ''


def _truncate(value: str, limit: int = 220) -> str:
    if not value:
        return ''
    return value if len(value) <= limit else value[:limit] + '...'


def _sanitize_url(url: str) -> str:
    if not url:
        return ''
    return url if len(url) <= 220 else url[:220] + '...'


def _get_header_case_insensitive(headers: dict[str, Any] | None, name: str) -> str:
    if not headers:
        return ''
    wanted = str(name or '').strip().lower()
    for key, value in headers.items():
        if str(key).strip().lower() == wanted:
            return str(value or '')
    return ''
