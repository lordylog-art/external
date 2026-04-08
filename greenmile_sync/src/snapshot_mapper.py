"""
Maps raw GreenMile API responses (stop views + route summaries) into the
routeProgressByKey format expected by the Apps Script atualizarQtdEntregasViagens logic.

The schema here must match exactly what buildEntregaProgressFromRouteKeys_,
buildCanhotoDigitalCountFromRouteKeys_, buildViagensStatusFromRouteKeys_, etc. consume.
"""
from datetime import timezone
from typing import Any

# ── Public API ─────────────────────────────────────────────────────────────────


def build_snapshots_from_responses(
    route_keys: list[str],
    stop_views: dict[str, dict],
    summaries: dict[str, dict],
) -> dict[str, dict]:
    """
    Build a routeProgressByKey map from GreenMile fetch results.

    Args:
        route_keys: all requested route.keys
        stop_views: dict route_key → stop_view response from GreenMile
        summaries: dict route_key → route summary from GreenMile

    Returns:
        dict route_key → snapshot (routeProgressByKey entry)
    """
    result: dict[str, dict] = {}
    for key in route_keys:
        stop_view = stop_views.get(key)
        summary = summaries.get(key)
        result[key] = build_snapshot_for_route_key(key, stop_view, summary)
    return result


def build_snapshot_for_route_key(
    route_key: str,
    stop_view: dict | None,
    summary: dict | None,
) -> dict:
    """
    Build a single routeProgressByKey entry for one route.key.
    If stop_view or summary is missing, returns an unresolved fallback snapshot.
    """
    if stop_view is None and summary is None:
        return _unresolved_snapshot(route_key)

    try:
        return _build_resolved_snapshot(route_key, stop_view or {}, summary or {})
    except Exception:
        return _unresolved_snapshot(route_key)


# ── Internal builders ─────────────────────────────────────────────────────────


def _build_resolved_snapshot(route_key: str, stop_view: dict, summary: dict) -> dict:
    stops = stop_view.get('stops') or []
    route_obj = summary.get('route') or {}
    status = _normalize_status(
        stop_view.get('status')
        or route_obj.get('status')
        or summary.get('status')
        or ''
    )

    total_clients = len(stops)
    with_arrival = 0
    with_departure = 0
    with_signature = 0
    order_numbers: list[str] = []
    seen_orders: set[str] = set()

    # For currentClientName: track latest open (arrived, not departed) stop
    latest_open_arrival_ms: float | None = None
    current_client_name = ''
    current_client_arrival = ''
    current_client_arrival_ms: float | None = None

    # For routeStartMs: earliest arrival
    route_start_ms: float | None = None
    # For routeEndMs: latest departure
    route_end_ms: float | None = None

    for stop in stops:
        arrival_str = stop.get('actualArrival') or ''
        departure_str = stop.get('actualDeparture') or ''
        arrival_ms = _parse_iso_to_ms(arrival_str)
        departure_ms = _parse_iso_to_ms(departure_str)
        has_sig = _is_truthy_signature(stop.get('hasSignature'))

        if arrival_ms is not None:
            with_arrival += 1
            if route_start_ms is None or arrival_ms < route_start_ms:
                route_start_ms = arrival_ms

        if departure_ms is not None:
            with_departure += 1
            if route_end_ms is None or departure_ms > route_end_ms:
                route_end_ms = departure_ms

        if has_sig:
            with_signature += 1

        # Open arrival = arrived but not yet departed → candidate for currentClient
        if arrival_ms is not None and departure_ms is None:
            if latest_open_arrival_ms is None or arrival_ms > latest_open_arrival_ms:
                latest_open_arrival_ms = arrival_ms
                current_client_name = _str(
                    stop.get('locationName') or stop.get('description') or ''
                )
                current_client_arrival = arrival_str
                current_client_arrival_ms = arrival_ms

        # Collect order numbers (de-duplicated)
        for number in stop.get('orderNumbers') or []:
            normalized = str(number).strip()
            if normalized and normalized not in seen_orders:
                seen_orders.add(normalized)
                order_numbers.append(normalized)

    route_finished = total_clients > 0 and with_departure >= total_clients

    # Build fingerprint: routeId + lastModificationDate
    route_id = str(route_obj.get('id') or summary.get('routeId') or route_key)
    last_mod = str(route_obj.get('lastModificationDate') or summary.get('lastModificationDate') or '')
    fingerprint = f"{route_id}:{last_mod}" if last_mod else route_id

    return {
        'totalClients': total_clients,
        'withArrival': with_arrival,
        'withDeparture': with_departure,
        'withSignature': with_signature,
        'routeStatus': status,
        'currentClientName': current_client_name,
        'currentClientArrival': current_client_arrival,
        'currentClientArrivalMs': current_client_arrival_ms,
        'routeStartMs': route_start_ms,
        'routeEndMs': route_end_ms,
        'latestDepartureMs': route_end_ms,
        'routeFinished': route_finished,
        'routeResolved': True,
        'orderNumbers': order_numbers,
        'fingerprint': fingerprint,
    }


def _unresolved_snapshot(route_key: str) -> dict:
    return {
        'totalClients': 0,
        'withArrival': 0,
        'withDeparture': 0,
        'withSignature': 0,
        'routeStatus': '',
        'currentClientName': '',
        'currentClientArrival': '',
        'currentClientArrivalMs': None,
        'routeStartMs': None,
        'routeEndMs': None,
        'latestDepartureMs': None,
        'routeFinished': False,
        'routeResolved': False,
        'orderNumbers': [],
        'fingerprint': str(route_key),
    }


# ── Utilities ──────────────────────────────────────────────────────────────────


def _normalize_status(status: str) -> str:
    """Uppercase and normalize GreenMile route status string."""
    return str(status).upper().strip() if status else ''


def _parse_iso_to_ms(value: str) -> float | None:
    """Parse ISO-8601 datetime string to milliseconds since epoch. Returns None on failure."""
    if not value:
        return None
    from datetime import datetime
    text = str(value).strip()
    if not text:
        return None
    if text.endswith('Z'):
        normalized = text
    else:
        normalized = text.replace(' ', 'T')
        if len(normalized) >= 5 and normalized[-5] in ['+', '-'] and normalized[-3] != ':':
            normalized = normalized[:-5] + normalized[-5:-2] + ':' + normalized[-2:]
    formats = [
        '%Y-%m-%dT%H:%M:%SZ',
        '%Y-%m-%dT%H:%M:%S.%fZ',
        '%Y-%m-%dT%H:%M:%S%z',
        '%Y-%m-%dT%H:%M:%S.%f%z',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%dT%H:%M:%S.%f',
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(normalized, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt.timestamp() * 1000
        except ValueError:
            continue
    return None


def _is_truthy_signature(value: Any) -> bool:
    """GreenMile hasSignature can be bool, string 'true', or other truthy value."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() == 'true'
    return bool(value)


def _str(value: Any) -> str:
    return str(value).strip() if value is not None else ''
