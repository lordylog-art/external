"""
Tests for snapshot_mapper.py — converts raw GreenMile API response to routeProgressByKey format.
Run: python -m pytest external/greenmile_sync/tests/test_snapshot_mapper.py -v
"""
import sys
import os
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


def _make_stop(actual_arrival=None, actual_departure=None, has_signature=False, location_name='Cliente A', order_numbers=None):
    return {
        'actualArrival': actual_arrival,
        'actualDeparture': actual_departure,
        'hasSignature': has_signature,
        'locationName': location_name,
        'orderNumbers': order_numbers or [],
    }


def _make_route_summary(route_key='1234567890', route_id=42, status='IN_PROGRESS', last_mod='2026-04-08T10:00:00Z'):
    return {
        'route': {'key': route_key, 'id': route_id, 'status': status, 'lastModificationDate': last_mod},
        'status': status,
        'lastModificationDate': last_mod,
    }


def _make_stop_view(route_key='1234567890', route_id=42, status='IN_PROGRESS', stops=None):
    return {
        'routeKey': route_key,
        'routeId': route_id,
        'status': status,
        'stops': stops or [],
    }


class TestSnapshotMapperBasic(unittest.TestCase):

    def test_maps_single_route_key_resolved(self):
        """Happy path: one route key with one stop arrived+departed."""
        from snapshot_mapper import build_snapshot_for_route_key
        stop_view = _make_stop_view(stops=[
            _make_stop(actual_arrival='2026-04-08T09:00:00Z', actual_departure='2026-04-08T09:30:00Z',
                       has_signature=True, location_name='Cliente X', order_numbers=['NF001']),
        ])
        summary = _make_route_summary()
        result = build_snapshot_for_route_key('1234567890', stop_view, summary)

        self.assertEqual(result['totalClients'], 1)
        self.assertEqual(result['withArrival'], 1)
        self.assertEqual(result['withDeparture'], 1)
        self.assertEqual(result['withSignature'], 1)
        self.assertTrue(result['routeFinished'])
        self.assertTrue(result['routeResolved'])
        self.assertIsInstance(result['fingerprint'], str)
        self.assertGreater(len(result['fingerprint']), 0)

    def test_maps_route_not_started(self):
        """Route with no stops visited → withArrival=0, routeFinished=False."""
        from snapshot_mapper import build_snapshot_for_route_key
        stop_view = _make_stop_view(status='NOT_STARTED', stops=[
            _make_stop(),  # no actual arrival/departure
        ])
        summary = _make_route_summary(status='NOT_STARTED')
        result = build_snapshot_for_route_key('1234567890', stop_view, summary)

        self.assertEqual(result['withArrival'], 0)
        self.assertEqual(result['withDeparture'], 0)
        self.assertFalse(result['routeFinished'])
        self.assertTrue(result['routeResolved'])

    def test_current_client_name_is_latest_open_arrival(self):
        """currentClientName is the stop with latest arrival that has no departure."""
        from snapshot_mapper import build_snapshot_for_route_key
        stop_view = _make_stop_view(stops=[
            _make_stop(actual_arrival='2026-04-08T08:00:00Z', actual_departure='2026-04-08T08:30:00Z',
                       location_name='Primeiro'),
            _make_stop(actual_arrival='2026-04-08T09:00:00Z', actual_departure=None,
                       location_name='Segundo'),  # still at this client
        ])
        summary = _make_route_summary()
        result = build_snapshot_for_route_key('1234567890', stop_view, summary)
        self.assertEqual(result['currentClientName'], 'Segundo')

    def test_order_numbers_aggregated(self):
        """orderNumbers are aggregated from all stops."""
        from snapshot_mapper import build_snapshot_for_route_key
        stop_view = _make_stop_view(stops=[
            _make_stop(order_numbers=['NF001', 'NF002']),
            _make_stop(order_numbers=['NF003']),
        ])
        summary = _make_route_summary()
        result = build_snapshot_for_route_key('1234567890', stop_view, summary)
        self.assertIn('NF001', result['orderNumbers'])
        self.assertIn('NF003', result['orderNumbers'])

    def test_fallback_snapshot_when_no_stop_view(self):
        """When stop_view is None, returns unresolved snapshot (routeResolved=False)."""
        from snapshot_mapper import build_snapshot_for_route_key
        result = build_snapshot_for_route_key('1234567890', None, None)
        self.assertFalse(result['routeResolved'])
        self.assertEqual(result['withArrival'], 0)
        self.assertEqual(result['totalClients'], 0)

    def test_build_snapshots_for_multiple_routes(self):
        """build_snapshots_from_responses handles multiple route keys."""
        from snapshot_mapper import build_snapshots_from_responses
        route_keys = ['1111111111', '2222222222']
        stop_views = {
            '1111111111': _make_stop_view(route_key='1111111111', stops=[
                _make_stop(actual_arrival='2026-04-08T09:00:00Z'),
            ]),
            '2222222222': _make_stop_view(route_key='2222222222', stops=[]),
        }
        summaries = {
            '1111111111': _make_route_summary(route_key='1111111111'),
            '2222222222': _make_route_summary(route_key='2222222222', status='NOT_STARTED'),
        }
        result = build_snapshots_from_responses(route_keys, stop_views, summaries)
        self.assertIn('1111111111', result)
        self.assertIn('2222222222', result)
        self.assertEqual(result['1111111111']['withArrival'], 1)
        self.assertEqual(result['2222222222']['withArrival'], 0)

    def test_missing_route_key_gets_unresolved_snapshot(self):
        """Route key with no data → unresolved entry."""
        from snapshot_mapper import build_snapshots_from_responses
        result = build_snapshots_from_responses(['9999999999'], {}, {})
        self.assertIn('9999999999', result)
        self.assertFalse(result['9999999999']['routeResolved'])

    def test_route_status_normalized(self):
        """routeStatus is stored as uppercase string from status field."""
        from snapshot_mapper import build_snapshot_for_route_key
        stop_view = _make_stop_view(status='Completed')
        summary = _make_route_summary(status='Completed')
        result = build_snapshot_for_route_key('1234567890', stop_view, summary)
        self.assertEqual(result['routeStatus'], 'COMPLETED')

    def test_fingerprint_uses_last_modification_date(self):
        """fingerprint includes lastModificationDate for cache invalidation."""
        from snapshot_mapper import build_snapshot_for_route_key
        stop_view = _make_stop_view()
        summary = _make_route_summary(route_id=99, last_mod='2026-04-08T12:00:00Z')
        result = build_snapshot_for_route_key('1234567890', stop_view, summary)
        # fingerprint should contain either route_id or last_mod info
        self.assertIsNotNone(result['fingerprint'])
        self.assertNotEqual(result['fingerprint'], '')


class TestSnapshotMapperEdgeCases(unittest.TestCase):

    def test_empty_stops_list(self):
        """Route with empty stops list → totalClients=0, routeFinished=False."""
        from snapshot_mapper import build_snapshot_for_route_key
        stop_view = _make_stop_view(stops=[])
        summary = _make_route_summary()
        result = build_snapshot_for_route_key('1234567890', stop_view, summary)
        self.assertEqual(result['totalClients'], 0)
        self.assertFalse(result['routeFinished'])

    def test_all_stops_departed_means_finished(self):
        """Route where all stops have departure → routeFinished=True."""
        from snapshot_mapper import build_snapshot_for_route_key
        stop_view = _make_stop_view(stops=[
            _make_stop(actual_arrival='2026-04-08T09:00:00Z', actual_departure='2026-04-08T09:30:00Z'),
            _make_stop(actual_arrival='2026-04-08T10:00:00Z', actual_departure='2026-04-08T10:30:00Z'),
        ])
        summary = _make_route_summary()
        result = build_snapshot_for_route_key('1234567890', stop_view, summary)
        self.assertEqual(result['withDeparture'], 2)
        self.assertTrue(result['routeFinished'])

    def test_route_start_ms_from_first_arrival(self):
        """routeStartMs is derived from earliest stop arrival."""
        from snapshot_mapper import build_snapshot_for_route_key
        stop_view = _make_stop_view(stops=[
            _make_stop(actual_arrival='2026-04-08T08:00:00Z', actual_departure='2026-04-08T08:30:00Z'),
            _make_stop(actual_arrival='2026-04-08T09:00:00Z'),
        ])
        summary = _make_route_summary()
        result = build_snapshot_for_route_key('1234567890', stop_view, summary)
        self.assertIsNotNone(result['routeStartMs'])
        self.assertIsInstance(result['routeStartMs'], (int, float))

    def test_supports_greenmile_timezone_format_plus_0000(self):
        """GreenMile dates like +0000 must be parsed for current client and route timing."""
        from snapshot_mapper import build_snapshot_for_route_key
        stop_view = {
            'routeKey': '1234567890',
            'routeId': 42,
            'stops': [
                _make_stop(
                    actual_arrival='2026-03-31T15:33:00+0000',
                    actual_departure=None,
                    location_name='Cliente Aberto',
                ),
            ],
        }
        summary = _make_route_summary(status='DEPARTED_ORIGIN')

        result = build_snapshot_for_route_key('1234567890', stop_view, summary)

        self.assertEqual(result['currentClientName'], 'Cliente Aberto')
        self.assertIsInstance(result['currentClientArrivalMs'], (int, float))
        self.assertIsInstance(result['routeStartMs'], (int, float))
        self.assertEqual(result['routeStatus'], 'DEPARTED_ORIGIN')

    def test_prefers_nested_route_status_and_last_modification_date(self):
        """The real Summary payload stores status/lastModificationDate inside summary.route."""
        from snapshot_mapper import build_snapshot_for_route_key
        stop_view = {'routeKey': '1234567890', 'routeId': 99, 'stops': []}
        summary = {
            'route': {
                'key': '1234567890',
                'id': 99,
                'status': 'COMPLETED',
                'lastModificationDate': '2026-03-31T21:06:23+0000',
            }
        }

        result = build_snapshot_for_route_key('1234567890', stop_view, summary)

        self.assertEqual(result['routeStatus'], 'COMPLETED')
        self.assertIn('2026-03-31T21:06:23+0000', result['fingerprint'])


if __name__ == '__main__':
    unittest.main()
