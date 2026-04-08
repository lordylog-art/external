"""
Tests for apps_gateway.py — HTTP client for the Apps Script Web App gateway.
Run: python -m pytest external/greenmile_sync/tests/test_apps_gateway.py -v
"""
import sys
import os
import json
import unittest
from unittest.mock import patch, MagicMock

# Make src importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


class TestAppsGatewayPullPendingRouteKeys(unittest.TestCase):

    def _make_config(self, url='https://script.google.com/exec', token='secret'):
        from config import Config
        c = Config.__new__(Config)
        c.apps_script_url = url
        c.apps_script_token = token
        c.chunk_size = 50
        c.request_timeout = 30
        c.max_retries = 3
        return c

    def test_pull_returns_route_keys_list(self):
        """Happy path: Apps Script returns routeKeys array."""
        from apps_gateway import AppsGateway
        cfg = self._make_config()
        gw = AppsGateway(cfg)

        fake_response = {
            'ok': True,
            'result': {
                'routeKeys': ['1234567890', '9876543210'],
                'pulledAt': '2026-04-08T10:00:00Z',
                'totalRows': 2,
                'pendingRows': 2,
            }
        }
        with patch.object(gw, '_post', return_value=fake_response) as mock_post:
            result = gw.pull_pending_route_keys()
        mock_post.assert_called_once_with('greenmile_sync_pull_pending_route_keys', {})
        self.assertIsInstance(result, list)
        self.assertIn('1234567890', result)
        self.assertIn('9876543210', result)

    def test_pull_returns_empty_list_when_no_pending(self):
        """Apps Script returns empty routeKeys."""
        from apps_gateway import AppsGateway
        cfg = self._make_config()
        gw = AppsGateway(cfg)
        fake_response = {
            'ok': True,
            'result': {'routeKeys': [], 'pulledAt': '2026-04-08T10:00:00Z', 'totalRows': 0, 'pendingRows': 0},
        }
        with patch.object(gw, '_post', return_value=fake_response):
            result = gw.pull_pending_route_keys()
        self.assertEqual(result, [])

    def test_pull_raises_on_ok_false(self):
        """Apps Script returns ok=False → raises RuntimeError."""
        from apps_gateway import AppsGateway
        cfg = self._make_config()
        gw = AppsGateway(cfg)
        fake_response = {'ok': False, 'error': 'Token invalido.'}
        with patch.object(gw, '_post', return_value=fake_response):
            with self.assertRaises(RuntimeError) as ctx:
                gw.pull_pending_route_keys()
        self.assertIn('Token invalido', str(ctx.exception))

    def test_pull_raises_on_missing_result(self):
        """Response has ok=True but no result key → raises RuntimeError."""
        from apps_gateway import AppsGateway
        cfg = self._make_config()
        gw = AppsGateway(cfg)
        fake_response = {'ok': True}
        with patch.object(gw, '_post', return_value=fake_response):
            with self.assertRaises(RuntimeError):
                gw.pull_pending_route_keys()

    def test_pull_context_includes_existing_notas_by_route_key(self):
        """Apps Script may return existingNotasByRouteKey metadata for NF reuse."""
        from apps_gateway import AppsGateway
        cfg = self._make_config()
        gw = AppsGateway(cfg)
        fake_response = {
            'ok': True,
            'result': {
                'routeKeys': ['1234567890'],
                'existingNotasByRouteKey': {'1234567890': ['4242668']},
                'pulledAt': '2026-04-08T10:00:00Z',
                'totalRows': 1,
                'pendingRows': 1,
            },
        }
        with patch.object(gw, '_post', return_value=fake_response):
            result = gw.pull_pending_context()
        self.assertEqual(result['routeKeys'], ['1234567890'])
        self.assertEqual(result['existingNotasByRouteKey']['1234567890'], ['4242668'])


class TestAppsGatewayPushSnapshots(unittest.TestCase):

    def _make_config(self):
        from config import Config
        c = Config.__new__(Config)
        c.apps_script_url = 'https://script.google.com/exec'
        c.apps_script_token = 'secret'
        c.chunk_size = 2
        c.request_timeout = 30
        c.max_retries = 3
        return c

    def _make_snapshot(self, route_key='1234567890'):
        return {
            route_key: {
                'totalClients': 3,
                'withArrival': 1,
                'withDeparture': 1,
                'withSignature': 1,
                'routeStatus': 'IN_PROGRESS',
                'currentClientName': 'Cliente Teste',
                'currentClientArrival': '2026-04-08T10:00:00Z',
                'currentClientArrivalMs': 1744099200000,
                'routeStartMs': 1744095600000,
                'routeEndMs': None,
                'latestDepartureMs': None,
                'routeFinished': False,
                'routeResolved': True,
                'orderNumbers': ['NF001'],
                'fingerprint': 'fp1',
            }
        }

    def test_push_calls_action_with_snapshots(self):
        """Push sends snapshots map to Apps Script."""
        from apps_gateway import AppsGateway
        cfg = self._make_config()
        gw = AppsGateway(cfg)
        snapshots = self._make_snapshot()
        fake_response = {
            'ok': True,
            'result': {'processedRows': 1, 'updatedRows': 1, 'routeKeys': 1},
        }
        with patch.object(gw, '_post', return_value=fake_response) as mock_post:
            result = gw.push_route_snapshots(snapshots)
        mock_post.assert_called_once_with(
            'greenmile_sync_push_route_snapshots',
            {'snapshots': snapshots}
        )
        self.assertEqual(result['processedRows'], 1)

    def test_push_sends_in_chunks(self):
        """chunk_size=2 with 3 snapshots → 2 POST calls."""
        from apps_gateway import AppsGateway
        cfg = self._make_config()
        cfg.chunk_size = 2
        gw = AppsGateway(cfg)
        snapshots = {
            '0000000001': {**self._make_snapshot()['1234567890']},
            '0000000002': {**self._make_snapshot()['1234567890']},
            '0000000003': {**self._make_snapshot()['1234567890']},
        }
        fake_response = {'ok': True, 'result': {'processedRows': 2, 'updatedRows': 1, 'routeKeys': 2}}
        with patch.object(gw, '_post', return_value=fake_response) as mock_post:
            gw.push_route_snapshots(snapshots)
        self.assertEqual(mock_post.call_count, 2)

    def test_push_raises_on_ok_false(self):
        """Push raises RuntimeError when Apps Script rejects."""
        from apps_gateway import AppsGateway
        cfg = self._make_config()
        gw = AppsGateway(cfg)
        snapshots = self._make_snapshot()
        fake_response = {'ok': False, 'error': 'Acao nao suportada'}
        with patch.object(gw, '_post', return_value=fake_response):
            with self.assertRaises(RuntimeError):
                gw.push_route_snapshots(snapshots)

    def test_push_empty_snapshots_skips_call(self):
        """Empty snapshots dict → no HTTP call, returns empty summary."""
        from apps_gateway import AppsGateway
        cfg = self._make_config()
        gw = AppsGateway(cfg)
        with patch.object(gw, '_post') as mock_post:
            result = gw.push_route_snapshots({})
        mock_post.assert_not_called()
        self.assertEqual(result.get('processedRows', 0), 0)


class TestAppsGatewayPost(unittest.TestCase):
    """Tests for the internal _post method (HTTP layer)."""

    def _make_config(self):
        from config import Config
        c = Config.__new__(Config)
        c.apps_script_url = 'https://script.google.com/macros/s/abc/exec'
        c.apps_script_token = 'mytoken'
        c.chunk_size = 50
        c.request_timeout = 10
        c.max_retries = 1
        return c

    def test_post_sends_correct_body(self):
        """_post constructs correct JSON body with token and action."""
        import urllib.request
        from apps_gateway import AppsGateway
        cfg = self._make_config()
        gw = AppsGateway(cfg)

        captured = {}
        def fake_urlopen(req, timeout=None):
            captured['url'] = req.full_url
            captured['body'] = json.loads(req.data.decode())
            mock_resp = MagicMock()
            mock_resp.read.return_value = json.dumps({'ok': True, 'result': {}}).encode()
            mock_resp.__enter__ = lambda s: s
            mock_resp.__exit__ = MagicMock(return_value=False)
            return mock_resp

        with patch('urllib.request.urlopen', side_effect=fake_urlopen):
            gw._post('ping', {'x': 1})

        self.assertEqual(captured['body']['token'], 'mytoken')
        self.assertEqual(captured['body']['action'], 'ping')
        self.assertEqual(captured['body']['payload'], {'x': 1})
        self.assertIn('requestedAt', captured['body'])

    def test_post_strips_json_prefix(self):
        """_post handles Apps Script's while(1); prefix."""
        from apps_gateway import AppsGateway
        cfg = self._make_config()
        gw = AppsGateway(cfg)

        mock_resp = MagicMock()
        mock_resp.read.return_value = b'while(1);{"ok":true,"result":{"pong":true}}'
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        with patch('urllib.request.urlopen', return_value=mock_resp):
            result = gw._post('ping', {})

        self.assertTrue(result['ok'])
        self.assertTrue(result['result']['pong'])

    def test_post_uses_resilient_http_client_with_timeout_and_retries(self):
        """_post delegates to post_json with config timeout/retry values."""
        from apps_gateway import AppsGateway
        cfg = self._make_config()
        cfg.request_timeout = 75
        cfg.max_retries = 4
        gw = AppsGateway(cfg)

        with patch('apps_gateway.post_json', return_value={'ok': True, 'result': {'pong': True}}) as mock_post_json:
            result = gw._post('ping', {'x': 1})

        self.assertTrue(result['ok'])
        _, kwargs = mock_post_json.call_args
        self.assertEqual(kwargs['timeout'], 75)
        self.assertEqual(kwargs['max_retries'], 4)
        self.assertEqual(kwargs['body']['action'], 'ping')


if __name__ == '__main__':
    unittest.main()
