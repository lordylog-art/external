import json
import os
import sys
import unittest
import urllib.error
import urllib.parse
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


class _FakeHeaders(dict):
    def get(self, key, default=None):
        return super().get(key, default)


class _FakeResponse:
    def __init__(self, payload, headers=None, status=200):
        self._payload = payload
        self.headers = _FakeHeaders(headers or {})
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        if isinstance(self._payload, str):
            return self._payload.encode('utf-8')
        return json.dumps(self._payload).encode('utf-8')


class TestGreenmileClientContract(unittest.TestCase):

    def _make_config(self):
        from config import Config

        config = Config.__new__(Config)
        config.greenmile_url = 'https://3coracoes.greenmile.com'
        config.greenmile_username = 'user'
        config.greenmile_password = 'pass'
        config.request_timeout = 30
        config.max_retries = 1
        return config

    def test_login_uses_form_post_and_stores_cookie_and_bearer(self):
        from greenmile_client import GreenmileClient

        requests = []

        def fake_urlopen(req, timeout=None):
            requests.append((req, timeout))
            return _FakeResponse(
                {
                    'analyticsToken': {
                        'access_token': 'bearer-token',
                        'expires_in': 180,
                    },
                    'jsessionid': 'abc123',
                },
                headers={'Set-Cookie': 'JSESSIONID=abc123; Path=/, XSRF=zzz; Path=/'},
            )

        client = GreenmileClient(self._make_config())

        with patch('urllib.request.urlopen', side_effect=fake_urlopen):
            auth = client.login()

        self.assertEqual(auth['token'], 'bearer-token')
        self.assertEqual(auth['cookie'], 'JSESSIONID=abc123; XSRF=zzz')
        self.assertEqual(len(requests), 1)
        req, timeout = requests[0]
        self.assertEqual(timeout, 30)
        self.assertEqual(req.full_url, 'https://3coracoes.greenmile.com/login')
        self.assertEqual(req.get_method(), 'POST')
        self.assertEqual(req.headers['Content-type'], 'application/x-www-form-urlencoded')
        self.assertEqual(req.headers['Greenmile-module'], 'LIVE')
        body = urllib.parse.parse_qs(req.data.decode('utf-8'))
        self.assertEqual(body['j_username'], ['user'])
        self.assertEqual(body['j_password'], ['pass'])

    def test_fetch_route_summaries_uses_routeview_summary_post_contract(self):
        from greenmile_client import GreenmileClient

        requests = []

        def fake_urlopen(req, timeout=None):
            requests.append(req)
            if req.full_url.endswith('/login'):
                return _FakeResponse(
                    {'analyticsToken': {'access_token': 'bearer-token'}},
                    headers={'set-cookie': 'JSESSIONID=abc123; Path=/'},
                )
            return _FakeResponse(
                [
                    {
                        'route': {
                            'id': 2640528,
                            'key': '6103050235',
                            'status': 'DEPARTED_ORIGIN',
                        }
                    },
                    {
                        'route': {
                            'id': 2642360,
                            'key': '6103051747',
                            'status': 'DEPARTED_ORIGIN',
                        }
                    },
                ]
            )

        client = GreenmileClient(self._make_config())

        with patch('urllib.request.urlopen', side_effect=fake_urlopen):
            summaries = client.fetch_route_summaries(['6103050235', '6103051747'])

        self.assertIn('6103050235', summaries)
        self.assertIn('6103051747', summaries)
        self.assertEqual(len(requests), 2)
        req = requests[1]
        self.assertEqual(req.get_method(), 'POST')
        self.assertIn('/RouteView/Summary?criteria=', req.full_url)
        criteria = json.loads(urllib.parse.unquote(req.full_url.split('criteria=')[1]))
        self.assertEqual(criteria['firstResult'], 0)
        self.assertEqual(criteria['maxResults'], 2)
        self.assertIn('route.status', criteria['filters'])
        payload = json.loads(req.data.decode('utf-8'))
        self.assertEqual(payload['sort'][0]['attr'], 'route.date')
        filters = payload['criteriaChain'][0]['or']
        self.assertEqual(filters[0]['attr'], 'route.key')
        self.assertEqual(filters[0]['eq'], '6103050235')
        self.assertEqual(filters[0]['matchMode'], 'EXACT')
        self.assertEqual(filters[1]['eq'], '6103051747')
        self.assertEqual(req.headers['Authorization'], 'Bearer bearer-token')
        self.assertEqual(req.headers['Cookie'], 'JSESSIONID=abc123')
        self.assertEqual(req.headers['Greenmile-build'], '1705315')
        self.assertEqual(req.headers['Greenmile-version'], '26.0130')

    def test_fetch_stop_views_resolves_route_id_and_calls_stopview_restrictions(self):
        from greenmile_client import GreenmileClient

        requests = []

        def fake_urlopen(req, timeout=None):
            requests.append(req)
            if req.full_url.endswith('/login'):
                return _FakeResponse(
                    {'analyticsToken': {'access_token': 'bearer-token'}},
                    headers={'set-cookie': 'JSESSIONID=abc123; Path=/'},
                )
            if '/RouteView/Summary?' in req.full_url:
                return _FakeResponse(
                    [
                        {
                            'route': {
                                'id': 2640528,
                                'key': '6103050235',
                                'lastModificationDate': '2026-03-31T21:06:23+0000',
                            }
                        },
                        {
                            'route': {
                                'id': 2642360,
                                'key': '6103051747',
                                'lastModificationDate': '2026-03-31T21:07:00+0000',
                            }
                        }
                    ]
                )
            if '/Order/restrictions?' in req.full_url:
                stop_id = json.loads(req.data.decode('utf-8'))['criteriaChain'][0]['and'][0]['eq']
                number = 'NF-1' if stop_id == '18457923' else 'NF-2'
                return _FakeResponse([{'number': number}])
            return _FakeResponse(
                [
                    {
                        'route': {'id': 2640528},
                        'stop': {'id': 18457923, 'key': '1000397074'},
                    }
                    ,
                    {
                        'route': {'id': 2642360},
                        'stop': {'id': 18460000, 'key': '1000500000'},
                    }
                ]
            )

        client = GreenmileClient(self._make_config())

        with patch('urllib.request.urlopen', side_effect=fake_urlopen):
            stop_views = client.fetch_stop_views(['6103050235', '6103051747'])

        self.assertIn('6103050235', stop_views)
        self.assertIn('6103051747', stop_views)
        self.assertEqual(stop_views['6103050235']['routeId'], 2640528)
        self.assertEqual(len(stop_views['6103050235']['stops']), 1)
        self.assertEqual(stop_views['6103050235']['stops'][0]['orderNumbers'], ['NF-1'])
        self.assertEqual(stop_views['6103051747']['routeId'], 2642360)
        self.assertEqual(len(stop_views['6103051747']['stops']), 1)
        self.assertEqual(stop_views['6103051747']['stops'][0]['orderNumbers'], ['NF-2'])
        self.assertEqual(len(requests), 5)
        req = requests[2]
        self.assertEqual(req.get_method(), 'POST')
        self.assertIn('/StopView/restrictions?criteria=', req.full_url)
        criteria = json.loads(urllib.parse.unquote(req.full_url.split('criteria=')[1]))
        self.assertIn('geofence', criteria['including'])
        self.assertIn('stop.location.*', criteria['filters'])
        payload = json.loads(req.data.decode('utf-8'))
        self.assertEqual(payload['sort'][0]['attr'], 'stop.plannedSequenceNum')
        filters = payload['criteriaChain'][0]['or']
        self.assertEqual(filters[0]['attr'], 'route.id')
        self.assertEqual(filters[0]['eq'], '2640528')
        self.assertEqual(filters[1]['eq'], '2642360')

    def test_route_summary_requests_last_modification_date_fields(self):
        from greenmile_client import GreenmileClient

        requests = []

        def fake_urlopen(req, timeout=None):
            requests.append(req)
            if req.full_url.endswith('/login'):
                return _FakeResponse(
                    {'analyticsToken': {'access_token': 'bearer-token'}},
                    headers={'set-cookie': 'JSESSIONID=abc123; Path=/'},
                )
            return _FakeResponse([])

        client = GreenmileClient(self._make_config())

        with patch('urllib.request.urlopen', side_effect=fake_urlopen):
            client.fetch_route_summaries(['6103050235'])

        req = requests[1]
        criteria = json.loads(urllib.parse.unquote(req.full_url.split('criteria=')[1]))
        self.assertIn('route.lastModificationDate', criteria['filters'])
        self.assertIn('lastModificationDate', criteria['filters'])

    def test_order_restrictions_request_matches_browser_filter_list(self):
        from greenmile_client import GreenmileClient

        requests = []

        def fake_urlopen(req, timeout=None):
            requests.append(req)
            if req.full_url.endswith('/login'):
                return _FakeResponse(
                    {'analyticsToken': {'access_token': 'bearer-token'}},
                    headers={'Set-Cookie': 'SESSION=abc; Path=/'},
                )
            if '/RouteView/Summary?' in req.full_url:
                return _FakeResponse(
                    [{'route': {'id': 2640528, 'key': '6103050235'}}]
                )
            if '/StopView/restrictions?' in req.full_url:
                return _FakeResponse(
                    [{'route': {'id': 2640528}, 'stop': {'id': 18391077, 'key': '1000397074'}}]
                )
            return _FakeResponse([{'number': 'NF001'}])

        client = GreenmileClient(self._make_config())

        with patch('urllib.request.urlopen', side_effect=fake_urlopen):
            client.fetch_stop_views(['6103050235'])

        order_req = requests[-1]
        self.assertIn('/Order/restrictions?criteria=', order_req.full_url)
        criteria = json.loads(urllib.parse.unquote(order_req.full_url.split('criteria=')[1]))
        self.assertNotIn('*', criteria['filters'])
        self.assertNotIn('invoiceValue', criteria['filters'])
        self.assertNotIn('totalValue', criteria['filters'])
        self.assertNotIn('orderValue', criteria['filters'])
        self.assertIn('lineItems.lineItemID', criteria['filters'])
        payload = json.loads(order_req.data.decode('utf-8'))
        self.assertEqual(payload['criteriaChain'][0]['and'][0]['attr'], 'stop.id')
        self.assertEqual(payload['criteriaChain'][0]['and'][0]['eq'], '18391077')

    def test_fetch_stop_views_keeps_sync_alive_when_order_restrictions_fail(self):
        from greenmile_client import GreenmileClient

        requests = []

        def fake_urlopen(req, timeout=None):
            requests.append(req)
            if req.full_url.endswith('/login'):
                return _FakeResponse(
                    {'analyticsToken': {'access_token': 'bearer-token'}},
                    headers={'set-cookie': 'JSESSIONID=abc123; Path=/'},
                )
            if '/RouteView/Summary?' in req.full_url:
                return _FakeResponse(
                    [{'route': {'id': 2640528, 'key': '6103050235'}}]
                )
            if '/StopView/restrictions?' in req.full_url:
                return _FakeResponse(
                    [{'route': {'id': 2640528}, 'stop': {'id': 18457923, 'key': '1000397074'}}]
                )
            raise urllib.error.HTTPError(
                req.full_url,
                500,
                'Internal Server Error',
                None,
                None,
            )

        client = GreenmileClient(self._make_config())

        with patch('urllib.request.urlopen', side_effect=fake_urlopen):
            stop_views = client.fetch_stop_views(['6103050235'])

        self.assertEqual(stop_views['6103050235']['stops'][0]['orderNumbers'], [])


if __name__ == '__main__':
    unittest.main()
