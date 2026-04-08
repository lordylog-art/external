"""
Tests for sync_runner.py — end-to-end orchestration with mocks.
Run: python -m pytest external/greenmile_sync/tests/test_sync_runner.py -v
"""
import sys
import os
import unittest
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


class TestSyncRunnerOrchestration(unittest.TestCase):

    def _make_config(self):
        from config import Config
        c = Config.__new__(Config)
        c.apps_script_url = 'https://script.google.com/macros/s/abc/exec'
        c.apps_script_token = 'token'
        c.greenmile_url = 'https://3coracoes.greenmile.com'
        c.greenmile_username = 'user'
        c.greenmile_password = 'pass'
        c.chunk_size = 50
        c.request_timeout = 30
        c.max_retries = 3
        c.snapshot_reuse_ttl_seconds = 600
        return c

    def test_full_run_happy_path(self):
        """Full sync: pull 2 keys → fetch GreenMile → build snapshots → push."""
        from sync_runner import SyncRunner
        cfg = self._make_config()
        mock_gw = MagicMock()
        mock_gm = MagicMock()

        mock_gw.pull_pending_route_keys.return_value = ['1234567890', '9876543210']
        mock_gm.fetch_stop_views.return_value = {
            '1234567890': {'routeKey': '1234567890', 'status': 'IN_PROGRESS', 'stops': []},
            '9876543210': {'routeKey': '9876543210', 'status': 'COMPLETED', 'stops': []},
        }
        mock_gm.fetch_route_summaries.return_value = {
            '1234567890': {'route': {'key': '1234567890', 'id': 1}, 'status': 'IN_PROGRESS', 'lastModificationDate': '2026-04-08T10:00:00Z'},
            '9876543210': {'route': {'key': '9876543210', 'id': 2}, 'status': 'COMPLETED', 'lastModificationDate': '2026-04-08T09:00:00Z'},
        }
        mock_gw.push_route_snapshots.return_value = {'processedRows': 2, 'updatedRows': 1}

        runner = SyncRunner(cfg, apps_gateway=mock_gw, greenmile_client=mock_gm)
        result = runner.run()

        mock_gw.pull_pending_route_keys.assert_called_once()
        mock_gm.fetch_stop_views.assert_called_once_with(
            ['1234567890', '9876543210'],
            skip_order_numbers_for_route_keys=set(),
        )
        mock_gm.fetch_route_summaries.assert_called_once_with(['1234567890', '9876543210'])
        mock_gw.push_route_snapshots.assert_called_once()

        self.assertEqual(result['route_keys_found'], 2)
        self.assertIn('push_result', result)

    def test_run_reuses_existing_nf_and_skips_refetch(self):
        """When Apps Script reports existing NF, the cycle skips refetch and preserves snapshot orderNumbers."""
        from sync_runner import SyncRunner
        cfg = self._make_config()
        mock_gw = MagicMock()
        mock_gm = MagicMock()

        mock_gw.pull_pending_context.return_value = {
            'routeKeys': ['6103062156'],
            'existingNotasByRouteKey': {'6103062156': ['4242668']},
        }
        mock_gm.fetch_stop_views.return_value = {
            '6103062156': {'routeKey': '6103062156', 'status': 'COMPLETED', 'stops': [{'orderNumbers': []}]},
        }
        mock_gm.fetch_route_summaries.return_value = {
            '6103062156': {
                'route': {
                    'key': '6103062156',
                    'id': 2654833,
                    'status': 'COMPLETED',
                    'lastModificationDate': '2026-04-08T14:47:48+0000',
                },
            }
        }
        mock_gw.push_route_snapshots.return_value = {'processedRows': 1, 'updatedRows': 1}

        runner = SyncRunner(cfg, apps_gateway=mock_gw, greenmile_client=mock_gm)
        runner.run()

        mock_gm.fetch_stop_views.assert_called_once_with(
            ['6103062156'],
            skip_order_numbers_for_route_keys={'6103062156'},
        )
        args, _ = mock_gw.push_route_snapshots.call_args
        pushed_snapshot = args[0]['6103062156']
        self.assertEqual(pushed_snapshot['orderNumbers'], ['4242668'])

    def test_run_returns_panel_indicators_from_pull_and_push(self):
        """Run result exposes pending rows, moved rows and last successful POST timestamp."""
        from sync_runner import SyncRunner
        cfg = self._make_config()
        mock_gw = MagicMock()
        mock_gm = MagicMock()

        mock_gw.pull_pending_context.return_value = {
            'routeKeys': ['1234567890', '9876543210'],
            'existingNotasByRouteKey': {},
            'pendingRows': 7,
        }
        mock_gm.fetch_stop_views.return_value = {
            '1234567890': {'routeKey': '1234567890', 'status': 'IN_PROGRESS', 'stops': []},
            '9876543210': {'routeKey': '9876543210', 'status': 'COMPLETED', 'stops': []},
        }
        mock_gm.fetch_route_summaries.return_value = {
            '1234567890': {'route': {'key': '1234567890', 'id': 1}, 'status': 'IN_PROGRESS', 'lastModificationDate': '2026-04-08T10:00:00Z'},
            '9876543210': {'route': {'key': '9876543210', 'id': 2}, 'status': 'COMPLETED', 'lastModificationDate': '2026-04-08T09:00:00Z'},
        }
        mock_gw.push_route_snapshots.return_value = {'processedRows': 2, 'updatedRows': 3}

        runner = SyncRunner(cfg, apps_gateway=mock_gw, greenmile_client=mock_gm)
        result = runner.run()

        self.assertEqual(result['pending_rows'], 7)
        self.assertEqual(result['moved_rows'], 3)
        self.assertTrue(isinstance(result['last_post_succeeded_at'], str) and result['last_post_succeeded_at'])

    def test_run_with_no_pending_route_keys_skips_greenmile(self):
        """When no pending route.keys, GreenMile is NOT called."""
        from sync_runner import SyncRunner
        cfg = self._make_config()
        mock_gw = MagicMock()
        mock_gm = MagicMock()
        mock_gw.pull_pending_route_keys.return_value = []

        runner = SyncRunner(cfg, apps_gateway=mock_gw, greenmile_client=mock_gm)
        result = runner.run()

        mock_gm.fetch_stop_views.assert_not_called()
        mock_gm.fetch_route_summaries.assert_not_called()
        mock_gw.push_route_snapshots.assert_not_called()
        self.assertEqual(result['route_keys_found'], 0)
        self.assertEqual(result.get('skipped'), True)

    def test_run_greenmile_fetch_error_raises(self):
        """GreenMile fetch failure propagates as exception."""
        from sync_runner import SyncRunner
        cfg = self._make_config()
        mock_gw = MagicMock()
        mock_gm = MagicMock()

        mock_gw.pull_pending_route_keys.return_value = ['1234567890']
        mock_gm.fetch_stop_views.side_effect = ConnectionError('GreenMile unreachable')

        runner = SyncRunner(cfg, apps_gateway=mock_gw, greenmile_client=mock_gm)
        with self.assertRaises(ConnectionError):
            runner.run()

        mock_gw.push_route_snapshots.assert_not_called()

    def test_run_push_error_propagates(self):
        """Push failure propagates; logs route_keys_found before raising."""
        from sync_runner import SyncRunner
        cfg = self._make_config()
        mock_gw = MagicMock()
        mock_gm = MagicMock()

        mock_gw.pull_pending_route_keys.return_value = ['1234567890']
        mock_gm.fetch_stop_views.return_value = {'1234567890': {'routeKey': '1234567890', 'status': 'NOT_STARTED', 'stops': []}}
        mock_gm.fetch_route_summaries.return_value = {'1234567890': {'route': {'key': '1234567890', 'id': 1}, 'status': 'NOT_STARTED', 'lastModificationDate': '2026-04-08T10:00:00Z'}}
        mock_gw.push_route_snapshots.side_effect = RuntimeError('Apps Script rejected push')

        runner = SyncRunner(cfg, apps_gateway=mock_gw, greenmile_client=mock_gm)
        with self.assertRaises(RuntimeError) as ctx:
            runner.run()
        self.assertIn('Apps Script rejected push', str(ctx.exception))

    def test_snapshots_passed_to_push_are_dict(self):
        """Snapshots passed to push_route_snapshots are a dict keyed by route_key."""
        from sync_runner import SyncRunner
        cfg = self._make_config()
        mock_gw = MagicMock()
        mock_gm = MagicMock()

        mock_gw.pull_pending_route_keys.return_value = ['1234567890']
        mock_gm.fetch_stop_views.return_value = {
            '1234567890': {'routeKey': '1234567890', 'status': 'IN_PROGRESS', 'stops': []}
        }
        mock_gm.fetch_route_summaries.return_value = {
            '1234567890': {'route': {'key': '1234567890', 'id': 1}, 'status': 'IN_PROGRESS', 'lastModificationDate': '2026-04-08T10:00:00Z'}
        }
        mock_gw.push_route_snapshots.return_value = {'processedRows': 1}

        runner = SyncRunner(cfg, apps_gateway=mock_gw, greenmile_client=mock_gm)
        runner.run()

        args, _ = mock_gw.push_route_snapshots.call_args
        snapshots = args[0]
        self.assertIsInstance(snapshots, dict)
        self.assertIn('1234567890', snapshots)

    def test_run_result_contains_timing_info(self):
        """Run result includes started_at and finished_at timestamps."""
        from sync_runner import SyncRunner
        cfg = self._make_config()
        mock_gw = MagicMock()
        mock_gm = MagicMock()

        mock_gw.pull_pending_route_keys.return_value = []

        runner = SyncRunner(cfg, apps_gateway=mock_gw, greenmile_client=mock_gm)
        result = runner.run()

        self.assertIn('started_at', result)
        self.assertIn('finished_at', result)

    def test_run_reuses_last_snapshots_when_pending_route_keys_do_not_change_and_cache_is_fresh(self):
        """If pending route.keys are unchanged and cache is fresh/valid, skip GreenMile once."""
        from sync_runner import SyncRunner
        cfg = self._make_config()
        mock_gw = MagicMock()
        mock_gm = MagicMock()

        mock_gw.pull_pending_context.side_effect = [
            {'routeKeys': ['1234567890'], 'existingNotasByRouteKey': {}},
            {'routeKeys': ['1234567890'], 'existingNotasByRouteKey': {}},
        ]
        mock_gm.fetch_stop_views.return_value = {
            '1234567890': {'routeKey': '1234567890', 'status': 'IN_PROGRESS', 'stops': []},
        }
        mock_gm.fetch_route_summaries.return_value = {
            '1234567890': {
                'route': {'key': '1234567890', 'id': 1},
                'status': 'IN_PROGRESS',
                'lastModificationDate': '2026-04-08T10:00:00Z',
            }
        }
        mock_gw.push_route_snapshots.return_value = {'processedRows': 1, 'updatedRows': 1}

        runner = SyncRunner(cfg, apps_gateway=mock_gw, greenmile_client=mock_gm)

        first_result = runner.run()
        second_result = runner.run()

        self.assertEqual(mock_gm.fetch_stop_views.call_count, 1)
        self.assertEqual(mock_gm.fetch_route_summaries.call_count, 1)
        self.assertEqual(mock_gw.push_route_snapshots.call_count, 2)
        self.assertEqual(first_result.get('sync_mode'), 'greenmile_refresh')
        self.assertEqual(second_result.get('sync_mode'), 'apps_script_refresh_only')

    def test_run_does_not_reuse_stale_cached_snapshots(self):
        """If snapshot cache expired, same route.keys must refresh GreenMile again."""
        from sync_runner import SyncRunner
        cfg = self._make_config()
        cfg.snapshot_reuse_ttl_seconds = 0
        mock_gw = MagicMock()
        mock_gm = MagicMock()

        mock_gw.pull_pending_context.side_effect = [
            {'routeKeys': ['1234567890'], 'existingNotasByRouteKey': {}},
            {'routeKeys': ['1234567890'], 'existingNotasByRouteKey': {}},
        ]
        mock_gm.fetch_stop_views.return_value = {
            '1234567890': {'routeKey': '1234567890', 'status': 'IN_PROGRESS', 'stops': []},
        }
        mock_gm.fetch_route_summaries.return_value = {
            '1234567890': {
                'route': {'key': '1234567890', 'id': 1},
                'status': 'IN_PROGRESS',
                'lastModificationDate': '2026-04-08T10:00:00Z',
            }
        }
        mock_gw.push_route_snapshots.return_value = {'processedRows': 1, 'updatedRows': 1}

        runner = SyncRunner(cfg, apps_gateway=mock_gw, greenmile_client=mock_gm)
        runner.run()
        second_result = runner.run()

        self.assertEqual(mock_gm.fetch_stop_views.call_count, 2)
        self.assertEqual(mock_gm.fetch_route_summaries.call_count, 2)
        self.assertEqual(second_result.get('sync_mode'), 'greenmile_refresh')

    def test_run_does_not_reuse_unresolved_cached_snapshots(self):
        """If the last snapshot was unresolved, same route.keys must refresh GreenMile again."""
        from sync_runner import SyncRunner
        cfg = self._make_config()
        mock_gw = MagicMock()
        mock_gm = MagicMock()

        mock_gw.pull_pending_context.side_effect = [
            {'routeKeys': ['1234567890'], 'existingNotasByRouteKey': {}},
            {'routeKeys': ['1234567890'], 'existingNotasByRouteKey': {}},
        ]
        mock_gm.fetch_stop_views.side_effect = [
            {},
            {'1234567890': {'routeKey': '1234567890', 'status': 'IN_PROGRESS', 'stops': []}},
        ]
        mock_gm.fetch_route_summaries.side_effect = [
            {},
            {'1234567890': {
                'route': {'key': '1234567890', 'id': 1},
                'status': 'IN_PROGRESS',
                'lastModificationDate': '2026-04-08T10:00:00Z',
            }},
        ]
        mock_gw.push_route_snapshots.return_value = {'processedRows': 1, 'updatedRows': 1}

        runner = SyncRunner(cfg, apps_gateway=mock_gw, greenmile_client=mock_gm)
        runner.run()
        second_result = runner.run()

        self.assertEqual(mock_gm.fetch_stop_views.call_count, 2)
        self.assertEqual(mock_gm.fetch_route_summaries.call_count, 2)
        self.assertEqual(second_result.get('sync_mode'), 'greenmile_refresh')


class TestSyncRunnerConfig(unittest.TestCase):

    def test_config_loads_from_env_file(self):
        """Config reads .env file values."""
        import tempfile
        from config import Config

        env_content = (
            'APPS_SCRIPT_URL=https://script.google.com/exec\n'
            'APPS_SCRIPT_TOKEN=mytoken\n'
            'GREENMILE_URL=https://3coracoes.greenmile.com\n'
            'GREENMILE_USERNAME=user\n'
            'GREENMILE_PASSWORD=pass\n'
        )
        with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
            f.write(env_content)
            env_path = f.name

        try:
            cfg = Config(env_path=env_path)
            self.assertEqual(cfg.apps_script_url, 'https://script.google.com/exec')
            self.assertEqual(cfg.apps_script_token, 'mytoken')
            self.assertEqual(cfg.greenmile_url, 'https://3coracoes.greenmile.com')
            self.assertEqual(cfg.greenmile_username, 'user')
            self.assertEqual(cfg.greenmile_password, 'pass')
        finally:
            os.unlink(env_path)

    def test_config_missing_required_key_raises(self):
        """Config without APPS_SCRIPT_TOKEN raises ValueError."""
        import tempfile
        from config import Config

        env_content = 'APPS_SCRIPT_URL=https://script.google.com/exec\n'
        with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
            f.write(env_content)
            env_path = f.name

        try:
            with self.assertRaises(ValueError) as ctx:
                Config(env_path=env_path)
            self.assertIn('APPS_SCRIPT_TOKEN', str(ctx.exception))
        finally:
            os.unlink(env_path)

    def test_config_password_not_in_repr(self):
        """Config __repr__ or __str__ does not expose password."""
        import tempfile
        from config import Config

        env_content = (
            'APPS_SCRIPT_URL=https://script.google.com/exec\n'
            'APPS_SCRIPT_TOKEN=mytoken\n'
            'GREENMILE_URL=https://3coracoes.greenmile.com\n'
            'GREENMILE_USERNAME=user\n'
            'GREENMILE_PASSWORD=supersecret\n'
        )
        with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
            f.write(env_content)
            env_path = f.name

        try:
            cfg = Config(env_path=env_path)
            text = repr(cfg) + str(cfg)
            self.assertNotIn('supersecret', text)
        finally:
            os.unlink(env_path)


class TestSyncRunnerNFTolerance(unittest.TestCase):
    """Order/restrictions falha parcial não deve interromper o ciclo."""

    def _make_config(self):
        from config import Config
        c = Config.__new__(Config)
        c.apps_script_url = 'https://script.google.com/macros/s/abc/exec'
        c.apps_script_token = 'token'
        c.greenmile_url = 'https://3coracoes.greenmile.com'
        c.greenmile_username = 'user'
        c.greenmile_password = 'pass'
        c.chunk_size = 50
        c.request_timeout = 30
        c.max_retries = 3
        c.loop_interval = 120
        return c

    def test_cycle_continues_when_snapshots_have_empty_order_numbers(self):
        """
        Quando orderNumbers está vazio (Order/restrictions indisponível),
        o ciclo deve continuar e fazer push dos snapshots parciais.
        """
        from sync_runner import SyncRunner
        cfg = self._make_config()
        mock_gw = MagicMock()
        mock_gm = MagicMock()

        mock_gw.pull_pending_route_keys.return_value = ['1234567890']
        mock_gm.fetch_stop_views.return_value = {
            '1234567890': {
                'routeKey': '1234567890',
                'status': 'IN_PROGRESS',
                'stops': [{'orderNumbers': []}],  # NF indisponível → lista vazia
            }
        }
        mock_gm.fetch_route_summaries.return_value = {
            '1234567890': {
                'route': {'key': '1234567890', 'id': 1},
                'status': 'IN_PROGRESS',
                'lastModificationDate': '2026-04-08T10:00:00Z',
            }
        }
        mock_gw.push_route_snapshots.return_value = {'processedRows': 1, 'updatedRows': 1}

        runner = SyncRunner(cfg, apps_gateway=mock_gw, greenmile_client=mock_gm)
        result = runner.run()

        mock_gw.push_route_snapshots.assert_called_once()
        self.assertEqual(result['route_keys_found'], 1)

    def test_result_includes_nf_unavailable_count(self):
        """
        Resultado do ciclo deve incluir nf_unavailable_route_keys
        contendo as rotas onde orderNumbers ficou vazio.
        """
        from sync_runner import SyncRunner
        cfg = self._make_config()
        mock_gw = MagicMock()
        mock_gm = MagicMock()

        mock_gw.pull_pending_route_keys.return_value = ['1111111111', '2222222222']
        mock_gm.fetch_stop_views.return_value = {
            '1111111111': {
                'routeKey': '1111111111',
                'status': 'IN_PROGRESS',
                'stops': [{'orderNumbers': []}],  # NF indisponível
            },
            '2222222222': {
                'routeKey': '2222222222',
                'status': 'COMPLETED',
                'stops': [{'orderNumbers': ['NF001']}],  # NF disponível
            },
        }
        mock_gm.fetch_route_summaries.return_value = {
            '1111111111': {'route': {'key': '1111111111', 'id': 1}, 'status': 'IN_PROGRESS', 'lastModificationDate': '2026-04-08T10:00:00Z'},
            '2222222222': {'route': {'key': '2222222222', 'id': 2}, 'status': 'COMPLETED', 'lastModificationDate': '2026-04-08T10:00:00Z'},
        }
        mock_gw.push_route_snapshots.return_value = {'processedRows': 2, 'updatedRows': 2}

        runner = SyncRunner(cfg, apps_gateway=mock_gw, greenmile_client=mock_gm)
        result = runner.run()

        self.assertIn('nf_unavailable_route_keys', result,
                      'resultado deve conter nf_unavailable_route_keys')
        # '1111111111' tem orderNumbers vazio em todos os stops — deve ser reportada
        self.assertIn('1111111111', result['nf_unavailable_route_keys'])
        # '2222222222' tem NF001 — não deve aparecer como indisponível
        self.assertNotIn('2222222222', result['nf_unavailable_route_keys'])

    def test_reused_nf_does_not_appear_as_unavailable(self):
        """
        Quando a NF veio da planilha (existingNotasByRouteKey),
        a rota nao deve ser reportada como NF indisponivel.
        """
        from sync_runner import SyncRunner
        cfg = self._make_config()
        mock_gw = MagicMock()
        mock_gm = MagicMock()

        mock_gw.pull_pending_context.return_value = {
            'routeKeys': ['6103062156'],
            'existingNotasByRouteKey': {'6103062156': ['4242668']},
        }
        mock_gm.fetch_stop_views.return_value = {
            '6103062156': {
                'routeKey': '6103062156',
                'status': 'COMPLETED',
                'stops': [{'orderNumbers': []}],
            },
        }
        mock_gm.fetch_route_summaries.return_value = {
            '6103062156': {
                'route': {'key': '6103062156', 'id': 2654833},
                'status': 'COMPLETED',
                'lastModificationDate': '2026-04-08T10:00:00Z',
            }
        }
        mock_gw.push_route_snapshots.return_value = {'processedRows': 1, 'updatedRows': 1}

        runner = SyncRunner(cfg, apps_gateway=mock_gw, greenmile_client=mock_gm)
        result = runner.run()

        self.assertEqual(result['nf_unavailable_route_keys'], [])

    def test_cycle_survives_stop_view_partial_failure(self):
        """
        Se fetch_stop_views retorna apenas um subconjunto das rotas,
        o ciclo continua e faz push do que foi obtido.
        """
        from sync_runner import SyncRunner
        cfg = self._make_config()
        mock_gw = MagicMock()
        mock_gm = MagicMock()

        mock_gw.pull_pending_route_keys.return_value = ['1111111111', '2222222222']
        # Apenas 1 rota retornou dados; a outra falhou silenciosamente
        mock_gm.fetch_stop_views.return_value = {
            '1111111111': {'routeKey': '1111111111', 'status': 'IN_PROGRESS', 'stops': []},
        }
        mock_gm.fetch_route_summaries.return_value = {
            '1111111111': {'route': {'key': '1111111111', 'id': 1}, 'status': 'IN_PROGRESS', 'lastModificationDate': '2026-04-08T10:00:00Z'},
        }
        mock_gw.push_route_snapshots.return_value = {'processedRows': 1, 'updatedRows': 1}

        runner = SyncRunner(cfg, apps_gateway=mock_gw, greenmile_client=mock_gm)
        result = runner.run()

        mock_gw.push_route_snapshots.assert_called_once()
        self.assertEqual(result['route_keys_found'], 2)
        # snapshots_built inclui unresolved — o push acontece com o que foi obtido
        self.assertGreaterEqual(result['snapshots_built'], 1)


if __name__ == '__main__':
    unittest.main()
