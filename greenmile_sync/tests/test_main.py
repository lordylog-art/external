import os
import sys
import unittest
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


class TestMainPanelDecision(unittest.TestCase):

    def test_should_open_panel_when_running_frozen(self):
        from main import should_open_panel
        self.assertEqual(should_open_panel(True, False, True), True)

    def test_should_open_panel_when_user_requests_configure(self):
        from main import should_open_panel
        self.assertEqual(should_open_panel(False, True, True), True)

    def test_should_open_panel_when_env_is_missing(self):
        from main import should_open_panel
        self.assertEqual(should_open_panel(False, False, False), True)

    def test_should_not_open_panel_in_cli_mode_with_existing_env(self):
        from main import should_open_panel
        self.assertEqual(should_open_panel(False, False, True), False)


class TestLoopRunner(unittest.TestCase):
    """LoopRunner executa ciclos contínuos e notifica via callbacks."""

    def _make_runner(self):
        from loop_runner import LoopRunner
        return LoopRunner

    def test_loop_runner_executes_cycle_at_least_once(self):
        """LoopRunner deve executar pelo menos um ciclo antes de parar."""
        from loop_runner import LoopRunner

        calls = []
        def fake_run():
            calls.append('run')
            return {'route_keys_found': 1}

        runner = LoopRunner(run_fn=fake_run, interval=0, max_cycles=1)
        runner.start_loop()

        self.assertGreaterEqual(len(calls), 1)

    def test_loop_runner_runs_multiple_cycles(self):
        """LoopRunner deve executar N ciclos se max_cycles=N."""
        from loop_runner import LoopRunner

        calls = []
        def fake_run():
            calls.append('run')
            return {'route_keys_found': 0}

        runner = LoopRunner(run_fn=fake_run, interval=0, max_cycles=3)
        runner.start_loop()

        self.assertEqual(len(calls), 3)

    def test_loop_runner_calls_on_cycle_start_callback(self):
        """LoopRunner deve chamar on_cycle_start no início de cada ciclo."""
        from loop_runner import LoopRunner

        started = []
        def fake_run():
            return {'route_keys_found': 2}

        runner = LoopRunner(
            run_fn=fake_run,
            interval=0,
            max_cycles=2,
            on_cycle_start=lambda cycle_num: started.append(cycle_num),
        )
        runner.start_loop()

        self.assertEqual(started, [1, 2])

    def test_loop_runner_calls_on_cycle_done_callback(self):
        """LoopRunner deve chamar on_cycle_done com o resultado do ciclo."""
        from loop_runner import LoopRunner

        results = []
        def fake_run():
            return {'route_keys_found': 5, 'push_result': {'updatedRows': 3}}

        runner = LoopRunner(
            run_fn=fake_run,
            interval=0,
            max_cycles=1,
            on_cycle_done=lambda result: results.append(result),
        )
        runner.start_loop()

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['route_keys_found'], 5)

    def test_loop_runner_calls_on_cycle_error_on_exception(self):
        """LoopRunner não deve parar quando um ciclo falha; chama on_cycle_error."""
        from loop_runner import LoopRunner

        errors = []
        run_count = [0]

        def fake_run():
            run_count[0] += 1
            if run_count[0] == 1:
                raise RuntimeError('GreenMile unreachable')
            return {'route_keys_found': 0}

        runner = LoopRunner(
            run_fn=fake_run,
            interval=0,
            max_cycles=2,
            on_cycle_error=lambda e: errors.append(str(e)),
        )
        runner.start_loop()

        self.assertEqual(run_count[0], 2, 'deve continuar após erro no ciclo 1')
        self.assertEqual(len(errors), 1)
        self.assertIn('GreenMile unreachable', errors[0])

    def test_loop_runner_calls_on_next_cycle_with_countdown(self):
        """LoopRunner deve chamar on_next_cycle com os segundos restantes."""
        from loop_runner import LoopRunner

        countdowns = []

        runner = LoopRunner(
            run_fn=lambda: {'route_keys_found': 0},
            interval=0,
            max_cycles=1,
            on_next_cycle=lambda secs: countdowns.append(secs),
        )
        runner.start_loop()

        # Com interval=0 deve chamar on_next_cycle pelo menos uma vez com 0
        self.assertGreaterEqual(len(countdowns), 1)

    def test_loop_runner_stop_flag_halts_loop(self):
        """LoopRunner deve respeitar stop() entre ciclos."""
        from loop_runner import LoopRunner

        calls = []

        runner = LoopRunner(run_fn=lambda: calls.append('x') or {}, interval=0, max_cycles=100)
        # Paramos após o primeiro ciclo via on_cycle_done
        runner_ref = [runner]

        def stop_after_first(result):
            runner_ref[0].stop()

        runner.on_cycle_done = stop_after_first
        runner.start_loop()

        # Deve ter executado apenas 1 ciclo
        self.assertEqual(len(calls), 1)

    def test_config_has_loop_interval_default(self):
        """Config deve expor loop_interval com default de 300 segundos."""
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
            self.assertIsInstance(cfg.loop_interval, int)
            self.assertEqual(cfg.loop_interval, 300)
        finally:
            os.unlink(env_path)

    def test_config_loop_interval_from_env(self):
        """Config deve ler LOOP_INTERVAL do .env."""
        import tempfile
        from config import Config

        env_content = (
            'APPS_SCRIPT_URL=https://script.google.com/exec\n'
            'APPS_SCRIPT_TOKEN=mytoken\n'
            'GREENMILE_URL=https://3coracoes.greenmile.com\n'
            'GREENMILE_USERNAME=user\n'
            'GREENMILE_PASSWORD=pass\n'
            'LOOP_INTERVAL=60\n'
        )
        with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
            f.write(env_content)
            env_path = f.name

        try:
            cfg = Config(env_path=env_path)
            self.assertEqual(cfg.loop_interval, 60)
        finally:
            os.unlink(env_path)


if __name__ == '__main__':
    unittest.main()
