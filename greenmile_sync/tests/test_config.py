import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


class TestConfigDefaultsAndPersistence(unittest.TestCase):

    def test_config_uses_fixed_apps_script_url_when_missing_from_env_file(self):
        from config import Config, DEFAULT_APPS_SCRIPT_URL

        env_content = (
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
            self.assertEqual(cfg.apps_script_url, DEFAULT_APPS_SCRIPT_URL)
        finally:
            os.unlink(env_path)

    def test_save_env_file_persists_only_editable_fields(self):
        from config import save_env_file

        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = os.path.join(temp_dir, '.env')
            save_env_file(env_path, {
                'APPS_SCRIPT_TOKEN': 'token-123',
                'GREENMILE_URL': 'https://3coracoes.greenmile.com',
                'GREENMILE_USERNAME': 'gm-user',
                'GREENMILE_PASSWORD': 'gm-pass',
                'CHUNK_SIZE': '25',
                'REQUEST_TIMEOUT': '45',
                'MAX_RETRIES': '5',
                'SNAPSHOT_REUSE_TTL_SECONDS': '900',
                'APPS_SCRIPT_URL': 'https://should-not-be-written.example.com',
            })

            with open(env_path, 'r', encoding='utf-8') as fh:
                text = fh.read()

            self.assertIn('APPS_SCRIPT_TOKEN=token-123', text)
            self.assertIn('GREENMILE_USERNAME=gm-user', text)
            self.assertIn('MAX_RETRIES=5', text)
            self.assertIn('SNAPSHOT_REUSE_TTL_SECONDS=900', text)
            self.assertNotIn('should-not-be-written', text)

    def test_config_still_requires_token_and_greenmile_credentials(self):
        from config import Config

        env_content = 'GREENMILE_URL=https://3coracoes.greenmile.com\n'
        with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
            f.write(env_content)
            env_path = f.name

        try:
            with self.assertRaises(ValueError) as ctx:
                Config(env_path=env_path)
            self.assertIn('APPS_SCRIPT_TOKEN', str(ctx.exception))
        finally:
            os.unlink(env_path)

    def test_config_uses_safer_default_request_timeout(self):
        from config import Config

        env_content = (
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
            self.assertEqual(cfg.request_timeout, 75)
        finally:
            os.unlink(env_path)

    def test_config_uses_five_minute_loop_interval_by_default(self):
        from config import Config

        env_content = (
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
            self.assertEqual(cfg.loop_interval, 300)
        finally:
            os.unlink(env_path)

    def test_config_uses_snapshot_reuse_ttl_default(self):
        from config import Config

        env_content = (
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
            self.assertEqual(cfg.snapshot_reuse_ttl_seconds, 600)
        finally:
            os.unlink(env_path)

    def test_config_reads_snapshot_reuse_ttl_from_env(self):
        from config import Config

        env_content = (
            'APPS_SCRIPT_TOKEN=mytoken\n'
            'GREENMILE_URL=https://3coracoes.greenmile.com\n'
            'GREENMILE_USERNAME=user\n'
            'GREENMILE_PASSWORD=pass\n'
            'SNAPSHOT_REUSE_TTL_SECONDS=1200\n'
        )
        with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
            f.write(env_content)
            env_path = f.name

        try:
            cfg = Config(env_path=env_path)
            self.assertEqual(cfg.snapshot_reuse_ttl_seconds, 1200)
        finally:
            os.unlink(env_path)


if __name__ == '__main__':
    unittest.main()
