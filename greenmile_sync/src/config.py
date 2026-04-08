"""
Configuration loader for greenmile_sync worker.
Reads from a .env file (or environment variables) and validates required keys.
"""
import os
import sys


DEFAULT_APPS_SCRIPT_URL = (
    'https://script.google.com/macros/s/'
    'AKfycbwgyg51wEvQZFtWTHVIazEOXF3Kb6QgO6_WUchLzzPvaU8p3fRGv-e_PUJZjzIpK6eL/exec'
)

EDITABLE_ENV_KEYS = [
    'APPS_SCRIPT_TOKEN',
    'GREENMILE_URL',
    'GREENMILE_USERNAME',
    'GREENMILE_PASSWORD',
    'CHUNK_SIZE',
    'REQUEST_TIMEOUT',
    'MAX_RETRIES',
    'LOOP_INTERVAL',
    'SNAPSHOT_REUSE_TTL_SECONDS',
]

_REQUIRED = [
    'APPS_SCRIPT_TOKEN',
    'GREENMILE_URL',
    'GREENMILE_USERNAME',
    'GREENMILE_PASSWORD',
]

_DEFAULTS = {
    'CHUNK_SIZE': '50',
    'REQUEST_TIMEOUT': '75',
    'MAX_RETRIES': '3',
    'LOOP_INTERVAL': '300',
    'SNAPSHOT_REUSE_TTL_SECONDS': '600',
}


def _load_env_file(path: str) -> dict:
    """Parse a simple KEY=VALUE .env file. Ignores comments and blank lines."""
    result = {}
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' not in line:
                continue
            key, _, value = line.partition('=')
            result[key.strip()] = value.strip()
    return result


def get_runtime_dir() -> str:
    """Return the directory where the executable/script is running."""
    if getattr(sys, 'frozen', False):
        return os.path.dirname(os.path.abspath(sys.executable))
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_default_env_path() -> str:
    """Return the default .env path next to the executable/project folder."""
    return os.path.join(get_runtime_dir(), '.env')


def save_env_file(path: str, values: dict) -> None:
    """Persist only editable runtime config keys to a .env file."""
    rows = []
    source = values or {}
    for key in EDITABLE_ENV_KEYS:
        value = str(source.get(key, '')).strip()
        if value:
            rows.append(f'{key}={value}')

    with open(path, 'w', encoding='utf-8', newline='\n') as f:
        f.write('\n'.join(rows) + ('\n' if rows else ''))


class Config:
    """Parsed and validated configuration."""

    def __init__(self, env_path: str | None = None):
        # Start with environment variables
        values = dict(os.environ)

        # Override/supplement with .env file if provided
        if env_path:
            file_values = _load_env_file(env_path)
            values.update(file_values)

        # Apply defaults for optional keys
        for key, default in _DEFAULTS.items():
            values.setdefault(key, default)
        values.setdefault('APPS_SCRIPT_URL', DEFAULT_APPS_SCRIPT_URL)

        # Validate required keys
        missing = [k for k in _REQUIRED if not values.get(k, '').strip()]
        if missing:
            raise ValueError(
                f"Missing required configuration keys: {', '.join(missing)}. "
                f"Set them in your .env file or environment."
            )

        self.apps_script_url: str = values['APPS_SCRIPT_URL'].rstrip('/')
        self.apps_script_token: str = values['APPS_SCRIPT_TOKEN']
        self.greenmile_url: str = values['GREENMILE_URL'].rstrip('/')
        self.greenmile_username: str = values['GREENMILE_USERNAME']
        self.greenmile_password: str = values['GREENMILE_PASSWORD']
        self.chunk_size: int = int(values.get('CHUNK_SIZE', 50))
        self.request_timeout: int = int(values.get('REQUEST_TIMEOUT', 75))
        self.max_retries: int = int(values.get('MAX_RETRIES', 3))
        self.loop_interval: int = int(values.get('LOOP_INTERVAL', 300))
        self.snapshot_reuse_ttl_seconds: int = int(values.get('SNAPSHOT_REUSE_TTL_SECONDS', 600))

    def __repr__(self) -> str:
        return (
            f"Config(apps_script_url={self.apps_script_url!r}, "
            f"greenmile_url={self.greenmile_url!r}, "
            f"greenmile_username={self.greenmile_username!r}, "
            f"chunk_size={self.chunk_size}, "
            f"max_retries={self.max_retries})"
        )

    def __str__(self) -> str:
        return self.__repr__()
