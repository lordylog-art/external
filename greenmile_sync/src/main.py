"""
Entry point for the greenmile_sync worker.
Usage: python main.py [--env .env] [--loop INTERVAL_SECONDS] [--configure]
"""
import argparse
import logging
import os
import sys
import time

# Allow running from the src/ directory directly
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from config import Config, get_default_env_path
from sync_runner import SyncRunner
from ui_panel import launch_config_panel

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%SZ',
)
logger = logging.getLogger('greenmile_sync.main')


def _find_env_file(explicit: str | None) -> str | None:
    if explicit:
        return explicit
    candidates = [
        get_default_env_path(),
        os.path.join(os.getcwd(), '.env'),
    ]
    for candidate in candidates:
        if os.path.isfile(candidate):
            return os.path.abspath(candidate)
    return None


def should_open_panel(is_frozen: bool, user_requested_configure: bool, has_env_file: bool) -> bool:
    if user_requested_configure:
        return True
    if is_frozen:
        return True
    return not has_env_file


def run_sync_from_env(env_path: str) -> dict:
    logger.info('Loading config from: %s', env_path)
    config = Config(env_path=env_path)
    runner = SyncRunner(config)
    return runner.run()


def main():
    parser = argparse.ArgumentParser(description='GreenMile -> Apps Script sync worker')
    parser.add_argument('--env', default=None, help='Path to .env file (default: auto-detect)')
    parser.add_argument(
        '--configure',
        action='store_true',
        help='Open the local hacker panel and create/update the .env file',
    )
    parser.add_argument(
        '--loop',
        type=int,
        default=0,
        metavar='SECONDS',
        help='Run in a loop, sleeping SECONDS between cycles (0 = run once and exit)',
    )
    args = parser.parse_args()

    env_path = _find_env_file(args.env)
    open_panel = should_open_panel(getattr(sys, 'frozen', False), args.configure, bool(env_path))
    if open_panel:
        logger.info('Opening local configuration panel...')
        env_path = launch_config_panel(
            env_path=env_path or get_default_env_path(),
            run_callback=run_sync_from_env,
            auto_run=bool(getattr(sys, 'frozen', False) and env_path and not args.configure),
        )
        if not env_path:
            logger.info('Configuration panel closed without saving.')
            sys.exit(0)

    if not env_path:
        logger.info('No .env file found; using environment variables only.')
    try:
        config = Config(env_path=env_path)
    except ValueError as e:
        logger.error('Configuration error: %s', e)
        sys.exit(1)

    runner = SyncRunner(config)

    if args.loop > 0:
        logger.info('Running in loop mode - interval=%ds', args.loop)
        while True:
            try:
                result = runner.run()
                logger.info('Cycle complete: %s', result)
            except Exception as e:
                logger.error('Cycle failed: %s', e, exc_info=True)
            logger.info('Sleeping %ds...', args.loop)
            time.sleep(args.loop)
    else:
        try:
            result = runner.run()
            logger.info('Done: %s', result)
        except Exception as e:
            logger.error('Sync failed: %s', e, exc_info=True)
            sys.exit(1)


if __name__ == '__main__':
    main()
