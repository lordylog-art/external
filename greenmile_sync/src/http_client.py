"""
Thin HTTP client using Python standard library only.
Handles retries with exponential backoff and JSON prefix stripping.
"""
import json
import logging
import re
import time
import urllib.request
import urllib.error
from typing import Any

logger = logging.getLogger(__name__)

# GreenMile / Apps Script sometimes prefix JSON with this anti-XSSI guard.
_JSON_PREFIX_RE = re.compile(r'^[^{\[]*')


def strip_json_prefix(text: str) -> str:
    """Remove any non-JSON prefix (e.g. 'while(1);') before first { or [."""
    return _JSON_PREFIX_RE.sub('', text, count=1)


def post_json(
    url: str,
    body: dict,
    headers: dict | None = None,
    timeout: int = 30,
    max_retries: int = 3,
) -> Any:
    """
    POST JSON body to url. Returns parsed JSON response.
    Retries on transient errors (5xx, connection errors) with exponential backoff.
    Raises RuntimeError on unrecoverable errors.
    """
    data = json.dumps(body).encode('utf-8')
    req_headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
    if headers:
        req_headers.update(headers)

    last_error: Exception | None = None
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(url, data=data, headers=req_headers, method='POST')
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode('utf-8', errors='replace')
            text = strip_json_prefix(raw)
            return json.loads(text)
        except urllib.error.HTTPError as e:
            status = e.code
            if status < 500:
                # 4xx: not retryable
                body_text = ''
                try:
                    body_text = e.read().decode('utf-8', errors='replace')
                except Exception:
                    pass
                raise RuntimeError(f"HTTP {status} from {url}: {body_text[:200]}") from e
            last_error = e
            logger.warning("HTTP %s on attempt %d/%d, retrying...", status, attempt + 1, max_retries)
        except (urllib.error.URLError, OSError, TimeoutError) as e:
            last_error = e
            logger.warning("Connection error on attempt %d/%d: %s", attempt + 1, max_retries, e)

        if attempt < max_retries - 1:
            wait = 2 ** attempt
            logger.debug("Sleeping %ss before retry...", wait)
            time.sleep(wait)

    raise RuntimeError(f"Failed after {max_retries} attempts: {last_error}") from last_error


def get_json(
    url: str,
    headers: dict | None = None,
    timeout: int = 30,
    max_retries: int = 3,
) -> Any:
    """GET JSON from url. Same retry logic as post_json."""
    req_headers = {'Accept': 'application/json'}
    if headers:
        req_headers.update(headers)

    last_error: Exception | None = None
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(url, headers=req_headers, method='GET')
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode('utf-8', errors='replace')
            text = strip_json_prefix(raw)
            return json.loads(text)
        except urllib.error.HTTPError as e:
            if e.code < 500:
                raise RuntimeError(f"HTTP {e.code} from {url}") from e
            last_error = e
        except (urllib.error.URLError, OSError, TimeoutError) as e:
            last_error = e

        if attempt < max_retries - 1:
            time.sleep(2 ** attempt)

    raise RuntimeError(f"GET failed after {max_retries} attempts: {last_error}") from last_error
