"""
Shared utilities for all CF_Data scrapers.

Proxy strategy
--------------
Two proxy modes are supported, detected automatically from env vars at import:

  Mode A — Generic rotating residential (proxy.cheap, etc.)
    Set PROXY_HOST + PROXY_PORT + PROXY_USER + PROXY_PASS
    or the single PROXY_URL variable.
    SmartSession uses plain requests with socks5h:// scheme.
    socks5h lets the proxy resolve DNS, which is required for
    proxy.cheap rotating residential pools.

  Mode B — DataImpulse residential (legacy / backward-compatible)
    Set DATAIMPULSE_USER (including __cr.XX geo suffix) + DATAIMPULSE_PASS.
    SmartSession uses curl_cffi TLS impersonation as before.

  No proxy
    SmartSession falls back to curl_cffi with no proxy.

Required R2 env vars (all modes):
    CF_R2_ACCESS_KEY_ID
    CF_R2_SECRET_ACCESS_KEY
    CF_R2_ENDPOINT_URL
    CF_R2_BUCKET_NAME

Anti-detection delays (same as original):
    Between pages         : random 1.5 – 3.5 s  (callers use random_delay())
    Between subcategories : random 3.0 – 7.0 s  (callers use random_delay(3.0, 7.0))
    Between detail pages  : 0.3 s minimum
"""

import os
import random
import time
import asyncio
import logging
from typing import Optional
from curl_cffi import requests as curl_requests

logger = logging.getLogger(__name__)


# ── User-agent / header pools ─────────────────────────────────────────────────

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0',
]

ACCEPT_LANGUAGES = [
    'en-US,en;q=0.9',
    'en-US,en;q=0.9,ar;q=0.8',
    'ar,en-US;q=0.9,en;q=0.8',
    'en-GB,en;q=0.9,ar;q=0.8',
    'en-US,en;q=0.8',
]


def get_random_user_agent() -> str:
    return random.choice(USER_AGENTS)


def get_random_headers() -> dict:
    return {
        'User-Agent': get_random_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': random.choice(ACCEPT_LANGUAGES),
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Cache-Control': 'max-age=0',
    }


def random_delay(min_seconds: float = 1.5, max_seconds: float = 3.5) -> None:
    """Synchronous random delay. Defaults tuned for anti-detection (1.5–3.5 s)."""
    time.sleep(random.uniform(min_seconds, max_seconds))


async def async_random_delay(min_seconds: float = 1.5, max_seconds: float = 3.5) -> None:
    """Asynchronous random delay. Defaults tuned for anti-detection (1.5–3.5 s)."""
    await asyncio.sleep(random.uniform(min_seconds, max_seconds))


def setup_session_with_random_headers(session) -> None:
    session.headers.update(get_random_headers())


def rotate_user_agent(session) -> None:
    """No-op for SmartSession — proxy/impersonation handles headers."""
    if isinstance(session, SmartSession):
        return
    if not isinstance(session, curl_requests.Session):
        session.headers['User-Agent'] = get_random_user_agent()


# ── Proxy configuration ───────────────────────────────────────────────────────

def _build_proxies() -> tuple[dict, bool]:
    """
    Build proxy dict from environment variables.
    Returns (proxies_dict, is_generic_proxy).

    is_generic_proxy=True  → proxy.cheap / any HTTP proxy → use requests
    is_generic_proxy=False → DataImpulse (curl_cffi) or no proxy
    """
    # ── Mode A: generic rotating residential (proxy.cheap etc.) ──────────────
    proxy_url = os.environ.get("PROXY_URL", "").strip()
    if proxy_url:
        logger.info(f"Proxy mode: generic (PROXY_URL)  host={proxy_url.split('@')[-1]}")
        return {"http": proxy_url, "https": proxy_url}, True

    host = os.environ.get("PROXY_HOST", "").strip()
    port = os.environ.get("PROXY_PORT", "").strip()
    user = os.environ.get("PROXY_USER", "").strip()
    pwd  = os.environ.get("PROXY_PASS", "").strip()
    if host and port:
        # socks5h → proxy resolves DNS (required for proxy.cheap rotating residential).
        # Override scheme with PROXY_SCHEME env var if needed (e.g. "http").
        scheme = os.environ.get("PROXY_SCHEME", "socks5h").strip()
        if user and pwd:
            proxy_url = f"{scheme}://{user}:{pwd}@{host}:{port}"
            logger.info(f"Proxy mode: generic (PROXY_HOST)  {scheme}://{host}:{port}  user={user}")
        else:
            proxy_url = f"{scheme}://{host}:{port}"
            logger.info(f"Proxy mode: generic (PROXY_HOST)  {scheme}://{host}:{port}  (no auth)")
        return {"http": proxy_url, "https": proxy_url}, True

    # ── Mode B: DataImpulse residential (curl_cffi) ───────────────────────────
    di_user = os.environ.get("DATAIMPULSE_USER", "").strip()
    di_pass = os.environ.get("DATAIMPULSE_PASS", "").strip()
    if di_user and di_pass:
        di_host = os.environ.get("DATAIMPULSE_HOST", "gw.dataimpulse.com")
        di_port = os.environ.get("DATAIMPULSE_PORT", "823")
        geo = di_user.split("__cr.")[-1] if "__cr." in di_user else "not-set"
        proxy_url = f"http://{di_user}:{di_pass}@{di_host}:{di_port}"
        logger.info(f"Proxy mode: DataImpulse  geo={geo}  {di_host}:{di_port}")
        return {"http": proxy_url, "https": proxy_url}, False

    # ── No proxy ──────────────────────────────────────────────────────────────
    logger.warning(
        "No proxy configured — running WITHOUT proxy (your real IP will be used). "
        "Set PROXY_HOST + PROXY_PORT [+ PROXY_USER + PROXY_PASS]  or  PROXY_URL  "
        "or  DATAIMPULSE_USER + DATAIMPULSE_PASS."
    )
    return {}, False


# Built once at import time — consistent for the lifetime of the process
PROXIES, _GENERIC_PROXY = _build_proxies()


def configure_session_proxy(session) -> None:
    """Configure a plain requests.Session to use the detected proxy."""
    if PROXIES:
        session.proxies.update(PROXIES)


# ── TLS impersonation profiles (used when DataImpulse / no proxy) ─────────────

_IMPERSONATION_PROFILES = [
    "chrome124",
    "chrome120",
    "chrome131",
    "safari18_0",
    "chrome116",
]


# ── SmartSession ──────────────────────────────────────────────────────────────

class _CachedResponse:
    """Zero-cost response object served from the in-memory URL cache."""

    status_code = 200

    def __init__(self, text: str):
        self.text = text
        self.content = text.encode("utf-8")

    def raise_for_status(self):
        pass


class SmartSession:
    """
    Drop-in replacement for requests.Session that automatically selects the
    right HTTP backend based on which proxy env vars are set:

    ┌──────────────────────────────┬───────────────────────────────────────────┐
    │ Env vars present             │ Backend                                   │
    ├──────────────────────────────┼───────────────────────────────────────────┤
    │ PROXY_HOST / PROXY_URL       │ plain requests  (3 retries, rotating UA)  │
    │ DATAIMPULSE_USER             │ curl_cffi       (TLS impersonation + retry)│
    │ (none)                       │ curl_cffi       (no proxy)                │
    └──────────────────────────────┴───────────────────────────────────────────┘

    All modes:
    - In-memory URL cache (duplicate fetches cost 0 extra requests)
    - Tracks request_count and cache_hits
    - Compatible header/proxy properties for callers that inspect them
    """

    def __init__(self):
        self._proxies   = PROXIES
        self._use_requests = _GENERIC_PROXY   # True for proxy.cheap / generic HTTP proxy
        self._headers: dict = {}
        self._cache: dict   = {}
        self.request_count  = 0
        self.cache_hits     = 0

        # curl_cffi session — only created when not using requests backend
        self._profile_index = 0
        if not self._use_requests:
            self._curl_session = curl_requests.Session(
                impersonate=_IMPERSONATION_PROFILES[0],
                proxies=self._proxies,
            )
        else:
            self._curl_session = None

    # ── Public API ────────────────────────────────────────────────────────────

    def get(self, url: str, **kwargs):
        if url in self._cache:
            self.cache_hits += 1
            logger.debug(f"Cache hit: {url}")
            return _CachedResponse(self._cache[url])

        if self._use_requests:
            return self._get_with_requests(url, **kwargs)
        else:
            return self._get_with_curl(url, **kwargs)

    def close(self):
        if self._curl_session:
            self._curl_session.close()

    @property
    def headers(self):
        """Mutable dict — updates are merged into every requests call."""
        if self._curl_session:
            return self._curl_session.headers
        return self._headers

    @property
    def proxies(self):
        if self._curl_session:
            return self._curl_session.proxies
        return self._proxies

    # ── requests backend (proxy.cheap / generic) ──────────────────────────────

    def _get_with_requests(self, url: str, **kwargs):
        import requests as _req

        req_headers = {**get_random_headers(), **self._headers}
        timeout = kwargs.get("timeout", 60)

        for attempt in range(1, 4):
            try:
                resp = _req.get(
                    url,
                    headers=req_headers,
                    proxies=self._proxies or None,
                    timeout=timeout,
                )
                if resp.status_code == 403:
                    logger.warning(f"HTTP 403 (attempt {attempt}) for {url} — retrying")
                    time.sleep(random.uniform(2.0, 4.0))
                    req_headers = {**get_random_headers(), **self._headers}  # rotate UA
                    continue
                resp.raise_for_status()
                self.request_count += 1
                self._cache[url] = resp.text
                return resp
            except Exception as exc:
                err = str(exc)
                logger.warning(f"requests attempt {attempt} failed: {err[:120]}")
                if attempt < 3:
                    time.sleep(random.uniform(1.5, 3.0))
                else:
                    raise

        raise Exception(f"All 3 requests attempts failed for {url}")

    # ── curl_cffi backend (DataImpulse / no proxy) ────────────────────────────

    def _next_profile(self) -> str:
        self._profile_index = (self._profile_index + 1) % len(_IMPERSONATION_PROFILES)
        return _IMPERSONATION_PROFILES[self._profile_index]

    def _get_with_curl(self, url: str, **kwargs):
        last_exc = None

        for attempt in range(len(_IMPERSONATION_PROFILES)):
            try:
                if attempt > 0:
                    profile = self._next_profile()
                    logger.warning(
                        f"Retrying with profile '{profile}' "
                        f"(attempt {attempt + 1}/{len(_IMPERSONATION_PROFILES)})"
                    )
                    time.sleep(random.uniform(2.0, 4.0))
                    self._curl_session = curl_requests.Session(
                        impersonate=profile,
                        proxies=self._proxies,
                    )

                response = self._curl_session.get(url, **kwargs)

                if response.status_code == 403:
                    last_exc = Exception(f"HTTP 403 for {url}")
                    continue

                self.request_count += 1
                self._cache[url] = response.text
                return response

            except Exception as e:
                err_str = str(e)
                is_retryable = any(
                    token in err_str
                    for token in ("403", "timed out", "timeout", "(28)", "(6)", "(7)", "(35)",
                                  "Connection", "CONNECT", "SSL", "RemoteDisconnected")
                )
                if is_retryable:
                    last_exc = e
                    logger.warning(
                        f"Connection error on attempt {attempt + 1}: {err_str[:120]} — retrying"
                    )
                    continue
                raise

        raise last_exc or Exception(
            f"All {len(_IMPERSONATION_PROFILES)} retry attempts failed for {url}"
        )


def create_session() -> SmartSession:
    """
    Return a SmartSession configured for whichever proxy is detected.
    Proxy mode is logged at INFO level on creation.
    """
    mode = "generic-proxy (requests)" if _GENERIC_PROXY else (
        "DataImpulse (curl_cffi)" if PROXIES else "no-proxy (curl_cffi)"
    )
    logger.info(f"SmartSession ready — mode={mode}")
    return SmartSession()
