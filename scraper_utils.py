"""
Shared utilities for all scrapers to make requests appear more human-like
Includes random delays, user-agent rotation, and request header randomization
"""

import os
import random
import time
import asyncio
import logging
from typing import Optional
from urllib.parse import quote_plus
from curl_cffi import requests as curl_requests

logger = logging.getLogger(__name__)


# Pool of realistic user agents
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0',
]

# Pool of common Accept-Language headers
ACCEPT_LANGUAGES = [
    'en-US,en;q=0.9',
    'en-US,en;q=0.9,ar;q=0.8',
    'ar,en-US;q=0.9,en;q=0.8',
    'en-GB,en;q=0.9,ar;q=0.8',
    'en-US,en;q=0.8',
]


def get_random_user_agent() -> str:
    """Return a random user agent from the pool"""
    return random.choice(USER_AGENTS)


def get_random_headers() -> dict:
    """
    Generate randomized request headers to mimic real browser behavior
    
    Returns:
        dict: Headers dictionary with randomized values
    """
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


def random_delay(min_seconds: float = 1.0, max_seconds: float = 3.0) -> None:
    """
    Add a random delay to mimic human behavior (synchronous)
    
    Args:
        min_seconds: Minimum delay in seconds (default: 1.0)
        max_seconds: Maximum delay in seconds (default: 3.0)
    """
    delay = random.uniform(min_seconds, max_seconds)
    time.sleep(delay)


async def async_random_delay(min_seconds: float = 1.0, max_seconds: float = 3.0) -> None:
    """
    Add a random delay to mimic human behavior (asynchronous)
    
    Args:
        min_seconds: Minimum delay in seconds (default: 1.0)
        max_seconds: Maximum delay in seconds (default: 3.0)
    """
    delay = random.uniform(min_seconds, max_seconds)
    await asyncio.sleep(delay)


def setup_session_with_random_headers(session) -> None:
    """
    Configure a requests.Session object with randomized headers
    
    Args:
        session: requests.Session object to configure
    """
    session.headers.update(get_random_headers())


def rotate_user_agent(session) -> None:
    """
    Rotate the user agent in an existing session.
    For curl_cffi/SmartSession this is a no-op — impersonation handles headers.
    """
    if isinstance(session, SmartSession):
        return
    if not isinstance(session, curl_requests.Session):
        session.headers['User-Agent'] = get_random_user_agent()


# Webshare Kuwait residential rotating proxy
# Using Kuwait geo-targeted residential IPs to match the target site's region
PROXIES = {
    "http": "http://yicfjheyresidential-KW-1:u9agrcjm8k8x@p.webshare.io:80/",
    "https": "http://yicfjheyresidential-KW-1:u9agrcjm8k8x@p.webshare.io:80/",
}


def configure_session_proxy(session) -> None:
    """Configure a session to use the Kuwait residential proxy."""
    session.proxies.update(PROXIES)


# Impersonation profiles to cycle through on retries
_IMPERSONATION_PROFILES = [
    "chrome124",
    "chrome120",
    "chrome131",
    "safari18_0",
    "chrome116",
]


class _CachedResponse:
    """Minimal response-like object served from the URL cache (costs 0 credits)."""

    status_code = 200

    def __init__(self, text: str):
        self.text = text
        self.content = text.encode("utf-8")

    def raise_for_status(self):
        pass


class SmartSession:
    """
    Drop-in replacement for requests.Session that:
    1. Uses curl_cffi to impersonate a real browser TLS fingerprint
    2. Always routes through the Kuwait residential proxy (required for q84sale.com)
    3. Auto-retries with rotating impersonation profiles on 403
    4. Caches page responses — duplicate URL fetches (e.g. catchilds + listings
       page 1 hit the same URL) are served from memory at zero cost.
    """

    def __init__(self):
        self._profile_index = 0
        self._session = curl_requests.Session(
            impersonate=_IMPERSONATION_PROFILES[0],
            proxies=PROXIES,
        )
        self._cache: dict = {}  # URL → response text; avoids duplicate fetches
        self.request_count = 0
        self.cache_hits = 0

    def _next_profile(self):
        self._profile_index = (self._profile_index + 1) % len(_IMPERSONATION_PROFILES)
        return _IMPERSONATION_PROFILES[self._profile_index]

    def get(self, url, **kwargs):
        # Serve from cache — avoids duplicate page fetches at zero cost
        if url in self._cache:
            self.cache_hits += 1
            return _CachedResponse(self._cache[url])

        last_exc = None
        for attempt in range(len(_IMPERSONATION_PROFILES)):
            try:
                if attempt > 0:
                    profile = self._next_profile()
                    time.sleep(random.uniform(2.0, 4.0))
                    self._session = curl_requests.Session(
                        impersonate=profile,
                        proxies=PROXIES,
                    )

                response = self._session.get(url, **kwargs)

                if response.status_code == 403:
                    last_exc = Exception(f"HTTP Error 403: ")
                    continue

                self.request_count += 1
                self._cache[url] = response.text
                return response

            except Exception as e:
                if "403" in str(e):
                    last_exc = e
                    continue
                raise

        raise last_exc or Exception("All retry attempts failed with 403")

    def close(self):
        if self._session:
            self._session.close()

    @property
    def headers(self):
        return self._session.headers

    @property
    def proxies(self):
        return self._session.proxies


class ScrapedoSession:
    """
    Drop-in SmartSession replacement that routes ALL page requests through
    scrape.do (Kuwait residential + super gateway) to bypass 403 on q84sale.com.

    Optimisations vs SmartSession:
    - URL-level cache: duplicate requests (e.g. catchilds + listings page 1
      point to the same URL) cost 0 scrape.do credits.
    - Falls back through three strategies: super-only → render-only → render+super,
      stopping on first success so credits are spent only as needed.

    Activated automatically by create_session() when SCRAPEDO_TOKEN is set.
    """

    _ENDPOINT = "https://api.scrape.do"
    _STRATEGIES = [
        {"super": "true", "geoCode": "kw"},
        {"render": "true", "geoCode": "kw"},
        {"render": "true", "super": "true", "geoCode": "kw"},
    ]

    def __init__(self, token: str):
        self._token = token
        self._cache: dict = {}  # URL → response text
        self.request_count = 0  # scrape.do API calls made (credits used)
        self.cache_hits = 0     # requests served from cache (free)

    def get(self, url, **kwargs):
        # Serve from cache — costs 0 credits
        if url in self._cache:
            self.cache_hits += 1
            return _CachedResponse(self._cache[url])

        last_resp = None
        for params in self._STRATEGIES:
            qs = "&".join(f"{k}={v}" for k, v in params.items())
            api_url = f"{self._ENDPOINT}?token={self._token}&url={quote_plus(url)}&{qs}"
            label = ", ".join(f"{k}={v}" for k, v in params.items())
            try:
                resp = curl_requests.get(api_url, timeout=120)
                self.request_count += 1
                if resp.status_code == 200:
                    self._cache[url] = resp.text
                    return resp
                logger.warning(f"scrape.do [{label}] → HTTP {resp.status_code}, trying next strategy")
                last_resp = resp
            except Exception as exc:
                logger.warning(f"scrape.do [{label}] error: {exc}")

        if last_resp is not None:
            return last_resp
        raise Exception("All scrape.do strategies failed")

    def close(self):
        pass

    @property
    def headers(self):
        return {}

    @property
    def proxies(self):
        return {}


def create_session():
    """
    Return a ScrapedoSession when SCRAPEDO_TOKEN is set in the environment,
    otherwise fall back to the curl_cffi-based SmartSession.
    """
    token = os.environ.get("SCRAPEDO_TOKEN", "")
    if token:
        logger.info("Using ScrapedoSession — routing via scrape.do (Kuwait, super proxy)")
        return ScrapedoSession(token)
    logger.info("Using SmartSession — curl_cffi + Webshare Kuwait residential proxy")
    return SmartSession()
