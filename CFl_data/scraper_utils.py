"""
Shared utilities for all scrapers to make requests appear more human-like
Includes random delays, user-agent rotation, and request header randomization
"""

import random
import time
import asyncio
from typing import Optional
from curl_cffi import requests as curl_requests


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


class SmartSession:
    """
    Drop-in replacement for requests.Session that:
    1. Uses curl_cffi to impersonate a real browser TLS fingerprint
    2. Always routes through the Kuwait residential proxy (required for q84sale.com)
    3. Auto-retries with rotating impersonation profiles on 403
    """

    def __init__(self):
        self._profile_index = 0
        self._session = curl_requests.Session(
            impersonate=_IMPERSONATION_PROFILES[0],
            proxies=PROXIES,
        )

    def _next_profile(self):
        self._profile_index = (self._profile_index + 1) % len(_IMPERSONATION_PROFILES)
        return _IMPERSONATION_PROFILES[self._profile_index]

    def get(self, url, **kwargs):
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


def create_session() -> SmartSession:
    """
    Create a SmartSession that impersonates Chrome's TLS fingerprint and
    automatically retries with different profiles and proxy fallback on 403.
    """
    return SmartSession()
