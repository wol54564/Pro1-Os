"""Count ads published on 4Sale directly from its public website pages."""

from __future__ import annotations

import json
import logging
import re
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from datetime import date
from typing import Any, Dict, List, Set, Tuple
from urllib.parse import urlparse

log = logging.getLogger("monitor")

_BASE_URL = "https://www.q84sale.com/ar"
_CATEGORY_ROOTS = (
    "automotive",
    "animals",
    "electronics",
    "contracting",
    "education",
    "others",
    "services",
    "camping",
    "fashion-and-family",
    "furniture",
    "jobs",
    "property",
)
_MAX_PAGES_PER_CATEGORY = 1000
_MAX_CONCURRENT_CATEGORIES = 2
_MIN_REQUEST_INTERVAL_SECONDS = 0.5
_REQUEST_LOCK = threading.Lock()
_LAST_REQUEST_STARTED = 0.0
_NEXT_DATA_RE = re.compile(
    rb'<script\b[^>]*\bid=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
    re.IGNORECASE | re.DOTALL,
)


def _normalize_category_path(value: str) -> str:
    value = value.strip()
    if value.startswith(("http://", "https://")):
        parsed = urlparse(value)
        if parsed.netloc.lower() not in {"q84sale.com", "www.q84sale.com"}:
            raise ValueError(f"Unexpected 4Sale category host: {parsed.netloc}")
        value = parsed.path

    value = value.strip("/")
    if value.startswith("ar/"):
        value = value[3:]
    parts = value.split("/")
    if parts and parts[-1].isdigit():
        parts.pop()
    if not parts or any(part in {"", ".", ".."} for part in parts):
        raise ValueError(f"Invalid 4Sale category path: {value!r}")
    return "/".join(parts)


def _category_path(category: Dict[str, Any], parent_path: str) -> str:
    slug_url = category.get("slug_url")
    if isinstance(slug_url, str) and slug_url.strip():
        return _normalize_category_path(slug_url)

    slug = category.get("slug")
    if not isinstance(slug, str) or not slug.strip():
        raise ValueError(f"4Sale category has no slug or slug_url: {category!r}")
    return _normalize_category_path(f"{parent_path}/{slug}")


def _fetch_page_props(path: str, page_number: int) -> Dict[str, Any]:
    global _LAST_REQUEST_STARTED
    url = f"{_BASE_URL}/{path}/{page_number}"
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; MonitorHub/1.0)",
            "Accept": "text/html,application/xhtml+xml",
        },
    )
    for attempt in range(3):
        with _REQUEST_LOCK:
            wait_seconds = (
                _MIN_REQUEST_INTERVAL_SECONDS - (time.monotonic() - _LAST_REQUEST_STARTED)
            )
            if wait_seconds > 0:
                time.sleep(wait_seconds)
            _LAST_REQUEST_STARTED = time.monotonic()

        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                html = response.read()
            break
        except urllib.error.HTTPError as exc:
            if exc.code not in (408, 429) and exc.code < 500:
                raise RuntimeError(f"4Sale returned HTTP {exc.code} for {url}") from exc
            last_error = exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last_error = exc

        if attempt == 2:
            raise RuntimeError(f"Could not fetch 4Sale page {url}: {last_error}") from last_error
        time.sleep(2 ** attempt)

    match = _NEXT_DATA_RE.search(html)
    if not match:
        raise ValueError(f"4Sale page did not contain __NEXT_DATA__: {url}")

    try:
        data = json.loads(match.group(1).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Could not parse 4Sale page data: {url}") from exc

    page_props = data.get("props", {}).get("pageProps")
    if not isinstance(page_props, dict):
        raise ValueError(f"4Sale page has no pageProps object: {url}")
    return page_props


def count_4sale_ads_published_on(published_date: str) -> Dict[str, Any]:
    """Count unique ads dated *published_date* by scanning live 4Sale listings."""
    target_date = date.fromisoformat(published_date)
    listing_ids: Set[str] = set()
    visited_categories: Set[str] = set()
    pages_scanned = 0

    def scan_category(path: str) -> Tuple[Set[str], List[str], int]:
        path = _normalize_category_path(path)
        page_props = _fetch_page_props(path, 1)
        children: Dict[str, Dict[str, Any]] = {}
        for key in ("verticalSubcats", "catChilds"):
            categories = page_props.get(key) or []
            if not isinstance(categories, list):
                raise ValueError(f"Invalid {key} collection for 4Sale category {path}")
            for category in categories:
                if isinstance(category, dict):
                    child_path = _category_path(category, path)
                    children[child_path] = category

        page_number = 1
        total_pages = page_props.get("totalPages")
        try:
            total_pages = int(total_pages) if total_pages is not None else None
        except (TypeError, ValueError):
            total_pages = None

        category_ids: Set[str] = set()
        category_pages = 0
        while True:
            category_pages += 1
            listings = page_props.get("listings") or []
            if not isinstance(listings, list):
                raise ValueError(f"Invalid listings collection for 4Sale category {path}")

            listing_dates: List[date] = []
            for listing in listings:
                if not isinstance(listing, dict) or listing.get("adBoxId"):
                    continue
                raw_date = listing.get("date_published")
                if not isinstance(raw_date, str) or len(raw_date) < 10:
                    continue
                try:
                    listing_date = date.fromisoformat(raw_date[:10])
                except ValueError:
                    continue

                listing_dates.append(listing_date)
                if listing_date != target_date:
                    continue

                listing_id = listing.get("id")
                if listing_id is None or not str(listing_id).strip():
                    raise ValueError(
                        f"4Sale listing published on {published_date} has no ID "
                        f"in category {path}"
                    )
                category_ids.add(str(listing_id))

            # 4Sale pages are newest-first. Once a page is entirely older than
            # the target date, later pages in this category cannot add matches.
            if listing_dates and max(listing_dates) < target_date:
                break
            if not listings or (total_pages is not None and page_number >= total_pages):
                break
            if page_number >= _MAX_PAGES_PER_CATEGORY:
                raise RuntimeError(
                    f"Stopped after {_MAX_PAGES_PER_CATEGORY} pages in 4Sale category {path}"
                )

            page_number += 1
            page_props = _fetch_page_props(path, page_number)

        return category_ids, list(children), category_pages

    pending = list(_CATEGORY_ROOTS)
    futures: Dict[Future, str] = {}
    with ThreadPoolExecutor(max_workers=_MAX_CONCURRENT_CATEGORIES) as executor:
        while pending or futures:
            while pending and len(futures) < _MAX_CONCURRENT_CATEGORIES:
                path = _normalize_category_path(pending.pop())
                if path in visited_categories:
                    continue
                visited_categories.add(path)
                futures[executor.submit(scan_category, path)] = path

            if not futures:
                continue
            completed, _ = wait(futures, return_when=FIRST_COMPLETED)
            for future in completed:
                futures.pop(future)
                category_ids, children, category_pages = future.result()
                listing_ids.update(category_ids)
                pages_scanned += category_pages
                pending.extend(
                    child for child in children
                    if _normalize_category_path(child) not in visited_categories
                )

    log.info(
        "4Sale website count for %s: %s unique ads across %s categories and %s pages",
        published_date,
        len(listing_ids),
        len(visited_categories),
        pages_scanned,
    )
    return {
        "count": len(listing_ids),
        "categories_scanned": len(visited_categories),
        "pages_scanned": pages_scanned,
        "source": "q84sale_public_listing_pages",
    }
