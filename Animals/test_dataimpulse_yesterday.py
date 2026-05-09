"""
Animals scraper — DataImpulse residential proxies, Kuwait, YESTERDAY data only.

What this does:
  1.  Fetches all Animals subcategories from q84sale.com/ar/animals
  2.  For each subcategory checks for child categories
  3.  Paginates through listing pages keeping ONLY yesterday's listings
      (stops pagination as soon as a page returns nothing from yesterday)
  4.  Fetches the detail page for EVERY listing found — no skipping, no cap
  5.  Saves the full result to a local JSON file (no S3 needed)
  6.  Prints a final report with the TOTAL NUMBER OF HTTP REQUESTS made

Proxy:  DataImpulse residential  (gw.dataimpulse.com:823)
Geo:    Kuwait  (__cr.kw appended to the username)
Filter: Yesterday's date only — today's or older listings are skipped

Usage:
    python test_dataimpulse_yesterday.py
    python test_dataimpulse_yesterday.py --max-pages 10
    python test_dataimpulse_yesterday.py --output my_output.json

CLI arguments:
    --max-pages    Safety cap on listing pages per subcategory
                   (0 = unlimited, default: 10)
    --output       Output JSON file path
                   (default: animals_yesterday_result.json)

Environment variables (override proxy defaults):
    DATAIMPULSE_USER   (default: 2ced8727319e5746c47f__cr.kw)
    DATAIMPULSE_PASS   (default: ee22dc54c002e494)
    DATAIMPULSE_HOST   (default: gw.dataimpulse.com)
    DATAIMPULSE_PORT   (default: 823)
"""

import argparse
import asyncio
import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from curl_cffi import requests as curl_requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

YESTERDAY = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


# ─── DataImpulse residential proxy session ────────────────────────────────────

_IMPERSONATION_PROFILES = [
    "chrome124",
    "chrome120",
    "chrome131",
    "safari18_0",
    "chrome116",
]


class _CachedResponse:
    """Zero-cost response served from in-memory URL cache."""

    status_code = 200

    def __init__(self, text: str):
        self.text = text
        self.content = text.encode("utf-8")

    def raise_for_status(self):
        pass


class DataImpulseSession:
    """
    curl_cffi session routed through DataImpulse residential proxies (Kuwait).

    Counters
    --------
    request_count  — real HTTP requests sent through the proxy
    cache_hits     — duplicate URL hits served from memory at zero cost

    Auto-retries on HTTP 403 by rotating TLS impersonation profiles.
    """

    def __init__(
        self,
        user: str,
        password: str,
        host: str = "gw.dataimpulse.com",
        port: int = 823,
    ):
        proxy_url = f"http://{user}:{password}@{host}:{port}"
        self._proxies = {"http": proxy_url, "https": proxy_url}
        self._profile_index = 0
        self._session = curl_requests.Session(
            impersonate=_IMPERSONATION_PROFILES[0],
            proxies=self._proxies,
        )
        self._cache: dict = {}
        self.request_count = 0
        self.cache_hits = 0

    def _next_profile(self) -> str:
        self._profile_index = (self._profile_index + 1) % len(_IMPERSONATION_PROFILES)
        return _IMPERSONATION_PROFILES[self._profile_index]

    def get(self, url: str, **kwargs):
        if url in self._cache:
            self.cache_hits += 1
            logger.debug(f"Cache hit: {url}")
            return _CachedResponse(self._cache[url])

        last_exc = None
        for attempt in range(len(_IMPERSONATION_PROFILES)):
            try:
                if attempt > 0:
                    profile = self._next_profile()
                    logger.warning(
                        f"403 — retrying with '{profile}' "
                        f"(attempt {attempt + 1}/{len(_IMPERSONATION_PROFILES)})"
                    )
                    time.sleep(random.uniform(2.0, 4.0))
                    self._session = curl_requests.Session(
                        impersonate=profile,
                        proxies=self._proxies,
                    )

                response = self._session.get(url, **kwargs)

                if response.status_code == 403:
                    last_exc = Exception(f"HTTP 403 for {url}")
                    continue

                self.request_count += 1
                self._cache[url] = response.text
                return response

            except Exception as exc:
                if "403" in str(exc):
                    last_exc = exc
                    continue
                raise

        raise last_exc or Exception(
            f"All {len(_IMPERSONATION_PROFILES)} retry attempts failed for {url}"
        )

    def close(self):
        if self._session:
            self._session.close()

    @property
    def headers(self):
        return self._session.headers

    @property
    def proxies(self):
        return self._proxies


# ─── Core scraping logic ──────────────────────────────────────────────────────

async def fetch_all_details(scraper, listings: list) -> list:
    """
    Fetch the detail page for EVERY listing in the list.
    Returns the listings list with '_details' populated on each item.
    """
    for listing in listings:
        lslug = listing.get("slug")
        if not lslug:
            logger.warning("Listing has no slug, skipping detail fetch")
            continue
        try:
            details = await scraper.get_listing_details(lslug, listing.get("status"))
            if details:
                listing["_details"] = details
                logger.info(f"      ✓ Detail: {lslug}")
            else:
                logger.warning(f"      ✗ No detail returned for: {lslug}")
        except Exception as exc:
            logger.error(f"      Error fetching detail for {lslug}: {exc}")
        await asyncio.sleep(0.3)
    return listings


async def scrape_all(scraper, max_pages: int) -> list:
    """
    Scrape all Animals subcategories — YESTERDAY only, ALL detail pages.

    max_pages == 0  → paginate until the page returns no yesterday listings.
    """
    results = []

    logger.info("─" * 60)
    logger.info(f"Target date : {YESTERDAY}  (yesterday)")
    logger.info("─" * 60)
    logger.info("Step 1 — Fetching Animals subcategories")

    subcats = await scraper.get_subcategories()
    if not subcats:
        logger.error("No subcategories returned — check proxy credentials")
        return []

    logger.info(f"Found {len(subcats)} subcategories")

    for idx, subcat in enumerate(subcats, 1):
        slug = subcat["slug"]
        name = subcat.get("name_en") or subcat.get("name_ar") or slug

        logger.info("─" * 60)
        logger.info(f"[{idx}/{len(subcats)}] {name}  (slug: {slug})")

        entry = {
            "subcategory": subcat,
            "has_children": False,
            "children": [],
            "listings": [],
            "pages_scraped": 0,
            "detail_fetches": 0,
            "errors": [],
        }

        try:
            children = await scraper.get_catchilds(slug)

            if children:
                entry["has_children"] = True
                entry["children"] = [c.get("slug") for c in children]
                logger.info(f"  {len(children)} child categories found")

                for child in children:
                    child_slug = child["slug"]
                    child_name = child.get("name_en") or child.get("name_ar") or child_slug
                    logger.info(f"  → Child: {child_name}  ({child_slug})")

                    page = 1
                    while True:
                        if max_pages > 0 and page > max_pages:
                            logger.info(f"    Reached max_pages={max_pages}, stopping")
                            break

                        # filter_yesterday=True — only yesterday's listings come back
                        listings = await scraper.get_listings(
                            slug,
                            page_num=page,
                            child_slug=child_slug,
                            filter_yesterday=True,
                        )

                        if not listings:
                            logger.info(f"    Page {page}: no yesterday listings — stopping")
                            break

                        logger.info(
                            f"    Page {page}: {len(listings)} yesterday listing(s)"
                            f" — fetching ALL detail pages..."
                        )

                        listings = await fetch_all_details(scraper, listings)
                        entry["listings"].extend(listings)
                        entry["pages_scraped"] += 1
                        entry["detail_fetches"] += len(
                            [l for l in listings if "_details" in l]
                        )

                        page += 1
                        await asyncio.sleep(0.5)

            else:
                logger.info("  No child categories — scraping main pages")

                page = 1
                while True:
                    if max_pages > 0 and page > max_pages:
                        logger.info(f"  Reached max_pages={max_pages}, stopping")
                        break

                    listings = await scraper.get_listings(
                        slug,
                        page_num=page,
                        filter_yesterday=True,
                    )

                    if not listings:
                        logger.info(f"  Page {page}: no yesterday listings — stopping")
                        break

                    logger.info(
                        f"  Page {page}: {len(listings)} yesterday listing(s)"
                        f" — fetching ALL detail pages..."
                    )

                    listings = await fetch_all_details(scraper, listings)
                    entry["listings"].extend(listings)
                    entry["pages_scraped"] += 1
                    entry["detail_fetches"] += len(
                        [l for l in listings if "_details" in l]
                    )

                    page += 1
                    await asyncio.sleep(0.5)

        except Exception as exc:
            logger.error(f"  Error processing {name}: {exc}")
            entry["errors"].append(str(exc))

        logger.info(
            f"  Subtotal → {len(entry['listings'])} listings, "
            f"{entry['pages_scraped']} pages, "
            f"{entry['detail_fetches']} details fetched"
        )
        results.append(entry)
        await asyncio.sleep(1.0)

    return results


# ─── Entry point ─────────────────────────────────────────────────────────────

async def run(args):
    user     = os.environ.get("DATAIMPULSE_USER", "2ced8727319e5746c47f__cr.kw")
    password = os.environ.get("DATAIMPULSE_PASS", "ee22dc54c002e494")
    host     = os.environ.get("DATAIMPULSE_HOST", "gw.dataimpulse.com")
    port     = int(os.environ.get("DATAIMPULSE_PORT", "823"))

    session = DataImpulseSession(user=user, password=password, host=host, port=port)

    logger.info("=" * 60)
    logger.info("ANIMALS SCRAPER — DataImpulse Residential Proxy (Kuwait)")
    logger.info("Yesterday data only  |  ALL detail pages fetched")
    logger.info("=" * 60)
    logger.info(f"Proxy       : {host}:{port}  (Kuwait residential)")
    logger.info(f"Target date : {YESTERDAY}  (yesterday)")
    logger.info(f"Max pages   : {'unlimited' if args.max_pages == 0 else args.max_pages}  per subcategory")
    logger.info(f"Details     : ALL — every listing's detail page is fetched")
    logger.info(f"Output      : {args.output}")
    logger.info("=" * 60)

    from json_scraper import AnimalsJsonScraper  # noqa: E402

    scraper = AnimalsJsonScraper()
    try:
        scraper.session.close()
    except Exception:
        pass
    scraper.session = session

    start_time = time.time()
    try:
        results = await scrape_all(scraper, args.max_pages)
    finally:
        await scraper.close_browser()

    elapsed = time.time() - start_time

    # ── Compile stats ─────────────────────────────────────────────────────────
    total_listings   = sum(len(r["listings"]) for r in results)
    total_details    = sum(r["detail_fetches"] for r in results)
    total_requests   = session.request_count
    total_cache_hits = session.cache_hits
    total_combined   = total_requests + total_cache_hits

    output = {
        "test_run":    datetime.now().isoformat(),
        "proxy":       f"{host}:{port} (Kuwait residential — DataImpulse)",
        "target_date": YESTERDAY,
        "config": {
            "max_pages":   args.max_pages,
            "date_filter": f"yesterday ({YESTERDAY}) only",
            "details":     "all — no cap, no skip",
        },
        "stats": {
            "elapsed_seconds":               round(elapsed, 1),
            "total_http_requests":           total_requests,
            "cache_hits_free":               total_cache_hits,
            "total_fetches_including_cache": total_combined,
            "total_subcategories":           len(results),
            "total_listings":                total_listings,
            "total_detail_fetches":          total_details,
        },
        "subcategory_summary": [
            {
                "slug":           r["subcategory"]["slug"],
                "name_en":        r["subcategory"].get("name_en"),
                "name_ar":        r["subcategory"].get("name_ar"),
                "has_children":   r["has_children"],
                "children":       r["children"],
                "pages_scraped":  r["pages_scraped"],
                "listings_count": len(r["listings"]),
                "detail_fetches": r["detail_fetches"],
                "errors":         r["errors"],
            }
            for r in results
        ],
        "full_results": results,
    }

    output_path = Path(args.output)
    output_path.write_text(
        json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # ── Final report ──────────────────────────────────────────────────────────
    logger.info("")
    logger.info("=" * 60)
    logger.info("FINAL REPORT")
    logger.info("=" * 60)
    logger.info(f"  Target date                : {YESTERDAY}")
    logger.info(f"  Elapsed time               : {elapsed:.1f}s")
    logger.info(f"  ┌─ Total HTTP requests made : {total_requests}   (real proxy requests)")
    logger.info(f"  ├─ Cache hits (free)        : {total_cache_hits}   (served from memory)")
    logger.info(f"  └─ All fetches combined     : {total_combined}")
    logger.info(f"  Subcategories scraped       : {len(results)}")
    logger.info(f"  Total listings (yesterday)  : {total_listings}")
    logger.info(f"  Total detail pages fetched  : {total_details}")
    logger.info(f"  Output saved to             : {output_path.resolve()}")
    logger.info("")

    if results:
        logger.info(f"  {'Category':<32} {'Listings':>8}  {'Details':>7}  {'Pages':>5}  {'Errors':>6}")
        logger.info("  " + "-" * 62)
        for r in results:
            sc   = r["subcategory"]
            name = sc.get("name_en") or sc.get("name_ar") or sc["slug"]
            err  = len(r["errors"])
            logger.info(
                f"  {name:<32} {len(r['listings']):>8}  "
                f"{r['detail_fetches']:>7}  {r['pages_scraped']:>5}  "
                + (f"{'⚠ ' + str(err):>6}" if err else f"{'OK':>6}")
            )
    logger.info("")

    if total_listings == 0:
        logger.warning(
            "No listings found for yesterday — this may be normal if "
            "q84sale had no new animals listings posted yesterday."
        )
    else:
        logger.info(
            f"PASS  {total_listings} yesterday listings scraped, "
            f"{total_details} detail pages fetched, "
            f"{total_requests} proxy requests made"
        )


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Scrape Animals listings from q84sale.com posted YESTERDAY via "
            "DataImpulse residential proxies (Kuwait). "
            "Detail pages are always fetched for every listing found."
        )
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=10,
        help="Safety cap on listing pages per subcategory (0 = unlimited, default: 10)",
    )
    parser.add_argument(
        "--output",
        default="animals_yesterday_result.json",
        help="Output JSON file path (default: animals_yesterday_result.json)",
    )
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
