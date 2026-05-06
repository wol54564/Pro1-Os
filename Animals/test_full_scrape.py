"""
Full Animals scrape test using scrape.do as HTTP transport.

What this does:
  1. Fetches all Animals subcategories                       (1 request)
  2. For each subcategory, checks for child categories       (1 req/subcat — cached if
     listing page 1 was already fetched for the same URL)
  3. Paginates through listings up to --max-pages            (1 req/page)
  4. Optionally fetches individual listing detail pages      (1 req each, limited by
     --max-details per subcategory to control credit spend)
  5. Saves the full result to a local JSON file (no S3 needed)
  6. Prints a final report: listings found, scrape.do credits used, cache hits

Usage:
    SCRAPEDO_TOKEN=<token> python test_full_scrape.py
    python test_full_scrape.py --max-pages 2 --max-details 3 --output result.json

Environment:
    SCRAPEDO_TOKEN  — scrape.do API token (required if not passed via --token)
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# Allow imports from project root (scraper_utils, etc.)
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────

async def scrape_all(scraper, max_pages: int, max_details: int) -> list:
    """
    Run the full Animals scrape:
      subcategories → (catchilds or main pages) → listings → optional details
    Returns a list of per-subcategory result dicts.
    """
    results = []

    logger.info("─" * 60)
    logger.info("Step 1 — Fetching subcategories")
    subcats = await scraper.get_subcategories()

    if not subcats:
        logger.error("No subcategories returned — check token and connectivity")
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
            # ── Step 2: check for child categories ──────────────────────────
            # get_catchilds fetches /{slug}/1 — the session URL cache means the
            # subsequent get_listings(slug, page_num=1) call hits the same URL
            # for free when there are NO children.
            children = await scraper.get_catchilds(slug)

            if children:
                entry["has_children"] = True
                entry["children"] = [c.get("slug") for c in children]
                logger.info(f"  {len(children)} child categories found")

                details_fetched = 0
                for child in children:
                    child_slug = child["slug"]
                    child_name = child.get("name_en") or child.get("name_ar") or child_slug
                    logger.info(f"  → Child: {child_name}  ({child_slug})")

                    for page in range(1, max_pages + 1):
                        listings = await scraper.get_listings(
                            slug,
                            page_num=page,
                            child_slug=child_slug,
                            filter_yesterday=False,
                        )
                        if not listings:
                            logger.info(f"    Page {page}: empty — stopping pagination")
                            break

                        logger.info(f"    Page {page}: {len(listings)} listings")

                        # Fetch details for the first max_details listings total
                        if max_details > 0:
                            for listing in listings:
                                if details_fetched >= max_details:
                                    break
                                lslug = listing.get("slug")
                                if lslug:
                                    details = await scraper.get_listing_details(
                                        lslug, listing.get("status")
                                    )
                                    if details:
                                        listing["_details"] = details
                                        details_fetched += 1
                                        logger.info(f"      Detail fetched: {lslug}")

                        entry["listings"].extend(listings)
                        entry["pages_scraped"] += 1
                        await asyncio.sleep(0.5)

                entry["detail_fetches"] = details_fetched

            else:
                # ── No children — scrape main category pages ─────────────────
                # Page 1 URL == catchilds URL → session cache serves it FREE
                details_fetched = 0
                for page in range(1, max_pages + 1):
                    listings = await scraper.get_listings(
                        slug,
                        page_num=page,
                        filter_yesterday=False,
                    )
                    if not listings:
                        logger.info(f"  Page {page}: empty — stopping pagination")
                        break

                    logger.info(f"  Page {page}: {len(listings)} listings")

                    if max_details > 0:
                        for listing in listings:
                            if details_fetched >= max_details:
                                break
                            lslug = listing.get("slug")
                            if lslug:
                                details = await scraper.get_listing_details(
                                    lslug, listing.get("status")
                                )
                                if details:
                                    listing["_details"] = details
                                    details_fetched += 1
                                    logger.info(f"    Detail fetched: {lslug}")

                    entry["listings"].extend(listings)
                    entry["pages_scraped"] += 1
                    await asyncio.sleep(0.5)

                entry["detail_fetches"] = details_fetched

        except Exception as exc:
            logger.error(f"  Error processing {name}: {exc}")
            entry["errors"].append(str(exc))

        logger.info(
            f"  Subtotal: {len(entry['listings'])} listings, "
            f"{entry['pages_scraped']} pages, "
            f"{entry['detail_fetches']} details fetched"
        )
        results.append(entry)

        await asyncio.sleep(1.0)  # polite gap between subcategories

    return results


async def run(args):
    token = args.token or os.environ.get("SCRAPEDO_TOKEN", "")
    if not token:
        logger.error("No SCRAPEDO_TOKEN provided. Use --token or set the env var.")
        sys.exit(1)

    # Set BEFORE importing AnimalsJsonScraper so create_session() picks up the token
    os.environ["SCRAPEDO_TOKEN"] = token

    from json_scraper import AnimalsJsonScraper  # noqa: E402 (intentional late import)

    scraper = AnimalsJsonScraper()
    session = scraper.session  # Should be ScrapedoSession

    logger.info("=" * 60)
    logger.info("ANIMALS FULL SCRAPE TEST  —  scrape.do transport")
    logger.info("=" * 60)
    logger.info(f"Token       : {token[:8]}...{token[-4:]}  (masked)")
    logger.info(f"Max pages   : {args.max_pages}  per subcategory")
    logger.info(f"Max details : {args.max_details}  per subcategory  (0 = skip)")
    logger.info(f"Output      : {args.output}")

    start_time = time.time()

    try:
        results = await scrape_all(scraper, args.max_pages, args.max_details)
    finally:
        await scraper.close_browser()

    elapsed = time.time() - start_time

    # ── Compile stats ─────────────────────────────────────────────────────────
    total_listings = sum(len(r["listings"]) for r in results)
    total_details  = sum(r["detail_fetches"] for r in results)
    credits_used   = session.request_count
    cache_saved    = session.cache_hits

    output = {
        "test_run": datetime.now().isoformat(),
        "config": {
            "max_pages": args.max_pages,
            "max_details": args.max_details,
        },
        "stats": {
            "elapsed_seconds": round(elapsed, 1),
            "scrape_do_requests": credits_used,
            "cache_hits_free":    cache_saved,
            "total_subcategories": len(results),
            "total_listings":      total_listings,
            "total_detail_fetches": total_details,
        },
        "subcategory_summary": [
            {
                "slug":          r["subcategory"]["slug"],
                "name_en":       r["subcategory"].get("name_en"),
                "has_children":  r["has_children"],
                "children":      r["children"],
                "pages_scraped": r["pages_scraped"],
                "listings":      len(r["listings"]),
                "detail_fetches": r["detail_fetches"],
                "errors":        r["errors"],
            }
            for r in results
        ],
        "full_results": results,
    }

    # ── Save to file ──────────────────────────────────────────────────────────
    output_path = Path(args.output)
    output_path.write_text(
        json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # ── Print final report ────────────────────────────────────────────────────
    logger.info("")
    logger.info("=" * 60)
    logger.info("FINAL REPORT")
    logger.info("=" * 60)
    logger.info(f"  Elapsed time          : {elapsed:.1f}s")
    logger.info(f"  scrape.do requests    : {credits_used}  (credits charged)")
    logger.info(f"  Cache hits (free)     : {cache_saved}  (0 credits)")
    logger.info(f"  Subcategories scraped : {len(results)}")
    logger.info(f"  Total listings found  : {total_listings}")
    logger.info(f"  Total detail fetches  : {total_details}")
    logger.info(f"  Output saved to       : {output_path.resolve()}")
    logger.info("")

    if results:
        logger.info("  Per-subcategory breakdown:")
        for r in results:
            sc = r["subcategory"]
            name = sc.get("name_en") or sc.get("name_ar") or sc["slug"]
            err  = f"  ⚠ {len(r['errors'])} errors" if r["errors"] else ""
            logger.info(
                f"    {name:<30} {len(r['listings']):>4} listings  "
                f"{r['pages_scraped']} pages{err}"
            )
    logger.info("")

    if total_listings == 0:
        logger.error("FAIL  No listings found — scraping did not work as expected")
        sys.exit(1)

    logger.info("PASS  Full scrape completed successfully")


def main():
    parser = argparse.ArgumentParser(
        description="Full Animals scrape test via scrape.do"
    )
    parser.add_argument(
        "--token",
        default="",
        help="scrape.do API token (or set SCRAPEDO_TOKEN env var)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=2,
        help="Max listing pages per subcategory (default: 2)",
    )
    parser.add_argument(
        "--max-details",
        type=int,
        default=3,
        help="Max detail-page fetches per subcategory (default: 3, 0 = skip)",
    )
    parser.add_argument(
        "--output",
        default="animals_test_output.json",
        help="Output JSON file path (default: animals_test_output.json)",
    )
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
