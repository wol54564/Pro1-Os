"""
Full Used-Cars scrape test using scrape.do as HTTP transport.

Hierarchy scraped:
  Used Cars (main page)
    └── Make  (Toyota, Lexus, …)         — get_main_categories()
          └── Model  (Land Cruiser, …)   — get_subcategories(make)
                └── Listings  (pages)    — get_listings(make, model, page)
                      └── Detail page   — get_listing_details(slug)   [optional]

Cache wins built into ScrapedoSession (scraper_utils.py):
  • get_main_categories()  → fetches /used-cars/1
    If any later call hits the same URL it is served FREE from memory.
  • get_subcategories(make) → fetches /used-cars/{make}/1
    get_listings(make, model=None, page=1) also hits /used-cars/{make}/1
    → second call is FREE (no credit charged).

Usage:
    SCRAPEDO_TOKEN=<token> python test_full_scrape_used_cars.py
    python test_full_scrape_used_cars.py \\
        --token <tok> --max-makes 3 --max-models 2 --max-pages 2 --max-details 3

Environment:
    SCRAPEDO_TOKEN  — scrape.do API token (set by the workflow from SCRAPEDO_TOKEN2 secret)
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
async def scrape_all(scraper, max_makes: int, max_models: int,
                     max_pages: int, max_details: int) -> list:
    """
    Run the full Used-Cars scrape pipeline (no S3 / no Excel).
    Returns a list of per-make result dicts.
    """
    results = []

    logger.info("─" * 60)
    logger.info("Step 1 — Fetching main categories (car makes)")
    # Fetches /used-cars/1 → cached for rest of session
    makes = await scraper.get_main_categories()

    if not makes:
        logger.error("No main categories returned — check token and connectivity")
        return []

    if max_makes > 0:
        makes = makes[:max_makes]
        logger.info(f"Limiting to {max_makes} makes for this test run")

    logger.info(f"Processing {len(makes)} make(s)")

    for make_idx, make in enumerate(makes, 1):
        make_slug = make["slug"]
        make_name = make.get("name_en") or make.get("name_ar") or make_slug

        logger.info("─" * 60)
        logger.info(f"[Make {make_idx}/{len(makes)}]  {make_name}  ({make_slug})")

        make_entry = {
            "make": make,
            "models": [],
            "total_listings": 0,
            "total_pages_scraped": 0,
            "total_detail_fetches": 0,
            "errors": [],
        }

        try:
            # ── Step 2: get models for this make ───────────────────────────
            # Fetches /used-cars/{make}/1
            # If get_listings(make, None, 1) is later called it hits the same URL → FREE
            models = await scraper.get_subcategories(make_slug)

            if not models:
                logger.info(f"  No models found for {make_name}, skipping")
                results.append(make_entry)
                continue

            if max_models > 0:
                models = models[:max_models]

            logger.info(f"  {len(models)} model(s) found")

            for model_idx, model in enumerate(models, 1):
                model_slug = model["slug"]
                model_name = model.get("name_en") or model.get("name_ar") or model_slug
                model_count = model.get("listings_count", 0)

                logger.info(f"  [{model_idx}/{len(models)}] {model_name} — "
                            f"{model_count} listings available")

                if model_count == 0:
                    logger.info("    Skipping — 0 listings")
                    continue

                model_entry = {
                    "model": model,
                    "listings": [],
                    "pages_scraped": 0,
                    "detail_fetches": 0,
                    "errors": [],
                }

                details_fetched = 0

                # ── Step 3: paginate listings ──────────────────────────────
                for page in range(1, max_pages + 1):
                    listings, total_pages = await scraper.get_listings(
                        make_slug,
                        model_slug,
                        page_num=page,
                        filter_yesterday=False,
                    )

                    if not listings:
                        logger.info(f"    Page {page}: empty — stopping pagination")
                        break

                    logger.info(f"    Page {page}/{total_pages}: {len(listings)} listings")

                    # ── Step 4: optional detail fetches ───────────────────
                    if max_details > 0:
                        for listing in listings:
                            if details_fetched >= max_details:
                                break
                            lslug = listing.get("slug")
                            if not lslug:
                                continue
                            details = await scraper.get_listing_details(
                                lslug, listing.get("status")
                            )
                            if details:
                                listing["_details"] = details
                                details_fetched += 1
                                logger.info(f"      Detail OK: {lslug}")

                    model_entry["listings"].extend(listings)
                    model_entry["pages_scraped"] += 1

                    # Respect totalPages from the API
                    if total_pages and page >= total_pages:
                        logger.info(f"    Reached last page ({total_pages})")
                        break

                    await asyncio.sleep(0.5)

                model_entry["detail_fetches"] = details_fetched
                make_entry["models"].append(model_entry)
                make_entry["total_listings"] += len(model_entry["listings"])
                make_entry["total_pages_scraped"] += model_entry["pages_scraped"]
                make_entry["total_detail_fetches"] += details_fetched

                logger.info(
                    f"    Subtotal: {len(model_entry['listings'])} listings, "
                    f"{model_entry['pages_scraped']} pages, "
                    f"{details_fetched} details"
                )
                await asyncio.sleep(0.5)

        except Exception as exc:
            logger.error(f"  Error processing {make_name}: {exc}")
            make_entry["errors"].append(str(exc))

        logger.info(
            f"Make total: {make_entry['total_listings']} listings across "
            f"{len(make_entry['models'])} model(s)"
        )
        results.append(make_entry)
        await asyncio.sleep(1.0)

    return results


# ─────────────────────────────────────────────────────────────────────────────
async def run(args):
    token = args.token or os.environ.get("SCRAPEDO_TOKEN", "")
    if not token:
        logger.error(
            "No token provided. Use --token or set SCRAPEDO_TOKEN env var.\n"
            "(The workflow maps SCRAPEDO_TOKEN2 secret → SCRAPEDO_TOKEN env var automatically.)"
        )
        sys.exit(1)

    # Must be set BEFORE importing UsedCarsJsonScraper so create_session() picks it up
    os.environ["SCRAPEDO_TOKEN"] = token

    from json_scraper_used_cars import UsedCarsJsonScraper  # noqa: E402

    scraper = UsedCarsJsonScraper()
    session = scraper.session  # ScrapedoSession when SCRAPEDO_TOKEN is set

    logger.info("=" * 60)
    logger.info("USED CARS FULL SCRAPE TEST  —  scrape.do transport")
    logger.info("=" * 60)
    logger.info(f"Token       : {token[:8]}...{token[-4:]}  (masked)")
    logger.info(f"Max makes   : {args.max_makes}   (0 = all)")
    logger.info(f"Max models  : {args.max_models}  per make  (0 = all)")
    logger.info(f"Max pages   : {args.max_pages}   per model")
    logger.info(f"Max details : {args.max_details} per model  (0 = skip)")
    logger.info(f"Output      : {args.output}")

    start_time = time.time()

    try:
        results = await scrape_all(
            scraper,
            max_makes=args.max_makes,
            max_models=args.max_models,
            max_pages=args.max_pages,
            max_details=args.max_details,
        )
    finally:
        await scraper.close_browser()

    elapsed = time.time() - start_time

    # ── Compile stats ─────────────────────────────────────────────────────────
    total_listings = sum(m["total_listings"] for m in results)
    total_details  = sum(m["total_detail_fetches"] for m in results)
    credits_used   = session.request_count
    cache_saved    = session.cache_hits

    output = {
        "test_run": datetime.now().isoformat(),
        "config": {
            "max_makes":   args.max_makes,
            "max_models":  args.max_models,
            "max_pages":   args.max_pages,
            "max_details": args.max_details,
        },
        "stats": {
            "elapsed_seconds":      round(elapsed, 1),
            "scrape_do_requests":   credits_used,
            "cache_hits_free":      cache_saved,
            "total_makes":          len(results),
            "total_models":         sum(len(m["models"]) for m in results),
            "total_listings":       total_listings,
            "total_detail_fetches": total_details,
        },
        "make_summary": [
            {
                "slug":          m["make"]["slug"],
                "name_en":       m["make"].get("name_en"),
                "models_scraped": len(m["models"]),
                "total_listings": m["total_listings"],
                "total_pages":   m["total_pages_scraped"],
                "detail_fetches": m["total_detail_fetches"],
                "errors":        m["errors"],
                "models": [
                    {
                        "slug":     md["model"]["slug"],
                        "name_en":  md["model"].get("name_en"),
                        "listings": len(md["listings"]),
                        "pages":    md["pages_scraped"],
                        "details":  md["detail_fetches"],
                        "errors":   md["errors"],
                    }
                    for md in m["models"]
                ],
            }
            for m in results
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
    logger.info(f"  Elapsed time           : {elapsed:.1f}s")
    logger.info(f"  scrape.do requests     : {credits_used}  (credits charged)")
    logger.info(f"  Cache hits (free)      : {cache_saved}  (0 credits)")
    logger.info(f"  Makes scraped          : {len(results)}")
    logger.info(f"  Models scraped         : {sum(len(m['models']) for m in results)}")
    logger.info(f"  Total listings found   : {total_listings}")
    logger.info(f"  Total detail fetches   : {total_details}")
    logger.info(f"  Output saved to        : {output_path.resolve()}")
    logger.info("")

    if results:
        logger.info("  Per-make breakdown:")
        for m in results:
            name = m["make"].get("name_en") or m["make"]["slug"]
            err  = f"  ⚠ {len(m['errors'])} errors" if m["errors"] else ""
            logger.info(
                f"    {name:<25} {m['total_listings']:>5} listings  "
                f"{len(m['models'])} models{err}"
            )
    logger.info("")

    if total_listings == 0:
        logger.error("FAIL  No listings found — scraping did not work as expected")
        sys.exit(1)

    logger.info("PASS  Full Used-Cars scrape completed successfully")


# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Full Used-Cars scrape test via scrape.do"
    )
    parser.add_argument(
        "--token",
        default="",
        help="scrape.do API token (or set SCRAPEDO_TOKEN env var)",
    )
    parser.add_argument(
        "--max-makes", type=int, default=3,
        help="Max number of car makes to scrape (default: 3, 0 = all)",
    )
    parser.add_argument(
        "--max-models", type=int, default=2,
        help="Max models per make (default: 2, 0 = all)",
    )
    parser.add_argument(
        "--max-pages", type=int, default=2,
        help="Max listing pages per model (default: 2)",
    )
    parser.add_argument(
        "--max-details", type=int, default=3,
        help="Max detail-page fetches per model (default: 3, 0 = skip)",
    )
    parser.add_argument(
        "--output", default="used_cars_test_output.json",
        help="Output JSON file (default: used_cars_test_output.json)",
    )
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
