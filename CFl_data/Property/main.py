import asyncio
import pandas as pd
from datetime import datetime, timedelta
import json
import logging
import requests
from bs4 import BeautifulSoup
from details_scraping import PropertyDetailsScraper
from s3_uploader import S3Uploader
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scraper_utils import get_random_headers, random_delay, rotate_user_agent, configure_session_proxy, create_session

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

YESTERDAY = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
YEAR, MONTH, DAY = datetime.now().year, datetime.now().month, datetime.now().day

AWS_BUCKET = "data-collection-dl"
s3 = S3Uploader(AWS_BUCKET)

BASE_URL = "https://www.q84sale.com/ar/property"
SESSION = create_session()

async def get_property_subcategories():
    logger.info(f"Fetching property subcategories from {BASE_URL}")
    try:
        random_delay(1.0, 3.0)  # Random delay before request
        rotate_user_agent(SESSION)  # Rotate user agent
        response = SESSION.get(BASE_URL, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        script = soup.find('script', {'id': '__NEXT_DATA__'})
        
        data = json.loads(script.string)
        page_props = data["props"]["pageProps"]
        subcats = (
            page_props.get("verticalSubcats")
            or page_props.get("propertySubcats")
            or []
        )
        logger.info(f"Found {len(subcats)} property subcategories")
        return subcats
    except Exception as e:
        logger.error(f"Error fetching subcategories: {e}")
        return []

async def get_business_listings(url):
    try:
        random_delay(1.0, 3.0)  # Random delay before request
        rotate_user_agent(SESSION)  # Rotate user agent
        response = SESSION.get(url, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        script = soup.find('script', {'id': '__NEXT_DATA__'})

        if not script:
            return []

        data = json.loads(script.string)

        return (
            data.get("props", {})
                .get("pageProps", {})
                .get("businessListings", {})
                .get("data", {})
                .get("listings", {})
                .get("normal_items", [])
        )
    except Exception as e:
        logger.error(f"Error getting business listings from {url}: {e}")
        return []

async def scrape_subcategory(subcat):
    name = subcat["name_en"]
    slug = subcat["slug"]
    logger.info(f"Starting scrape for subcategory: {name} ({slug})")

    base_url = f"https://www.q84sale.com/ar/property/{slug}/{{}}"

    # Load first page
    try:
        random_delay(1.0, 3.0)  # Random delay before request
        rotate_user_agent(SESSION)  # Rotate user agent
        response = SESSION.get(base_url.format(1), timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        script = soup.find('script', {'id': '__NEXT_DATA__'})
        
        data = json.loads(script.string)
    except Exception as e:
        logger.error(f"Error loading first page for {slug}: {e}")
        return

    page_props = data.get("props", {}).get("pageProps", {})
    sub_subcats = page_props.get("subcategories", [])
    businesses = page_props.get("businessesData", {}).get("data", [])

    results = []

    # ---------------------------------------------------
    # CASE 1: SALE / RENT (has subcategories)
    # ---------------------------------------------------
    if sub_subcats:
        logger.info(f"Found {len(sub_subcats)} sub-subcategories for {name}")

        for sub in sub_subcats:
            sub_name = sub["name_en"]
            sub_slug = sub["slug"]
            total_pages = sub.get("totalPages", 1)
            sub_url = f"https://www.q84sale.com/ar/property/{slug}/{sub_slug}/{{}}"

            logger.info(f"Processing {sub_name} ({total_pages} pages)")

            for page_no in range(1, total_pages + 1):
                scraper = PropertyDetailsScraper(sub_url.format(page_no))
                listings = await scraper.get_listings()

                listings = [
                    {**l, "sheet": sub_name}
                    for l in listings
                    if l.get("date_published")
                    and l["date_published"].split(" ")[0] == YESTERDAY
                ]

                for l in listings:
                    s3_paths = []
                    for idx, img in enumerate(l.get("images", [])):
                        img_url = img if isinstance(img, str) else img.get("url", "")
                        if img_url:
                            path = (
                                f"4sale-data/property/year={YEAR}/month={MONTH}/day={DAY}/"
                                f"images/{slug}/{sub_slug}/{l['id']}_{idx}.jpg"
                            )
                            result = await s3.upload_image(img_url, path)
                            if result:
                                s3_paths.append(path)
                    l["s3_images_paths"] = s3_paths

                results.extend(listings)

    # ---------------------------------------------------
    # CASE 2: PROPERTY OFFICES / 4SALE REALTY (businesses)
    # ---------------------------------------------------
    elif businesses:
        logger.info(f"{name} has {len(businesses)} businesses")

        for biz in businesses:
            biz_slug = biz["slug"]
            biz_name = biz["name"]
            page_no = 1

            while True:
                biz_url = (
                    f"https://www.q84sale.com/ar/businesses/"
                    f"{slug}/{biz_slug}/all?page={page_no}"
                )

                raw_listings = await get_business_listings(biz_url)
                if not raw_listings:
                    break

                for item in raw_listings:
                    if not item.get("date_published"):
                        continue
                    if item["date_published"].split(" ")[0] != YESTERDAY:
                        continue

                    details_url = f"https://www.q84sale.com/ar/listing/{item['slug']}"
                    details = PropertyDetailsScraper(details_url)
                    data = await details.get_listings()

                    if not data:
                        continue

                    record = data[0]
                    record["sheet"] = biz_name

                    s3_paths = []
                    for idx, img in enumerate(record.get("images", [])):
                        img_url = img if isinstance(img, str) else img.get("url", "")
                        if img_url:
                            path = (
                                f"4sale-data/property/year={YEAR}/month={MONTH}/day={DAY}/"
                                f"images/{slug}/{biz_slug}/{record['id']}_{idx}.jpg"
                            )
                            result = await s3.upload_image(img_url, path)
                            if result:
                                s3_paths.append(path)

                    record["s3_images_paths"] = s3_paths
                    results.append(record)

                page_no += 1
    # ---------------------------------------------------
    # CASE 3: DIRECT LISTINGS (exchange, international…)
    # ---------------------------------------------------
    else:
        total_pages = page_props.get("totalPages", 1)
        logger.info(f"No subcategories. Processing {total_pages} pages")

        for page_no in range(1, total_pages + 1):
            scraper = PropertyDetailsScraper(base_url.format(page_no))
            listings = await scraper.get_listings()

            listings = [
                {**l, "sheet": "All Listings"}
                for l in listings
                if l.get("date_published")
                and l["date_published"].split(" ")[0] == YESTERDAY
            ]

            for l in listings:
                s3_paths = []
                for idx, img in enumerate(l.get("images", [])):
                    img_url = img if isinstance(img, str) else img.get("url", "")
                    if img_url:
                        path = (
                            f"4sale-data/property/year={YEAR}/month={MONTH}/day={DAY}/"
                            f"images/{slug}/all/{l['id']}_{idx}.jpg"
                        )
                        result = await s3.upload_image(img_url, path)
                        if result:
                            s3_paths.append(path)
                l["s3_images_paths"] = s3_paths

            results.extend(listings)

    # ---------------------------------------------------
    # SAVE EXCEL (MULTI-SHEET, CORRECTLY LABELED)
    # ---------------------------------------------------
    if not results:
        logger.warning(f"No listings found for {slug} from {YESTERDAY}")
        return

    temp_file = f"temp_{slug}.xlsx"
    excel_path = f"4sale-data/property/year={YEAR}/month={MONTH}/day={DAY}/excel-files/{slug}.xlsx"

    with pd.ExcelWriter(temp_file, engine="openpyxl") as writer:
        for sheet in sorted(set(r["sheet"] for r in results)):
            rows = [r for r in results if r["sheet"] == sheet]
            pd.DataFrame(rows).to_excel(
                writer, index=False, sheet_name=sheet[:31]
            )

    with open(temp_file, "rb") as f:
        await s3.upload_fileobj(f, excel_path)

    logger.info(f"✓ {slug} saved to S3 with {len(set(r['sheet'] for r in results))} sheets")


async def main():
    logger.info("="*50)
    logger.info("Starting Property Scraper")
    logger.info(f"Scraping for date: {YESTERDAY}")
    logger.info("="*50)
    
    try:
        subcategories = await get_property_subcategories()
        logger.info(f"Creating tasks for {len(subcategories)} subcategories")
        tasks = [scrape_subcategory(sub) for sub in subcategories]
        await asyncio.gather(*tasks)
        logger.info("All scraping tasks completed successfully")
    except Exception as e:
        logger.error(f"Error during scraping: {e}", exc_info=True)
    
    logger.info("="*50)
    logger.info("Property Scraper finished")
    logger.info("="*50)

if __name__ == "__main__":
    asyncio.run(main())