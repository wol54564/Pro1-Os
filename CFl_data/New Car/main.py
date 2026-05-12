import asyncio
import pandas as pd
import boto3
import io
import json
import re
import logging
import requests
from datetime import datetime, timedelta
from pathlib import Path
from bs4 import BeautifulSoup

from details_scraping import DetailsScraping
from car_scraper import CarScraper
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scraper_utils import get_random_headers, random_delay, rotate_user_agent, configure_session_proxy, create_session

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MainScraper:
    def __init__(self, url, R2_bucket):
        self.url = url
        self.R2_bucket = R2_bucket
        self.R2_client = boto3.client("R2")
        self.chunk_size = 3
        self.chunk_delay = 5
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        self.session = create_session()
        logger.info(f"Scraping data for date: {self.yesterday}")

    async def upload_bytes_to_R2(self, data_bytes, R2_path):
        """Upload bytes directly to R2."""
        self.R2_client.put_object(
            Bucket=self.R2_bucket,
            Key=R2_path,
            Body=data_bytes
        )
        return f"R2://{self.R2_bucket}/{R2_path}"

    async def download_image(self, url, brand_name, filename):
        """
        Download image using requests with proper headers.
        """
        try:
            today = datetime.now()
            R2_path = (
                f"4sale-data/new-cars/year={today.year}/month={today.month}/day={today.day}/"
                f"images/{brand_name}/{filename}"
            )

            random_delay(1.0, 3.0)  # Random delay before request
            rotate_user_agent(self.session)  # Rotate user agent
            response = self.session.get(url, timeout=30)

            if response.status_code != 200:
                print(f"[IMG FAIL] {url} Status={response.status_code}")
                return None

            img_bytes = response.content

            await self.upload_bytes_to_R2(img_bytes, R2_path)
            return R2_path

        except Exception as e:
            print(f"Image download error for {url}: {e}")
            return None

    async def process_brand_chunk(self, brand_chunk):
        for brand_info in brand_chunk:
            brand_name = brand_info["brand"].replace(" ", "_")
            all_car_details = []

            for car_type in brand_info["types"]:
                type_name = re.sub(r'[\\/*?:[\]]', '', car_type["title"].replace(" ", "_"))[:31]
                type_page_url = car_type["type_link"]

                try:
                    random_delay(1.0, 3.0)  # Random delay before request
                    rotate_user_agent(self.session)  # Rotate user agent
                    response = self.session.get(type_page_url, timeout=30)
                    response.raise_for_status()
                    
                    soup = BeautifulSoup(response.content, 'html.parser')
                    script = soup.find('script', {'id': '__NEXT_DATA__'})
                    
                    data = script and json.loads(script.string)

                    page_props = data.get("props", {}).get("pageProps", {}) if data else {}
                    listings = page_props.get("listings", [])

                    type_details = []

                    for item in listings:
                        # Filter for yesterday's listings only
                        if item.get("date_published"):
                            item_date = item["date_published"].split()[0]
                            if item_date != self.yesterday:
                                logger.debug(f"Skipping listing from {item_date} (not yesterday's date)")
                                continue
                        else:
                            logger.warning(f"Listing missing date_published, skipping")
                            continue
                        
                        listing_slug = item.get("slug")
                        listing_url = f"https://www.q84sale.com/ar/listing/{listing_slug}"

                        details_scraper = DetailsScraping(listing_url)
                        detail_list = await details_scraper.get_car_details()

                        if detail_list:
                            for car_detail in detail_list:
                                img_url = car_detail.get("image")
                                img_file = car_detail.get("image_filename")

                                if img_url and img_file:
                                    # Use requests-based image download
                                    R2_img_path = await self.download_image(
                                        img_url, brand_name, img_file
                                    )
                                    car_detail["image_R2_path"] = R2_img_path

                            type_details.extend(detail_list)

                    if type_details:
                        all_car_details.append({
                            "type_name": type_name,
                            "details": type_details
                        })
                
                except Exception as e:
                    logger.error(f"Error processing type {type_name}: {e}")
                    continue

            # If brand has any data ? generate Excel and upload
            if all_car_details:
                today = datetime.now()

                R2_prefix = (
                    f"4sale-data/new-cars/year={today.year}/month={today.month}/day={today.day}/excel_files/"
                )

                excel_name = f"{brand_name}.xlsx"
                output = io.BytesIO()

                with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                    for t in all_car_details:
                        df = pd.DataFrame(t["details"])
                        sheet_name = t["type_name"][:31]
                        df.to_excel(writer, sheet_name=sheet_name, index=False)

                output.seek(0)

                # Upload excel to R2
                await self.upload_bytes_to_R2(output.read(), f"{R2_prefix}{excel_name}")

    async def scrape_and_save(self):
        scraper = CarScraper(self.url)
        brand_data = await scraper.scrape_brands_and_types()

        for i in range(0, len(brand_data), self.chunk_size):
            chunk = brand_data[i:i + self.chunk_size]
            await self.process_brand_chunk(chunk)

            if i + self.chunk_size < len(brand_data):
                await asyncio.sleep(self.chunk_delay)


if __name__ == "__main__":
    url = "https://www.q84sale.com/ar/automotive/new-cars-1"
    R2_bucket = "data-collection-dl"

    main_scraper = MainScraper(url, R2_bucket)
    asyncio.run(main_scraper.scrape_and_save())