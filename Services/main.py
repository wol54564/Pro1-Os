import asyncio
import pandas as pd
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional
from json_scraper import ServicesJsonScraper
from s3_helper import S3Helper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ServicesScraperOrchestrator:
    """
    Orchestrates the scraping of services data with AWS S3 integration
    """
    
    def __init__(self, bucket_name: str, profile_name: Optional[str] = None, temp_dir: str = "temp_data"):
        self.scraper = None
        self.s3_helper = None
        self.bucket_name = bucket_name
        self.profile_name = profile_name
        self.temp_dir = Path(temp_dir)
        self.temp_dir.mkdir(exist_ok=True)
        self.scrape_date = datetime.now() - timedelta(days=1)  # Yesterday's data for scraping
        self.save_date = datetime.now()  # Today's date for S3 folder partitioning
        logger.info(f"Scraping data for date: {self.scrape_date.strftime('%Y-%m-%d')}")
        logger.info(f"Saving to S3 with date: {self.save_date.strftime('%Y-%m-%d')}")
        
    async def initialize(self):
        """Initialize the scraper and S3 client"""
        self.scraper = ServicesJsonScraper()
        # No browser initialization needed with BeautifulSoup
        
        try:
            self.s3_helper = S3Helper(
                bucket_name=self.bucket_name,
                profile_name=self.profile_name
            )
        except Exception as e:
            logger.error(f"Failed to initialize S3: {e}")
            raise
        
    async def cleanup(self):
        """Clean up resources"""
        if self.scraper:
            await self.scraper.close_browser()
        
        try:
            import shutil
            if self.temp_dir.exists():
                shutil.rmtree(self.temp_dir)
                logger.info(f"Cleaned up temporary directory: {self.temp_dir}")
        except Exception as e:
            logger.warning(f"Error cleaning up temp directory: {e}")
    
    async def fetch_listing_details_batch(self, listings: List[Dict], subcategory_slug: str) -> List[Dict]:
        """
        Fetch detailed information for each listing from the listings page and download images
        
        Args:
            listings: List of basic listing info from listings page
            subcategory_slug: Category slug for organizing images
        
        Returns:
            List of detailed listing information with S3 image URLs
        """
        detailed_listings = []
        
        for listing in listings:
            try:
                slug = listing.get("slug")
                status = listing.get("status")
                
                if not slug:
                    logger.warning("Listing without slug, skipping...")
                    continue
                
                logger.info(f"Fetching details for {slug}...")
                
                details = await self.scraper.get_listing_details(slug, status=status)
                
                if details:
                    # Download and upload images if available
                    images = details.get("images", [])
                    listing_id = details.get("id")
                    
                    if images:
                        logger.info(f"Processing {len(images)} images for {slug} (ID: {listing_id})...")
                        s3_image_urls = []
                        
                        for img_index, image_url in enumerate(images):
                            try:
                                image_data = await self.scraper.download_image(image_url)
                                if image_data:
                                    s3_path = await asyncio.to_thread(
                                        self.s3_helper.upload_image,
                                        image_url,
                                        image_data,
                                        subcategory_slug,
                                        self.save_date,
                                        listing_id,
                                        img_index
                                    )
                                    if s3_path:
                                        s3_url = self.s3_helper.generate_s3_url(s3_path)
                                        s3_image_urls.append(s3_url)
                                        logger.info(f"  Image {img_index}: {listing_id}_{img_index}.jpg ✓")
                                
                                await asyncio.sleep(0.1)
                            except Exception as e:
                                logger.warning(f"Failed to download/upload image {image_url}: {e}")
                                continue
                        
                        # Add S3 image URLs to details
                        details["s3_images"] = s3_image_urls
                        logger.info(f"Successfully uploaded {len(s3_image_urls)} images")
                    
                    detailed_listings.append(details)
                    logger.debug(f"✓ Retrieved details for {slug}")
                else:
                    logger.warning(f"Failed to get details for {slug}")
                
                await asyncio.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error fetching details for listing: {e}")
                continue
        
        logger.info(f"Successfully fetched {len(detailed_listings)}/{len(listings)} detailed listings")
        return detailed_listings
    
    async def scrape_subcategory_with_districts(self, subcategory: Dict) -> Dict:
        """Scrape a subcategory and all its districts/listings with detailed information"""
        subcat_slug = subcategory["slug"]
        logger.info(f"\nProcessing: {subcategory['name_ar']}")
        
        result = {
            "subcategory": subcategory,
            "districts": [],
            "listings_by_district": {},
            "all_listings": []
        }
        
        try:
            logger.info(f"Getting main listings for {subcategory['name_ar']}...")
            main_listings = await self.scraper.get_listings(
                subcat_slug, 
                page_num=1,
                filter_yesterday=True
            )
            
            if main_listings:
                logger.info(f"Fetching detailed information for {len(main_listings)} listings...")
                detailed_listings = await self.fetch_listing_details_batch(main_listings, subcat_slug)
                result["all_listings"].extend(detailed_listings)
                result["listings_by_district"]["Main"] = detailed_listings
            
            if subcategory.get("has_districts"):
                logger.info(f"Getting districts...")
                districts = await self.scraper.get_districts(subcat_slug)
                result["districts"] = districts
                
                for district in districts:
                    logger.info(f"  Processing district: {district['name_ar']}...")
                    district_slug = district["full_path_en"].lower().replace(" ", "-") + "--district"
                    
                    try:
                        district_listings = await self.scraper.get_listings(
                            subcat_slug,
                            page_num=1,
                            district_slug=district_slug,
                            filter_yesterday=True
                        )
                        
                        if district_listings:
                            logger.info(f"Fetching detailed information for {len(district_listings)} listings in {district['name_ar']}...")
                            detailed_listings = await self.fetch_listing_details_batch(district_listings, subcat_slug)
                            result["all_listings"].extend(detailed_listings)
                            result["listings_by_district"][district["name_ar"]] = detailed_listings
                            logger.info(f"  Found {len(detailed_listings)} detailed listings")
                        
                        await asyncio.sleep(1)
                    except Exception as e:
                        logger.error(f"Error processing district {district['name_ar']}: {e}")
                        continue
            
            logger.info(f"Total detailed listings: {len(result['all_listings'])}")
            return result
            
        except Exception as e:
            logger.error(f"Error processing {subcategory['name_ar']}: {e}")
            return result
    
    async def scrape_all_subcategories(self) -> List[Dict]:
        """Scrape all service subcategories"""
        try:
            logger.info("Fetching all subcategories...")
            subcategories = await self.scraper.get_subcategories()
            
            if not subcategories:
                logger.error("No subcategories found!")
                return []
            
            logger.info(f"Found {len(subcategories)} subcategories")
            
            all_results = []
            for i, subcategory in enumerate(subcategories, 1):
                logger.info(f"[{i}/{len(subcategories)}] Processing...")
                result = await self.scrape_subcategory_with_districts(subcategory)
                all_results.append(result)
                
                if i < len(subcategories):
                    await asyncio.sleep(2)
            
            return all_results
            
        except Exception as e:
            logger.error(f"Error scraping subcategories: {e}")
            return []
    
    async def save_all_to_s3(self, results: List[Dict]) -> Dict:
        """Save all data to S3 with proper partitioning"""
        upload_summary = {
            "excel_files": [],
            "json_files": [],
            "total_listings": 0,
            "upload_time": datetime.now().isoformat()
        }
        
        try:
            total_listings = sum(len(r["all_listings"]) for r in results)
            upload_summary["total_listings"] = total_listings
            
            if total_listings == 0:
                logger.warning("No data to upload!")
                return upload_summary
            
            logger.info("\nUploading to AWS S3...")
            
            for result in results:
                try:
                    subcategory = result["subcategory"]
                    slug = subcategory["slug"]
                    listings_count = len(result["all_listings"])
                    
                    if listings_count > 0:
                        logger.info(f"Creating Excel for {subcategory['name_ar']}...")
                        
                        temp_excel = self.temp_dir / f"{slug}_temp.xlsx"
                        with pd.ExcelWriter(temp_excel, engine='openpyxl') as writer:
                            info_data = [{
                                "Category (Arabic)": subcategory["name_ar"],
                                "Category (English)": subcategory["name_en"],
                                "Total Listings": listings_count,
                                "Districts": len(result["districts"]),
                                "Data Scraped Date": self.scrape_date.strftime('%Y-%m-%d'),
                                "Saved to S3 Date": self.save_date.strftime('%Y-%m-%d'),
                            }]
                            pd.DataFrame(info_data).to_excel(writer, sheet_name='Info', index=False)
                            
                            for district_name, listings in result["listings_by_district"].items():
                                if listings:
                                    sheet_name = district_name[:31] if len(district_name) <= 31 else district_name[:28] + "..."
                                    df = pd.DataFrame(listings)
                                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                        
                        s3_excel_path = await asyncio.to_thread(
                            self.s3_helper.upload_file,
                            str(temp_excel),
                            f"excel-files/{slug}.xlsx",
                            self.save_date,
                            retries=3
                        )
                        if s3_excel_path:
                            s3_url = self.s3_helper.generate_s3_url(s3_excel_path)
                            upload_summary["excel_files"].append({
                                "category": subcategory["name_ar"],
                                "slug": slug,
                                "listings": listings_count,
                                "s3_path": s3_excel_path,
                                "s3_url": s3_url
                            })
                            logger.info(f"✓ Uploaded: {slug}.xlsx ({listings_count} listings)")
                        
                        temp_excel.unlink(missing_ok=True)
                
                except Exception as e:
                    logger.error(f"Error processing {subcategory['name_ar']}: {e}")
                    continue
            
            logger.info("Uploading JSON summary...")
            json_summary = {
                "scraped_at": datetime.now().isoformat(),
                "data_scraped_date": self.scrape_date.strftime('%Y-%m-%d'),
                "saved_to_s3_date": self.save_date.strftime('%Y-%m-%d'),
                "total_subcategories": len(results),
                "total_listings": total_listings,
                "subcategories": []
            }
            
            for result in results:
                if result["all_listings"]:
                    json_summary["subcategories"].append({
                        "name_ar": result["subcategory"]["name_ar"],
                        "name_en": result["subcategory"]["name_en"],
                        "slug": result["subcategory"]["slug"],
                        "listings_count": len(result["all_listings"]),
                        "districts": [d["name_ar"] for d in result["districts"]]
                    })
            
            temp_json = self.temp_dir / f"summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(temp_json, 'w', encoding='utf-8') as f:
                json.dump(json_summary, f, ensure_ascii=False, indent=2)
            
            s3_json_path = await asyncio.to_thread(
                self.s3_helper.upload_file,
                str(temp_json),
                f"json-files/summary_{self.save_date.strftime('%Y%m%d')}.json",
                self.save_date
            )
            
            if s3_json_path:
                upload_summary["json_files"].append(s3_json_path)
                logger.info(f"✓ Uploaded JSON summary")
            
            temp_json.unlink(missing_ok=True)
            
        except Exception as e:
            logger.error(f"Error in S3 upload: {e}")
        
        return upload_summary


async def main():
    """Main entry point for the scraper"""
    orchestrator = None
    
    try:
        bucket_name = os.environ.get("S3_BUCKET_NAME", "data-collection-dl")  # Update with your actual bucket name
        profile_name = os.environ.get("AWS_PROFILE", None)
        
        logger.info("\n" + "="*60)
        logger.info("SERVICES SCRAPER STARTING")
        logger.info("="*60)
        logger.info(f"Bucket: {bucket_name}")
        
        orchestrator = ServicesScraperOrchestrator(bucket_name=bucket_name, profile_name=profile_name)
        await orchestrator.initialize()
        
        logger.info("\nStarting scraping...")
        results = await orchestrator.scrape_all_subcategories()
        
        if results:
            logger.info("\n" + "="*60)
            logger.info("UPLOADING TO S3")
            logger.info("="*60)
            
            upload_summary = await orchestrator.save_all_to_s3(results)
            
            logger.info("\n" + "="*60)
            logger.info("SCRAPING COMPLETED")
            logger.info("="*60)
            logger.info(f"Excel files uploaded: {len(upload_summary['excel_files'])}")
            logger.info(f"Total listings: {upload_summary['total_listings']}")
            
            for excel_file in upload_summary['excel_files']:
                logger.info(f"  - {excel_file['category']}: {excel_file['listings']} listings")
            
        else:
            logger.error("Scraping failed - no results!")
            
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        
    finally:
        if orchestrator:
            await orchestrator.cleanup()


if __name__ == "__main__":
    asyncio.run(main())

