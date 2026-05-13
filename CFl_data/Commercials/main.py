import asyncio
import pandas as pd
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
from json_scraper import CommercialsJsonScraper
from s3_helper import R2Helper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class CommercialsScraperOrchestrator:
    """
    Orchestrates the scraping of commercials data with AWS R2 integration
    
    Features:
    - Scrapes all categories from commercials section
    - Fetches ad details for each category
    - Downloads and uploads images to R2
    - Saves Excel files to R2 with partitioning
    - Partitions by date: 4sale-data/commercials/year=YYYY/month=MM/day=DD/
    """
    
    def __init__(self, bucket_name: str, profile_name: Optional[str] = None, temp_dir: str = "temp_commercials"):
        self.scraper = None
        self.R2_helper = None
        self.bucket_name = bucket_name
        self.profile_name = profile_name
        self.temp_dir = Path(temp_dir)
        self.temp_dir.mkdir(exist_ok=True)
        self.save_date = datetime.now()
        logger.info(f"Saving to R2 with date: {self.save_date.strftime('%Y-%m-%d')}")
        
    async def initialize(self):
        """Initialize the scraper and R2 client"""
        self.scraper = CommercialsJsonScraper()
        
        try:
            self.R2_helper = R2Helper(
                bucket_name=self.bucket_name,
                profile_name=self.profile_name
            )
        except Exception as e:
            logger.error(f"Failed to initialize R2: {e}")
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
    
    async def fetch_ad_details_with_images(self, ads: List[Dict], category_slug: str) -> List[Dict]:
        """
        Fetch detailed information for each ad and download images
        
        Args:
            ads: List of basic ad info from category page
            category_slug: Category slug for organizing images
        
        Returns:
            List of detailed ad information with R2 image paths
        """
        detailed_ads = []
        
        for ad in ads:
            try:
                ad_id = ad.get("id")
                
                if not ad_id:
                    logger.warning("Ad without ID, skipping...")
                    continue
                
                logger.info(f"Fetching details for ad {ad_id}...")
                
                # Get detailed ad information
                details = await self.scraper.get_ad_details(ad_id)
                
                if details:
                    # Download and upload image if available
                    image_url = details.get("image")
                    R2_image_path = None
                    
                    if image_url:
                        logger.info(f"Processing image for ad {ad_id}...")
                        
                        try:
                            # Download image
                            image_data = await self.scraper.download_image(image_url)
                            
                            if image_data:
                                # Upload to R2
                                R2_path = await asyncio.to_thread(
                                    self.R2_helper.upload_image,
                                    image_url,
                                    image_data,
                                    category_slug,
                                    self.save_date,
                                    ad_id,
                                    0
                                )
                                
                                if R2_path:
                                    R2_image_path = self.R2_helper.generate_R2_url(R2_path)
                                    logger.info(f"  [OK] Image uploaded: {R2_image_path}")
                            
                            await asyncio.sleep(0.1)
                            
                        except Exception as e:
                            logger.warning(f"Failed to download/upload image {image_url}: {e}")
                    
                    # Add R2 image path to details
                    details["R2_image_path"] = R2_image_path
                    detailed_ads.append(details)
                    
                else:
                    logger.warning(f"Failed to get details for ad {ad_id}")
                
                await asyncio.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error fetching details for ad: {e}")
                continue
        
        logger.info(f"Successfully fetched {len(detailed_ads)}/{len(ads)} detailed ads")
        return detailed_ads
    
    async def scrape_category(self, category: Dict) -> Dict:
        """
        Scrape all ads from a category
        
        Args:
            category: Category dictionary with id, name, slug, total_pages
        
        Returns:
            Dictionary with category info and scraped ads
        """
        category_slug = category["slug"]
        category_name = category["name"]
        total_pages = category.get("total_pages", 1)
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing Category: {category_name} ({category_slug})")
        logger.info(f"Total Pages: {total_pages}")
        logger.info(f"{'='*60}")
        
        all_ads = []
        
        try:
            # Iterate through all pages
            for page_num in range(1, total_pages + 1):
                logger.info(f"\nFetching page {page_num}/{total_pages} for {category_name}...")
                
                # Get ads from this page
                ads, _ = await self.scraper.get_category_ads(category_slug, page_num)
                
                if not ads:
                    logger.info(f"No ads found on page {page_num}")
                    continue
                
                # Fetch detailed information with images
                logger.info(f"Fetching details for {len(ads)} ads...")
                detailed_ads = await self.fetch_ad_details_with_images(ads, category_slug)
                all_ads.extend(detailed_ads)
                
                logger.info(f"Page {page_num}: Collected {len(detailed_ads)} ads")
                
                await asyncio.sleep(1)  # Rate limiting between pages
            
            logger.info(f"\n[OK] Category {category_name}: Total {len(all_ads)} ads collected")
            
            return {
                "category": category,
                "ads": all_ads,
                "total_ads": len(all_ads)
            }
            
        except Exception as e:
            logger.error(f"Error scraping category {category_name}: {e}")
            return {
                "category": category,
                "ads": all_ads,
                "total_ads": len(all_ads)
            }
    
    def save_category_to_excel(self, category_data: Dict) -> Optional[str]:
        """
        Save category data to Excel file
        
        Args:
            category_data: Dictionary with category info and ads
        
        Returns:
            Path to saved Excel file or None if failed
        """
        try:
            category = category_data["category"]
            ads = category_data["ads"]
            
            if not ads:
                logger.warning(f"No ads to save for category {category['name']}")
                return None
            
            # Create DataFrame
            df = pd.DataFrame(ads)
            
            # Reorder columns for better readability
            column_order = [
                'id', 'title', 'category_slug', 'category_id',
                'phone', 'whatsapp_phone', 'views_count',
                'image', 'R2_image_path', 'target_url', 'open_target_url',
                'is_landing', 'url'
            ]
            
            # Only include columns that exist
            available_columns = [col for col in column_order if col in df.columns]
            remaining_columns = [col for col in df.columns if col not in column_order]
            final_columns = available_columns + remaining_columns
            
            df = df[final_columns]
            
            # Save to Excel
            category_slug = category["slug"]
            filename = f"commercials_{category_slug}.xlsx"
            filepath = self.temp_dir / filename
            
            df.to_excel(filepath, index=False, engine='openpyxl')
            logger.info(f"[OK] Saved {len(ads)} ads to {filepath}")
            
            return str(filepath)
            
        except Exception as e:
            logger.error(f"Error saving Excel file: {e}")
            return None
    
    async def upload_excel_to_R2(self, excel_path: str, category_slug: str) -> Optional[str]:
        """
        Upload Excel file to R2
        
        Args:
            excel_path: Local path to Excel file
            category_slug: Category slug for naming
        
        Returns:
            R2 path or None if failed
        """
        try:
            filename = Path(excel_path).name
            
            R2_path = await asyncio.to_thread(
                self.R2_helper.upload_file,
                excel_path,
                filename,
                self.save_date,
                subfolder='excel files'
            )
            
            if R2_path:
                R2_url = self.R2_helper.generate_R2_url(R2_path)
                logger.info(f"[OK] Uploaded Excel to: {R2_url}")
                return R2_url
            else:
                logger.error(f"Failed to upload Excel file to R2")
                return None
                
        except Exception as e:
            logger.error(f"Error uploading Excel to R2: {e}")
            return None
    
    async def run(self):
        """
        Main execution method - scrapes all categories and uploads to R2
        """
        try:
            await self.initialize()
            
            # Get all categories
            logger.info("\n" + "="*60)
            logger.info("STEP 1: Fetching Categories")
            logger.info("="*60)
            categories = await self.scraper.get_categories()
            
            if not categories:
                logger.error("No categories found. Exiting.")
                return
            
            logger.info(f"Found {len(categories)} categories to scrape")
            
            # Process each category
            results = []
            for idx, category in enumerate(categories, 1):
                logger.info(f"\n{'='*60}")
                logger.info(f"CATEGORY {idx}/{len(categories)}")
                logger.info(f"{'='*60}")
                
                # Scrape category
                category_data = await self.scrape_category(category)
                
                # Save to Excel
                excel_path = self.save_category_to_excel(category_data)
                
                # Upload to R2
                R2_url = None
                if excel_path:
                    R2_url = await self.upload_excel_to_R2(excel_path, category["slug"])
                
                results.append({
                    "category": category["name"],
                    "slug": category["slug"],
                    "total_ads": category_data["total_ads"],
                    "excel_R2_url": R2_url
                })
                
                logger.info(f"\n[OK] Completed: {category['name']} - {category_data['total_ads']} ads")
            
            # Print summary
            logger.info("\n" + "="*60)
            logger.info("SCRAPING COMPLETE - SUMMARY")
            logger.info("="*60)
            
            total_ads = sum(r["total_ads"] for r in results)
            logger.info(f"\nTotal Categories: {len(results)}")
            logger.info(f"Total Ads Scraped: {total_ads}")
            logger.info(f"\nResults by Category:")
            
            for result in results:
                logger.info(f"  - {result['category']}: {result['total_ads']} ads")
                if result['excel_R2_url']:
                    logger.info(f"    R2: {result['excel_R2_url']}")
            
            logger.info(f"\nR2 Partition: {self.R2_helper.get_partition_prefix(self.save_date)}")
            logger.info("="*60)
            
        except Exception as e:
            logger.error(f"Error in main execution: {e}")
            raise
        finally:
            await self.cleanup()


async def main():
    """Entry point for the scraper"""
    
    # Get bucket name from environment variable (for GitHub Actions)
    bucket_name = os.getenv('AWS_R2_BUCKET', 'your-bucket-name')
    
    if bucket_name == 'your-bucket-name':
        logger.warning("Using default bucket name. Set AWS_R2_BUCKET environment variable for production.")
    
    orchestrator = CommercialsScraperOrchestrator(
        bucket_name=bucket_name,
        profile_name=None  # Not used, kept for compatibility
    )
    
    await orchestrator.run()


if __name__ == "__main__":
    asyncio.run(main())
