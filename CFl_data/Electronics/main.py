import asyncio
import pandas as pd
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional
from json_scraper import ElectronicsJsonScraper, ElectronicsCategoryStructure
from s3_helper import S3Helper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ElectronicsScraperOrchestrator:
    """
    Orchestrates the scraping of electronics data with AWS S3 integration
    Handles three category structure types:
    - Case 1: Categories with catChilds (mobile phones, etc.)
    - Case 2: Categories with subcategories (cameras, etc.)
    - Case 3: Categories with direct listings (no children)
    """
    
    def __init__(self, bucket_name: str, profile_name: Optional[str] = None, temp_dir: str = "temp_data"):
        self.scraper = None
        self.s3_helper = None
        self.bucket_name = bucket_name
        self.profile_name = profile_name
        self.temp_dir = Path(temp_dir)
        self.temp_dir.mkdir(exist_ok=True)
        self.save_date = datetime.now()
        logger.info(f"Saving to S3 with date: {self.save_date.strftime('%Y-%m-%d')}")
        logger.info("Mode: Scrape ALL available pages (no limit)")
        
    async def initialize(self):
        """Initialize the scraper and S3 client"""
        self.scraper = ElectronicsJsonScraper()
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
    
    async def fetch_listing_details_batch(self, listings: List[Dict], category_slug: str) -> List[Dict]:
        """
        Fetch detailed information for each listing from the listings page and download images
        
        Args:
            listings: List of basic listing info from listings page
            category_slug: Category slug for organizing images
        
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
                                        category_slug,
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
    
    async def scrape_child_category(self, child: Dict, parent_slug: str) -> Dict:
        """
        Scrape a child category (catChild or subcategory)
        
        Args:
            child: Child category dictionary
            parent_slug: Parent category slug
        
        Returns:
            Dictionary with scraped data
        """
        child_slug = child.get("slug")
        child_name_ar = child.get("name_ar")
        
        logger.info(f"  Processing child: {child_name_ar}")
        
        result = {
            "child": child,
            "listings": [],
            "total_pages": 0
        }
        
        try:
            page_num = 1
            child_listings = []
            total_pages = 0
            
            # Build the correct URL based on the slug_url pattern
            # For mobile-phones-and-accessories/iphone-2285, extract the last part
            slug_url = child.get("slug_url", "")
            
            # Construct the full category path
            if slug_url:
                # Extract everything after "electronics/" from slug_url
                path_parts = slug_url.replace("electronics/", "").rsplit("/1", 1)[0]
                category_path = path_parts
            else:
                category_path = f"{parent_slug}/{child_slug}"
            
            while True:
                listings, total_pages = await self.scraper.get_listings(
                    category_path,
                    page_num=page_num,
                    filter_yesterday=True
                )
                
                if not listings:
                    logger.info(f"No listings found on page {page_num}, stopping pagination")
                    break
                
                logger.info(f"Fetching detailed information for {len(listings)} listings on page {page_num}/{total_pages}...")
                detailed_listings = await self.fetch_listing_details_batch(listings, category_path)
                child_listings.extend(detailed_listings)
                
                page_num += 1
                
                if page_num > total_pages:
                    logger.info(f"Reached total pages ({total_pages})")
                    break
                
                await asyncio.sleep(1)
            
            result["listings"] = child_listings
            result["total_pages"] = total_pages
            
            logger.info(f"Total listings for {child_name_ar}: {len(child_listings)} (across {page_num - 1} pages)")
            return result
            
        except Exception as e:
            logger.error(f"Error processing child category {child_name_ar}: {e}")
            return result
    
    async def scrape_main_category(self, main_category: Dict) -> Dict:
        """
        Scrape a main category with automatic structure detection
        Handles catChilds, subcategories, or direct listings
        
        Args:
            main_category: Main category dictionary
            
        Returns:
            Dictionary with scraped data organized by structure type
        """
        main_slug = main_category["slug"]
        logger.info(f"\nProcessing: {main_category['name_ar']} ({main_slug})")
        
        result = {
            "main_category": main_category,
            "structure_type": None,
            "children": [],
            "total_listings": 0
        }
        
        try:
            # Determine category structure
            structure_type, children = await self.scraper.get_category_structure(main_slug)
            result["structure_type"] = structure_type
            
            if structure_type == ElectronicsCategoryStructure.CASE_3:
                # Case 3: Direct listings - no children to process
                logger.info(f"{main_slug}: Scraping direct listings...")
                
                page_num = 1
                main_listings = []
                total_pages = 0
                
                while True:
                    listings, total_pages = await self.scraper.get_listings(main_slug, page_num=page_num, filter_yesterday=True)
                    
                    if not listings:
                        break
                    
                    logger.info(f"Fetching details for {len(listings)} listings on page {page_num}/{total_pages}...")
                    detailed_listings = await self.fetch_listing_details_batch(listings, main_slug)
                    main_listings.extend(detailed_listings)
                    
                    page_num += 1
                    if page_num > total_pages:
                        break
                    
                    await asyncio.sleep(1)
                
                # For Case 3, create a single entry with all listings
                result["children"] = [{
                    "child": main_category,
                    "listings": main_listings,
                    "total_pages": total_pages
                }]
                result["total_listings"] = len(main_listings)
                
            else:
                # Case 1 or 2: Has children to process
                children_count = len(children)
                logger.info(f"{main_slug}: Processing {children_count} children...")
                
                for i, child in enumerate(children, 1):
                    logger.info(f"[{i}/{children_count}] {child.get('name_ar')}")
                    
                    child_result = await self.scrape_child_category(child, main_slug)
                    result["children"].append(child_result)
                    result["total_listings"] += len(child_result["listings"])
                    
                    if children_count > i:
                        await asyncio.sleep(1)
            
            logger.info(f"Total listings for {main_category['name_ar']}: {result['total_listings']}")
            return result
            
        except Exception as e:
            logger.error(f"Error processing {main_category['name_ar']}: {e}")
            return result
    
    async def scrape_all_main_categories(self) -> List[Dict]:
        """
        Scrape all main electronics categories
        Automatically discovers and scrapes all subcategories
        """
        try:
            logger.info("Fetching electronics main categories...")
            main_categories = await self.scraper.get_main_subcategories()
            
            if not main_categories:
                logger.error("No main categories found!")
                return []
            
            logger.info(f"Found {len(main_categories)} main categories")
            
            all_results = []
            for i, main_cat in enumerate(main_categories, 1):
                logger.info(f"\n[{i}/{len(main_categories)}] Processing: {main_cat['name_ar']} ({main_cat['slug']})")
                
                result = await self.scrape_main_category(main_cat)
                all_results.append(result)
                
                if len(main_categories) > i:
                    await asyncio.sleep(2)
            
            return all_results
            
        except Exception as e:
            logger.error(f"Error scraping main categories: {e}")
            return []
    
    async def save_all_to_s3(self, results: List[Dict]) -> Dict:
        """
        Save all data to S3 with proper partitioning
        Creates Excel files named after main subcategories
        If category has children, creates sheets for each child
        """
        upload_summary = {
            "excel_files": [],
            "json_files": [],
            "total_listings": 0,
            "upload_time": datetime.now().isoformat()
        }
        
        try:
            total_listings = sum(r.get("total_listings", 0) for r in results)
            upload_summary["total_listings"] = total_listings
            
            if total_listings == 0:
                logger.warning("No data to upload!")
                return upload_summary
            
            logger.info("\nUploading to AWS S3...")
            
            # Create separate Excel files for each main category
            for result in results:
                main_category = result.get("main_category", {})
                main_cat_name = main_category.get("name_ar", "unknown")
                main_cat_slug = main_category.get("slug", "unknown")
                
                if result.get("total_listings", 0) == 0:
                    logger.info(f"Skipping {main_cat_name} - no listings")
                    continue
                
                logger.info(f"Creating Excel file for {main_cat_name}...")
                
                temp_excel = self.temp_dir / f"{main_cat_slug}_temp.xlsx"
                
                with pd.ExcelWriter(temp_excel, engine='openpyxl') as writer:
                    # Create Info sheet
                    info_data = [{
                        "Category": main_cat_name,
                        "Structure Type": result.get("structure_type"),
                        "Total Children/Sheets": len(result.get("children", [])),
                        "Total Listings": result.get("total_listings"),
                        "Saved to S3 Date": self.save_date.strftime('%Y-%m-%d'),
                    }]
                    pd.DataFrame(info_data).to_excel(writer, sheet_name='Info', index=False)
                    
                    # Create sheets for each child or for direct listings
                    for child_result in result.get("children", []):
                        child = child_result.get("child", {})
                        listings = child_result.get("listings", [])
                        
                        if listings:
                            child_name = child.get("name_ar", "Unknown")
                            # Sanitize sheet name: remove invalid Excel characters and limit to 31 chars
                            invalid_chars = ['/', '\\', '?', '*', '[', ']', ':']
                            sanitized_name = child_name
                            for char in invalid_chars:
                                sanitized_name = sanitized_name.replace(char, ' ')
                            sheet_name = sanitized_name[:31] if len(sanitized_name) <= 31 else sanitized_name[:28] + "..."
                            
                            df = pd.DataFrame(listings)
                            df.to_excel(writer, sheet_name=sheet_name, index=False)
                            logger.info(f"  Created sheet: {sheet_name} ({len(listings)} listings)")
                
                # Upload Excel to S3
                s3_excel_path = await asyncio.to_thread(
                    self.s3_helper.upload_file,
                    str(temp_excel),
                    f"excel-files/{main_cat_slug}.xlsx",
                    self.save_date,
                    retries=3
                )
                
                if s3_excel_path:
                    s3_url = self.s3_helper.generate_s3_url(s3_excel_path)
                    upload_summary["excel_files"].append({
                        "name": main_cat_slug,
                        "category_ar": main_cat_name,
                        "sheets_count": len(result.get("children", [])),
                        "total_listings": result.get("total_listings"),
                        "s3_path": s3_excel_path,
                        "s3_url": s3_url
                    })
                    logger.info(f"✓ Uploaded: {main_cat_slug}.xlsx ({result.get('total_listings')} listings across {len(result.get('children', []))} sheets)")
                
                temp_excel.unlink(missing_ok=True)
            
            # Upload JSON summary
            logger.info("Uploading JSON summary...")
            json_summary = {
                "scraped_at": datetime.now().isoformat(),
                "saved_to_s3_date": self.save_date.strftime('%Y-%m-%d'),
                "total_main_categories": len(results),
                "total_listings": total_listings,
                "main_categories": []
            }
            
            for result in results:
                if result.get("total_listings", 0) > 0:
                    main_category = result.get("main_category", {})
                    json_summary["main_categories"].append({
                        "name_ar": main_category.get("name_ar"),
                        "name_en": main_category.get("name_en"),
                        "slug": main_category.get("slug"),
                        "structure_type": result.get("structure_type"),
                        "children_count": len(result.get("children", [])),
                        "total_listings": result.get("total_listings"),
                    })
            
            temp_json = self.temp_dir / f"summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(temp_json, 'w', encoding='utf-8') as f:
                json.dump(json_summary, f, ensure_ascii=False, indent=2)
            
            s3_json_path = await asyncio.to_thread(
                self.s3_helper.upload_file,
                str(temp_json),
                f"json-files/electronics_summary_{self.save_date.strftime('%Y%m%d')}.json",
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
    """Main entry point for the electronics scraper"""
    orchestrator = None
    
    try:
        bucket_name = os.environ.get("S3_BUCKET_NAME")
        profile_name = os.environ.get("AWS_PROFILE", None)
        
        logger.info("\n" + "="*60)
        logger.info("ELECTRONICS SCRAPER STARTING")
        logger.info("="*60)
        logger.info(f"Bucket: {bucket_name}")
        logger.info(f"Mode: Scrape ALL available pages per category")
        
        orchestrator = ElectronicsScraperOrchestrator(bucket_name=bucket_name, profile_name=profile_name)
        await orchestrator.initialize()
        
        logger.info("\nStarting scraping...")
        results = await orchestrator.scrape_all_main_categories()
        
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
                logger.info(f"  - {excel_file['name']}: {excel_file['total_listings']} listings ({excel_file['sheets_count']} sheets)")
            
        else:
            logger.error("Scraping failed - no results!")
            
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        
    finally:
        if orchestrator:
            await orchestrator.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
