import asyncio
import pandas as pd
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional
from json_scraper import EducationJsonScraper
from s3_helper import S3Helper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class EducationScraperOrchestrator:
    """
    Orchestrates the scraping of education data with AWS S3 integration
    Handles two types of categories:
    - Case 1: Categories with direct listings (e.g., school-supplies)
    - Case 2: Categories with child categories (e.g., languages with arabic-teaching, english-teaching, etc.)
    Creates one Excel file per subcategory with sheets for child categories
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
        logger.info("Mode: Scrape YESTERDAY data only")
        logger.info(f"Saving to S3 with date: {self.save_date.strftime('%Y-%m-%d')}")
        logger.info("Mode: Scrape ALL available pages per category")
        
    async def initialize(self):
        """Initialize the scraper and S3 client"""
        self.scraper = EducationJsonScraper()
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
    
    async def fetch_listing_details_batch(self, listings: List[Dict], subcategory_slug: str, category_name: str = None) -> List[Dict]:
        """
        Fetch detailed information for each listing from the listings page and download images
        
        Args:
            listings: List of basic listing info from listings page
            subcategory_slug: Category slug for organizing images
            category_name: Parent category name for organizing images in S3 (optional)
        
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
                    
                    logger.info(f"Listing {slug} (ID: {listing_id}): {len(images)} images available")
                    
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
                                        img_index,
                                        category_name
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
                        logger.info(f"Successfully uploaded {len(s3_image_urls)}/{len(images)} images")
                    else:
                        logger.warning(f"No images found for listing {slug}")
                        details["s3_images"] = []
                    
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
    
    async def scrape_child_category(self, parent_slug: str, parent_subcategory: Dict, child_category: Dict) -> Dict:
        """
        Scrape a child category with all its listings and detailed information
        
        Args:
            parent_slug: Parent category slug
            parent_subcategory: Parent subcategory object (for getting folder name)
            child_category: Child category dictionary with slug, name_ar, name_en, etc.
            
        Returns:
            Dictionary with scraped data for the child category
        """
        child_slug = child_category["slug"]
        logger.info(f"\n  Processing child category: {child_category['name_ar']} ({child_slug})")
        
        result = {
            "child_category": child_category,
            "listings": [],
            "total_pages": 0
        }
        
        try:
            # Fetch all pages for this child category
            page_num = 1
            child_listings = []
            total_pages = 0
            
            while True:
                # Build category slug path: parent/child
                category_slug = f"{parent_slug}/{child_slug}"
                
                listings, total_pages = await self.scraper.get_listings(
                    category_slug, 
                    page_num=page_num,
                    filter_yesterday=True
                )
                
                if not listings:
                    logger.info(f"    No listings found on page {page_num}, stopping pagination")
                    break
                
                logger.info(f"    Fetching details for {len(listings)} listings on page {page_num}/{total_pages}...")
                detailed_listings = await self.fetch_listing_details_batch(listings, child_slug, parent_subcategory["slug"])
                child_listings.extend(detailed_listings)
                
                page_num += 1
                
                # Stop if we've reached the total pages
                if page_num > total_pages:
                    logger.info(f"    Reached total pages ({total_pages})")
                    break
                
                await asyncio.sleep(1)  # Rate limiting between pages
            
            result["listings"] = child_listings
            result["total_pages"] = total_pages
            
            logger.info(f"    Total listings for {child_category['name_ar']}: {len(child_listings)} (across {page_num - 1} pages)")
            return result
            
        except Exception as e:
            logger.error(f"    Error processing {child_category['name_ar']}: {e}")
            return result
    
    async def scrape_subcategory(self, subcategory: Dict) -> Dict:
        """
        Scrape a subcategory - handles both cases:
        Case 1: Direct listings (no child categories)
        Case 2: Child categories (catChilds)
        
        Args:
            subcategory: Subcategory dictionary with slug, name_ar, name_en, etc.
            
        Returns:
            Dictionary with scraped data organized by subcategory
        """
        subcat_slug = subcategory["slug"]
        logger.info(f"\nProcessing: {subcategory['name_ar']} ({subcat_slug})")
        
        result = {
            "subcategory": subcategory,
            "children": [],  # Child categories (if any)
            "listings": [],  # Direct listings (if no children)
            "total_pages": 0,
            "has_children": False
        }
        
        try:
            # Check if this category has child categories
            child_categories = await self.scraper.get_child_categories(subcat_slug)
            
            if child_categories:
                # Case 2: Category has child categories
                logger.info(f"Found {len(child_categories)} child categories")
                result["has_children"] = True
                
                # Scrape each child category
                all_children_results = []
                for i, child_cat in enumerate(child_categories, 1):
                    logger.info(f"[{i}/{len(child_categories)}]")
                    child_result = await self.scrape_child_category(subcat_slug, subcategory, child_cat)
                    all_children_results.append(child_result)
                    
                    if len(child_categories) > i:
                        await asyncio.sleep(1)  # Rate limiting between child categories
                
                result["children"] = all_children_results
                
            else:
                # Case 1: Direct listings (no child categories)
                logger.info("No child categories found - scraping direct listings")
                
                page_num = 1
                subcat_listings = []
                total_pages = 0
                
                while True:
                    listings, total_pages = await self.scraper.get_listings(
                        subcat_slug, 
                        page_num=page_num,
                        filter_yesterday=True
                    )
                    
                    if not listings:
                        logger.info(f"No listings found on page {page_num}, stopping pagination")
                        break
                    
                    logger.info(f"Fetching details for {len(listings)} listings on page {page_num}/{total_pages}...")
                    detailed_listings = await self.fetch_listing_details_batch(listings, subcat_slug, None)
                    subcat_listings.extend(detailed_listings)
                    
                    page_num += 1
                    
                    # Stop if we've reached the total pages
                    if page_num > total_pages:
                        logger.info(f"Reached total pages ({total_pages})")
                        break
                    
                    await asyncio.sleep(1)  # Rate limiting between pages
                
                result["listings"] = subcat_listings
                result["total_pages"] = total_pages
            
            logger.info(f"✓ Completed: {subcategory['name_ar']}")
            return result
            
        except Exception as e:
            logger.error(f"Error processing {subcategory['name_ar']}: {e}")
            return result
    
    async def scrape_all_subcategories(self) -> List[Dict]:
        """
        Scrape all education vertical subcategories from the main page
        Automatically discovers and scrapes all available pages for each category
        """
        try:
            logger.info("Fetching education vertical subcategories...")
            subcategories = await self.scraper.get_vertical_subcategories()
            
            if not subcategories:
                logger.error("No subcategories found!")
                return []
            
            logger.info(f"Found {len(subcategories)} vertical subcategories")
            
            all_results = []
            for i, subcat in enumerate(subcategories, 1):
                logger.info(f"\n[{i}/{len(subcategories)}] Processing: {subcat['name_ar']} ({subcat['slug']})")
                
                result = await self.scrape_subcategory(subcat)
                all_results.append(result)
                
                if len(subcategories) > i:
                    await asyncio.sleep(2)  # Rate limiting between subcategories
            
            return all_results
            
        except Exception as e:
            logger.error(f"Error scraping subcategories: {e}")
            return []
    
    async def save_all_to_s3(self, results: List[Dict]) -> Dict:
        """
        Save all data to S3 with proper partitioning
        Creates one Excel file per subcategory with sheets for child categories (if any)
        """
        upload_summary = {
            "excel_files": [],
            "json_files": [],
            "total_listings": 0,
            "upload_time": datetime.now().isoformat()
        }
        
        try:
            # Calculate total listings across all categories
            total_listings = 0
            for result in results:
                if result["has_children"]:
                    for child in result["children"]:
                        total_listings += len(child["listings"])
                else:
                    total_listings += len(result["listings"])
            
            upload_summary["total_listings"] = total_listings
            
            if total_listings == 0:
                logger.warning("No data to upload!")
                return upload_summary
            
            logger.info("\nUploading to AWS S3...")
            
            # Create one Excel file per subcategory
            for result in results:
                if (result["has_children"] and result["children"]) or (not result["has_children"] and result["listings"]):
                    subcategory = result["subcategory"]
                    logger.info(f"Creating Excel file for: {subcategory['name_ar']}...")
                    
                    # Sanitize filename
                    safe_filename = subcategory["slug"].replace("/", "_")
                    temp_excel = self.temp_dir / f"{safe_filename}_temp.xlsx"
                    
                    with pd.ExcelWriter(temp_excel, engine='openpyxl') as writer:
                        # Create Info sheet with summary
                        if result["has_children"]:
                            child_count = len(result["children"])
                            child_listings_count = sum(len(child["listings"]) for child in result["children"])
                            info_data = [{
                                "Project": "Education",
                                "Subcategory": subcategory['name_ar'],
                                "Subcategory (EN)": subcategory['name_en'],
                                "Type": "Has Child Categories",
                                "Child Categories Count": child_count,
                                "Total Listings": child_listings_count,
                                "Data Scraped Date": self.scrape_date.strftime('%Y-%m-%d'),
                                "Saved to S3 Date": self.save_date.strftime('%Y-%m-%d'),
                            }]
                        else:
                            info_data = [{
                                "Project": "Education",
                                "Subcategory": subcategory['name_ar'],
                                "Subcategory (EN)": subcategory['name_en'],
                                "Type": "Direct Listings",
                                "Total Listings": len(result["listings"]),
                                "Total Pages": result["total_pages"],
                                "Data Scraped Date": self.scrape_date.strftime('%Y-%m-%d'),
                                "Saved to S3 Date": self.save_date.strftime('%Y-%m-%d'),
                            }]
                        
                        pd.DataFrame(info_data).to_excel(writer, sheet_name='Info', index=False)
                        
                        if result["has_children"]:
                            # Create a sheet for each child category
                            for child_result in result["children"]:
                                if child_result["listings"]:
                                    child_cat = child_result["child_category"]
                                    # Sanitize sheet name (max 31 chars in Excel)
                                    sheet_name = child_cat["name_ar"][:31] if len(child_cat["name_ar"]) <= 31 else child_cat["name_ar"][:28] + "..."
                                    
                                    df = pd.DataFrame(child_result["listings"])
                                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                                    logger.info(f"  Created sheet: {sheet_name} ({len(child_result['listings'])} listings)")
                        else:
                            # Create a single sheet for direct listings
                            if result["listings"]:
                                df = pd.DataFrame(result["listings"])
                                df.to_excel(writer, sheet_name="Listings", index=False)
                                logger.info(f"  Created sheet: Listings ({len(result['listings'])} listings)")
                    
                    # Upload Excel to S3
                    excel_filename = f"{safe_filename}.xlsx"
                    s3_excel_path = await asyncio.to_thread(
                        self.s3_helper.upload_file,
                        str(temp_excel),
                        f"excel-files/{excel_filename}",
                        self.save_date,
                        retries=3
                    )
                    
                    if s3_excel_path:
                        s3_url = self.s3_helper.generate_s3_url(s3_excel_path)
                        if result["has_children"]:
                            listings_count = sum(len(child["listings"]) for child in result["children"])
                        else:
                            listings_count = len(result["listings"])
                        
                        upload_summary["excel_files"].append({
                            "name": safe_filename,
                            "category_ar": subcategory['name_ar'],
                            "category_en": subcategory['name_en'],
                            "has_children": result["has_children"],
                            "children_count": len(result["children"]) if result["has_children"] else 0,
                            "total_listings": listings_count,
                            "s3_path": s3_excel_path,
                            "s3_url": s3_url
                        })
                        logger.info(f"✓ Uploaded: {excel_filename} ({listings_count} listings)")
                    
                    temp_excel.unlink(missing_ok=True)
            
            # Upload JSON summary
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
                if (result["has_children"] and result["children"]) or (not result["has_children"] and result["listings"]):
                    subcategory = result["subcategory"]
                    
                    if result["has_children"]:
                        listings_count = sum(len(child["listings"]) for child in result["children"])
                        children_info = [
                            {
                                "name_ar": child["child_category"]["name_ar"],
                                "name_en": child["child_category"]["name_en"],
                                "slug": child["child_category"]["slug"],
                                "listings_count": len(child["listings"]),
                                "total_pages_scraped": child["total_pages"],
                            }
                            for child in result["children"]
                        ]
                    else:
                        listings_count = len(result["listings"])
                        children_info = None
                    
                    json_summary["subcategories"].append({
                        "name_ar": subcategory["name_ar"],
                        "name_en": subcategory["name_en"],
                        "slug": subcategory["slug"],
                        "has_children": result["has_children"],
                        "listings_count": listings_count,
                        "total_pages_scraped": result.get("total_pages", 0),
                        "children": children_info,
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
    """Main entry point for the education scraper"""
    orchestrator = None
    
    try:
        bucket_name = os.environ.get("S3_BUCKET_NAME")
        profile_name = os.environ.get("AWS_PROFILE", None)
        
        logger.info("\n" + "="*60)
        logger.info("EDUCATION SCRAPER STARTING")
        logger.info("="*60)
        logger.info(f"Bucket: {bucket_name}")
        logger.info(f"Mode: Scrape ALL available pages per category")
        
        orchestrator = EducationScraperOrchestrator(bucket_name=bucket_name, profile_name=profile_name)
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
                logger.info(f"  - {excel_file['category_ar']}: {excel_file['total_listings']} listings" +
                           (f" ({excel_file['children_count']} child categories)" if excel_file['has_children'] else ""))
            
        else:
            logger.error("Scraping failed - no results!")
            
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        
    finally:
        if orchestrator:
            await orchestrator.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
