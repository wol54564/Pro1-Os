import boto3
import logging
import os
from pathlib import Path
from datetime import datetime
from typing import Optional
import mimetypes

logger = logging.getLogger(__name__)

# AWS Configuration - Use environment variables for GitHub Actions
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')


class S3Helper:
    """
    Helper class for AWS S3 operations with partition structure
    Partitions data by date: education/year=YYYY/month=MM/day=DD/
    """
    
    def __init__(self, bucket_name: str, profile_name: Optional[str] = None, region_name: str = None):
        """
        Initialize S3 client using AWS access key and secret key
        
        Args:
            bucket_name: S3 bucket name
            profile_name: Deprecated (kept for backward compatibility)
            region_name: AWS region (defaults to us-east-1)
        """
        if region_name is None:
            region_name = AWS_REGION
            
        self.bucket_name = bucket_name
        self.region_name = region_name
        
        try:
            # Create session using access key and secret key from environment variables
            if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
                raise ValueError("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables must be set")
            
            logger.info(f"Connecting to S3 with region: {region_name}, bucket: {bucket_name}")
            session = boto3.Session(
                aws_access_key_id=AWS_ACCESS_KEY,
                aws_secret_access_key=AWS_SECRET_KEY,
                region_name=region_name
            )
            self.s3_client = session.client('s3')
            
            # Test connection with HeadBucket (optional - some IAM roles may not have this permission)
            try:
                self.s3_client.head_bucket(Bucket=self.bucket_name)
                logger.info(f"Successfully verified access to S3 bucket: {self.bucket_name}")
            except self.s3_client.exceptions.NoSuchBucket:
                logger.error(f"Bucket does not exist: {self.bucket_name}")
                raise
            except Exception as e:
                # HeadBucket might fail due to IAM permissions, but we can still proceed
                logger.warning(f"Could not verify bucket access (this may be due to IAM permissions): {e}")
                logger.info(f"Proceeding with S3 client - bucket: {self.bucket_name}, profile: {profile_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {e}")
            raise
    
    def get_partition_prefix(self, target_date: datetime = None) -> str:
        """
        Get S3 partition prefix based on date
        Format: 4sale-data/education/year=YYYY/month=MM/day=DD/
        
        Args:
            target_date: Date to partition by (defaults to today)
        
        Returns:
            Partition prefix string
        """
        if target_date is None:
            target_date = datetime.now()
        
        year = target_date.strftime('%Y')
        month = target_date.strftime('%m')
        day = target_date.strftime('%d')
        
        return f"4sale-data/education/year={year}/month={month}/day={day}"
    
    def upload_file(self, local_file_path: str, s3_filename: str, 
                    target_date: datetime = None, retries: int = 3) -> Optional[str]:
        """
        Upload a file to S3 with automatic partitioning
        
        Args:
            local_file_path: Path to local file
            s3_filename: Filename in S3 (relative path without partition)
            target_date: Date for partitioning (defaults to today)
            retries: Number of retry attempts
        
        Returns:
            Full S3 path or None if failed
        """
        partition = self.get_partition_prefix(target_date)
        s3_key = f"{partition}/{s3_filename}"
        
        for attempt in range(retries):
            try:
                logger.info(f"Uploading to S3: {s3_key} (Attempt {attempt + 1}/{retries})...")
                
                with open(local_file_path, 'rb') as f:
                    self.s3_client.upload_fileobj(
                        f,
                        self.bucket_name,
                        s3_key,
                        ExtraArgs={'ContentType': self._get_content_type(local_file_path)}
                    )
                
                logger.info(f"✓ Successfully uploaded: {s3_key}")
                return s3_key
                
            except Exception as e:
                logger.warning(f"Upload attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    import time
                    time.sleep(2 ** attempt)  # Exponential backoff
        
        logger.error(f"Failed to upload file after {retries} attempts: {local_file_path}")
        return None
    
    def upload_image(self, image_url: str, image_data: bytes, subcategory_slug: str,
                    target_date: datetime = None, listing_id: int = None, 
                    image_index: int = 0, category_name: str = None) -> Optional[str]:
        """
        Upload image data to S3 with organized folder structure
        
        Args:
            image_url: Original image URL
            image_data: Image file bytes
            subcategory_slug: Subcategory slug for folder organization
            target_date: Date for partitioning
            listing_id: ID of the listing
            image_index: Index of the image
            category_name: Parent category name for folder organization (optional)
        
        Returns:
            S3 key path or None if failed
        """
        partition = self.get_partition_prefix(target_date)
        
        # Determine file extension from URL
        extension = 'jpg'
        if '.' in image_url.split('/')[-1]:
            extension = image_url.split('.')[-1]
        
        # Create S3 key with structure: partition/images/category_name/subcategory/listing_id_image_index.ext
        if category_name:
            s3_key = f"{partition}/images/{category_name}/{subcategory_slug}/{listing_id}_{image_index}.{extension}"
        else:
            s3_key = f"{partition}/images/{subcategory_slug}/{listing_id}_{image_index}.{extension}"
        
        try:
            logger.debug(f"Uploading image to S3: {s3_key}...")
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=image_data,
                ContentType=f'image/{extension}'
            )
            
            logger.debug(f"✓ Image uploaded: {s3_key}")
            return s3_key
            
        except Exception as e:
            logger.error(f"Error uploading image to S3: {e}")
            return None
    
    def generate_s3_url(self, s3_key: str) -> str:
        """
        Generate public HTTPS URL for S3 object
        
        Args:
            s3_key: S3 key/path
        
        Returns:
            HTTPS URL to access the object
        """
        return f"https://{self.bucket_name}.s3.{self.region_name}.amazonaws.com/{s3_key}"
    
    def _get_content_type(self, file_path: str) -> str:
        """
        Determine MIME type from file path
        
        Args:
            file_path: Path to file
        
        Returns:
            MIME type string
        """
        mime_type, _ = mimetypes.guess_type(file_path)
        return mime_type or 'application/octet-stream'
