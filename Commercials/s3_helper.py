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
    Partitions data by date: 4sale-data/commercials/year=YYYY/month=MM/day=DD/
    Subfolders: excel files/ and images/
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
                logger.info(f"Proceeding with S3 client - bucket: {self.bucket_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {e}")
            raise
    
    def get_partition_prefix(self, target_date: datetime = None, subfolder: str = None) -> str:
        """
        Get S3 partition prefix based on date
        Format: 4sale-data/commercials/year=YYYY/month=MM/day=DD/[subfolder/]
        
        Args:
            target_date: Date to partition by (defaults to today)
            subfolder: Optional subfolder (e.g., 'excel files' or 'images')
        
        Returns:
            Partition prefix string
        """
        if target_date is None:
            target_date = datetime.now()
        
        year = target_date.strftime('%Y')
        month = target_date.strftime('%m')
        day = target_date.strftime('%d')
        
        prefix = f"4sale-data/commercials/year={year}/month={month}/day={day}"
        
        if subfolder:
            prefix = f"{prefix}/{subfolder}"
        
        return prefix
    
    def upload_file(self, local_file_path: str, s3_filename: str, 
                    target_date: datetime = None, subfolder: str = None, retries: int = 3) -> Optional[str]:
        """
        Upload a file to S3 with automatic partitioning
        
        Args:
            local_file_path: Path to local file
            s3_filename: Filename in S3 (without partition path)
            target_date: Date for partitioning (defaults to today)
            subfolder: Subfolder within partition (e.g., 'excel files' or 'images')
            retries: Number of retry attempts
        
        Returns:
            Full S3 path or None if failed
        """
        for attempt in range(retries):
            try:
                # Get partition prefix
                partition_prefix = self.get_partition_prefix(target_date, subfolder)
                
                # Construct full S3 key
                s3_key = f"{partition_prefix}/{s3_filename}"
                
                # Detect content type
                content_type, _ = mimetypes.guess_type(local_file_path)
                if content_type is None:
                    content_type = 'application/octet-stream'
                
                # Upload file
                logger.info(f"Uploading {local_file_path} to s3://{self.bucket_name}/{s3_key}")
                
                extra_args = {'ContentType': content_type}
                self.s3_client.upload_file(
                    local_file_path,
                    self.bucket_name,
                    s3_key,
                    ExtraArgs=extra_args
                )
                
                logger.info(f"✓ Successfully uploaded to {s3_key}")
                return s3_key
                
            except Exception as e:
                logger.error(f"Attempt {attempt + 1}/{retries} failed to upload {local_file_path}: {e}")
                if attempt == retries - 1:
                    logger.error(f"Failed to upload {local_file_path} after {retries} attempts")
                    return None
        
        return None
    
    def upload_image(self, image_url: str, image_data: bytes, category_slug: str,
                    target_date: datetime = None, ad_id: int = None, img_index: int = 0) -> Optional[str]:
        """
        Upload an image to S3 in the images subfolder organized by category
        
        Args:
            image_url: Original URL of the image
            image_data: Image data as bytes
            category_slug: Category slug for organizing images
            target_date: Date for partitioning (defaults to today)
            ad_id: Ad ID for naming the image
            img_index: Index of the image if multiple
        
        Returns:
            Full S3 path or None if failed
        """
        try:
            # Extract file extension from URL
            ext = '.jpg'
            if '.' in image_url:
                ext = '.' + image_url.rsplit('.', 1)[-1].split('?')[0]
                if ext not in ['.jpg', '.jpeg', '.png', '.gif', '.webp']:
                    ext = '.jpg'
            
            # Create filename
            if ad_id:
                filename = f"{ad_id}_{img_index}{ext}"
            else:
                filename = f"{img_index}{ext}"
            
            # Get partition prefix with images subfolder and category subfolder
            partition_prefix = self.get_partition_prefix(target_date, subfolder=f'images/{category_slug}')
            s3_key = f"{partition_prefix}/{filename}"
            
            # Detect content type
            content_type_map = {
                '.jpg': 'image/jpeg',
                '.jpeg': 'image/jpeg',
                '.png': 'image/png',
                '.gif': 'image/gif',
                '.webp': 'image/webp'
            }
            content_type = content_type_map.get(ext, 'image/jpeg')
            
            # Upload image
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=image_data,
                ContentType=content_type
            )
            
            logger.debug(f"✓ Uploaded image to {s3_key}")
            return s3_key
            
        except Exception as e:
            logger.error(f"Error uploading image: {e}")
            return None
    
    def generate_s3_url(self, s3_key: str) -> str:
        """
        Generate S3 URL for a given key
        
        Args:
            s3_key: S3 key/path
        
        Returns:
            Full S3 URL
        """
        return f"s3://{self.bucket_name}/{s3_key}"
    
    def list_files(self, prefix: str, max_keys: int = 1000) -> list:
        """
        List files in S3 with given prefix
        
        Args:
            prefix: S3 prefix to search
            max_keys: Maximum number of keys to return
        
        Returns:
            List of S3 keys
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            else:
                return []
                
        except Exception as e:
            logger.error(f"Error listing files with prefix {prefix}: {e}")
            return []
    
    def file_exists(self, s3_key: str) -> bool:
        """
        Check if a file exists in S3
        
        Args:
            s3_key: S3 key to check
        
        Returns:
            True if file exists, False otherwise
        """
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except:
            return False
