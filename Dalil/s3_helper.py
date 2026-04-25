import boto3
import logging
import os
from pathlib import Path
from datetime import datetime
from typing import Optional, List
import mimetypes
import requests
import asyncio
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# AWS Configuration - Use environment variables for GitHub Actions
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')


class S3Helper:
    """
    Helper class for AWS S3 operations with partition structure
    Partitions data by date: 4sale-data/Dalil/year=YYYY/month=MM/day=DD/
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
    
    def get_partition_prefix(self, target_date: datetime = None) -> str:
        """
        Get S3 partition prefix based on date
        Format: 4sale-data/Dalil/year=YYYY/month=MM/day=DD/
        
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
        
        return f"4sale-data/Dalil/year={year}/month={month}/day={day}"
    
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
                local_path = Path(local_file_path)
                if not local_path.exists():
                    logger.error(f"Local file not found: {local_file_path}")
                    return None
                
                # Determine content type
                content_type, _ = mimetypes.guess_type(local_file_path)
                if content_type is None:
                    content_type = "application/octet-stream"
                
                logger.info(f"Uploading to S3 (attempt {attempt + 1}/{retries}): s3://{self.bucket_name}/{s3_key}")
                
                self.s3_client.upload_file(
                    local_file_path,
                    self.bucket_name,
                    s3_key,
                    ExtraArgs={'ContentType': content_type}
                )
                
                logger.info(f"Successfully uploaded: {s3_key}")
                return s3_key
                
            except Exception as e:
                logger.warning(f"Upload attempt {attempt + 1} failed: {e}")
                if attempt == retries - 1:
                    logger.error(f"Failed to upload after {retries} attempts: {local_file_path}")
                    return None
        
        return None
    
    def upload_file_obj(self, file_obj, s3_filename: str, 
                       target_date: datetime = None, retries: int = 3) -> Optional[str]:
        """
        Upload a file object to S3 with automatic partitioning
        
        Args:
            file_obj: File-like object
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
                # Determine content type
                content_type = "application/octet-stream"
                if hasattr(file_obj, 'name'):
                    content_type, _ = mimetypes.guess_type(file_obj.name)
                    if content_type is None:
                        content_type = "application/octet-stream"
                
                logger.info(f"Uploading file object to S3 (attempt {attempt + 1}/{retries}): {s3_key}")
                
                self.s3_client.upload_fileobj(
                    file_obj,
                    self.bucket_name,
                    s3_key,
                    ExtraArgs={'ContentType': content_type}
                )
                
                logger.info(f"Successfully uploaded: {s3_key}")
                return s3_key
                
            except Exception as e:
                logger.warning(f"Upload attempt {attempt + 1} failed: {e}")
                if attempt == retries - 1:
                    logger.error(f"Failed to upload after {retries} attempts")
                    return None
        
        return None
    
    async def download_and_upload_image(self, image_url: str, s3_path: str, 
                                       target_date: datetime = None, 
                                       retries: int = 3) -> Optional[str]:
        """
        Download an image from URL and upload to S3
        
        Args:
            image_url: URL of the image to download
            s3_path: S3 path (relative, without partition)
            target_date: Date for partitioning
            retries: Number of retry attempts
        
        Returns:
            Full S3 path or None if failed
        """
        partition = self.get_partition_prefix(target_date)
        s3_key = f"{partition}/{s3_path}"
        
        for attempt in range(retries):
            try:
                # Download image
                response = requests.get(image_url, timeout=30, stream=True)
                response.raise_for_status()
                
                # Determine content type from response or URL
                content_type = response.headers.get('Content-Type', 'image/jpeg')
                
                # Upload to S3
                self.s3_client.upload_fileobj(
                    response.raw,
                    self.bucket_name,
                    s3_key,
                    ExtraArgs={'ContentType': content_type}
                )
                
                logger.debug(f"Uploaded image to S3: {s3_key}")
                return s3_key
                
            except Exception as e:
                logger.warning(f"Image upload attempt {attempt + 1} failed for {image_url}: {e}")
                if attempt == retries - 1:
                    logger.error(f"Failed to upload image after {retries} attempts: {image_url}")
                    return None
                
                await asyncio.sleep(1)  # Wait before retry
        
        return None
    
    def list_files(self, prefix: str = "", max_keys: int = 1000) -> List[str]:
        """
        List files in S3 bucket with optional prefix
        
        Args:
            prefix: S3 prefix to filter by
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
            
            if 'Contents' not in response:
                return []
            
            return [obj['Key'] for obj in response['Contents']]
            
        except Exception as e:
            logger.error(f"Error listing files with prefix {prefix}: {e}")
            return []
    
    def file_exists(self, s3_key: str) -> bool:
        """
        Check if a file exists in S3
        
        Args:
            s3_key: Full S3 key
        
        Returns:
            True if file exists, False otherwise
        """
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except:
            return False
    
    def delete_file(self, s3_key: str) -> bool:
        """
        Delete a file from S3
        
        Args:
            s3_key: Full S3 key
        
        Returns:
            True if successful, False otherwise
        """
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
            logger.info(f"Deleted: {s3_key}")
            return True
        except Exception as e:
            logger.error(f"Error deleting {s3_key}: {e}")
            return False
    
    def generate_presigned_url(self, s3_key: str, expiration: int = 3600) -> Optional[str]:
        """
        Generate a presigned URL for an S3 object
        
        Args:
            s3_key: Full S3 key
            expiration: URL expiration time in seconds (default: 1 hour)
        
        Returns:
            Presigned URL or None if failed
        """
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket_name, 'Key': s3_key},
                ExpiresIn=expiration
            )
            return url
        except Exception as e:
            logger.error(f"Error generating presigned URL for {s3_key}: {e}")
            return None
    
    def generate_s3_url(self, s3_key: str) -> str:
        """
        Generate a standard S3 URL (not presigned)
        
        Args:
            s3_key: Full S3 key
        
        Returns:
            S3 URL
        """
        return f"s3://{self.bucket_name}/{s3_key}"
    
    def get_file_size(self, s3_key: str) -> Optional[int]:
        """
        Get file size in bytes
        
        Args:
            s3_key: Full S3 key
        
        Returns:
            File size in bytes or None if failed
        """
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return response['ContentLength']
        except Exception as e:
            logger.error(f"Error getting file size for {s3_key}: {e}")
            return None
