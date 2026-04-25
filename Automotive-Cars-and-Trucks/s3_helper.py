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
    Partitions data by date: automative-cars-and-trucks/year=YYYY/month=MM/day=DD/
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
        Format: automative-cars-and-trucks/year=YYYY/month=MM/day=DD/
        
        Args:
            target_date: Date to partition by (defaults to yesterday)
        
        Returns:
            Partition prefix string
        """
        if target_date is None:
            from datetime import timedelta
            target_date = datetime.now() - timedelta(days=1)
        
        year = target_date.strftime('%Y')
        month = target_date.strftime('%m')
        day = target_date.strftime('%d')
        
        return f"4sale-data/automative-cars-and-trucks/year={year}/month={month}/day={day}"
    
    def upload_file(self, local_file_path: str, s3_filename: str, 
                    target_date: datetime = None, retries: int = 3) -> Optional[str]:
        """
        Upload a file to S3 with automatic partitioning
        
        Args:
            local_file_path: Path to local file
            s3_filename: Filename in S3 (relative path without partition)
            target_date: Date for partitioning (defaults to yesterday)
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
            target_date: Date for partitioning (defaults to yesterday)
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
                
                logger.info(f"Uploading to S3 (attempt {attempt + 1}/{retries}): s3://{self.bucket_name}/{s3_key}")
                
                # Reset file pointer
                if hasattr(file_obj, 'seek'):
                    file_obj.seek(0)
                
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
                    logger.error(f"Failed to upload after {retries} attempts: {s3_filename}")
                    return None
        
        return None
    
    def upload_json_data(self, data: dict, s3_filename: str, 
                        target_date: datetime = None, retries: int = 3) -> Optional[str]:
        """
        Upload JSON data directly to S3
        
        Args:
            data: Dictionary to upload as JSON
            s3_filename: Filename in S3 (relative path without partition)
            target_date: Date for partitioning (defaults to yesterday)
            retries: Number of retry attempts
        
        Returns:
            Full S3 path or None if failed
        """
        import json
        from io import BytesIO
        
        partition = self.get_partition_prefix(target_date)
        s3_key = f"{partition}/{s3_filename}"
        
        for attempt in range(retries):
            try:
                json_data = json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8')
                file_obj = BytesIO(json_data)
                
                logger.info(f"Uploading JSON to S3 (attempt {attempt + 1}/{retries}): s3://{self.bucket_name}/{s3_key}")
                
                self.s3_client.upload_fileobj(
                    file_obj,
                    self.bucket_name,
                    s3_key,
                    ExtraArgs={'ContentType': 'application/json'}
                )
                
                logger.info(f"Successfully uploaded: {s3_key}")
                return s3_key
                
            except Exception as e:
                logger.warning(f"Upload attempt {attempt + 1} failed: {e}")
                if attempt == retries - 1:
                    logger.error(f"Failed to upload after {retries} attempts: {s3_filename}")
                    return None
        
        return None
    
    def get_s3_url(self, s3_key: str) -> str:
        """
        Get public URL for an S3 object
        
        Args:
            s3_key: S3 key (path) to the object
        
        Returns:
            Public S3 URL
        """
        return f"https://{self.bucket_name}.s3.{self.region_name}.amazonaws.com/{s3_key}"
    
    def list_files(self, prefix: str = None, target_date: datetime = None) -> list:
        """
        List files in S3 with optional partitioning
        
        Args:
            prefix: Custom prefix (if not provided, uses date partition)
            target_date: Date for partitioning (defaults to yesterday)
        
        Returns:
            List of S3 keys
        """
        if prefix is None:
            prefix = self.get_partition_prefix(target_date)
        
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                return []
            
            return [obj['Key'] for obj in response['Contents']]
            
        except Exception as e:
            logger.error(f"Error listing files: {e}")
            return []
    
    def delete_file(self, s3_key: str) -> bool:
        """
        Delete a file from S3
        
        Args:
            s3_key: S3 key (path) to the object
        
        Returns:
            True if successful, False otherwise
        """
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
            logger.info(f"Successfully deleted: {s3_key}")
            return True
        except Exception as e:
            logger.error(f"Error deleting file: {e}")
            return False
    
    def upload_image(self, image_url: str, image_data: bytes, subcategory_slug: str,
                     target_date: datetime = None, listing_id: Optional[str] = None, 
                     image_index: int = 0) -> Optional[str]:
        """
        Upload image bytes to S3 with ID-based naming
        
        Args:
            image_url: Original image URL
            image_data: Image bytes to upload
            subcategory_slug: Category slug for organization
            target_date: Date for partitioning
            listing_id: Listing ID for image naming (if provided, image will be named as listing_id_index.jpg)
            image_index: Index of image in the list (0, 1, 2, etc.)
        
        Returns:
            Full S3 path or None if failed
        """
        try:
            # Generate filename based on listing_id if provided
            if listing_id:
                filename = f"{listing_id}_{image_index}.jpg"
            else:
                # Fallback: extract filename from URL
                filename = image_url.split('/')[-1]
                if not filename:
                    filename = f"image_{int(__import__('time').time())}.jpg"
            
            partition = self.get_partition_prefix(target_date)
            s3_key = f"{partition}/images/{subcategory_slug}/{filename}"
            
            logger.info(f"Uploading image: {filename}")
            logger.info(f"Full S3 path: s3://{self.bucket_name}/{s3_key}")
            
            for attempt in range(3):
                try:
                    self.s3_client.put_object(
                        Bucket=self.bucket_name,
                        Key=s3_key,
                        Body=image_data,
                        ContentType='image/jpeg'
                    )
                    
                    logger.info(f"✓ Successfully uploaded: {filename}")
                    return s3_key
                    
                except Exception as e:
                    logger.warning(f"Image upload attempt {attempt + 1} failed: {e}")
                    if attempt < 2:
                        import time
                        time.sleep(1)
            
            return None
            
        except Exception as e:
            logger.error(f"Error uploading image {image_url}: {e}")
            return None
    
    def generate_s3_url(self, s3_key: str) -> str:
        """
        Generate public S3 URL for a key
        
        Args:
            s3_key: S3 object key
        
        Returns:
            Full S3 URL
        """
        return f"https://{self.bucket_name}.s3.{self.region_name}.amazonaws.com/{s3_key}"
    
    def list_files_in_partition(self, prefix: str = None, target_date: datetime = None) -> list:
        """
        List all files in a partition
        
        Args:
            prefix: Additional prefix to search (relative to partition)
            target_date: Date for partition (defaults to yesterday)
        
        Returns:
            List of file keys
        """
        partition = self.get_partition_prefix(target_date)
        if prefix:
            full_prefix = f"{partition}/{prefix}"
        else:
            full_prefix = partition
        
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=full_prefix
            )
            
            files = []
            if 'Contents' in response:
                files = [obj['Key'] for obj in response['Contents']]
            
            logger.info(f"Found {len(files)} files in partition {full_prefix}")
            return files
            
        except Exception as e:
            logger.error(f"Error listing files: {e}")
            return []
