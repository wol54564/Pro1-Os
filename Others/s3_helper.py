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
    Partitions data by date: others/year=YYYY/month=MM/day=DD/
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
        Format: 4sale-data/others/year=YYYY/month=MM/day=DD/
        
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
        
        return f"4sale-data/others/year={year}/month={month}/day={day}"
    
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
                logger.info(f"Uploading {local_file_path} to s3://{self.bucket_name}/{s3_key} (attempt {attempt + 1}/{retries})")
                
                # Detect content type
                content_type, _ = mimetypes.guess_type(local_file_path)
                if content_type is None:
                    content_type = 'application/octet-stream'
                
                # Upload file
                self.s3_client.upload_file(
                    local_file_path,
                    self.bucket_name,
                    s3_key,
                    ExtraArgs={'ContentType': content_type}
                )
                
                logger.info(f"Successfully uploaded to s3://{self.bucket_name}/{s3_key}")
                return s3_key
                
            except Exception as e:
                logger.error(f"Attempt {attempt + 1}/{retries} failed: {e}")
                if attempt < retries - 1:
                    import time
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"Failed to upload {local_file_path} after {retries} attempts")
                    return None
    
    def upload_image(self, image_url: str, image_data: bytes, subcategory_slug: str,
                    target_date: datetime, listing_id: int, image_index: int) -> Optional[str]:
        """
        Upload an image to S3 with organized structure
        Path: 4sale-data/others/year=YYYY/month=MM/day=DD/images/subcategory_slug/listing_id_index.jpg
        
        Args:
            image_url: Original image URL (for extension detection)
            image_data: Image binary data
            subcategory_slug: Subcategory slug (e.g., 'currencies-stamps-and-antiques')
            target_date: Date for partitioning
            listing_id: Listing ID
            image_index: Image index in listing
        
        Returns:
            Full S3 path or None if failed
        """
        try:
            # Determine file extension
            ext = Path(image_url).suffix or '.jpg'
            
            # Build S3 key with partitioning and organization
            partition = self.get_partition_prefix(target_date)
            filename = f"{listing_id}_{image_index}{ext}"
            s3_key = f"{partition}/images/{subcategory_slug}/{filename}"
            
            # Upload directly from memory
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=image_data,
                ContentType='image/jpeg'
            )
            
            logger.debug(f"Uploaded image to s3://{self.bucket_name}/{s3_key}")
            return s3_key
            
        except Exception as e:
            logger.error(f"Failed to upload image: {e}")
            return None
    
    def generate_s3_url(self, s3_key: str) -> str:
        """
        Generate public S3 URL for a given key
        
        Args:
            s3_key: S3 object key
        
        Returns:
            Public S3 URL
        """
        return f"https://{self.bucket_name}.s3.{self.region_name}.amazonaws.com/{s3_key}"
    
    def list_files(self, prefix: str = "", max_keys: int = 1000) -> list:
        """
        List files in S3 bucket with given prefix
        
        Args:
            prefix: S3 key prefix to filter (e.g., '4sale-data/others/')
            max_keys: Maximum number of keys to return
        
        Returns:
            List of S3 object keys
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
                logger.info(f"No objects found with prefix: {prefix}")
                return []
                
        except Exception as e:
            logger.error(f"Failed to list files: {e}")
            return []
    
    def download_file(self, s3_key: str, local_path: str) -> bool:
        """
        Download a file from S3
        
        Args:
            s3_key: S3 object key
            local_path: Local destination path
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Downloading s3://{self.bucket_name}/{s3_key} to {local_path}")
            
            # Create parent directories if needed
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            
            self.s3_client.download_file(
                self.bucket_name,
                s3_key,
                local_path
            )
            
            logger.info(f"Successfully downloaded to {local_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to download {s3_key}: {e}")
            return False
    
    def delete_file(self, s3_key: str) -> bool:
        """
        Delete a file from S3
        
        Args:
            s3_key: S3 object key to delete
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Deleting s3://{self.bucket_name}/{s3_key}")
            
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            
            logger.info(f"Successfully deleted {s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete {s3_key}: {e}")
            return False
    
    def get_file_metadata(self, s3_key: str) -> Optional[dict]:
        """
        Get metadata for an S3 object
        
        Args:
            s3_key: S3 object key
        
        Returns:
            Metadata dict or None if failed
        """
        try:
            response = self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            
            return {
                'size': response['ContentLength'],
                'last_modified': response['LastModified'],
                'content_type': response.get('ContentType'),
                'etag': response.get('ETag')
            }
            
        except Exception as e:
            logger.error(f"Failed to get metadata for {s3_key}: {e}")
            return None
    
    def check_file_exists(self, s3_key: str) -> bool:
        """
        Check if a file exists in S3
        
        Args:
            s3_key: S3 object key
        
        Returns:
            True if exists, False otherwise
        """
        try:
            self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            return True
        except:
            return False
    
    def copy_file(self, source_key: str, dest_key: str) -> bool:
        """
        Copy a file within S3
        
        Args:
            source_key: Source S3 object key
            dest_key: Destination S3 object key
        
        Returns:
            True if successful, False otherwise
        """
        try:
            copy_source = {
                'Bucket': self.bucket_name,
                'Key': source_key
            }
            
            self.s3_client.copy_object(
                CopySource=copy_source,
                Bucket=self.bucket_name,
                Key=dest_key
            )
            
            logger.info(f"Copied {source_key} to {dest_key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to copy {source_key} to {dest_key}: {e}")
            return False
    
    def get_partition_stats(self, target_date: datetime = None) -> dict:
        """
        Get statistics for a specific date partition
        
        Args:
            target_date: Date to check (defaults to yesterday)
        
        Returns:
            Dictionary with partition statistics
        """
        partition_prefix = self.get_partition_prefix(target_date)
        
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=partition_prefix
            )
            
            if 'Contents' in response:
                total_size = sum(obj['Size'] for obj in response['Contents'])
                file_count = len(response['Contents'])
                
                return {
                    'partition': partition_prefix,
                    'file_count': file_count,
                    'total_size_bytes': total_size,
                    'total_size_mb': round(total_size / (1024 * 1024), 2),
                    'files': [obj['Key'] for obj in response['Contents']]
                }
            else:
                return {
                    'partition': partition_prefix,
                    'file_count': 0,
                    'total_size_bytes': 0,
                    'total_size_mb': 0,
                    'files': []
                }
                
        except Exception as e:
            logger.error(f"Failed to get partition stats: {e}")
            return {
                'partition': partition_prefix,
                'error': str(e)
            }
