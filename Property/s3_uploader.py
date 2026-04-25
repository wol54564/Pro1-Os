import boto3
import aiohttp
import asyncio
from io import BytesIO
import os
import logging

logger = logging.getLogger(__name__)

class S3Uploader:
    def __init__(self, bucket_name, region_name="us-east-1"):
        aws_access_key = os.environ.get("AWS_KEY")
        aws_secret_key = os.environ.get("AWS_SECRET")
        self.bucket_name = bucket_name
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region_name
        )

    async def upload_fileobj(self, file_obj, s3_path, content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"):
        try:
            logger.info(f"Uploading file to S3: {s3_path}")
            self.s3.upload_fileobj(file_obj, self.bucket_name, s3_path, ExtraArgs={"ContentType": content_type})
            s3_url = f"s3://{self.bucket_name}/{s3_path}"
            logger.info(f"✓ File uploaded successfully: {s3_url}")
            return s3_url
        except Exception as e:
            logger.error(f"Failed to upload file to S3 {s3_path}: {e}", exc_info=True)
            raise

    async def upload_image(self, url, s3_path):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        content = await resp.read()
                        file_obj = BytesIO(content)
                        logger.debug(f"Uploading image to S3: {s3_path}")
                        self.s3.upload_fileobj(file_obj, self.bucket_name, s3_path, ExtraArgs={"ContentType": "image/jpeg"})
                        s3_url = f"s3://{self.bucket_name}/{s3_path}"
                        logger.debug(f"✓ Image uploaded: {s3_url}")
                        return s3_url
                    else:
                        logger.warning(f"Failed to download {url} status={resp.status}")
                        return None
        except Exception as e:
            logger.error(f"Error uploading image from {url} to {s3_path}: {e}", exc_info=True)
            return None
