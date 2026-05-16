"""
Move misplaced Commercials files in R2.

Wrong : your-bucket-name/4sale-data/commercials/...
Correct:              4sale-data/commercials/...

Uses boto3 (S3-compatible) copy + delete — no local download needed.
"""

import os
import sys
import logging
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ── R2 connection ─────────────────────────────────────────────────────────────

BUCKET      = os.environ["CF_R2_BUCKET_NAME"]
ENDPOINT    = os.environ["CF_R2_ENDPOINT_URL"].rstrip("/")
ACCESS_KEY  = os.environ["CF_R2_ACCESS_KEY_ID"]
SECRET_KEY  = os.environ["CF_R2_SECRET_ACCESS_KEY"]

BAD_PREFIX  = "your-bucket-name/4sale-data/commercials/"
GOOD_PREFIX = "4sale-data/commercials/"

client = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name="auto",
)

# ── List every object under the bad prefix ────────────────────────────────────

paginator = client.get_paginator("list_objects_v2")
pages = paginator.paginate(Bucket=BUCKET, Prefix=BAD_PREFIX)

keys = []
for page in pages:
    for obj in page.get("Contents", []):
        keys.append(obj["Key"])

if not keys:
    logger.info("No objects found under '%s' — nothing to move.", BAD_PREFIX)
    sys.exit(0)

logger.info("Found %d object(s) to move.", len(keys))

# ── Copy → Delete ─────────────────────────────────────────────────────────────

moved, failed = 0, 0

for src_key in keys:
    # Strip the bad prefix, keep everything after it
    relative = src_key[len(BAD_PREFIX):]
    dst_key  = GOOD_PREFIX + relative

    try:
        # Server-side copy (no bandwidth cost, no size limit)
        client.copy_object(
            Bucket=BUCKET,
            CopySource={"Bucket": BUCKET, "Key": src_key},
            Key=dst_key,
        )
        logger.info("Copied  %s  →  %s", src_key, dst_key)

        # Delete original only after successful copy
        client.delete_object(Bucket=BUCKET, Key=src_key)
        logger.info("Deleted %s", src_key)

        moved += 1

    except ClientError as e:
        logger.error("FAILED  %s : %s", src_key, e)
        failed += 1

# ── Summary ───────────────────────────────────────────────────────────────────

logger.info("=" * 60)
logger.info("Done — moved: %d  |  failed: %d", moved, failed)
if failed:
    sys.exit(1)
