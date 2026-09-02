"""
Delete aged listing images from Cloudflare R2 (CFl_data categories).

Only objects under partition paths that contain `/images/` and have an image
extension are eligible. JSON, Excel, and other scraper outputs are never touched.

Usage:
    # Preview what would be deleted (default — safe)
    python cleanup_old_images.py --retention-days 7

    # Actually delete
    python cleanup_old_images.py --retention-days 7 --execute

    # Limit to specific categories
    python cleanup_old_images.py --retention-days 14 --categories electronics furniture --execute

Required env vars (same as scrapers):
    CF_R2_ACCESS_KEY_ID, CF_R2_SECRET_ACCESS_KEY, CF_R2_ENDPOINT_URL, CF_R2_BUCKET_NAME
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from datetime import date, datetime, timedelta
from typing import Iterable, Iterator, List, Optional, Sequence, Set

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Categories that upload images to R2 (must match get_partition_prefix slugs).
IMAGE_CATEGORIES: Sequence[str] = (
    "animals",
    "automative-cars-and-trucks",
    "bikes",
    "camping",
    "commercials",
    "contracting",
    "Dalil",
    "education",
    "electronics",
    "fashion-and-family",
    "furniture",
    "gifts",
    "jobs",
    "new-cars",
    "others",
    "property",
    "rest-automotive-part1",
    "rest-automotive-part2",
    "rest-automotive-part3",
    "services",
    "sport",
    "used-cars",
    "wanted-cars",
)

IMAGE_EXTENSIONS = {".webp", ".jpg", ".jpeg", ".png", ".gif"}
PARTITION_RE = re.compile(r"year=(\d{4})/month=(\d{2})/day=(\d{2})")
DELETE_BATCH_SIZE = 1000


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        logger.error("Missing required environment variable: %s", name)
        sys.exit(1)
    return value


def make_r2_client():
    bucket = _require_env("CF_R2_BUCKET_NAME")
    endpoint = _require_env("CF_R2_ENDPOINT_URL").rstrip("/")
    endpoint = endpoint.removesuffix("/" + bucket)

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=_require_env("CF_R2_ACCESS_KEY_ID"),
        aws_secret_access_key=_require_env("CF_R2_SECRET_ACCESS_KEY"),
        region_name="auto",
    )
    return client, bucket


def partition_date(key: str) -> Optional[date]:
    match = PARTITION_RE.search(key)
    if not match:
        return None
    year, month, day = (int(g) for g in match.groups())
    try:
        return date(year, month, day)
    except ValueError:
        return None


def is_image_key(key: str) -> bool:
    if "/images/" not in key:
        return False
    ext = os.path.splitext(key.rstrip("/"))[1].lower()
    return ext in IMAGE_EXTENSIONS


def iter_category_keys(client, bucket: str, category: str) -> Iterator[str]:
    prefix = f"4sale-data/{category}/"
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]


def collect_stale_image_keys(
    client,
    bucket: str,
    categories: Sequence[str],
    cutoff: date,
) -> List[str]:
    stale: List[str] = []
    for category in categories:
        cat_count = 0
        cat_stale = 0
        for key in iter_category_keys(client, bucket, category):
            cat_count += 1
            if not is_image_key(key):
                continue
            part_date = partition_date(key)
            if part_date is None:
                logger.debug("Skip (no partition date): %s", key)
                continue
            if part_date < cutoff:
                stale.append(key)
                cat_stale += 1
        logger.info(
            "Category %-30s  listed=%6d  stale_images=%6d",
            category,
            cat_count,
            cat_stale,
        )
    return stale


def delete_keys(client, bucket: str, keys: Sequence[str]) -> tuple[int, int]:
    deleted = 0
    failed = 0

    for i in range(0, len(keys), DELETE_BATCH_SIZE):
        batch = keys[i : i + DELETE_BATCH_SIZE]
        try:
            response = client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
            )
            errors = response.get("Errors", [])
            deleted += len(batch) - len(errors)
            failed += len(errors)
            for err in errors:
                logger.error("Delete failed: %s — %s", err.get("Key"), err.get("Message"))
        except ClientError as exc:
            logger.error("Batch delete failed (%d keys): %s", len(batch), exc)
            failed += len(batch)

    return deleted, failed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Delete old CFl_data listing images from Cloudflare R2.",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=7,
        help="Keep images from the last N days (inclusive). Default: 7.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually delete objects. Without this flag, dry-run only.",
    )
    parser.add_argument(
        "--categories",
        nargs="+",
        metavar="CATEGORY",
        help="Only process these category slugs (default: all image categories).",
    )
    return parser.parse_args()


def resolve_categories(selected: Optional[Iterable[str]]) -> List[str]:
    if not selected:
        return list(IMAGE_CATEGORIES)

    valid: Set[str] = set(IMAGE_CATEGORIES)
    unknown = [c for c in selected if c not in valid]
    if unknown:
        logger.error("Unknown categories: %s", ", ".join(unknown))
        logger.error("Valid options: %s", ", ".join(IMAGE_CATEGORIES))
        sys.exit(1)
    return list(selected)


def main() -> None:
    args = parse_args()
    if args.retention_days < 0:
        logger.error("--retention-days must be >= 0")
        sys.exit(1)

    categories = resolve_categories(args.categories)
    cutoff = date.today() - timedelta(days=args.retention_days)
    mode = "EXECUTE" if args.execute else "DRY-RUN"

    logger.info("Mode: %s", mode)
    logger.info("Retention: keep partitions on or after %s (%d day(s))", cutoff.isoformat(), args.retention_days)
    logger.info("Categories: %s", ", ".join(categories))

    client, bucket = make_r2_client()
    stale_keys = collect_stale_image_keys(client, bucket, categories, cutoff)

    logger.info("=" * 60)
    logger.info("Stale image objects found: %d", len(stale_keys))

    if not stale_keys:
        logger.info("Nothing to delete.")
        return

    sample = stale_keys[:5]
    logger.info("Sample keys:")
    for key in sample:
        logger.info("  %s", key)
    if len(stale_keys) > len(sample):
        logger.info("  ... and %d more", len(stale_keys) - len(sample))

    if not args.execute:
        logger.info("Dry-run complete. Re-run with --execute to delete.")
        return

    deleted, failed = delete_keys(client, bucket, stale_keys)
    logger.info("=" * 60)
    logger.info("Done — deleted: %d  |  failed: %d", deleted, failed)
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
