"""
Delete leftover listing images from AWS S3 for scrapers that no longer
download images (see IMAGE_DOWNLOAD_SCRAPERS in scraper_utils.py).

Active image scrapers are never touched:
  commercials, contracting, furniture, services, wanted-cars,
  rest-automotive-part2, property

Only objects under `/images/` with an image extension are deleted.
JSON, Excel, and other scraper outputs are never touched.

Usage:
    # Preview (safe default) — leftover images in no-longer-image categories
    python cleanup_old_images_s3.py

    # Actually delete
    python cleanup_old_images_s3.py --execute

    # Only older than N days (optional)
    python cleanup_old_images_s3.py --retention-days 7 --execute

    # Override category list
    python cleanup_old_images_s3.py --categories camping electronics --execute

Required env vars (same as S3 scrapers):
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET_NAME
    Optional: AWS_REGION / AWS_DEFAULT_REGION (default us-east-1)
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from datetime import date, timedelta
from typing import Iterable, Iterator, List, Optional, Sequence, Set

import boto3
from botocore.exceptions import ClientError

from scraper_utils import IMAGE_DOWNLOAD_SCRAPERS

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# S3 folder slug → scraper_utils key (for IMAGE_DOWNLOAD_SCRAPERS lookup).
# Categories still downloading images are excluded from cleanup by default.
S3_CATEGORY_TO_SCRAPER_KEY = {
    "animals": "animals",
    "automative-cars-and-trucks": "automotive-cars-and-trucks",
    "bikes": "bikes",
    "camping": "camping",
    "commercials": "commercials",
    "contracting": "contracting",
    "Dalil": "dalil",
    "education": "education",
    "electronics": "electronics",
    "fashion-and-family": "fashion-and-family",
    "furniture": "furniture",
    "gifts": "gifts",
    "jobs": "jobs",
    "new-cars": "new-car",
    "others": "others",
    "property": "property",  # still has selective image download — keep
    "rest-automotive-part1": "rest-automotive-part1",
    "rest-automotive-part2": "rest-automotive-part2",
    "rest-automotive-part3": "rest-automotive-part3",
    "services": "services",
    "sport": "sport",
    "used-cars": "used-car",
    "wanted-cars": "wanted-cars",
}

# Always keep these S3 folders (still upload images, or selective like property).
KEEP_IMAGE_S3_CATEGORIES = frozenset(
    {
        "commercials",
        "contracting",
        "furniture",
        "services",
        "wanted-cars",
        "rest-automotive-part2",
        "property",
    }
)

ALL_S3_CATEGORIES: Sequence[str] = tuple(S3_CATEGORY_TO_SCRAPER_KEY.keys())

IMAGE_EXTENSIONS = {".webp", ".jpg", ".jpeg", ".png", ".gif"}
PARTITION_RE = re.compile(r"year=(\d{4})/month=(\d{2})/day=(\d{2})")
DELETE_BATCH_SIZE = 1000


def categories_without_images() -> List[str]:
    """S3 folders for scrapers that no longer download listing images."""
    result = []
    for s3_slug, scraper_key in S3_CATEGORY_TO_SCRAPER_KEY.items():
        if s3_slug in KEEP_IMAGE_S3_CATEGORIES:
            continue
        if scraper_key in IMAGE_DOWNLOAD_SCRAPERS:
            continue
        result.append(s3_slug)
    return result


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        logger.error("Missing required environment variable: %s", name)
        sys.exit(1)
    return value


def make_s3_client():
    bucket = _require_env("S3_BUCKET_NAME")
    region = (
        os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or "us-east-1"
    )

    client = boto3.client(
        "s3",
        aws_access_key_id=_require_env("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=_require_env("AWS_SECRET_ACCESS_KEY"),
        region_name=region,
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


def collect_image_keys(
    client,
    bucket: str,
    categories: Sequence[str],
    cutoff: Optional[date],
) -> List[str]:
    """
    Collect image keys to delete.
    If cutoff is None, all images in those categories are eligible.
    If cutoff is set, only partitions strictly before cutoff are eligible.
    """
    to_delete: List[str] = []
    for category in categories:
        cat_listed = 0
        cat_images = 0
        cat_match = 0
        for key in iter_category_keys(client, bucket, category):
            cat_listed += 1
            if not is_image_key(key):
                continue
            cat_images += 1
            if cutoff is not None:
                part_date = partition_date(key)
                if part_date is None or part_date >= cutoff:
                    continue
            to_delete.append(key)
            cat_match += 1
        logger.info(
            "Category %-30s  objects=%6d  images=%6d  to_delete=%6d",
            category,
            cat_listed,
            cat_images,
            cat_match,
        )
    return to_delete


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
        description=(
            "Delete leftover S3 listing images for scrapers that no longer "
            "download images. Active image categories are left alone."
        ),
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=None,
        help=(
            "Only delete images older than N days. "
            "Omit to delete all leftover images in target categories."
        ),
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
        help=(
            "Override target S3 category slugs. "
            "Default: categories that no longer download images."
        ),
    )
    parser.add_argument(
        "--include-active",
        action="store_true",
        help=(
            "Allow --categories to include active image scrapers "
            "(commercials, furniture, etc.). Dangerous — use with care."
        ),
    )
    return parser.parse_args()


def resolve_categories(
    selected: Optional[Iterable[str]],
    allow_active: bool,
) -> List[str]:
    default = categories_without_images()
    if not selected:
        return default

    valid: Set[str] = set(ALL_S3_CATEGORIES)
    unknown = [c for c in selected if c not in valid]
    if unknown:
        logger.error("Unknown categories: %s", ", ".join(unknown))
        logger.error("Valid options: %s", ", ".join(ALL_S3_CATEGORIES))
        sys.exit(1)

    if not allow_active:
        blocked = [c for c in selected if c in KEEP_IMAGE_S3_CATEGORIES]
        if blocked:
            logger.error(
                "Refusing to target active image categories: %s. "
                "Pass --include-active if you really mean it.",
                ", ".join(blocked),
            )
            sys.exit(1)

    return list(selected)


def main() -> None:
    args = parse_args()
    if args.retention_days is not None and args.retention_days < 0:
        logger.error("--retention-days must be >= 0")
        sys.exit(1)

    categories = resolve_categories(args.categories, args.include_active)
    cutoff = (
        date.today() - timedelta(days=args.retention_days)
        if args.retention_days is not None
        else None
    )
    mode = "EXECUTE" if args.execute else "DRY-RUN"

    logger.info("Mode: %s", mode)
    logger.info(
        "Target: leftover S3 images from scrapers that no longer download images"
    )
    logger.info("Kept (still have images): %s", ", ".join(sorted(KEEP_IMAGE_S3_CATEGORIES)))
    if cutoff is None:
        logger.info("Retention: delete ALL leftover images in target categories")
    else:
        logger.info(
            "Retention: only partitions before %s (%d day(s))",
            cutoff.isoformat(),
            args.retention_days,
        )
    logger.info("Categories: %s", ", ".join(categories))

    client, bucket = make_s3_client()
    logger.info("Bucket: %s", bucket)
    keys = collect_image_keys(client, bucket, categories, cutoff)

    logger.info("=" * 60)
    logger.info("Image objects to delete: %d", len(keys))

    if not keys:
        logger.info("Nothing to delete.")
        return

    sample = keys[:5]
    logger.info("Sample keys:")
    for key in sample:
        logger.info("  %s", key)
    if len(keys) > len(sample):
        logger.info("  ... and %d more", len(keys) - len(sample))

    if not args.execute:
        logger.info("Dry-run complete. Re-run with --execute to delete.")
        return

    deleted, failed = delete_keys(client, bucket, keys)
    logger.info("=" * 60)
    logger.info("Done — deleted: %d  |  failed: %d", deleted, failed)
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
