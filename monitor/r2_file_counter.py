"""
r2_file_counter.py
==================
Count total objects stored in Cloudflare R2 for the monitor hub dashboard.

Per scraper: all objects under the scraper's R2 data prefix (all dates, all types).
Per site: all objects under the site's r2_prefix (includes monitor/ metadata).
Also tracks cumulative byte size for the same prefixes.
"""

from __future__ import annotations

import logging
from typing import Dict

log = logging.getLogger("monitor")


def count_r2_inventory(client, bucket: str, prefix: str) -> Dict[str, int]:
    """
    Count all objects and bytes under *prefix* using paginated list_objects_v2.

    Skips zero-byte folder marker keys ending with '/'.
    """
    normalized = prefix.strip("/")
    list_prefix = f"{normalized}/" if normalized else ""

    count = 0
    size_bytes = 0
    paginator = client.get_paginator("list_objects_v2")
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=list_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith("/"):
                    continue
                count += 1
                size_bytes += int(obj.get("Size") or 0)
    except Exception as exc:
        log.warning(f"R2 inventory failed for prefix {list_prefix!r}: {exc}")
        return {"objects": 0, "size_bytes": 0}

    return {"objects": count, "size_bytes": size_bytes}


def count_r2_objects(client, bucket: str, prefix: str) -> int:
    """Backward-compatible wrapper returning only object count."""
    return count_r2_inventory(client, bucket, prefix)["objects"]


def count_scraper_r2_files(client, bucket: str, r2_base: str) -> int:
    """Total objects under one scraper/category prefix."""
    base = r2_base.strip("/")
    if not base:
        return 0
    inventory = count_r2_inventory(client, bucket, base)
    log.debug(
        f"  R2 inventory {base}: {inventory['objects']} object(s), {inventory['size_bytes']} bytes"
    )
    return inventory["objects"]


def count_scraper_r2_inventory(client, bucket: str, r2_base: str) -> Dict[str, int]:
    """Total objects + bytes under one scraper/category prefix."""
    base = r2_base.strip("/")
    if not base:
        return {"objects": 0, "size_bytes": 0}
    inventory = count_r2_inventory(client, bucket, base)
    log.debug(
        f"  R2 inventory {base}: {inventory['objects']} object(s), {inventory['size_bytes']} bytes"
    )
    return inventory


def count_site_r2_files(client, bucket: str, r2_prefix: str) -> int:
    """Total objects under the site's data prefix (all scrapers + monitor artifacts)."""
    prefix = r2_prefix.strip("/")
    if not prefix:
        return 0
    inventory = count_r2_inventory(client, bucket, prefix)
    log.info(
        f"Site R2 inventory ({prefix}): {inventory['objects']} object(s), {inventory['size_bytes']} bytes"
    )
    return inventory["objects"]


def count_site_r2_inventory(client, bucket: str, r2_prefix: str) -> Dict[str, int]:
    """Total objects + bytes under the site's data prefix (all categories + monitor artifacts)."""
    prefix = r2_prefix.strip("/")
    if not prefix:
        return {"objects": 0, "size_bytes": 0}
    inventory = count_r2_inventory(client, bucket, prefix)
    log.info(
        f"Site R2 inventory ({prefix}): {inventory['objects']} object(s), {inventory['size_bytes']} bytes"
    )
    return inventory
