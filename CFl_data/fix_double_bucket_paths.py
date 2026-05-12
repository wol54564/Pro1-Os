"""
Fix double-bucket R2 paths.

Moves all objects from:
  data-collection-dl/4sale-data/...
to:
  4sale-data/...

(i.e. strips the leading "data-collection-dl/" prefix that was
 caused by the CF_R2_ENDPOINT_URL already containing the bucket name.)

Usage:
    CF_R2_ACCESS_KEY_ID=... CF_R2_SECRET_ACCESS_KEY=... \
    CF_R2_ENDPOINT_URL=... CF_R2_BUCKET_NAME=... \
    python CFl_data/fix_double_bucket_paths.py

    # Dry-run (no actual copy/delete):
    DRY_RUN=1 python CFl_data/fix_double_bucket_paths.py
"""

import os
import sys
import boto3
from botocore.config import Config

# ── Config ────────────────────────────────────────────────────────────────────

ACCESS_KEY   = os.environ.get("CF_R2_ACCESS_KEY_ID")
SECRET_KEY   = os.environ.get("CF_R2_SECRET_ACCESS_KEY")
ENDPOINT_URL = os.environ.get("CF_R2_ENDPOINT_URL", "")
BUCKET_NAME  = os.environ.get("CF_R2_BUCKET_NAME")
DRY_RUN      = os.environ.get("DRY_RUN", "0") == "1"

# Each tuple: (wrong_prefix, correct_prefix)
MIGRATIONS = [
    ("data-collection-dl/4sale-data/",  "4sale-data/"),   # bucket name duplicated
    ("your-bucket-name/4sale-data/",    "4sale-data/"),   # literal placeholder leaked
]

# ── Build client ──────────────────────────────────────────────────────────────

def build_client():
    # Strip bucket name from endpoint if it was included
    endpoint = ENDPOINT_URL.rstrip("/")
    if endpoint.endswith("/" + BUCKET_NAME):
        endpoint = endpoint[: -(len(BUCKET_NAME) + 1)]

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="auto",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


# ── List all objects under a prefix (handles pagination) ─────────────────────

def list_objects(client, prefix):
    keys = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not all([ACCESS_KEY, SECRET_KEY, ENDPOINT_URL, BUCKET_NAME]):
        print("ERROR: CF_R2_ACCESS_KEY_ID, CF_R2_SECRET_ACCESS_KEY, "
              "CF_R2_ENDPOINT_URL and CF_R2_BUCKET_NAME must all be set.")
        sys.exit(1)

    mode = "DRY-RUN" if DRY_RUN else "LIVE"
    print(f"=== fix_double_bucket_paths.py  [{mode}] ===")
    print(f"Bucket : {BUCKET_NAME}")
    print()

    client = build_client()

    total_copied = 0
    total_deleted = 0
    total_errors = 0

    for wrong_prefix, correct_prefix in MIGRATIONS:
        print(f"--- Source : {wrong_prefix}")
        print(f"    Dest   : {correct_prefix}")

        keys = list_objects(client, wrong_prefix)
        if not keys:
            print("    No objects found. Skipping.\n")
            continue

        print(f"    Found {len(keys)} object(s).\n")

        for key in keys:
            new_key = correct_prefix + key[len(wrong_prefix):]
            print(f"  COPY  {key}")
            print(f"     -> {new_key}")

            if not DRY_RUN:
                try:
                    client.copy_object(
                        Bucket=BUCKET_NAME,
                        CopySource={"Bucket": BUCKET_NAME, "Key": key},
                        Key=new_key,
                    )
                    total_copied += 1

                    client.delete_object(Bucket=BUCKET_NAME, Key=key)
                    total_deleted += 1
                    print(f"     OK")
                except Exception as e:
                    total_errors += 1
                    print(f"     ERROR: {e}")
            else:
                total_copied += 1

        print()

    if DRY_RUN:
        print(f"Dry-run complete. Would move {total_copied} object(s).")
    else:
        print(f"Done. Copied={total_copied}  Deleted={total_deleted}  Errors={total_errors}")
        if total_errors:
            sys.exit(1)


if __name__ == "__main__":
    main()
