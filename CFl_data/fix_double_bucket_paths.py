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

WRONG_PREFIX  = "data-collection-dl/4sale-data/"   # source (duplicated bucket)
CORRECT_PREFIX = "4sale-data/"                      # destination

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
    print(f"Source : {WRONG_PREFIX}")
    print(f"Dest   : {CORRECT_PREFIX}")
    print()

    client = build_client()

    keys = list_objects(client, WRONG_PREFIX)
    if not keys:
        print("No objects found under the wrong prefix. Nothing to do.")
        return

    print(f"Found {len(keys)} object(s) to move.\n")

    copied  = 0
    deleted = 0
    errors  = 0

    for key in keys:
        new_key = CORRECT_PREFIX + key[len(WRONG_PREFIX):]
        print(f"  COPY  {key}")
        print(f"     -> {new_key}")

        if not DRY_RUN:
            try:
                # Copy to correct path
                client.copy_object(
                    Bucket=BUCKET_NAME,
                    CopySource={"Bucket": BUCKET_NAME, "Key": key},
                    Key=new_key,
                )
                copied += 1

                # Delete old path
                client.delete_object(Bucket=BUCKET_NAME, Key=key)
                deleted += 1
                print(f"     OK")
            except Exception as e:
                errors += 1
                print(f"     ERROR: {e}")
        else:
            copied += 1

    print()
    if DRY_RUN:
        print(f"Dry-run complete. Would move {copied} object(s).")
    else:
        print(f"Done. Copied={copied}  Deleted={deleted}  Errors={errors}")
        if errors:
            sys.exit(1)


if __name__ == "__main__":
    main()
