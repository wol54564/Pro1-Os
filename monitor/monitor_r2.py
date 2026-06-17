"""
Shared R2 paths and loaders for the multi-site monitor hub.

Layout in bucket (default prefix: monitor-sites/):
  monitor-sites/registry.yml
  monitor-sites/{folder}/site.yml          ← site identity (4sale, boshamlan, …)
  monitor-sites/hub/{YYYY-MM-DD}/all-sites.json

Each site's scraper data + validation artifacts stay under its data prefix:
  {r2_prefix}/monitor/websites-config.yml
  {r2_prefix}/monitor/monitor_stats.yml
  {r2_prefix}/monitor/{YYYY-MM-DD}/report.json
"""

from __future__ import annotations

import logging
import os
from typing import Dict, Optional, Tuple

import boto3
import yaml

log = logging.getLogger("monitor")

MONITOR_SITES_ROOT = os.environ.get("MONITOR_SITES_PREFIX", "monitor-sites").strip("/")


def build_r2_client() -> Tuple:
    """Return a boto3 S3 client pointed at Cloudflare R2."""
    access_key  = os.environ["CF_R2_ACCESS_KEY_ID"]
    secret_key  = os.environ["CF_R2_SECRET_ACCESS_KEY"]
    endpoint    = os.environ["CF_R2_ENDPOINT_URL"].rstrip("/")
    bucket_name = os.environ["CF_R2_BUCKET_NAME"]

    if endpoint.endswith("/" + bucket_name):
        endpoint = endpoint[: -len("/" + bucket_name)]

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto",
    )
    return client, bucket_name


def registry_r2_key(root: str = MONITOR_SITES_ROOT) -> str:
    return f"{root}/registry.yml"


def site_config_r2_key(folder: str, root: str = MONITOR_SITES_ROOT) -> str:
    return f"{root}/{folder.strip('/')}/site.yml"


def hub_merged_r2_key(run_date: str, root: str = MONITOR_SITES_ROOT) -> str:
    return f"{root}/hub/{run_date}/all-sites.json"


def monitor_data_keys(site: Dict) -> Dict[str, str]:
    """Paths under the site's data prefix (excel/csv storage area)."""
    base = f"{site.get('r2_prefix', '').strip('/')}/monitor"
    return {
        "base":   base,
        "config": f"{base}/websites-config.yml",
        "stats":  f"{base}/monitor_stats.yml",
    }


def report_r2_key(site: Dict, run_date: str) -> str:
    return f"{monitor_data_keys(site)['base']}/{run_date}/report.json"


def resolve_site_folder(explicit: Optional[str] = None) -> str:
    """Folder slug under monitor-sites/ (from env or CLI --site-slug)."""
    if explicit:
        return explicit.strip("/")
    env_slug = os.environ.get("MONITOR_SITE_SLUG", "").strip()
    if env_slug:
        return env_slug
    raise EnvironmentError(
        "MONITOR_SITE_SLUG is required (e.g. 4sale, boshamlan, motorgy, bleems, kcsb, sheeel). "
        "Set it in the repo's monitor.yml workflow env block."
    )


def fetch_yaml_object(client, bucket: str, key: str) -> Dict:
    resp = client.get_object(Bucket=bucket, Key=key)
    return yaml.safe_load(resp["Body"].read().decode("utf-8")) or {}


def put_yaml_object(client, bucket: str, key: str, data: Dict, header: str = "") -> None:
    body = (header + yaml.dump(data, allow_unicode=True, default_flow_style=False, sort_keys=False)).encode("utf-8")
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="text/yaml")
    log.info(f"Uploaded → r2://{bucket}/{key}")


def put_bytes(client, bucket: str, key: str, body: bytes, content_type: str) -> None:
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)
    log.info(f"Uploaded → r2://{bucket}/{key}")


def load_site_config_from_r2(
    client,
    bucket: str,
    folder: Optional[str] = None,
    root: str = MONITOR_SITES_ROOT,
) -> Dict:
    slug = resolve_site_folder(folder)
    key  = site_config_r2_key(slug, root)
    try:
        site = fetch_yaml_object(client, bucket, key)
        site["folder"] = slug
        log.info(f"Loaded site config from r2://{bucket}/{key}")
        return site
    except client.exceptions.NoSuchKey:
        raise FileNotFoundError(
            f"Site config not found at r2://{bucket}/{key}. "
            f"Create monitor-sites/{slug}/site.yml in R2 (Cloudflare dashboard or aws s3 cp)."
        ) from None


def load_registry_from_r2(
    client,
    bucket: str,
    root: str = MONITOR_SITES_ROOT,
) -> Dict:
    key = registry_r2_key(root)
    try:
        reg = fetch_yaml_object(client, bucket, key)
        log.info(f"Loaded registry from r2://{bucket}/{key}")
        return reg
    except client.exceptions.NoSuchKey:
        raise FileNotFoundError(
            f"Registry not found at r2://{bucket}/{key}. "
            f"Create monitor-sites/registry.yml in R2."
        ) from None