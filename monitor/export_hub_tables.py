"""
export_hub_tables.py
====================
Flatten hub merged JSON into Parquet tables for MotherDuck / Evidence dashboard.

Reads:
  monitor-sites/hub/{partition-date}/all-sites.json   (from R2 or local file)

Writes:
  monitor-sites/hub/tables/hub_daily/{partition-date}.parquet
  monitor-sites/hub/tables/site_daily/{partition-date}.parquet
  monitor-sites/hub/tables/scraper_daily/{partition-date}.parquet
  monitor-sites/hub/tables/alerts/{partition-date}.parquet

Usage
-----
  python monitor/export_hub_tables.py --from-r2
  python monitor/export_hub_tables.py --date 2026-06-16 --from-r2
  python monitor/export_hub_tables.py --partition 2026-06-17 --from-r2
  python monitor/export_hub_tables.py --input ./all-sites.json --output-dir ./out
  python monitor/export_hub_tables.py --backfill --days 30 --from-r2
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from monitor_r2 import (
    MONITOR_SITES_ROOT,
    build_r2_client,
    hub_merged_r2_key,
    hub_table_parquet_key,
    list_hub_partition_dates,
    load_registry_from_r2,
    partition_date_for_listing,
    put_bytes,
    _normalize_schedule,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("export-hub-tables")

HUB_DAILY_COLS = [
    "hub_partition_date",
    "inspect_date",
    "generated_at",
    "sites_total",
    "sites_ok",
    "sites_failed",
    "sites_missing",
    "total_alerts",
    "total_unique_ads",
    "total_r2_files",
    "hub_prefix",
]

SITE_DAILY_COLS = [
    "hub_partition_date",
    "site_id",
    "folder",
    "display_name",
    "website",
    "country",
    "repo",
    "github_username",
    "run_place",
    "workflow_name",
    "workflow_run_number",
    "workflow_run_id",
    "workflow_status",
    "workflow_duration_sec",
    "schedule",
    "status",
    "scrapers_total",
    "scrapers_passed",
    "alert_count",
    "unique_ads",
    "r2_file_count",
    "run_date",
    "inspect_date",
    "report_fallback",
]

SCRAPER_DAILY_COLS = [
    "hub_partition_date",
    "site_id",
    "scraper",
    "files_found",
    "checks_passed",
    "checks_total",
    "all_passed",
    "files_optional",
    "unique_ads",
    "total_rows",
    "ads_source",
    "r2_file_count",
]

ALERTS_COLS = [
    "hub_partition_date",
    "site_id",
    "scraper",
    "severity",
    "alert_type",
    "check_name",
    "detail",
    "file_key",
    "alert_id",
]


def _scraper_entries(report: Dict) -> List[Tuple[str, Dict]]:
    """Return (name, result) pairs — scrapers may be dict or list in report JSON."""
    scrapers = report.get("scrapers") or {}
    if isinstance(scrapers, list):
        return [(s.get("scraper", f"scraper_{i}"), s) for i, s in enumerate(scrapers)]
    if isinstance(scrapers, dict):
        return [(name, sr) for name, sr in scrapers.items()]
    return []


def _registry_lookup(registry: Optional[Dict]) -> Dict[str, Dict]:
    """Map site_id and folder slug → registry site row."""
    if not registry:
        return {}
    lookup: Dict[str, Dict] = {}
    for site in registry.get("sites", []):
        if site.get("site_id"):
            lookup[str(site["site_id"])] = site
        if site.get("folder"):
            lookup[str(site["folder"])] = site
    return lookup


def flatten_hub(
    merged: Dict,
    registry: Optional[Dict] = None,
) -> Tuple[Dict, List[Dict], List[Dict], List[Dict]]:
    """
    Flatten all-sites.json into hub_daily, site_daily, scraper_daily, alerts rows.

    Returns (hub_row, site_rows, scraper_rows, alert_rows).
    """
    hub_partition = merged.get("run_date")
    if not hub_partition:
        raise ValueError("merged JSON missing run_date (hub partition date)")

    hub_row = {
        "hub_partition_date": hub_partition,
        "inspect_date": merged.get("inspect_date"),
        "generated_at": merged.get("generated_at"),
        "sites_total": merged.get("sites_total"),
        "sites_ok": merged.get("sites_ok"),
        "sites_failed": merged.get("sites_failed"),
        "sites_missing": merged.get("sites_missing"),
        "total_alerts": merged.get("total_alerts"),
        "total_unique_ads": merged.get("total_unique_ads"),
        "total_r2_files": merged.get("total_r2_files"),
        "hub_prefix": merged.get("hub_prefix"),
    }

    reg_lookup = _registry_lookup(registry)
    site_rows: List[Dict] = []
    scraper_rows: List[Dict] = []
    alert_rows: List[Dict] = []

    for site in merged.get("sites", []):
        site_id = site.get("site_id") or site.get("folder")
        if not site_id:
            continue

        reg = reg_lookup.get(site_id) or reg_lookup.get(site.get("folder", "")) or {}
        schedule = reg.get("schedule") or _normalize_schedule(reg) if reg else None
        if not schedule and site.get("folder"):
            schedule = _normalize_schedule({"folder": site.get("folder")})

        report = site.get("report") or {}
        github_run = report.get("github_run") or {}
        run_place = (
            reg.get("run_place")
            or report.get("run_place")
            or github_run.get("run_place")
            or "github"
        )

        site_rows.append({
            "hub_partition_date": hub_partition,
            "site_id": site_id,
            "folder": site.get("folder"),
            "display_name": site.get("display_name"),
            "website": site.get("website"),
            "country": site.get("country"),
            "repo": site.get("repo") or reg.get("repo"),
            "github_username": reg.get("github_username"),
            "run_place": run_place,
            "workflow_name": github_run.get("workflow_name"),
            "workflow_run_number": github_run.get("workflow_run_number"),
            "workflow_run_id": github_run.get("workflow_run_id"),
            "workflow_status": github_run.get("workflow_status"),
            "workflow_duration_sec": github_run.get("duration_sec"),
            "schedule": schedule,
            "status": site.get("status"),
            "scrapers_total": site.get("scrapers_total"),
            "scrapers_passed": site.get("scrapers_passed"),
            "alert_count": site.get("alert_count"),
            "unique_ads": site.get("unique_ads"),
            "r2_file_count": site.get("r2_file_count"),
            "run_date": site.get("run_date"),
            "inspect_date": site.get("inspect_date"),
            "report_fallback": bool(site.get("report_fallback", False)),
        })
        for scraper_name, sr in _scraper_entries(report):
            scraper_rows.append({
                "hub_partition_date": hub_partition,
                "site_id": site_id,
                "scraper": sr.get("scraper") or scraper_name,
                "files_found": sr.get("files_found"),
                "checks_passed": sr.get("checks_passed"),
                "checks_total": sr.get("checks_total"),
                "all_passed": sr.get("all_passed"),
                "files_optional": bool(sr.get("files_optional", False)),
                "unique_ads": sr.get("unique_ads"),
                "total_rows": sr.get("total_rows"),
                "ads_source": sr.get("ads_source"),
                "r2_file_count": sr.get("r2_file_count"),
            })

        alerts = report.get("alerts") or []
        for i, alert in enumerate(alerts):
            scraper = alert.get("scraper") or ""
            alert_rows.append({
                "hub_partition_date": hub_partition,
                "site_id": site_id,
                "scraper": scraper,
                "severity": alert.get("severity"),
                "alert_type": alert.get("type"),
                "check_name": alert.get("check"),
                "detail": alert.get("detail"),
                "file_key": alert.get("file"),
                "alert_id": f"{hub_partition}:{site_id}:{scraper}:{i}",
            })

    return hub_row, site_rows, scraper_rows, alert_rows


def _empty_df(columns: List[str]) -> pd.DataFrame:
    return pd.DataFrame({c: [] for c in columns})


def _to_dataframes(
    hub_row: Dict,
    site_rows: List[Dict],
    scraper_rows: List[Dict],
    alert_rows: List[Dict],
) -> Dict[str, pd.DataFrame]:
    return {
        "hub_daily": pd.DataFrame([hub_row]) if hub_row else _empty_df(HUB_DAILY_COLS),
        "site_daily": pd.DataFrame(site_rows) if site_rows else _empty_df(SITE_DAILY_COLS),
        "scraper_daily": pd.DataFrame(scraper_rows) if scraper_rows else _empty_df(SCRAPER_DAILY_COLS),
        "alerts": pd.DataFrame(alert_rows) if alert_rows else _empty_df(ALERTS_COLS),
    }


def parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


def write_tables_local(output_dir: Path, partition_date: str, tables: Dict[str, pd.DataFrame]) -> List[str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    written: List[str] = []
    for table_name, df in tables.items():
        path = output_dir / table_name / f"{partition_date}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)
        written.append(str(path))
        log.info(f"  wrote {path} ({len(df)} rows)")
    return written


def upload_tables_r2(
    client,
    bucket: str,
    partition_date: str,
    tables: Dict[str, pd.DataFrame],
    root: str = MONITOR_SITES_ROOT,
) -> List[str]:
    uploaded: List[str] = []
    for table_name, df in tables.items():
        key = hub_table_parquet_key(table_name, partition_date, root)
        body = parquet_bytes(df)
        put_bytes(client, bucket, key, body, "application/octet-stream")
        uploaded.append(key)
        log.info(f"  uploaded r2://{bucket}/{key} ({len(df)} rows)")
    return uploaded


def load_merged_from_r2(client, bucket: str, partition_date: str, root: str) -> Dict:
    key = hub_merged_r2_key(partition_date, root)
    try:
        resp = client.get_object(Bucket=bucket, Key=key)
        data = json.loads(resp["Body"].read().decode("utf-8"))
        log.info(f"Loaded r2://{bucket}/{key}")
        return data
    except client.exceptions.NoSuchKey:
        raise FileNotFoundError(f"Hub merged JSON not found at r2://{bucket}/{key}") from None


def load_merged_from_file(path: Path) -> Dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    log.info(f"Loaded local file {path}")
    return data


def export_partition(
    merged: Dict,
    partition_date: str,
    registry: Optional[Dict] = None,
    client=None,
    bucket: Optional[str] = None,
    root: str = MONITOR_SITES_ROOT,
    output_dir: Optional[Path] = None,
    upload_r2: bool = True,
) -> Dict[str, pd.DataFrame]:
    """Flatten one hub JSON and write Parquet locally and/or to R2."""
    if merged.get("run_date") != partition_date:
        log.warning(
            f"JSON run_date={merged.get('run_date')} differs from target partition {partition_date}"
        )

    hub_row, site_rows, scraper_rows, alert_rows = flatten_hub(merged, registry)
    tables = _to_dataframes(hub_row, site_rows, scraper_rows, alert_rows)

    log.info(
        f"Partition {partition_date}: "
        f"1 hub · {len(site_rows)} sites · {len(scraper_rows)} scrapers · {len(alert_rows)} alerts"
    )

    if output_dir is not None:
        write_tables_local(output_dir, partition_date, tables)

    if upload_r2 and client is not None and bucket:
        upload_tables_r2(client, bucket, partition_date, tables, root)

    return tables


def resolve_partition_date(args: argparse.Namespace) -> str:
    if args.partition:
        return args.partition
    if args.date:
        listing_dt = datetime.strptime(args.date, "%Y-%m-%d")
        return partition_date_for_listing(listing_dt).strftime("%Y-%m-%d")
    listing_dt = datetime.utcnow() - timedelta(days=1)
    return partition_date_for_listing(listing_dt).strftime("%Y-%m-%d")


def backfill_partitions(
    client,
    bucket: str,
    root: str,
    days: int,
    registry: Optional[Dict],
    output_dir: Optional[Path],
    upload_r2: bool,
) -> int:
    all_dates = list_hub_partition_dates(client, bucket, root)
    if not all_dates:
        log.warning("No hub partitions found in R2")
        return 0

    if days > 0:
        max_dt = datetime.strptime(all_dates[-1], "%Y-%m-%d")
        min_dt = max_dt - timedelta(days=days - 1)
        targets = [d for d in all_dates if min_dt <= datetime.strptime(d, "%Y-%m-%d") <= max_dt]
    else:
        targets = all_dates

    log.info(f"Backfill: {len(targets)} partition(s) from {targets[0]} … {targets[-1]}")

    ok = 0
    for partition_date in targets:
        try:
            merged = load_merged_from_r2(client, bucket, partition_date, root)
            export_partition(
                merged,
                partition_date,
                registry=registry,
                client=client,
                bucket=bucket,
                root=root,
                output_dir=output_dir,
                upload_r2=upload_r2,
            )
            ok += 1
        except Exception as exc:
            log.error(f"  skip {partition_date}: {exc}")

    return ok


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Export hub all-sites.json to flat Parquet tables.")
    p.add_argument("--date", help="Listing date (YYYY-MM-DD) → partition = date + 1 day")
    p.add_argument("--partition", help="Hub partition date (YYYY-MM-DD) — overrides --date")
    p.add_argument(
        "--from-r2",
        action="store_true",
        help="Load all-sites.json from R2 (requires CF_R2_* env vars)",
    )
    p.add_argument("--input", type=Path, help="Local path to all-sites.json (instead of R2)")
    p.add_argument(
        "--output-dir",
        type=Path,
        help="Also write Parquet files locally under this directory",
    )
    p.add_argument(
        "--no-upload",
        action="store_true",
        help="Skip uploading Parquet to R2 (local --output-dir only)",
    )
    p.add_argument("--backfill", action="store_true", help="Export all hub partitions in range")
    p.add_argument(
        "--days",
        type=int,
        default=30,
        help="With --backfill: number of days ending at latest partition (default 30)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    client = None
    bucket: Optional[str] = None
    root = MONITOR_SITES_ROOT
    registry: Optional[Dict] = None

    if args.from_r2 or args.backfill or (not args.input and not args.no_upload):
        client, env_bucket = build_r2_client()
        bucket = os.environ.get("CF_R2_BUCKET_NAME") or env_bucket
        try:
            registry = load_registry_from_r2(client, bucket, root)
            hub_cfg = registry.get("hub", {})
            root = hub_cfg.get("monitor_sites_prefix", root)
            bucket = os.environ.get("CF_R2_BUCKET_NAME") or hub_cfg.get("r2_bucket") or bucket
        except FileNotFoundError as exc:
            log.warning(f"Registry not loaded ({exc}) — site schedule enrichment skipped")

    upload_r2 = not args.no_upload and client is not None and bucket is not None

    if args.backfill:
        if client is None or bucket is None:
            log.error("--backfill requires R2 credentials and --from-r2")
            sys.exit(1)
        count = backfill_partitions(
            client,
            bucket,
            root,
            args.days,
            registry,
            args.output_dir,
            upload_r2,
        )
        log.info(f"Backfill complete: {count} partition(s) exported")
        return

    partition_date = resolve_partition_date(args)

    if args.input:
        merged = load_merged_from_file(args.input)
    elif args.from_r2 or client is not None:
        if client is None or bucket is None:
            log.error("R2 credentials required — set CF_R2_* env or use --input")
            sys.exit(1)
        merged = load_merged_from_r2(client, bucket, partition_date, root)
    else:
        log.error("Provide --from-r2, --input, or R2 env vars")
        sys.exit(1)

    export_partition(
        merged,
        partition_date,
        registry=registry,
        client=client,
        bucket=bucket,
        root=root,
        output_dir=args.output_dir,
        upload_r2=upload_r2,
    )
    log.info(f"Done — partition {partition_date}")


if __name__ == "__main__":
    main()
