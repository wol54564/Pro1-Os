"""
aggregate_reports.py
====================
Merges daily monitor reports from all websites into one JSON file.

Reads site list from R2:
  monitor-sites/registry.yml

Each site's report:
  {r2_prefix}/monitor/{partition-date}/report.json   (partition = listing date + 1 day)

Non-daily sites (monthly motorgy, quarterly kcsb, every-2-days sheeel) may not
have a report for today's partition — the hub reuses the latest report on or
before that date (see registry schedule / report_fallback).

Writes merged hub output to R2:
  monitor-sites/hub/{partition-date}/all-sites.json

Usage
-----
  python monitor/aggregate_reports.py
  python monitor/aggregate_reports.py --date 2026-06-13
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from monitor_r2 import (
    MONITOR_SITES_ROOT,
    build_r2_client,
    hub_merged_r2_key,
    list_report_partition_dates,
    load_registry_from_r2,
    partition_date_for_listing,
    put_bytes,
    report_lookback_days,
    report_r2_key,
    site_allows_report_fallback,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("monitor-hub")


def _scraper_results(report: Dict) -> List[Dict]:
    """Normalize scrapers field — dict (Pro1-Os) or list (other repos)."""
    scrapers = report.get("scrapers", {})
    if isinstance(scrapers, list):
        return scrapers
    if isinstance(scrapers, dict):
        return list(scrapers.values())
    return []


def _load_report_at_key(client, bucket: str, key: str) -> Dict:
    resp = client.get_object(Bucket=bucket, Key=key)
    return json.loads(resp["Body"].read().decode("utf-8"))


def fetch_report(
    client,
    bucket: str,
    site: Dict,
    partition_date: str,
) -> Tuple[Optional[Dict], str, bool]:
    """
    Fetch a site's report for the hub partition date.

    Returns (report, report_partition_date, used_fallback).
    Non-daily sites fall back to the latest report on or before partition_date.
    """
    label = site.get("folder", site.get("site_id"))
    key = report_r2_key(site, partition_date)
    try:
        data = _load_report_at_key(client, bucket, key)
        log.info(f"  ✓ {label}: r2://{bucket}/{key}")
        return data, partition_date, False
    except client.exceptions.NoSuchKey:
        pass
    except Exception as exc:
        log.warning(f"  ✗ {label}: {exc}")
        return None, partition_date, False

    if not site_allows_report_fallback(site):
        log.warning(f"  ✗ {label}: no report at {key}")
        return None, partition_date, False

    max_dt = datetime.strptime(partition_date, "%Y-%m-%d")
    min_dt = max_dt - timedelta(days=report_lookback_days(site))
    try:
        all_dates = list_report_partition_dates(client, bucket, site)
    except Exception as exc:
        log.warning(f"  ✗ {label}: no report at {key} · could not list earlier reports ({exc})")
        return None, partition_date, False

    candidates = [
        d for d in all_dates
        if min_dt <= datetime.strptime(d, "%Y-%m-%d") <= max_dt
    ]
    if not candidates:
        log.warning(
            f"  ✗ {label}: no report at {key} "
            f"and none in lookback {min_dt.strftime('%Y-%m-%d')} … {partition_date}"
        )
        return None, partition_date, False

    fallback_date = candidates[-1]
    fallback_key = report_r2_key(site, fallback_date)
    try:
        data = _load_report_at_key(client, bucket, fallback_key)
        log.info(
            f"  ↩ {label}: r2://{bucket}/{fallback_key} "
            f"(latest within lookback; hub partition {partition_date})"
        )
        return data, fallback_date, True
    except Exception as exc:
        log.warning(f"  ✗ {label}: failed to load fallback report at {fallback_key}: {exc}")
        return None, partition_date, False


def summarize_site(
    report: Optional[Dict],
    site: Dict,
    partition_date: str,
    report_partition_date: str,
    report_fallback: bool,
) -> Dict:
    base = {
        "folder":       site.get("folder"),
        "site_id":      site.get("site_id"),
        "display_name": site.get("display_name"),
        "website":      site.get("website"),
        "country":      site.get("country"),
        "repo":         site.get("repo"),
        "run_date":     report_partition_date,
        "hub_partition_date": partition_date,
    }
    if report_fallback:
        base["report_fallback"] = True
    if not report:
        return {
            **base,
            "status": "missing",
            "scrapers_total": 0,
            "scrapers_passed": 0,
            "alert_count": 0,
            "unique_ads": 0,
        }

    results = _scraper_results(report)
    total   = len(results)
    passed  = sum(1 for s in results if s.get("all_passed"))
    alerts  = report.get("alert_count", len(report.get("alerts", [])))
    unique_ads = report.get("total_unique_ads")
    if unique_ads is None:
        unique_ads = sum(s.get("unique_ads") or 0 for s in results)

    return {
        **base,
        "display_name":    report.get("display_name") or site.get("display_name"),
        "website":         report.get("website") or site.get("website"),
        "country":         report.get("country") or site.get("country"),
        "repo":            report.get("repo") or site.get("repo"),
        "run_date":        report.get("run_date", report_partition_date),
        "inspect_date":    report.get("inspect_date"),
        "status":          "ok" if passed == total and total > 0 else "failed",
        "scrapers_total":  total,
        "scrapers_passed": passed,
        "alert_count":     alerts,
        "unique_ads":      unique_ads,
        "report":          report,
    }


def upload_merged(client, bucket: str, run_date: str, merged: Dict, root: str = MONITOR_SITES_ROOT) -> str:
    key  = hub_merged_r2_key(run_date, root)
    body = json.dumps(merged, ensure_ascii=False, indent=2, default=str).encode("utf-8")
    put_bytes(client, bucket, key, body, "application/json")
    return key


def parse_args():
    p = argparse.ArgumentParser(description="Merge all website monitor reports for the hub dashboard.")
    p.add_argument(
        "--date",
        default=None,
        help="Listing date to aggregate (YYYY-MM-DD). Default: yesterday UTC → today's partition folder.",
    )
    return p.parse_args()


def main():
    args = parse_args()
    client, env_bucket = build_r2_client()
    registry = load_registry_from_r2(client, env_bucket)

    hub_cfg  = registry.get("hub", {})
    sites: List[Dict] = registry.get("sites", [])
    root     = hub_cfg.get("monitor_sites_prefix", MONITOR_SITES_ROOT)

    if not sites:
        log.error("No sites in registry — check monitor-sites/registry.yml in R2")
        sys.exit(1)

    if args.date:
        listing_dt = datetime.strptime(args.date, "%Y-%m-%d")
    else:
        listing_dt = datetime.utcnow() - timedelta(days=1)
    partition_date = partition_date_for_listing(listing_dt).strftime("%Y-%m-%d")
    listing_date   = listing_dt.strftime("%Y-%m-%d")

    bucket = os.environ.get("CF_R2_BUCKET_NAME") or hub_cfg.get("r2_bucket") or env_bucket

    log.info(
        f"Aggregating {len(sites)} sites for listing {listing_date} "
        f"· partition {partition_date} · hub prefix {root}/hub/ …"
    )

    site_summaries = []
    for site in sites:
        report, report_partition_date, used_fallback = fetch_report(
            client, bucket, site, partition_date
        )
        site_summaries.append(
            summarize_site(report, site, partition_date, report_partition_date, used_fallback)
        )

    sites_ok      = sum(1 for s in site_summaries if s["status"] == "ok")
    sites_missing = sum(1 for s in site_summaries if s["status"] == "missing")
    sites_failed  = sum(1 for s in site_summaries if s["status"] == "failed")
    total_alerts  = sum(s["alert_count"] for s in site_summaries)
    total_unique_ads = sum(s.get("unique_ads") or 0 for s in site_summaries)

    merged = {
        "run_date":      partition_date,
        "inspect_date":  listing_date,
        "generated_at":  datetime.utcnow().isoformat() + "Z",
        "hub_prefix":    root,
        "sites_total":   len(sites),
        "sites_ok":      sites_ok,
        "sites_failed":  sites_failed,
        "sites_missing": sites_missing,
        "total_alerts":  total_alerts,
        "total_unique_ads": total_unique_ads,
        "sites":         site_summaries,
    }

    key = upload_merged(client, bucket, partition_date, merged, root)

    print(f"\n{'SITE':<22} {'STATUS':<10} {'SCRAPERS':<12} {'ADS':<10} ALERTS")
    print("-" * 65)
    for s in site_summaries:
        sc = f"{s['scrapers_passed']}/{s['scrapers_total']}"
        ads = s.get("unique_ads", 0)
        print(f"{s['display_name']:<22} {s['status']:<10} {sc:<12} {ads:<10} {s['alert_count']}")
    print("-" * 65)
    print(
        f"Hub summary: {sites_ok}/{len(sites)} sites OK · "
        f"{total_unique_ads} unique ads · {total_alerts} total alerts"
    )
    print(f"Merged → r2://{bucket}/{key}\n")


if __name__ == "__main__":
    main()
