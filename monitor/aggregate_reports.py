"""
aggregate_reports.py
====================
Merges daily monitor reports from all websites into one JSON file.

Reads site list from R2:
  monitor-sites/registry.yml

Each site's daily report:
  {r2_prefix}/monitor/YYYY-MM-DD/report.json

Writes merged hub output to R2:
  monitor-sites/hub/YYYY-MM-DD/all-sites.json

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
from typing import Dict, List, Optional

from inspect_r2_schema import build_r2_client
from monitor_r2 import (
    MONITOR_SITES_ROOT,
    hub_merged_r2_key,
    load_registry_from_r2,
    put_bytes,
    report_r2_key,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("monitor-hub")


def fetch_report(client, bucket: str, site: Dict, run_date: str) -> Optional[Dict]:
    key = report_r2_key(site, run_date)
    try:
        resp = client.get_object(Bucket=bucket, Key=key)
        data = json.loads(resp["Body"].read().decode("utf-8"))
        log.info(f"  ✓ {site.get('folder', site.get('site_id'))}: r2://{bucket}/{key}")
        return data
    except client.exceptions.NoSuchKey:
        log.warning(f"  ✗ {site.get('folder', site.get('site_id'))}: no report at {key}")
        return None
    except Exception as exc:
        log.warning(f"  ✗ {site.get('folder', site.get('site_id'))}: {exc}")
        return None


def summarize_site(report: Optional[Dict], site: Dict, run_date: str) -> Dict:
    base = {
        "folder":       site.get("folder"),
        "site_id":      site.get("site_id"),
        "display_name": site.get("display_name"),
        "website":      site.get("website"),
        "country":      site.get("country"),
        "repo":         site.get("repo"),
        "run_date":     run_date,
    }
    if not report:
        return {
            **base,
            "status": "missing",
            "scrapers_total": 0,
            "scrapers_passed": 0,
            "alert_count": 0,
        }

    scrapers = report.get("scrapers", {})
    total    = len(scrapers)
    passed   = sum(1 for s in scrapers.values() if s.get("all_passed"))
    alerts   = report.get("alert_count", len(report.get("alerts", [])))

    return {
        **base,
        "display_name":    report.get("display_name") or site.get("display_name"),
        "website":         report.get("website") or site.get("website"),
        "country":         report.get("country") or site.get("country"),
        "repo":            report.get("repo") or site.get("repo"),
        "run_date":        report.get("run_date", run_date),
        "status":          "ok" if passed == total and total > 0 else "failed",
        "scrapers_total":  total,
        "scrapers_passed": passed,
        "alert_count":     alerts,
        "report":          report,
    }


def upload_merged(client, bucket: str, run_date: str, merged: Dict, root: str = MONITOR_SITES_ROOT) -> str:
    key  = hub_merged_r2_key(run_date, root)
    body = json.dumps(merged, ensure_ascii=False, indent=2, default=str).encode("utf-8")
    put_bytes(client, bucket, key, body, "application/json")
    return key


def parse_args():
    p = argparse.ArgumentParser(description="Merge all website monitor reports for the hub dashboard.")
    p.add_argument("--date", default=None, help="YYYY-MM-DD (default: yesterday UTC)")
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
        run_date = args.date
    else:
        run_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

    bucket = os.environ.get("CF_R2_BUCKET_NAME") or hub_cfg.get("r2_bucket") or env_bucket

    log.info(f"Aggregating {len(sites)} sites for {run_date} · hub prefix {root}/hub/ …")

    site_summaries = []
    for site in sites:
        report = fetch_report(client, bucket, site, run_date)
        site_summaries.append(summarize_site(report, site, run_date))

    sites_ok      = sum(1 for s in site_summaries if s["status"] == "ok")
    sites_missing = sum(1 for s in site_summaries if s["status"] == "missing")
    sites_failed  = sum(1 for s in site_summaries if s["status"] == "failed")
    total_alerts  = sum(s["alert_count"] for s in site_summaries)

    merged = {
        "run_date":      run_date,
        "generated_at":  datetime.utcnow().isoformat() + "Z",
        "hub_prefix":    root,
        "sites_total":   len(sites),
        "sites_ok":      sites_ok,
        "sites_failed":  sites_failed,
        "sites_missing": sites_missing,
        "total_alerts":  total_alerts,
        "sites":         site_summaries,
    }

    key = upload_merged(client, bucket, run_date, merged, root)

    print(f"\n{'SITE':<22} {'STATUS':<10} {'SCRAPERS':<12} ALERTS")
    print("-" * 55)
    for s in site_summaries:
        sc = f"{s['scrapers_passed']}/{s['scrapers_total']}"
        print(f"{s['display_name']:<22} {s['status']:<10} {sc:<12} {s['alert_count']}")
    print("-" * 55)
    print(f"Hub summary: {sites_ok}/{len(sites)} sites OK · {total_alerts} total alerts")
    print(f"Merged → r2://{bucket}/{key}\n")


if __name__ == "__main__":
    main()
