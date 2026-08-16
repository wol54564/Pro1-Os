"""
aggregate_reports.py
====================
Merges daily monitor reports from all websites into one JSON file.

Reads site list from R2:
  monitor-sites/registry.yml

Each site's report:
  {r2_prefix}/monitor/{partition-date}/report.json   (partition = listing date + 1 day)

Non-daily sites (monthly motorgy, quarterly kcsb, every-2-days sheeel) may not
have a report for today's partition — the hub reuses the latest usable report
on or before that date. A same-day empty/zero report (monitor ran, scraper did
not) also triggers fallback (see registry schedule / report_fallback).

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


AUTOMOTIVE_FOLDER_NAMES = frozenset({
    "Automotive-Cars-and-Trucks",
    "Rest-Automotive-Part1",
    "Rest-Automotive-Part2",
    "Rest-Automotive-Part3",
    "Wanted-Cars",
    "bikes",
})


def _fmt_size_bytes(size_bytes: Optional[int]) -> str:
    if size_bytes is None:
        return "—"
    value = float(size_bytes)
    if value < 1024:
        return f"{int(value)} B"
    if value < 1024 ** 2:
        return f"{value / 1024:.1f} KB"
    if value < 1024 ** 3:
        return f"{value / (1024 ** 2):.1f} MB"
    return f"{value / (1024 ** 3):.2f} GB"


def _automotive_display_name(site: Dict, report: Optional[Dict]) -> Optional[str]:
    """Normalize selected folders to '<name> (automotive)' labels for hub UI/tables."""
    folder = str(site.get("folder") or "").strip()
    raw_name = (
        (report or {}).get("display_name")
        or site.get("display_name")
        or folder
        or None
    )
    if raw_name is None:
        return None

    label = str(raw_name).strip()
    if folder not in AUTOMOTIVE_FOLDER_NAMES:
        return label

    if label.lower().endswith("(automotive)"):
        return label
    return f"{label.lower()} (automotive)"


def _scraper_results(report: Dict) -> List[Dict]:
    """Normalize scrapers field — dict (Pro1-Os) or list (other repos)."""
    scrapers = report.get("scrapers", {})
    if isinstance(scrapers, list):
        return scrapers
    if isinstance(scrapers, dict):
        return list(scrapers.values())
    return []


def _to_int(value: object) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _to_float(value: object) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _extract_request_metrics(report: Dict, results: List[Dict]) -> Tuple[Optional[int], Optional[int], Optional[float], Optional[float]]:
    """Extract HTTP metrics from all known report shapes."""
    requests_total = _to_int(report.get("requests_total"))
    requests_failed = _to_int(report.get("requests_failed"))
    error_rate_pct = _to_float(report.get("error_rate_pct"))
    requests_per_min = _to_float(report.get("requests_per_min"))

    # Flattened shape used by some monitor reports.
    if requests_total is None:
        request_metrics = report.get("request_metrics")
        if isinstance(request_metrics, dict):
            requests_total = _to_int(request_metrics.get("requests_total"))
            requests_failed = _to_int(request_metrics.get("requests_failed"))
            error_rate_pct = _to_float(request_metrics.get("error_rate_pct"))
            requests_per_min = _to_float(request_metrics.get("requests_per_min"))

    # report_schema_version=2 shape uses error_summary.http.
    if requests_total is None:
        http = ((report.get("error_summary") or {}).get("http"))
        if isinstance(http, dict):
            requests_total = _to_int(http.get("requests_total"))
            requests_failed = _to_int(http.get("requests_failed"))
            error_rate_pct = _to_float(http.get("error_rate_pct"))
            requests_per_min = _to_float(http.get("requests_per_min"))

    # Legacy fallback: aggregate from scraper-level metrics.
    if requests_total is None and results:
        total = 0
        failed = 0
        rpm_values: List[float] = []
        found = False
        for sr in results:
            rt = _to_int(sr.get("requests_total"))
            if rt is None:
                continue
            found = True
            total += rt
            failed += _to_int(sr.get("requests_failed")) or 0
            rpm = _to_float(sr.get("requests_per_min"))
            if rpm is not None:
                rpm_values.append(rpm)
        if found:
            requests_total = total
            requests_failed = failed
            if total > 0:
                error_rate_pct = round(failed / total * 100.0, 2)
            if rpm_values:
                requests_per_min = round(sum(rpm_values) / len(rpm_values), 2)

    if requests_total is not None and requests_failed is None:
        requests_failed = 0
    if requests_total is not None and requests_total > 0 and error_rate_pct is None and requests_failed is not None:
        error_rate_pct = round(requests_failed / requests_total * 100.0, 2)

    return requests_total, requests_failed, error_rate_pct, requests_per_min


def _load_report_at_key(client, bucket: str, key: str) -> Dict:
    resp = client.get_object(Bucket=bucket, Key=key)
    return json.loads(resp["Body"].read().decode("utf-8"))


def _is_missing_object(exc: Exception, client) -> bool:
    nosuch = getattr(client.exceptions, "NoSuchKey", None)
    if nosuch is not None and isinstance(exc, nosuch):
        return True
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        code = str(response.get("Error", {}).get("Code", ""))
        return code in ("NoSuchKey", "404", "NotFound")
    return False


def _extract_unique_ads(report: Dict, results: Optional[List[Dict]] = None) -> Optional[int]:
    if results is None:
        results = _scraper_results(report)
    unique_ads = report.get("total_unique_ads")
    if unique_ads is None:
        unique_ads = _to_int(report.get("total_ads"))
    if unique_ads is None and isinstance(report.get("categories"), list):
        unique_ads = sum(
            _to_int(c.get("total_ads")) or 0
            for c in report.get("categories", [])
            if isinstance(c, dict)
        )
    if unique_ads is None:
        unique_ads = sum(_to_int(s.get("unique_ads")) or 0 for s in results)
    return _to_int(unique_ads)


def _report_is_empty(report: Optional[Dict]) -> bool:
    """True when a report exists but has no listing data (typical off-day for non-daily sites)."""
    if not report:
        return True
    results = _scraper_results(report)
    if _extract_unique_ads(report, results):
        return False
    phones = _to_int(report.get("total_unique_phones"))
    if phones is None:
        phones = sum(_to_int(s.get("unique_phones")) or 0 for s in results)
    if phones:
        return False
    if any((_to_int(s.get("files_found")) or 0) > 0 for s in results):
        return False
    if any((_to_int(s.get("unique_ads")) or 0) > 0 for s in results):
        return False
    daily = _to_int(report.get("r2_daily_size"))
    if daily is None:
        daily = sum(_to_int(s.get("r2_daily_size")) or 0 for s in results)
    return not bool(daily)


def fetch_report(
    client,
    bucket: str,
    site: Dict,
    partition_date: str,
) -> Tuple[Optional[Dict], str, bool]:
    """
    Fetch a site's report for the hub partition date.

    Returns (report, report_partition_date, used_fallback).
    Non-daily sites fall back to the latest *usable* report on or before
    partition_date (missing or empty/zero same-day reports trigger fallback).
    """
    label = site.get("folder", site.get("site_id"))
    key = report_r2_key(site, partition_date)
    data: Optional[Dict] = None
    try:
        data = _load_report_at_key(client, bucket, key)
    except Exception as exc:
        if _is_missing_object(exc, client):
            data = None
        else:
            log.warning(f"  ✗ {label}: {exc}")
            data = None

    today_usable = data is not None and not _report_is_empty(data)
    if today_usable:
        log.info(f"  ✓ {label}: r2://{bucket}/{key}")
        return data, partition_date, False

    if data is not None:
        log.info(
            f"  ○ {label}: empty/zero report at r2://{bucket}/{key} "
            f"— looking for last usable report"
        )
    elif not site_allows_report_fallback(site):
        log.warning(f"  ✗ {label}: no report at {key}")
        return None, partition_date, False

    if not site_allows_report_fallback(site):
        return data, partition_date, False

    max_dt = datetime.strptime(partition_date, "%Y-%m-%d")
    min_dt = max_dt - timedelta(days=report_lookback_days(site))
    try:
        all_dates = list_report_partition_dates(client, bucket, site)
    except Exception as exc:
        log.warning(f"  ✗ {label}: no report at {key} · could not list earlier reports ({exc})")
        return data, partition_date, False

    candidates = [
        d for d in reversed(all_dates)
        if min_dt <= datetime.strptime(d, "%Y-%m-%d") <= max_dt
    ]
    for fallback_date in candidates:
        if fallback_date == partition_date:
            continue
        fallback_key = report_r2_key(site, fallback_date)
        try:
            fallback_data = _load_report_at_key(client, bucket, fallback_key)
        except Exception as exc:
            log.warning(f"  ✗ {label}: failed to load fallback report at {fallback_key}: {exc}")
            continue
        if _report_is_empty(fallback_data):
            continue
        log.info(
            f"  ↩ {label}: r2://{bucket}/{fallback_key} "
            f"(latest usable within lookback; hub partition {partition_date})"
        )
        return fallback_data, fallback_date, True

    if data is not None:
        log.warning(
            f"  ✗ {label}: only empty reports in lookback "
            f"{min_dt.strftime('%Y-%m-%d')} … {partition_date}; using {key}"
        )
        return data, partition_date, False

    log.warning(
        f"  ✗ {label}: no report at {key} "
        f"and none usable in lookback {min_dt.strftime('%Y-%m-%d')} … {partition_date}"
    )
    return None, partition_date, False


def summarize_site(
    report: Optional[Dict],
    site: Dict,
    partition_date: str,
    report_partition_date: str,
    report_fallback: bool,
) -> Dict:
    display_name = _automotive_display_name(site, report)

    base = {
        "folder":       site.get("folder"),
        "site_id":      site.get("site_id"),
        "display_name": display_name,
        "website":      site.get("website"),
        "country":      site.get("country"),
        "repo":         site.get("repo"),
        "uses_proxy":   site.get("uses_proxy"),
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
            "unique_phones": 0,
            "valid_phones": 0,
            "invalid_phones": 0,
            "outside_country_phones": 0,
            "r2_file_count": 0,
            "r2_size_bytes": 0,
            "r2_daily_size": 0,
        }

    results = _scraper_results(report)
    reported_total = len(results)
    expected_total = _to_int(site.get("scrapers")) or reported_total
    total = reported_total if reported_total > 0 else expected_total

    requests_total, requests_failed, error_rate_pct, requests_per_min = _extract_request_metrics(report, results)

    passed = sum(1 for s in results if s.get("all_passed"))
    if reported_total == 0 and total > 0:
        # Summary-only site reports may omit per-scraper blocks entirely.
        overall_pass = report.get("overall_pass")
        if overall_pass is False:
            passed = 0
        elif requests_total is not None or report.get("total_ads") is not None:
            passed = total

    alerts  = report.get("alert_count", len(report.get("alerts", [])))
    unique_ads = _extract_unique_ads(report, results)
    unique_phones = report.get("total_unique_phones")
    if unique_phones is None:
        unique_phones = sum(s.get("unique_phones") or 0 for s in results)
    valid_phones = report.get("total_valid_phones")
    if valid_phones is None:
        valid_phones = sum(s.get("valid_phones") or 0 for s in results)
    invalid_phones = report.get("total_invalid_phones")
    if invalid_phones is None:
        invalid_phones = sum(s.get("invalid_phones") or 0 for s in results)
    outside_country_phones = report.get("total_outside_country_phones")
    if outside_country_phones is None:
        outside_country_phones = sum(s.get("outside_country_phones") or 0 for s in results)
    r2_file_count = report.get("total_r2_files")
    if r2_file_count is None:
        r2_file_count = sum(s.get("r2_file_count") or 0 for s in results)
    r2_size_bytes = report.get("total_r2_size_bytes")
    if r2_size_bytes is None:
        r2_size_bytes = sum(s.get("r2_size_bytes") or 0 for s in results)
    r2_daily_size = report.get("r2_daily_size")
    if r2_daily_size is None:
        r2_daily_size = sum(s.get("r2_daily_size") or 0 for s in results)

    scrapers_failed = max(total - passed, 0)

    return {
        **base,
        "display_name":    display_name,
        "website":         report.get("website") or site.get("website"),
        "country":         report.get("country") or site.get("country"),
        "repo":            report.get("repo") or site.get("repo"),
        "run_date":        report.get("run_date", report_partition_date),
        "inspect_date":    report.get("inspect_date"),
        "status":          "ok" if passed == total and total > 0 else "failed",
        "scrapers_total":  total,
        "scrapers_passed": passed,
        "scrapers_failed": scrapers_failed,
        "alert_count":     alerts,
        "unique_ads":      unique_ads,
        "unique_phones":   unique_phones,
        "valid_phones":    valid_phones,
        "invalid_phones":  invalid_phones,
        "outside_country_phones": outside_country_phones,
        "r2_file_count":   r2_file_count,
        "r2_size_bytes":   r2_size_bytes,
        "r2_daily_size":   r2_daily_size,
        "requests_total":  requests_total,
        "requests_failed": requests_failed,
        "error_rate_pct":  error_rate_pct,
        "requests_per_min": requests_per_min,
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
    total_unique_phones = sum(s.get("unique_phones") or 0 for s in site_summaries)
    total_valid_phones = sum(s.get("valid_phones") or 0 for s in site_summaries)
    total_invalid_phones = sum(s.get("invalid_phones") or 0 for s in site_summaries)
    total_outside_country_phones = sum(
        s.get("outside_country_phones") or 0 for s in site_summaries
    )
    total_r2_files = sum(s.get("r2_file_count") or 0 for s in site_summaries)
    total_r2_size_bytes = sum(s.get("r2_size_bytes") or 0 for s in site_summaries)
    total_r2_daily_size = sum(s.get("r2_daily_size") or 0 for s in site_summaries)
    total_requests = sum(s.get("requests_total") or 0 for s in site_summaries if s.get("requests_total"))
    total_requests_failed = sum(
        s.get("requests_failed") or 0 for s in site_summaries if s.get("requests_total")
    )
    rpm_values = [
        float(s["requests_per_min"])
        for s in site_summaries
        if s.get("requests_per_min") is not None
    ]
    avg_requests_per_min = (
        round(sum(rpm_values) / len(rpm_values), 2) if rpm_values else None
    )
    avg_error_rate_pct = (
        round(total_requests_failed / total_requests * 100.0, 2)
        if total_requests > 0
        else None
    )

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
        "total_unique_phones": total_unique_phones,
        "total_valid_phones": total_valid_phones,
        "total_invalid_phones": total_invalid_phones,
        "total_outside_country_phones": total_outside_country_phones,
        "total_r2_files": total_r2_files,
        "total_r2_size_bytes": total_r2_size_bytes,
        "total_r2_daily_size": total_r2_daily_size,
        "total_requests": total_requests or None,
        "total_requests_failed": total_requests_failed or None,
        "avg_error_rate_pct": avg_error_rate_pct,
        "avg_requests_per_min": avg_requests_per_min,
        "sites":         site_summaries,
    }

    key = upload_merged(client, bucket, partition_date, merged, root)

    print(f"\n{'SITE':<22} {'STATUS':<10} {'SCRAPERS':<12} {'REQ/MIN':<10} {'ERR%':<8} {'ADS':<10} ALERTS")
    print("-" * 88)
    for s in site_summaries:
        sc = f"{s['scrapers_passed']}/{s['scrapers_total']}"
        ads = s.get("unique_ads", 0)
        rpm = s.get("requests_per_min")
        rpm_str = f"{rpm:.1f}" if rpm is not None else "—"
        err = s.get("error_rate_pct")
        err_str = f"{err:.1f}%" if err is not None else "—"
        print(
            f"{s['display_name']:<22} {s['status']:<10} {sc:<12} {rpm_str:<10} "
            f"{err_str:<8} {ads:<10} {s['alert_count']}"
        )
    print("-" * 88)
    print(
        f"Hub summary: {sites_ok}/{len(sites)} sites OK · "
        f"{total_unique_ads} unique ads · {total_r2_files} R2 files · "
        f"{_fmt_size_bytes(total_r2_size_bytes)} R2 total · "
        f"{_fmt_size_bytes(total_r2_daily_size)} R2 daily · {total_alerts} total alerts"
    )
    if avg_requests_per_min is not None:
        print(
            f"Throughput: {avg_requests_per_min} avg req/min · "
            f"HTTP error rate {avg_error_rate_pct or 0}% · "
            f"{total_requests_failed}/{total_requests} failed requests"
        )
    print(f"Merged → r2://{bucket}/{key}\n")


if __name__ == "__main__":
    main()
