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
from github_workflows import (
    format_workflow_label,
    is_monitor_workflow,
    resolve_workflow_names,
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
    "total_unique_phones",
    "total_r2_files",
    "total_requests",
    "total_requests_failed",
    "avg_error_rate_pct",
    "avg_requests_per_min",
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
    "unique_phones",
    "r2_file_count",
    "requests_total",
    "requests_failed",
    "error_rate_pct",
    "requests_per_min",
    "scrapers_failed",
    "run_date",
    "inspect_date",
    "report_fallback",
    "uses_proxy",
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
    "unique_phones",
    "total_rows",
    "ads_source",
    "r2_file_count",
    "requests_total",
    "requests_failed",
    "error_rate_pct",
    "requests_per_min",
    "duration_sec",
    "metrics_source",
    "failed_items_summary",
]

SCRAPER_SUBCATEGORY_DAILY_COLS = [
    "hub_partition_date",
    "site_id",
    "scraper",
    "subcategory",
    "level_3",
    "ads_count",
    "sheet_rows",
    "sheets_count",
    "source",
]

SCRAPER_HOURLY_DAILY_COLS = [
    "hub_partition_date",
    "site_id",
    "scraper",
    "hour",
    "ads_count",
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

SKIP_SUBCATEGORY_SHEETS = frozenset({"info", "no data"})
GENERIC_SHEET_NAMES = frozenset({"sheet1", "main", "data", "listings", "all listings"})


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


def _status_to_workflow_status(site_status: Optional[str]) -> Optional[str]:
    if site_status == "ok":
        return "success"
    if site_status == "failed":
        return "failure"
    return None


def _duration_from_github_run(github_run: Dict) -> Optional[int]:
    # Prefer an explicit duration field
    duration = github_run.get("duration_sec")
    if duration is not None:
        try:
            return int(duration)
        except (TypeError, ValueError):
            pass

    mr = github_run.get("monitor_run") or {}
    if isinstance(mr, dict):
        monitor_duration = mr.get("duration_sec")
        if monitor_duration is not None:
            try:
                return int(monitor_duration)
            except (TypeError, ValueError):
                pass

    # Check monitor_run timestamps if present
    started = mr.get("started_at") or github_run.get("started_at")
    finished = mr.get("finished_at") or github_run.get("finished_at")
    if started and finished:
        try:
            start_dt = datetime.fromisoformat(str(started).replace("Z", "+00:00"))
            finish_dt = datetime.fromisoformat(str(finished).replace("Z", "+00:00"))
            return max(0, int((finish_dt - start_dt).total_seconds()))
        except (ValueError, TypeError):
            pass

    # If workflows detail exists, sum per-workflow durations or compute from their timestamps
    wf_list = github_run.get("workflows") or []
    total = 0
    found = False
    for w in wf_list:
        d = w.get("duration_sec")
        if d is not None:
            try:
                total += int(d)
                found = True
                continue
            except (TypeError, ValueError):
                pass
        # try run_started_at and updated_at
        rs = w.get("run_started_at") or w.get("started_at")
        ru = w.get("updated_at") or w.get("finished_at")
        if rs and ru:
            try:
                sdt = datetime.fromisoformat(str(rs).replace("Z", "+00:00"))
                edt = datetime.fromisoformat(str(ru).replace("Z", "+00:00"))
                total += max(0, int((edt - sdt).total_seconds()))
                found = True
            except (ValueError, TypeError):
                pass

    if found:
        return total

    return None


def _is_missing_workflow_name(value: Optional[str]) -> bool:
    if value is None:
        return True
    if not isinstance(value, str):
        return False
    stripped = value.strip()
    return not stripped or stripped in {"—", "-", "None", "null", "N/A", "n/a"}


def _site_registry_row(reg: Optional[Dict], report: Dict) -> Dict:
    if not reg:
        return {}
    if isinstance(reg.get("sites"), list):
        folder = (report.get("folder") or "").strip()
        site_id = (report.get("site_id") or "").strip()
        for row in reg["sites"]:
            if isinstance(row, dict):
                if folder and (row.get("folder") or "").strip() == folder:
                    return row
                if site_id and (row.get("site_id") or "").strip() == site_id:
                    return row
        return {}
    return reg


def _resolve_workflow_meta(
    report: Dict,
    reg: Dict,
    site_status: Optional[str],
    run_place: str,
) -> Dict[str, Any]:
    """
    Resolve CI/workflow fields for site_daily.

    Uses scraper pipeline metadata from report.github_run. Ignores Schema Monitor
    runs stored in older reports; falls back to registry workflows list.
    """
    github_run = report.get("github_run") or {}
    stored_name = github_run.get("workflow_name")
    site_reg = _site_registry_row(reg, report)

    workflow_name = None
    if not _is_missing_workflow_name(stored_name) and not is_monitor_workflow(stored_name):
        workflow_name = stored_name

    if not workflow_name:
        configured = resolve_workflow_names(site_reg) or resolve_workflow_names(reg) or resolve_workflow_names(report)
        workflow_name = format_workflow_label(configured) if configured else None

    if not workflow_name and github_run.get("workflows"):
        api_names = [
            str(w.get("name"))
            for w in github_run["workflows"]
            if w.get("name") and not is_monitor_workflow(str(w.get("name")))
        ]
        workflow_name = format_workflow_label(api_names) if api_names else None

    if not workflow_name:
        legacy = site_reg.get("workflow_name") or reg.get("workflow_name") or report.get("workflow_name")
        if legacy and not is_monitor_workflow(str(legacy)):
            workflow_name = str(legacy)

    if not workflow_name:
        workflow_name = "—"

    workflow_status = github_run.get("workflow_status")
    if is_monitor_workflow(stored_name):
        workflow_status = None

    if workflow_status is None:
        conclusions = [w.get("conclusion") for w in github_run.get("workflows", []) if isinstance(w, dict)]
        if conclusions:
            workflow_status = "success"
            for conclusion in conclusions:
                if conclusion in (None, "cancelled", "skipped"):
                    continue
                if conclusion not in ("success", "skipped"):
                    workflow_status = "failure"
                    break
        if workflow_status is None:
            workflow_status = _status_to_workflow_status(site_status)

    duration = _duration_from_github_run(github_run)
    if is_monitor_workflow(stored_name):
        duration = None

    run_id = github_run.get("workflow_run_id")
    run_number = github_run.get("workflow_run_number")
    if is_monitor_workflow(stored_name):
        run_id = None
        run_number = None

    return {
        "workflow_name": workflow_name,
        "workflow_run_number": run_number,
        "workflow_run_id": run_id,
        "workflow_status": workflow_status,
        "workflow_duration_sec": duration,
    }


def _site_request_fields(report: Dict, site: Dict) -> Dict[str, Any]:
    """Extract rolled-up request/error metrics from a site report."""
    results = _scraper_entries(report)
    scrapers_failed = sum(1 for _, sr in results if not sr.get("all_passed"))

    requests_total = report.get("requests_total")
    requests_failed = report.get("requests_failed")
    error_rate_pct = report.get("error_rate_pct")
    requests_per_min = report.get("requests_per_min")

    if requests_total is None and results:
        total = 0
        failed = 0
        rpm_values = []
        found = False
        for _, sr in results:
            rt = sr.get("requests_total")
            if rt is None:
                continue
            found = True
            total += int(rt)
            failed += int(sr.get("requests_failed") or 0)
            rpm = sr.get("requests_per_min")
            if rpm is not None:
                rpm_values.append(float(rpm))
        if found:
            requests_total = total
            requests_failed = failed
            if total > 0:
                error_rate_pct = round(failed / total * 100.0, 2)
            if rpm_values:
                requests_per_min = round(sum(rpm_values) / len(rpm_values), 2)

    return {
        "requests_total": requests_total,
        "requests_failed": requests_failed,
        "error_rate_pct": error_rate_pct,
        "requests_per_min": requests_per_min,
        "scrapers_failed": scrapers_failed,
    }


def _hub_request_totals(site_rows: List[Dict]) -> Dict[str, Any]:
    total_requests = 0
    total_failed = 0
    rpm_values = []
    err_values = []
    has_requests = False

    for row in site_rows:
        rt = row.get("requests_total")
        if rt is None:
            continue
        has_requests = True
        total_requests += int(rt)
        total_failed += int(row.get("requests_failed") or 0)
        rpm = row.get("requests_per_min")
        if rpm is not None:
            rpm_values.append(float(rpm))
        err = row.get("error_rate_pct")
        if err is not None:
            err_values.append(float(err))

    if not has_requests:
        return {
            "total_requests": None,
            "total_requests_failed": None,
            "avg_error_rate_pct": None,
            "avg_requests_per_min": None,
        }

    avg_error_rate_pct = None
    if total_requests > 0:
        avg_error_rate_pct = round(total_failed / total_requests * 100.0, 2)
    elif err_values:
        avg_error_rate_pct = round(sum(err_values) / len(err_values), 2)

    avg_requests_per_min = None
    if rpm_values:
        avg_requests_per_min = round(sum(rpm_values) / len(rpm_values), 2)

    return {
        "total_requests": total_requests,
        "total_requests_failed": total_failed,
        "avg_error_rate_pct": avg_error_rate_pct,
        "avg_requests_per_min": avg_requests_per_min,
    }


def _resolve_uses_proxy(site: Dict, reg: Dict, report: Dict) -> Optional[bool]:
    """True/False from merged site row, report, or registry; None if unset."""
    for src in (site, report, reg):
        val = src.get("uses_proxy")
        if val is not None:
            return bool(val)
    return None


def _to_non_negative_int(value: Any, default: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default


def _collect_json_subcategory_rows(
    hub_partition: str,
    site_id: str,
    scraper: str,
    breakdown: Any,
) -> List[Dict[str, Any]]:
    if not isinstance(breakdown, list):
        return []

    rows: List[Dict[str, Any]] = []
    for raw in breakdown:
        if not isinstance(raw, dict):
            continue
        subcategory = str(raw.get("subcategory") or "").strip()
        if not subcategory:
            continue

        level_3 = str(raw.get("level_3") or "").strip()
        ads_count = _to_non_negative_int(
            raw.get("ads_count", raw.get("listings_count", raw.get("count", 0)))
        )
        sheet_rows = _to_non_negative_int(raw.get("sheet_rows"), default=ads_count)
        sheets_count = _to_non_negative_int(raw.get("sheets_count"), default=1)

        rows.append({
            "hub_partition_date": hub_partition,
            "site_id": site_id,
            "scraper": scraper,
            "subcategory": subcategory,
            "level_3": level_3,
            "ads_count": ads_count,
            "sheet_rows": sheet_rows,
            "sheets_count": sheets_count,
            "source": str(raw.get("source") or "json_summary"),
        })
    return rows


def _subcategory_from_file_key(file_key: Any) -> str:
    if not file_key:
        return ""
    try:
        stem = Path(str(file_key)).stem
    except Exception:
        return ""
    return stem.replace("_", " ").replace("-", " ").strip().title()


def _is_generic_sheet_name(sheet_name: str) -> bool:
    return sheet_name.strip().lower() in GENERIC_SHEET_NAMES


def flatten_hub(
    merged: Dict,
    registry: Optional[Dict] = None,
) -> Tuple[Dict, List[Dict], List[Dict], List[Dict], List[Dict], List[Dict]]:
    """
    Flatten all-sites.json into hub_daily, site_daily, scraper_daily, scraper_hourly_daily,
    and alerts rows.

    Returns (hub_row, site_rows, scraper_rows, hourly_rows, subcategory_rows, alert_rows).
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
        "total_unique_phones": merged.get("total_unique_phones"),
        "total_r2_files": merged.get("total_r2_files"),
        "hub_prefix": merged.get("hub_prefix"),
    }

    reg_lookup = _registry_lookup(registry)
    site_rows: List[Dict] = []
    scraper_rows: List[Dict] = []
    scraper_hourly_rows: List[Dict] = []
    subcategory_rows: List[Dict] = []
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
        workflow_meta = _resolve_workflow_meta(
            report, reg, site.get("status"), run_place
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
            "workflow_name": workflow_meta["workflow_name"],
            "workflow_run_number": workflow_meta["workflow_run_number"],
            "workflow_run_id": workflow_meta["workflow_run_id"],
            "workflow_status": workflow_meta["workflow_status"],
            "workflow_duration_sec": workflow_meta["workflow_duration_sec"],
            "schedule": schedule,
            "status": site.get("status"),
            "scrapers_total": site.get("scrapers_total"),
            "scrapers_passed": site.get("scrapers_passed"),
            "alert_count": site.get("alert_count"),
            "unique_ads": site.get("unique_ads"),
            "unique_phones": site.get("unique_phones"),
            "r2_file_count": site.get("r2_file_count"),
            **_site_request_fields(report, site),
            "run_date": site.get("run_date"),
            "inspect_date": site.get("inspect_date"),
            "report_fallback": bool(site.get("report_fallback", False)),
            "uses_proxy": _resolve_uses_proxy(site, reg, report),
        })
        for scraper_name, sr in _scraper_entries(report):
            scraper_label = sr.get("scraper") or scraper_name
            scraper_rows.append({
                "hub_partition_date": hub_partition,
                "site_id": site_id,
                "scraper": scraper_label,
                "files_found": sr.get("files_found"),
                "checks_passed": sr.get("checks_passed"),
                "checks_total": sr.get("checks_total"),
                "all_passed": sr.get("all_passed"),
                "files_optional": bool(sr.get("files_optional", False)),
                "unique_ads": sr.get("unique_ads"),
                "unique_phones": sr.get("unique_phones"),
                "total_rows": sr.get("total_rows"),
                "ads_source": sr.get("ads_source"),
                "r2_file_count": sr.get("r2_file_count"),
                "requests_total": sr.get("requests_total"),
                "requests_failed": sr.get("requests_failed"),
                "error_rate_pct": sr.get("error_rate_pct"),
                "requests_per_min": sr.get("requests_per_min"),
                "duration_sec": sr.get("duration_sec"),
                "metrics_source": sr.get("metrics_source"),
                "failed_items_summary": sr.get("failed_items_summary"),
            })

            for hour, count in (sr.get("date_published_hour_counts") or {}).items():
                try:
                    hour_int = int(hour)
                except (TypeError, ValueError):
                    continue
                scraper_hourly_rows.append({
                    "hub_partition_date": hub_partition,
                    "site_id": site_id,
                    "scraper": scraper_label,
                    "hour": hour_int,
                    "ads_count": _to_non_negative_int(count),
                })

            json_rows = _collect_json_subcategory_rows(
                hub_partition,
                site_id,
                scraper_label,
                sr.get("subcategory_breakdown"),
            )
            if json_rows:
                subcategory_rows.extend(json_rows)
                continue

            subcat_agg: Dict[Tuple[str, str], Dict[str, int]] = {}
            for file_result in sr.get("file_results") or []:
                file_subcategory = _subcategory_from_file_key(
                    file_result.get("file_key") or file_result.get("file")
                )
                for sheet in file_result.get("sheets") or []:
                    raw_name = str(sheet.get("name") or "").strip()
                    if not raw_name:
                        continue
                    if raw_name.lower() in SKIP_SUBCATEGORY_SHEETS:
                        continue

                    sheet_rows = int(sheet.get("row_count") or 0)
                    if sheet_rows < 0:
                        sheet_rows = 0

                    # Prefer file-based grouping for multi-sheet workbooks (e.g., Toyota.xlsx + Hilux sheet).
                    if file_subcategory:
                        subcategory = file_subcategory
                        level_3 = "" if _is_generic_sheet_name(raw_name) else raw_name
                    else:
                        normalized = (
                            raw_name.replace(" > ", "/")
                            .replace("::", "/")
                            .replace(" - ", "/")
                        )
                        parts = [p.strip() for p in normalized.split("/") if p.strip()]
                        subcategory = parts[0] if parts else raw_name
                        level_3 = parts[1] if len(parts) > 1 else ""
                    agg_key = (subcategory, level_3)

                    bucket = subcat_agg.setdefault(
                        agg_key,
                        {
                            "sheet_rows": 0,
                            "sheets_count": 0,
                        },
                    )
                    bucket["sheet_rows"] += sheet_rows
                    bucket["sheets_count"] += 1

            for (subcategory, level_3), stats in subcat_agg.items():
                subcategory_rows.append({
                    "hub_partition_date": hub_partition,
                    "site_id": site_id,
                    "scraper": scraper_label,
                    "subcategory": subcategory,
                    "level_3": level_3,
                    "ads_count": stats["sheet_rows"],
                    "sheet_rows": stats["sheet_rows"],
                    "sheets_count": stats["sheets_count"],
                    "source": "sheet_rows",
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

    hub_row.update(_hub_request_totals(site_rows))

    return hub_row, site_rows, scraper_rows, scraper_hourly_rows, subcategory_rows, alert_rows


def _empty_df(columns: List[str]) -> pd.DataFrame:
    return pd.DataFrame({c: [] for c in columns})


def _to_dataframes(
    hub_row: Dict,
    site_rows: List[Dict],
    scraper_rows: List[Dict],
    scraper_hourly_rows: List[Dict],
    subcategory_rows: List[Dict],
    alert_rows: List[Dict],
) -> Dict[str, pd.DataFrame]:
    return {
        "hub_daily": pd.DataFrame([hub_row]) if hub_row else _empty_df(HUB_DAILY_COLS),
        "site_daily": pd.DataFrame(site_rows) if site_rows else _empty_df(SITE_DAILY_COLS),
        "scraper_daily": pd.DataFrame(scraper_rows) if scraper_rows else _empty_df(SCRAPER_DAILY_COLS),
        "scraper_hourly_daily": (
            pd.DataFrame(scraper_hourly_rows)
            if scraper_hourly_rows
            else _empty_df(SCRAPER_HOURLY_DAILY_COLS)
        ),
        "scraper_subcategory_daily": (
            pd.DataFrame(subcategory_rows)
            if subcategory_rows
            else _empty_df(SCRAPER_SUBCATEGORY_DAILY_COLS)
        ),
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

    hub_row, site_rows, scraper_rows, scraper_hourly_rows, subcategory_rows, alert_rows = flatten_hub(merged, registry)
    tables = _to_dataframes(hub_row, site_rows, scraper_rows, scraper_hourly_rows, subcategory_rows, alert_rows)

    log.info(
        f"Partition {partition_date}: "
        f"1 hub · {len(site_rows)} sites · {len(scraper_rows)} scrapers · "
        f"{len(scraper_hourly_rows)} hourly rows · {len(subcategory_rows)} subcategories · {len(alert_rows)} alerts"
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
