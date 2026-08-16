"""
validate_phones.py
==================
Validate phone numbers and ad/listing IDs in scraper Excel files on R2.

Uses the same site config + partition layout as the schema monitor, but is a
separate pipeline under validation/ with its own workflow.

Rules follow CFl_data shapes (Kuwait 965 + 8 digits with local first digit
2/4/5/6/9; id / user_adv_id columns).

Usage
-----
  python validation/validate_phones.py
  python validation/validate_phones.py --date 2026-06-04
  python validation/validate_phones.py --days-lookback 3 --fail-on-error

Required env vars
-----------------
  CF_R2_ACCESS_KEY_ID, CF_R2_SECRET_ACCESS_KEY,
  CF_R2_ENDPOINT_URL,  CF_R2_BUCKET_NAME
  MONITOR_SITE_SLUG    — e.g. 4sale

Optional
--------
  MONITOR_ALERT_WEBHOOK_URL
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import pandas as pd
import yaml

# Reuse monitor R2 helpers (same bucket / site layout).
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "monitor"))

from monitor_r2 import (  # noqa: E402
    build_r2_client,
    load_registry_from_r2,
    load_site_config_from_r2,
    monitor_data_keys,
    partition_date_for_listing,
    put_bytes,
)
from github_workflows import merge_registry_site  # noqa: E402

from ads_rules import (  # noqa: E402
    STATUS_DUPLICATE,
    STATUS_MALFORMED as AD_MALFORMED,
    STATUS_MISSING as AD_MISSING,
    STATUS_VALID as AD_VALID,
    classify_ad_id,
    find_id_column,
)
from phone_rules import (  # noqa: E402
    STATUS_HIDDEN,
    STATUS_MISSING as PHONE_MISSING,
    STATUS_VALID as PHONE_VALID,
    classify_phone,
    find_hide_column,
    find_phone_columns,
    _truthy_hide,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("validation")

SKIP_SHEETS = frozenset({"info", "no data"})
SAMPLE_INVALID_LIMIT = 25
DEFAULT_MAX_INVALID_PCT = 15.0


def r2_base_prefix(r2_path_raw: str) -> str:
    path = (r2_path_raw or "").strip()
    if path.startswith("{"):
        path = path.split("/", 1)[1] if "/" in path else path
    return path.strip("/")


def excel_prefixes_for_date(base: str, dt: datetime) -> List[str]:
    seen: set = set()
    prefixes: List[str] = []
    for month in (f"{dt.month:02d}", str(dt.month)):
        for day in (f"{dt.day:02d}", str(dt.day)):
            prefix = f"{base}/year={dt.year}/month={month}/day={day}/"
            if prefix not in seen:
                seen.add(prefix)
                prefixes.append(prefix)
    return prefixes


def list_excel_files(client, bucket: str, prefix: str) -> List[Dict]:
    results = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".xlsx"):
                results.append(
                    {
                        "key": obj["Key"],
                        "size_bytes": obj["Size"],
                        "last_modified": obj["LastModified"].isoformat(),
                    }
                )
    return results


def download_excel(client, bucket: str, key: str) -> Optional[bytes]:
    try:
        resp = client.get_object(Bucket=bucket, Key=key)
        return resp["Body"].read()
    except Exception as exc:
        log.warning(f"Could not download {key}: {exc}")
        return None


def load_config(client, bucket: str, config_key: str) -> Dict:
    try:
        resp = client.get_object(Bucket=bucket, Key=config_key)
        return yaml.safe_load(resp["Body"].read().decode("utf-8")) or {}
    except Exception as exc:
        raise FileNotFoundError(
            f"websites-config.yml not found at r2://{bucket}/{config_key}: {exc}"
        ) from exc


def validation_report_key(site: Dict, partition_date: str) -> str:
    base = monitor_data_keys(site)["base"].rsplit("/monitor", 1)[0]
    # Prefer site r2_prefix/validation/... so it stays next to monitor artifacts.
    prefix = site.get("r2_prefix", "").strip("/")
    if not prefix:
        prefix = base
    return f"{prefix}/validation/{partition_date}/phone-ads-report.json"


def _empty_phone_stats() -> Dict[str, Any]:
    return {
        "values_seen": 0,
        "valid": 0,
        "invalid": 0,
        "missing": 0,
        "hidden": 0,
        "unique_valid": 0,
        "by_reason": {},
        "sample_invalid": [],
        "columns_found": [],
    }


def _empty_ads_stats() -> Dict[str, Any]:
    return {
        "values_seen": 0,
        "valid": 0,
        "invalid": 0,
        "missing": 0,
        "duplicates": 0,
        "unique_valid": 0,
        "by_reason": {},
        "sample_invalid": [],
        "id_column": None,
    }


def _bump(counter: Dict[str, int], key: str, n: int = 1) -> None:
    counter[key] = counter.get(key, 0) + n


def _add_sample(samples: List[Dict], item: Dict, limit: int = SAMPLE_INVALID_LIMIT) -> None:
    if len(samples) < limit:
        samples.append(item)


def validate_excel_bytes(raw: bytes, file_key: str) -> Dict[str, Any]:
    """Scan one Excel workbook for phone + ad ID validity."""
    phone_stats = _empty_phone_stats()
    ads_stats = _empty_ads_stats()
    phone_cols_seen: Set[str] = set()
    unique_phones: Set[str] = set()
    unique_ads: Set[str] = set()
    seen_ads_in_file: Set[str] = set()
    rows_scanned = 0
    sheets_scanned = 0

    try:
        xl = pd.ExcelFile(io.BytesIO(raw), engine="openpyxl")
    except Exception as exc:
        return {
            "file_key": file_key,
            "readable": False,
            "error": str(exc),
            "rows_scanned": 0,
            "sheets_scanned": 0,
            "phones": phone_stats,
            "ads": ads_stats,
        }

    for sheet_name in xl.sheet_names:
        if sheet_name.strip().lower() in SKIP_SHEETS:
            continue
        try:
            df = pd.read_excel(xl, sheet_name=sheet_name, engine="openpyxl")
        except Exception as exc:
            log.debug(f"Sheet '{sheet_name}' skipped: {exc}")
            continue
        if df.empty:
            continue

        sheets_scanned += 1
        rows_scanned += len(df)

        phone_cols = find_phone_columns(df.columns)
        hide_col = find_hide_column(df.columns)
        id_col = find_id_column(df.columns)

        for col in phone_cols:
            phone_cols_seen.add(str(col))

        if id_col and not ads_stats["id_column"]:
            ads_stats["id_column"] = str(id_col)

        for idx, row in df.iterrows():
            is_hidden = _truthy_hide(row[hide_col]) if hide_col is not None else False

            if phone_cols:
                # One logical contact per row: first non-empty phone column wins.
                status, normalized, raw_val, col = (
                    PHONE_MISSING,
                    None,
                    None,
                    phone_cols[0],
                )
                for candidate in phone_cols:
                    st, norm = classify_phone(row[candidate], is_hidden=is_hidden)
                    if st != PHONE_MISSING:
                        status, normalized, raw_val, col = st, norm, row[candidate], candidate
                        break
                else:
                    status, normalized = classify_phone(None, is_hidden=is_hidden)

                phone_stats["values_seen"] += 1
                _bump(phone_stats["by_reason"], status)
                if status == PHONE_VALID:
                    phone_stats["valid"] += 1
                    if normalized:
                        unique_phones.add(normalized)
                elif status == STATUS_HIDDEN:
                    phone_stats["hidden"] += 1
                elif status == PHONE_MISSING:
                    phone_stats["missing"] += 1
                else:
                    phone_stats["invalid"] += 1
                    _add_sample(
                        phone_stats["sample_invalid"],
                        {
                            "file": file_key,
                            "sheet": sheet_name,
                            "row": int(idx) + 2,
                            "column": str(col),
                            "value": "" if raw_val is None else str(raw_val)[:80],
                            "reason": status,
                        },
                    )

            if id_col is not None:
                ads_stats["values_seen"] += 1
                status, normalized = classify_ad_id(row[id_col])
                if status == AD_VALID and normalized is not None:
                    if normalized in seen_ads_in_file:
                        status = STATUS_DUPLICATE
                        ads_stats["duplicates"] += 1
                        ads_stats["invalid"] += 1
                        _bump(ads_stats["by_reason"], STATUS_DUPLICATE)
                        _add_sample(
                            ads_stats["sample_invalid"],
                            {
                                "file": file_key,
                                "sheet": sheet_name,
                                "row": int(idx) + 2,
                                "column": str(id_col),
                                "value": str(row[id_col])[:80],
                                "reason": STATUS_DUPLICATE,
                            },
                        )
                    else:
                        seen_ads_in_file.add(normalized)
                        unique_ads.add(normalized)
                        ads_stats["valid"] += 1
                        _bump(ads_stats["by_reason"], AD_VALID)
                elif status == AD_MISSING:
                    ads_stats["missing"] += 1
                    ads_stats["invalid"] += 1
                    _bump(ads_stats["by_reason"], AD_MISSING)
                    _add_sample(
                        ads_stats["sample_invalid"],
                        {
                            "file": file_key,
                            "sheet": sheet_name,
                            "row": int(idx) + 2,
                            "column": str(id_col),
                            "value": "",
                            "reason": AD_MISSING,
                        },
                    )
                else:
                    ads_stats["invalid"] += 1
                    _bump(ads_stats["by_reason"], status or AD_MALFORMED)
                    _add_sample(
                        ads_stats["sample_invalid"],
                        {
                            "file": file_key,
                            "sheet": sheet_name,
                            "row": int(idx) + 2,
                            "column": str(id_col),
                            "value": str(row[id_col])[:80],
                            "reason": status,
                        },
                    )

    phone_stats["columns_found"] = sorted(phone_cols_seen)
    phone_stats["unique_valid"] = len(unique_phones)
    ads_stats["unique_valid"] = len(unique_ads)

    return {
        "file_key": file_key,
        "readable": True,
        "error": None,
        "rows_scanned": rows_scanned,
        "sheets_scanned": sheets_scanned,
        "phones": phone_stats,
        "ads": ads_stats,
        "_unique_phones": unique_phones,
        "_unique_ads": unique_ads,
    }


def merge_stats(files: List[Dict[str, Any]]) -> Dict[str, Any]:
    phones = _empty_phone_stats()
    ads = _empty_ads_stats()
    all_phones: Set[str] = set()
    all_ads: Set[str] = set()
    cols: Set[str] = set()
    id_col = None
    rows = 0
    sheets = 0
    readable_files = 0

    for f in files:
        if f.get("readable"):
            readable_files += 1
        rows += f.get("rows_scanned") or 0
        sheets += f.get("sheets_scanned") or 0
        p = f.get("phones") or {}
        a = f.get("ads") or {}
        phones["values_seen"] += p.get("values_seen") or 0
        phones["valid"] += p.get("valid") or 0
        phones["invalid"] += p.get("invalid") or 0
        phones["missing"] += p.get("missing") or 0
        phones["hidden"] += p.get("hidden") or 0
        for reason, n in (p.get("by_reason") or {}).items():
            _bump(phones["by_reason"], reason, n)
        for sample in p.get("sample_invalid") or []:
            _add_sample(phones["sample_invalid"], sample)
        for c in p.get("columns_found") or []:
            cols.add(c)
        all_phones |= f.get("_unique_phones") or set()

        ads["values_seen"] += a.get("values_seen") or 0
        ads["valid"] += a.get("valid") or 0
        ads["invalid"] += a.get("invalid") or 0
        ads["missing"] += a.get("missing") or 0
        ads["duplicates"] += a.get("duplicates") or 0
        for reason, n in (a.get("by_reason") or {}).items():
            _bump(ads["by_reason"], reason, n)
        for sample in a.get("sample_invalid") or []:
            _add_sample(ads["sample_invalid"], sample)
        if a.get("id_column") and not id_col:
            id_col = a["id_column"]
        all_ads |= f.get("_unique_ads") or set()

    phones["columns_found"] = sorted(cols)
    phones["unique_valid"] = len(all_phones)
    ads["id_column"] = id_col
    ads["unique_valid"] = len(all_ads)

    def _pct(num: int, den: int) -> float:
        return round((num / den) * 100.0, 2) if den else 0.0

    phones["valid_pct"] = _pct(phones["valid"], phones["values_seen"])
    phones["invalid_pct"] = _pct(phones["invalid"], phones["values_seen"])
    ads["valid_pct"] = _pct(ads["valid"], ads["values_seen"])
    ads["invalid_pct"] = _pct(ads["invalid"], ads["values_seen"])

    return {
        "files_readable": readable_files,
        "rows_scanned": rows,
        "sheets_scanned": sheets,
        "phones": phones,
        "ads": ads,
    }


def send_alert(webhook_url: str, report: Dict[str, Any]) -> None:
    failing = [
        name
        for name, s in (report.get("scrapers") or {}).items()
        if not s.get("passed", True)
    ]
    if not failing:
        return
    payload = {
        "text": (
            f"Phone/ads validation failed for {report.get('website') or report.get('folder')} "
            f"listing {report.get('inspect_date')}: {', '.join(failing[:20])}"
            + ("…" if len(failing) > 20 else "")
        ),
        "failing_scrapers": failing,
        "report_date": report.get("run_date"),
        "inspect_date": report.get("inspect_date"),
        "totals": report.get("totals"),
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            log.info(f"Alert webhook status: {resp.status}")
    except urllib.error.URLError as exc:
        log.warning(f"Alert webhook failed: {exc}")


def write_step_summary(report: Dict[str, Any]) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    totals = report.get("totals") or {}
    phones = totals.get("phones") or {}
    ads = totals.get("ads") or {}
    lines = [
        "## Phone & ads number validation",
        "",
        f"- Listing date: `{report.get('inspect_date')}`",
        f"- Report partition: `{report.get('run_date')}`",
        f"- Scrapers: **{totals.get('scrapers_passed', 0)}/{totals.get('scrapers_total', 0)}** passed",
        f"- Phones valid: **{phones.get('valid', 0)}** / {phones.get('values_seen', 0)} "
        f"({phones.get('valid_pct', 0)}%) · invalid {phones.get('invalid', 0)} · "
        f"missing {phones.get('missing', 0)} · hidden {phones.get('hidden', 0)}",
        f"- Ads IDs valid: **{ads.get('valid', 0)}** / {ads.get('values_seen', 0)} "
        f"({ads.get('valid_pct', 0)}%) · invalid {ads.get('invalid', 0)} · "
        f"duplicates {ads.get('duplicates', 0)}",
        "",
        "| Scraper | Rows | Phone valid% | Phone invalid | Ads valid% | Ads invalid | Pass |",
        "|---|---:|---:|---:|---:|---:|:---:|",
    ]
    for name, s in sorted((report.get("scrapers") or {}).items()):
        p = s.get("phones") or {}
        a = s.get("ads") or {}
        lines.append(
            f"| {name} | {s.get('rows_scanned', 0)} | {p.get('valid_pct', 0)} | "
            f"{p.get('invalid', 0)} | {a.get('valid_pct', 0)} | {a.get('invalid', 0)} | "
            f"{'✅' if s.get('passed') else '❌'} |"
        )
    with open(path, "a", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


def parse_args():
    p = argparse.ArgumentParser(
        description="Validate phone numbers and ad IDs in R2 Excel exports."
    )
    p.add_argument("--date", default=None, help="YYYY-MM-DD listing date (default: yesterday)")
    p.add_argument("--days-lookback", type=int, default=1, help="Days to scan (default: 1)")
    p.add_argument(
        "--max-invalid-phone-pct",
        type=float,
        default=DEFAULT_MAX_INVALID_PCT,
        help="Fail scraper when invalid phones exceed this %% of values seen",
    )
    p.add_argument(
        "--max-invalid-ads-pct",
        type=float,
        default=DEFAULT_MAX_INVALID_PCT,
        help="Fail scraper when invalid ad IDs exceed this %% of values seen",
    )
    p.add_argument("--fail-on-error", action="store_true", help="Exit 1 if any scraper fails")
    p.add_argument("--no-alert", action="store_true", help="Skip webhook alerts")
    p.add_argument("--site-slug", default=None, help="Override MONITOR_SITE_SLUG")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    r2_client, bucket = build_r2_client()

    site = load_site_config_from_r2(r2_client, bucket, args.site_slug)
    try:
        registry = load_registry_from_r2(r2_client, bucket)
    except FileNotFoundError:
        registry = None
    site = merge_registry_site(site, registry)
    keys = monitor_data_keys(site)

    if args.date:
        end_date = datetime.strptime(args.date, "%Y-%m-%d")
    else:
        end_date = datetime.utcnow() - timedelta(days=1)

    dates_to_check = [end_date - timedelta(days=i) for i in range(args.days_lookback)]
    listing_date_str = end_date.strftime("%Y-%m-%d")
    report_date_str = partition_date_for_listing(end_date).strftime("%Y-%m-%d")

    log.info(
        f"Validating phones/ads for listing date(s) "
        f"{[d.strftime('%Y-%m-%d') for d in dates_to_check]} "
        f"· report → validation/{report_date_str}/phone-ads-report.json"
    )

    config = load_config(r2_client, bucket, keys["config"])
    scrapers_cfg = config.get("scrapers", [])
    meta = config.get("meta", {})

    report: Dict[str, Any] = {
        "run_date": report_date_str,
        "inspect_date": listing_date_str,
        "folder": site.get("folder"),
        "site_id": site.get("site_id"),
        "website": meta.get("website") or site.get("website"),
        "country": meta.get("country") or site.get("country") or "KW",
        "display_name": site.get("display_name"),
        "rules": {
            "phone": "Kuwait E.164: 965 + 8 digits; local prefixes 2/5/6/9",
            "ads": "Numeric listing id (min 3 digits); duplicates flagged per file",
            "max_invalid_phone_pct": args.max_invalid_phone_pct,
            "max_invalid_ads_pct": args.max_invalid_ads_pct,
        },
        "scrapers": {},
        "alerts": [],
    }

    log.info(f"Processing {len(scrapers_cfg)} scrapers …")

    for scraper_cfg in scrapers_cfg:
        scraper_name = scraper_cfg["name"]
        r2_base = r2_base_prefix(scraper_cfg.get("r2_path", ""))
        if not r2_base:
            log.warning(f"  {scraper_name}: no r2_path — skipping")
            continue

        all_xlsx: List[Dict] = []
        seen_keys: set = set()
        for dt in dates_to_check:
            part_dt = partition_date_for_listing(dt)
            for prefix in excel_prefixes_for_date(r2_base, part_dt):
                for f in list_excel_files(r2_client, bucket, prefix):
                    if f["key"] in seen_keys:
                        continue
                    seen_keys.add(f["key"])
                    all_xlsx.append(f)

        file_results: List[Dict[str, Any]] = []
        if not all_xlsx:
            log.warning(f"  {scraper_name}: no Excel files found")
            merged = merge_stats([])
            passed = bool(scraper_cfg.get("files_optional", False))
            scraper_entry = {
                "scraper": scraper_name,
                "files_found": 0,
                "passed": passed,
                "fail_reasons": [] if passed else ["no_excel_files"],
                **merged,
                "file_results": [],
            }
        else:
            for meta_f in all_xlsx:
                raw = download_excel(r2_client, bucket, meta_f["key"])
                if raw is None:
                    file_results.append(
                        {
                            "file_key": meta_f["key"],
                            "readable": False,
                            "error": "download_failed",
                            "rows_scanned": 0,
                            "sheets_scanned": 0,
                            "phones": _empty_phone_stats(),
                            "ads": _empty_ads_stats(),
                        }
                    )
                    continue
                file_results.append(validate_excel_bytes(raw, meta_f["key"]))

            merged = merge_stats(file_results)
            fail_reasons: List[str] = []
            phones = merged["phones"]
            ads = merged["ads"]

            if phones["values_seen"] == 0 and ads["values_seen"] == 0:
                fail_reasons.append("no_phone_or_id_columns")
            if phones["values_seen"] > 0 and phones["invalid_pct"] > args.max_invalid_phone_pct:
                fail_reasons.append(
                    f"phone_invalid_pct={phones['invalid_pct']}>{args.max_invalid_phone_pct}"
                )
            if ads["values_seen"] > 0 and ads["invalid_pct"] > args.max_invalid_ads_pct:
                fail_reasons.append(
                    f"ads_invalid_pct={ads['invalid_pct']}>{args.max_invalid_ads_pct}"
                )

            passed = not fail_reasons
            scraper_entry = {
                "scraper": scraper_name,
                "files_found": len(all_xlsx),
                "passed": passed,
                "fail_reasons": fail_reasons,
                **merged,
                # Drop internal sets before JSON serialize
                "file_results": [
                    {k: v for k, v in fr.items() if not k.startswith("_")}
                    for fr in file_results
                ],
            }

            log.info(
                f"  {scraper_name}: files={len(all_xlsx)} rows={merged['rows_scanned']} "
                f"phones={phones['valid']}/{phones['values_seen']} "
                f"({phones['valid_pct']}%) ads={ads['valid']}/{ads['values_seen']} "
                f"({ads['valid_pct']}%) {'PASS' if passed else 'FAIL ' + ','.join(fail_reasons)}"
            )

        report["scrapers"][scraper_name] = scraper_entry
        if not scraper_entry["passed"]:
            report["alerts"].append(
                {
                    "scraper": scraper_name,
                    "reasons": scraper_entry["fail_reasons"],
                }
            )

    # Hub totals
    scrapers = list(report["scrapers"].values())
    totals_phones = _empty_phone_stats()
    totals_ads = _empty_ads_stats()
    for s in scrapers:
        p = s.get("phones") or {}
        a = s.get("ads") or {}
        for key in ("values_seen", "valid", "invalid", "missing", "hidden"):
            totals_phones[key] += p.get(key) or 0
        for key in ("values_seen", "valid", "invalid", "missing", "duplicates"):
            totals_ads[key] += a.get(key) or 0
        for reason, n in (p.get("by_reason") or {}).items():
            _bump(totals_phones["by_reason"], reason, n)
        for reason, n in (a.get("by_reason") or {}).items():
            _bump(totals_ads["by_reason"], reason, n)
        totals_phones["unique_valid"] += p.get("unique_valid") or 0
        totals_ads["unique_valid"] += a.get("unique_valid") or 0

    def _pct(num: int, den: int) -> float:
        return round((num / den) * 100.0, 2) if den else 0.0

    totals_phones["valid_pct"] = _pct(totals_phones["valid"], totals_phones["values_seen"])
    totals_phones["invalid_pct"] = _pct(totals_phones["invalid"], totals_phones["values_seen"])
    totals_ads["valid_pct"] = _pct(totals_ads["valid"], totals_ads["values_seen"])
    totals_ads["invalid_pct"] = _pct(totals_ads["invalid"], totals_ads["values_seen"])

    report["totals"] = {
        "scrapers_total": len(scrapers),
        "scrapers_passed": sum(1 for s in scrapers if s.get("passed")),
        "files_found": sum(s.get("files_found") or 0 for s in scrapers),
        "rows_scanned": sum(s.get("rows_scanned") or 0 for s in scrapers),
        "phones": totals_phones,
        "ads": totals_ads,
    }

    # Upload report
    out_key = validation_report_key(site, report_date_str)
    body = json.dumps(report, ensure_ascii=False, indent=2).encode("utf-8")
    try:
        put_bytes(r2_client, bucket, out_key, body, "application/json")
    except Exception as exc:
        log.warning(f"Could not upload validation report: {exc}")

    write_step_summary(report)

    webhook = os.environ.get("MONITOR_ALERT_WEBHOOK_URL", "").strip()
    if webhook and not args.no_alert:
        send_alert(webhook, report)

    failed = report["totals"]["scrapers_passed"] < report["totals"]["scrapers_total"]
    if failed and args.fail_on_error:
        log.error("Validation finished with failures (--fail-on-error).")
        return 1

    log.info(
        f"Done. {report['totals']['scrapers_passed']}/{report['totals']['scrapers_total']} "
        f"scrapers passed · phones valid {totals_phones['valid_pct']}% · "
        f"ads valid {totals_ads['valid_pct']}%"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
