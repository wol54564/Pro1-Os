"""
inspect_r2_schema.py
====================
Reads every Excel file uploaded by the scraper pipeline from Cloudflare R2,
validates each file against the schema defined in websites-config.yml, and:

  1. Prints a pass/fail table for every scraper (today's date by default).
  2. Writes a JSON report back to R2  → 4sale-data/monitor/YYYY-MM-DD/report.json
  3. Writes/updates monitor_stats.yml in the repo with observed real statistics
     (min/max row counts, actual file sizes, actual column sets, actual sheet names).
  4. Emits a GitHub Actions step-summary  ($GITHUB_STEP_SUMMARY).

Usage
-----
  python monitor/inspect_r2_schema.py
  python monitor/inspect_r2_schema.py --date 2026-06-04
  python monitor/inspect_r2_schema.py --update-stats           # also write monitor_stats.yml
  python monitor/inspect_r2_schema.py --days-lookback 7        # sample last 7 days for stats

Required env vars
-----------------
  CF_R2_ACCESS_KEY_ID, CF_R2_SECRET_ACCESS_KEY,
  CF_R2_ENDPOINT_URL,  CF_R2_BUCKET_NAME
"""

import argparse
import boto3
import io
import json
import logging
import os
import sys
import tempfile
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import openpyxl
import pandas as pd
import yaml

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("monitor")

# ── Paths ─────────────────────────────────────────────────────────────────────
REPO_ROOT   = Path(__file__).resolve().parent.parent
CONFIG_FILE = REPO_ROOT / "websites-config.yml"

# Config and stats live in R2 for CI; local file is used when present (dev).
CONFIG_R2_KEY = "4sale-data/monitor/websites-config.yml"
STATS_R2_KEY  = "4sale-data/monitor/monitor_stats.yml"


# ══════════════════════════════════════════════════════════════════════════════
# R2 CLIENT
# ══════════════════════════════════════════════════════════════════════════════

def build_r2_client():
    """Return a boto3 S3 client pointed at Cloudflare R2."""
    access_key  = os.environ["CF_R2_ACCESS_KEY_ID"]
    secret_key  = os.environ["CF_R2_SECRET_ACCESS_KEY"]
    endpoint    = os.environ["CF_R2_ENDPOINT_URL"].rstrip("/")
    bucket_name = os.environ["CF_R2_BUCKET_NAME"]

    # Strip trailing bucket name from endpoint if present
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


def list_excel_files(client, bucket: str, prefix: str) -> List[Dict]:
    """
    Return all .xlsx objects under *prefix*.
    Each item: {key, size, last_modified}
    Handles pagination automatically.
    """
    results = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".xlsx"):
                results.append(
                    {
                        "key":           obj["Key"],
                        "size_bytes":    obj["Size"],
                        "last_modified": obj["LastModified"].isoformat(),
                    }
                )
    return results


def download_excel(client, bucket: str, key: str) -> Optional[bytes]:
    """Download an .xlsx file from R2 and return raw bytes."""
    try:
        resp = client.get_object(Bucket=bucket, Key=key)
        return resp["Body"].read()
    except Exception as exc:
        log.warning(f"Could not download {key}: {exc}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
# EXCEL INSPECTION
# ══════════════════════════════════════════════════════════════════════════════

def inspect_excel(raw: bytes, file_key: str) -> Dict:
    """
    Open an xlsx from bytes and return:
      sheets: [{name, row_count, columns: [str]}]
      readable: bool
      error: str | None
    """
    result = {"file_key": file_key, "readable": False, "sheets": [], "error": None}
    try:
        wb = openpyxl.load_workbook(io.BytesIO(raw), read_only=True, data_only=True)
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            # Read header row
            headers = []
            rows_iter = ws.iter_rows(values_only=True)
            header_row = next(rows_iter, None)
            if header_row:
                headers = [str(c) for c in header_row if c is not None]
            # Count data rows (fast — don't load everything into memory)
            row_count = sum(1 for _ in rows_iter)
            result["sheets"].append(
                {"name": sheet_name, "row_count": row_count, "columns": headers}
            )
        wb.close()
        result["readable"] = True
    except Exception as exc:
        result["error"] = str(exc)
        log.warning(f"Error inspecting {file_key}: {exc}")
    return result


def check_data_quality(raw: bytes, sheet_name: str) -> Dict:
    """
    Run deeper data-quality checks on a single sheet using pandas.
    Returns a dict of quality metrics.
    """
    metrics = {
        "duplicate_id_count": 0,
        "null_id_pct": 0.0,
        "null_title_pct": 0.0,
        "null_price_pct": 0.0,
        "stale_date_pct": 0.0,   # rows where date_published is NOT yesterday
        "total_rows": 0,
    }
    try:
        df = pd.read_excel(io.BytesIO(raw), sheet_name=sheet_name, engine="openpyxl")
        metrics["total_rows"] = len(df)
        if df.empty:
            return metrics

        # Duplicate IDs
        id_col = next((c for c in df.columns if str(c).lower() in ("id", "listing id")), None)
        if id_col:
            metrics["null_id_pct"]      = round(df[id_col].isna().mean() * 100, 1)
            metrics["duplicate_id_count"] = int(df[id_col].duplicated().sum())

        # Null title
        title_col = next((c for c in df.columns if str(c).lower() == "title"), None)
        if title_col:
            metrics["null_title_pct"] = round(df[title_col].isna().mean() * 100, 1)

        # Null price
        price_col = next((c for c in df.columns if str(c).lower() == "price"), None)
        if price_col:
            metrics["null_price_pct"] = round(df[price_col].isna().mean() * 100, 1)

        # Date freshness — date_published should be yesterday
        date_col = next(
            (c for c in df.columns if str(c).lower() in ("date_published", "date published")),
            None,
        )
        if date_col:
            yesterday_str = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
            stale = df[date_col].astype(str).apply(
                lambda v: not str(v).startswith(yesterday_str)
            )
            metrics["stale_date_pct"] = round(stale.mean() * 100, 1)

    except Exception as exc:
        log.debug(f"Quality check skipped for sheet '{sheet_name}': {exc}")

    return metrics


# ══════════════════════════════════════════════════════════════════════════════
# CONFIG LOADING
# ══════════════════════════════════════════════════════════════════════════════

def load_config(client=None, bucket: Optional[str] = None) -> Dict:
    """Load config from the local file (dev) or R2 (CI when the file is gitignored)."""
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, encoding="utf-8") as fh:
            log.info(f"Loaded config from {CONFIG_FILE}")
            return yaml.safe_load(fh)

    if client and bucket:
        try:
            resp = client.get_object(Bucket=bucket, Key=CONFIG_R2_KEY)
            raw  = resp["Body"].read().decode("utf-8")
            log.info(f"Loaded config from r2://{bucket}/{CONFIG_R2_KEY}")
            return yaml.safe_load(raw)
        except client.exceptions.NoSuchKey:
            log.debug(f"No config at r2://{bucket}/{CONFIG_R2_KEY}")
        except Exception as exc:
            log.warning(f"Could not load config from R2: {exc}")

    raise FileNotFoundError(
        f"{CONFIG_FILE.name} not found locally and not available in R2 "
        f"at {CONFIG_R2_KEY}. Create the file locally, then run with "
        f"--upload-config to publish it to R2 for CI."
    )


def upload_config(client, bucket: str) -> None:
    """Upload the local websites-config.yml to R2 for GitHub Actions."""
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(f"Cannot upload — {CONFIG_FILE} does not exist.")

    body = CONFIG_FILE.read_bytes()
    client.put_object(
        Bucket=bucket,
        Key=CONFIG_R2_KEY,
        Body=body,
        ContentType="text/yaml",
    )
    log.info(f"Config uploaded → r2://{bucket}/{CONFIG_R2_KEY}")


def r2_base_prefix(r2_path_raw: str) -> str:
    """
    Convert config r2_path like '{r2_bucket}/4sale-data/animals'
    to the actual in-bucket prefix  '4sale-data/animals'.
    """
    # Remove leading placeholder segment  e.g.  '{r2_bucket}/'
    path = r2_path_raw.strip()
    if path.startswith("{"):
        path = path.split("/", 1)[1] if "/" in path else path
    return path.strip("/")


def partition_date_for_data_date(dt: datetime) -> datetime:
    """Scrapers save to save_date = listing date + 1 day (see scrape_date vs save_date in main.py)."""
    return dt + timedelta(days=1)


def excel_prefixes_for_date(base: str, dt: datetime) -> List[str]:
    """
    Build R2 date-partition prefixes for Excel discovery.
    Most scrapers: year=2026/month=06/day=09/ (zero-padded save_date).
    Property:      year=2026/month=6/day=9/   (unpadded) — try both forms.
    """
    seen: set = set()
    prefixes: List[str] = []
    for month in (f"{dt.month:02d}", str(dt.month)):
        for day in (f"{dt.day:02d}", str(dt.day)):
            prefix = f"{base}/year={dt.year}/month={month}/day={day}/"
            if prefix not in seen:
                seen.add(prefix)
                prefixes.append(prefix)
    return prefixes


# ══════════════════════════════════════════════════════════════════════════════
# VALIDATION AGAINST SCHEMA
# ══════════════════════════════════════════════════════════════════════════════

# Sentinel patterns used as sheet-name templates in the YAML
_TEMPLATE_MARKERS = ("{", "or Main", "or All Listings")


def is_template(name: str) -> bool:
    return any(m in name for m in _TEMPLATE_MARKERS)


def validate_file(inspected: Dict, schema_entry: Dict) -> Dict:
    """
    Compare an inspected file against its schema entry.
    Returns a validation result dict:
      passed: bool
      checks: [{name, passed, detail}]
    """
    checks = []

    def add(name, passed, detail=""):
        checks.append({"name": name, "passed": passed, "detail": detail})

    # --- Readability ---
    add("file_readable", inspected["readable"], inspected.get("error", ""))

    if not inspected["readable"]:
        return {"passed": False, "checks": checks}

    observed_sheets = {s["name"]: s for s in inspected["sheets"]}
    schema_sheets   = schema_entry.get("sheets", [])

    for schema_sheet in schema_sheets:
        sname    = schema_sheet["name"]
        req_cols = schema_sheet.get("required_columns", [])
        row_min, row_max = schema_sheet.get("row_count_range", [0, 999999])

        # Template sheets  (e.g. "{child_category or Main}") — we validate
        # all non-Info observed sheets against this template
        if is_template(sname):
            data_sheets = [
                s for s in inspected["sheets"]
                if s["name"] != "Info" and s["name"] != "No Data"
            ]
            if not data_sheets:
                add("data_sheets_exist", False, "No data sheets found")
                continue
            for ds in data_sheets:
                missing_cols = [c for c in req_cols if c not in ds["columns"]]
                add(
                    f"columns_in_{ds['name'][:20]}",
                    len(missing_cols) == 0,
                    f"Missing: {missing_cols}" if missing_cols else "",
                )
                in_range = row_min <= ds["row_count"] <= row_max
                add(
                    f"rows_in_{ds['name'][:20]}",
                    in_range,
                    f"{ds['row_count']} rows (expected {row_min}–{row_max})",
                )
        else:
            # Exact sheet name
            if sname not in observed_sheets:
                add(f"sheet_exists_{sname}", False, f"Sheet '{sname}' not found")
                continue
            obs = observed_sheets[sname]
            missing_cols = [c for c in req_cols if c not in obs["columns"]]
            add(
                f"columns_in_{sname[:20]}",
                len(missing_cols) == 0,
                f"Missing: {missing_cols}" if missing_cols else "",
            )
            in_range = row_min <= obs["row_count"] <= row_max
            add(
                f"rows_in_{sname[:20]}",
                in_range,
                f"{obs['row_count']} rows (expected {row_min}–{row_max})",
            )

    # --- File size ---
    min_kb  = schema_entry.get("min_file_size_kb", 0)
    size_kb = inspected.get("size_bytes", 0) / 1024
    add(
        "file_size_ok",
        size_kb >= min_kb,
        f"{size_kb:.1f} KB (min {min_kb} KB)",
    )

    passed = all(c["passed"] for c in checks)
    return {"passed": passed, "checks": checks}


# ══════════════════════════════════════════════════════════════════════════════
# STATS ACCUMULATION & YAML WRITER
# ══════════════════════════════════════════════════════════════════════════════

def accumulate_stats(
    existing: Dict,
    scraper_name: str,
    inspected: Dict,
    size_bytes: int,
    run_date: str,
) -> None:
    """
    Merge new observations into the *existing* stats dict (in-place).
    Structure:
      scraper_name:
        observed_dates: [...]
        file_size_kb: {min, max, last}
        sheets:
          sheet_name:
            row_count: {min, max, last}
            columns: [...]
    """
    entry = existing.setdefault(scraper_name, {
        "observed_dates":  [],
        "file_size_kb":    {"min": None, "max": None, "last": None},
        "sheets":          {},
    })

    if run_date not in entry["observed_dates"]:
        entry["observed_dates"].append(run_date)
        entry["observed_dates"] = sorted(entry["observed_dates"])[-30:]  # keep last 30

    kb = round(size_bytes / 1024, 1)
    fs = entry["file_size_kb"]
    fs["last"] = kb
    fs["min"]  = min(kb, fs["min"]) if fs["min"] is not None else kb
    fs["max"]  = max(kb, fs["max"]) if fs["max"] is not None else kb

    for sheet in inspected.get("sheets", []):
        sname = sheet["name"]
        se    = entry["sheets"].setdefault(sname, {
            "row_count": {"min": None, "max": None, "last": None},
            "columns":   [],
        })
        rc = sheet["row_count"]
        se["row_count"]["last"] = rc
        se["row_count"]["min"]  = min(rc, se["row_count"]["min"]) if se["row_count"]["min"] is not None else rc
        se["row_count"]["max"]  = max(rc, se["row_count"]["max"]) if se["row_count"]["max"] is not None else rc
        # Merge new columns (union)
        new_cols = [c for c in sheet["columns"] if c not in se["columns"]]
        se["columns"].extend(new_cols)


def load_existing_stats(client, bucket: str) -> Dict:
    """Download monitor_stats.yml from R2 and parse it. Returns {} if not found."""
    try:
        resp = client.get_object(Bucket=bucket, Key=STATS_R2_KEY)
        raw  = resp["Body"].read().decode("utf-8")
        data = yaml.safe_load(raw) or {}
        log.info(f"Loaded existing stats from r2://{bucket}/{STATS_R2_KEY}")
        return data
    except client.exceptions.NoSuchKey:
        log.info("No existing monitor_stats.yml in R2 — starting fresh.")
        return {}
    except Exception as exc:
        log.warning(f"Could not load stats from R2: {exc} — starting fresh.")
        return {}


def save_stats(client, bucket: str, stats: Dict) -> None:
    """Serialise stats to YAML and upload to R2 (overwrites previous version)."""
    header = (
        "# Auto-generated by monitor/inspect_r2_schema.py\n"
        "# Stored in Cloudflare R2 — do not add to the repo.\n"
        "# Re-run the weekly monitor workflow to refresh.\n\n"
    )
    body = (header + yaml.dump(
        stats, allow_unicode=True, default_flow_style=False, sort_keys=True
    )).encode("utf-8")
    try:
        client.put_object(
            Bucket=bucket,
            Key=STATS_R2_KEY,
            Body=body,
            ContentType="text/yaml",
        )
        log.info(f"Saved observed stats → r2://{bucket}/{STATS_R2_KEY}")
    except Exception as exc:
        log.error(f"Failed to save stats to R2: {exc}")


# ══════════════════════════════════════════════════════════════════════════════
# REPORTING
# ══════════════════════════════════════════════════════════════════════════════

PASS = "✅"
FAIL = "❌"
WARN = "⚠️"
MISS = "🚫"


def print_summary_table(results: List[Dict]) -> None:
    """Print a human-readable table to stdout."""
    print("\n" + "=" * 70)
    print(f"{'SCRAPER':<30}  {'FILES':<6}  {'CHECKS':<10}  STATUS")
    print("=" * 70)
    for r in results:
        status  = PASS if r["all_passed"] else FAIL
        missing = MISS if r["files_found"] == 0 else ""
        print(
            f"{r['scraper']:<30}  "
            f"{r['files_found']:<6}  "
            f"{r['checks_passed']}/{r['checks_total']:<7}  "
            f"{status} {missing}"
        )
    print("=" * 70)
    total_pass = sum(1 for r in results if r["all_passed"])
    print(f"\nTotal: {total_pass}/{len(results)} scrapers fully passed\n")


def write_github_summary(results: List[Dict], run_date: str) -> None:
    """Write markdown to $GITHUB_STEP_SUMMARY if available."""
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    lines = [
        f"## R2 Schema Monitor — {run_date}\n",
        "| Scraper | Files | Checks | Status |",
        "|---------|-------|--------|--------|",
    ]
    for r in results:
        icon   = PASS if r["all_passed"] else (MISS if r["files_found"] == 0 else FAIL)
        detail = ""
        if not r["all_passed"]:
            if r["files_found"] == 0:
                detail = "no Excel files found"
            else:
                failed = [
                    c["name"]
                    for f in r["file_results"]
                    for c in f.get("validation", {}).get("checks", [])
                    if not c["passed"]
                ]
                detail = "<br>".join(failed[:5]) if failed else "validation failed"
        lines.append(
            f"| {r['scraper']} | {r['files_found']} | "
            f"{r['checks_passed']}/{r['checks_total']} | {icon} {detail} |"
        )
    total_pass = sum(1 for r in results if r["all_passed"])
    lines.append(f"\n**{total_pass}/{len(results)} scrapers fully passed**")
    with open(summary_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))


def upload_report(client, bucket: str, report: Dict, run_date: str) -> None:
    """Upload the JSON report to R2."""
    key  = f"4sale-data/monitor/{run_date}/report.json"
    body = json.dumps(report, ensure_ascii=False, indent=2, default=str).encode()
    try:
        client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
        log.info(f"Report uploaded → r2://{bucket}/{key}")
    except Exception as exc:
        log.warning(f"Could not upload report: {exc}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def parse_args():
    p = argparse.ArgumentParser(description="Inspect R2 Excel files against the project schema.")
    p.add_argument("--date",          default=None, help="YYYY-MM-DD to inspect (default: yesterday)")
    p.add_argument("--days-lookback", type=int, default=1, help="How many days to scan (default: 1 = yesterday only)")
    p.add_argument("--update-stats",  action="store_true", help="Write/update monitor_stats.yml with observed data")
    p.add_argument("--quality",       action="store_true", help="Run deep data-quality checks (slower)")
    p.add_argument("--fail-on-error", action="store_true", help="Exit with code 1 if any check fails")
    p.add_argument(
        "--upload-config",
        action="store_true",
        help="Upload local websites-config.yml to R2 (bootstrap CI), then exit",
    )
    return p.parse_args()


def main():
    args = parse_args()

    if args.upload_config:
        r2_client, bucket = build_r2_client()
        upload_config(r2_client, bucket)
        return

    # Date range to inspect
    if args.date:
        end_date   = datetime.strptime(args.date, "%Y-%m-%d")
    else:
        end_date   = datetime.utcnow() - timedelta(days=1)   # yesterday UTC
    dates_to_check = [end_date - timedelta(days=i) for i in range(args.days_lookback)]
    run_date_str   = end_date.strftime("%Y-%m-%d")

    log.info(f"Inspecting date(s): {[d.strftime('%Y-%m-%d') for d in dates_to_check]}")

    # Connect to R2 (needed for file inspection and for config when not in repo)
    r2_client, bucket = build_r2_client()
    log.info(f"Connected to R2 bucket: {bucket}")

    config = load_config(r2_client, bucket)

    # Build schema lookup: scraper_name → schema_entry
    schema_by_scraper = {
        e["scraper"]: e for e in config.get("excel_schema", [])
    }

    # Load existing stats from R2 (for --update-stats mode)
    stats = load_existing_stats(r2_client, bucket) if args.update_stats else {}

    all_results  = []
    full_report  = {"run_date": run_date_str, "scrapers": {}}

    scrapers_cfg = config.get("scrapers", [])
    log.info(f"Processing {len(scrapers_cfg)} scrapers …")

    for scraper_cfg in scrapers_cfg:
        scraper_name = scraper_cfg["name"]
        r2_base      = r2_base_prefix(scraper_cfg.get("r2_path", ""))
        schema_entry = schema_by_scraper.get(scraper_name)

        if not r2_base:
            log.warning(f"  {scraper_name}: no r2_path in config — skipping")
            continue

        scraper_result = {
            "scraper":        scraper_name,
            "files_found":    0,
            "checks_passed":  0,
            "checks_total":   0,
            "all_passed":     True,
            "file_results":   [],
        }

        all_xlsx: List[Dict] = []
        seen_keys: set = set()
        for dt in dates_to_check:
            part_dt = partition_date_for_data_date(dt)
            for prefix in excel_prefixes_for_date(r2_base, part_dt):
                for f in list_excel_files(r2_client, bucket, prefix):
                    if f["key"] in seen_keys:
                        continue
                    seen_keys.add(f["key"])
                    f["date"] = dt.strftime("%Y-%m-%d")
                    all_xlsx.append(f)

        scraper_result["files_found"] = len(all_xlsx)

        if not all_xlsx:
            log.warning(f"  {scraper_name}: NO Excel files found under {r2_base}")
            scraper_result["all_passed"] = False
            all_results.append(scraper_result)
            full_report["scrapers"][scraper_name] = scraper_result
            continue

        for xlsx_meta in all_xlsx:
            raw = download_excel(r2_client, bucket, xlsx_meta["key"])
            if raw is None:
                continue

            inspected = inspect_excel(raw, xlsx_meta["key"])
            inspected["size_bytes"] = xlsx_meta["size_bytes"]

            # Deep quality check on data sheets (if requested)
            if args.quality and inspected["readable"]:
                for sheet in inspected["sheets"]:
                    if sheet["name"] != "Info":
                        sheet["quality"] = check_data_quality(raw, sheet["name"])

            # Validate against schema
            file_validation = {"file": xlsx_meta["key"], "checks": []}
            if schema_entry:
                file_validation = validate_file(inspected, schema_entry)
                file_validation["file"] = xlsx_meta["key"]
            else:
                log.debug(f"  {scraper_name}: no schema entry — skipping validation")

            scraper_result["file_results"].append(
                {**inspected, **{"validation": file_validation}}
            )
            scraper_result["checks_passed"] += sum(
                1 for c in file_validation.get("checks", []) if c["passed"]
            )
            scraper_result["checks_total"] += len(file_validation.get("checks", []))
            if not file_validation.get("passed", True):
                scraper_result["all_passed"] = False

            # Accumulate stats for --update-stats
            if args.update_stats:
                accumulate_stats(
                    stats,
                    scraper_name,
                    inspected,
                    xlsx_meta["size_bytes"],
                    xlsx_meta["date"],
                )

        all_results.append(scraper_result)
        full_report["scrapers"][scraper_name] = scraper_result
        status = PASS if scraper_result["all_passed"] else FAIL
        log.info(
            f"  {status} {scraper_name}: "
            f"{scraper_result['files_found']} file(s), "
            f"{scraper_result['checks_passed']}/{scraper_result['checks_total']} checks"
        )

    # ── Outputs ───────────────────────────────────────────────────────────────
    print_summary_table(all_results)
    write_github_summary(all_results, run_date_str)
    upload_report(r2_client, bucket, full_report, run_date_str)

    if args.update_stats:
        save_stats(r2_client, bucket, stats)

    # ── Exit code ─────────────────────────────────────────────────────────────
    any_failure = any(not r["all_passed"] for r in all_results)
    if any_failure and args.fail_on_error:
        log.error("One or more scrapers failed schema validation.")
        sys.exit(1)
    elif any_failure:
        log.warning("Some scrapers failed — exit 0 (use --fail-on-error to change).")


if __name__ == "__main__":
    main()
