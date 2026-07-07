"""
ads_counter.py
==============
Count unique ads per scraper for the monitor hub dashboard.

Priority:
  1. Unique listing IDs from Excel data sheets (deduped across files/sheets)
  2. total_listings / total_ads from JSON summary in json-files/
  3. Sum of Excel data-row counts (excluding Info / No Data sheets)
"""

from __future__ import annotations

import io
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

log = logging.getLogger("monitor")

SKIP_SHEETS = frozenset({"info", "no data"})
ID_COLUMN_NAMES = frozenset({
    "id",
    "listing id",
    "listing_id",
    "user_adv_id",
    "user adv id",
    "ad_id",
    "ad id",
})
PHONE_COLUMN_NAMES = frozenset({
    "phone",
    "phone number",
    "phonenumber",
})
TOTAL_LISTINGS_KEYS = (
    "total_listings",
    "total_ads",
    "listings_count",
)


def _json_prefixes_for_date(base: str, dt: datetime) -> List[str]:
    seen: set = set()
    prefixes: List[str] = []
    for month in (f"{dt.month:02d}", str(dt.month)):
        for day in (f"{dt.day:02d}", str(dt.day)):
            prefix = f"{base}/year={dt.year}/month={month}/day={day}/json-files/"
            if prefix not in seen:
                seen.add(prefix)
                prefixes.append(prefix)
    return prefixes


def _find_id_column(columns) -> Optional[str]:
    for col in columns:
        if str(col).strip().lower() in ID_COLUMN_NAMES:
            return col
    return None


def _find_phone_column(columns) -> Optional[str]:
    for col in columns:
        if str(col).strip().lower() in PHONE_COLUMN_NAMES:
            return col
    return None


def _normalize_phone(value: Any) -> Optional[str]:
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none"}:
        return None

    # Keep digits only so formatting variants count as the same phone.
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits or None


def count_ads_from_excel_bytes(raw: bytes) -> Tuple[Optional[int], int, bool]:
    """
    Return (unique_ads, total_rows, found_id_column).

    unique_ads is None when no ID column exists on any data sheet.
    """
    unique_ids: Set[Any] = set()
    total_rows = 0
    found_id = False

    try:
        xl = pd.ExcelFile(io.BytesIO(raw), engine="openpyxl")
    except Exception as exc:
        log.debug(f"Excel ad count skipped: {exc}")
        return None, 0, False

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

        total_rows += len(df)
        id_col = _find_id_column(df.columns)
        if id_col is None:
            continue

        found_id = True
        for value in df[id_col].dropna().astype(str).str.strip():
            if value and value.lower() not in ("nan", "none"):
                unique_ids.add(value)

    unique_ads = len(unique_ids) if found_id else None
    return unique_ads, total_rows, found_id


def count_ads_from_downloads(downloads: List[Tuple[str, bytes]]) -> Dict[str, Any]:
    """Aggregate ad counts from in-memory Excel downloads."""
    combined_ids: Set[Any] = set()
    combined_phones: Set[str] = set()
    total_rows = 0
    found_id = False

    for _key, raw in downloads:
        unique_ads, rows, has_id = count_ads_from_excel_bytes(raw)
        total_rows += rows
        if has_id and unique_ads is not None:
            found_id = True
            # Re-read IDs for cross-file dedup (small daily files)
            try:
                xl = pd.ExcelFile(io.BytesIO(raw), engine="openpyxl")
                for sheet_name in xl.sheet_names:
                    if sheet_name.strip().lower() in SKIP_SHEETS:
                        continue
                    df = pd.read_excel(xl, sheet_name=sheet_name, engine="openpyxl")
                    id_col = _find_id_column(df.columns)
                    if id_col is None:
                        continue
                    for value in df[id_col].dropna().astype(str).str.strip():
                        if value and value.lower() not in ("nan", "none"):
                            combined_ids.add(value)

                    phone_col = _find_phone_column(df.columns)
                    if phone_col is not None:
                        for value in df[phone_col].dropna():
                            normalized = _normalize_phone(value)
                            if normalized:
                                combined_phones.add(normalized)
            except Exception:
                pass

        # Even if no ID columns exist, still capture unique phones from phone column.
        if not has_id:
            try:
                xl = pd.ExcelFile(io.BytesIO(raw), engine="openpyxl")
                for sheet_name in xl.sheet_names:
                    if sheet_name.strip().lower() in SKIP_SHEETS:
                        continue
                    df = pd.read_excel(xl, sheet_name=sheet_name, engine="openpyxl")
                    phone_col = _find_phone_column(df.columns)
                    if phone_col is None:
                        continue
                    for value in df[phone_col].dropna():
                        normalized = _normalize_phone(value)
                        if normalized:
                            combined_phones.add(normalized)
            except Exception:
                pass

    if found_id:
        return {
            "unique_ads": len(combined_ids),
            "unique_phones": len(combined_phones),
            "total_rows": total_rows,
            "ads_source": "excel_ids",
        }

    if total_rows > 0:
        return {
            "unique_ads": total_rows,
            "unique_phones": len(combined_phones),
            "total_rows": total_rows,
            "ads_source": "excel_rows",
        }

    return {
        "unique_ads": 0,
        "unique_phones": len(combined_phones),
        "total_rows": 0,
        "ads_source": "none",
    }


def extract_total_from_json(data: Any) -> Optional[int]:
    """Extract a total listing count from known JSON summary shapes."""
    if not isinstance(data, dict):
        return None

    for key in TOTAL_LISTINGS_KEYS:
        val = data.get(key)
        if isinstance(val, (int, float)) and val >= 0:
            return int(val)

    nested_lists = (
        data.get("subcategories"),
        data.get("main_categories"),
        data.get("categories"),
        data.get("excel_files"),
        data.get("items"),
    )
    partial = 0
    found = False
    for items in nested_lists:
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            for key in TOTAL_LISTINGS_KEYS:
                val = item.get(key)
                if isinstance(val, (int, float)) and val >= 0:
                    partial += int(val)
                    found = True
                    break
            if "listings" in item and isinstance(item.get("listings"), (int, float)):
                partial += int(item["listings"])
                found = True
    if found:
        return partial

    return None


def load_json_summaries(
    client,
    bucket: str,
    r2_base: str,
    partition_dt: datetime,
) -> Tuple[Optional[int], Optional[str]]:
    """
    List json-files/ under the scraper partition and return (total, source_key).

    When multiple JSON files exist, uses the largest total_listings value
    (handles upload-summary vs summary files).
    """
    best_total: Optional[int] = None
    best_key: Optional[str] = None

    for prefix in _json_prefixes_for_date(r2_base.strip("/"), partition_dt):
        try:
            paginator = client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if not key.lower().endswith(".json"):
                        continue
                    try:
                        resp = client.get_object(Bucket=bucket, Key=key)
                        data = json.loads(resp["Body"].read().decode("utf-8"))
                    except Exception as exc:
                        log.debug(f"Could not read JSON summary {key}: {exc}")
                        continue

                    total = extract_total_from_json(data)
                    if total is None:
                        continue
                    if best_total is None or total > best_total:
                        best_total = total
                        best_key = key
        except Exception as exc:
            log.debug(f"JSON listing under {prefix}: {exc}")

    return best_total, best_key


def count_scraper_ads(
    client,
    bucket: str,
    r2_base: str,
    partition_dt: datetime,
    downloads: List[Tuple[str, bytes]],
) -> Dict[str, Any]:
    """
    Count unique ads for one scraper.

    Returns dict with unique_ads, total_rows, ads_source, json_summary_key.
    """
    excel_stats = count_ads_from_downloads(downloads)

    json_total, json_key = load_json_summaries(client, bucket, r2_base, partition_dt)

    if excel_stats["ads_source"] == "excel_ids":
        result = dict(excel_stats)
        result["json_summary_key"] = json_key
        result["json_total_listings"] = json_total
        return result

    if json_total is not None:
        return {
            "unique_ads": json_total,
            "unique_phones": excel_stats.get("unique_phones", 0),
            "total_rows": excel_stats["total_rows"] or json_total,
            "ads_source": "json_summary",
            "json_summary_key": json_key,
            "json_total_listings": json_total,
        }

    return {
        **excel_stats,
        "json_summary_key": json_key,
        "json_total_listings": json_total,
    }
