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

import ast
import io
import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

# Reuse Kuwait phone classification from validation/
_VALIDATION_DIR = Path(__file__).resolve().parent.parent / "validation"
if str(_VALIDATION_DIR) not in sys.path:
    sys.path.insert(0, str(_VALIDATION_DIR))

from phone_rules import (  # noqa: E402
    STATUS_HIDDEN,
    STATUS_MISSING,
    STATUS_VALID,
    STATUS_WRONG_COUNTRY,
    classify_phone,
)

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
    "user_phone",
    "user phone",
    "whatsapp_phone",
    "whatsapp phone",
    "contacts",
    "contact_no",
    "contact no",
    "main_branch_phone",
    "main branch phone",
})
PHONE_COLUMN_CANONICAL = frozenset({
    "phone",
    "phonenumber",
    "userphone",
    "whatsappphone",
    "contacts",
    "contactno",
    "mainbranchphone",
})
TOTAL_LISTINGS_KEYS = (
    "total_listings",
    "total_ads",
    "listings_count",
)


def _int_or_none(value: Any) -> Optional[int]:
    if isinstance(value, (int, float)):
        return int(value)
    return None


def _first_non_empty_str(row: Dict[str, Any], keys: Tuple[str, ...]) -> str:
    for key in keys:
        value = row.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _row_count_from_json_item(item: Dict[str, Any]) -> Optional[int]:
    for key in (
        "ads_count",
        "listings_count",
        "total_listings",
        "total_ads",
        "listings",
        "total_businesses",
        "count",
    ):
        val = _int_or_none(item.get(key))
        if val is not None and val >= 0:
            return val
    return None


def extract_subcategory_breakdown(data: Any) -> List[Dict[str, Any]]:
    """
    Extract normalized subcategory breakdown rows from summary JSON.

    Output row shape:
      {
        "subcategory": str,
        "level_3": str,
        "ads_count": int,
        "sheet_rows": int,
        "sheets_count": int,
        "source": "json_summary"
      }
    """
    if not isinstance(data, dict):
        return []

    agg: Dict[Tuple[str, str], Dict[str, int]] = {}

    top_lists = [
        data.get("categories"),
        data.get("main_categories"),
        data.get("subcategories"),
        data.get("items"),
    ]

    for items in top_lists:
        if not isinstance(items, list):
            continue

        for item in items:
            if not isinstance(item, dict):
                continue

            category_name = _first_non_empty_str(
                item,
                ("name_en", "name", "name_ar", "category", "category_name", "slug"),
            )
            category_count = _row_count_from_json_item(item)

            children = item.get("subcategories") or item.get("brands") or item.get("models")
            if isinstance(children, list) and children:
                for child in children:
                    if not isinstance(child, dict):
                        continue
                    child_name = _first_non_empty_str(
                        child,
                        ("name_en", "name", "name_ar", "model", "brand", "slug"),
                    )
                    if not child_name:
                        continue
                    child_count = _row_count_from_json_item(child)
                    if child_count is None:
                        child_count = 0

                    key = (category_name or "(unknown)", child_name)
                    bucket = agg.setdefault(key, {"ads_count": 0, "sheet_rows": 0, "sheets_count": 0})
                    bucket["ads_count"] += child_count
                    bucket["sheet_rows"] += child_count
                    bucket["sheets_count"] += 1
                continue

            # Flat category-only summaries still provide useful subcategory-level rows.
            if category_name and category_count is not None:
                key = (category_name, "")
                bucket = agg.setdefault(key, {"ads_count": 0, "sheet_rows": 0, "sheets_count": 0})
                bucket["ads_count"] += category_count
                bucket["sheet_rows"] += category_count
                bucket["sheets_count"] += 1

    rows: List[Dict[str, Any]] = []
    for (subcategory, level_3), stats in sorted(agg.items()):
        rows.append({
            "subcategory": subcategory,
            "level_3": level_3,
            "ads_count": stats["ads_count"],
            "sheet_rows": stats["sheet_rows"],
            "sheets_count": stats["sheets_count"],
            "source": "json_summary",
        })
    return rows


def _partition_prefixes_for_date(base: str, dt: datetime) -> List[str]:
    seen: set = set()
    prefixes: List[str] = []
    for month in (f"{dt.month:02d}", str(dt.month)):
        for day in (f"{dt.day:02d}", str(dt.day)):
            prefix = f"{base}/year={dt.year}/month={month}/day={day}/"
            if prefix not in seen:
                seen.add(prefix)
                prefixes.append(prefix)
    return prefixes


def _json_prefixes_for_date(base: str, dt: datetime) -> List[str]:
    seen: set = set()
    prefixes: List[str] = []
    for partition in _partition_prefixes_for_date(base, dt):
        prefix = f"{partition}json-files/"
        if prefix not in seen:
            seen.add(prefix)
            prefixes.append(prefix)
    return prefixes


def _find_id_column(columns) -> Optional[str]:
    for col in columns:
        if str(col).strip().lower() in ID_COLUMN_NAMES:
            return col
    return None


def _normalize_header(name: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(name).strip().lower())


def _find_phone_columns(columns) -> List[str]:
    out: List[str] = []
    for col in columns:
        raw = str(col).strip().lower()
        canon = _normalize_header(col)
        if raw in PHONE_COLUMN_NAMES or canon in PHONE_COLUMN_CANONICAL:
            out.append(col)
    return out


def _extract_phone_tokens(value: Any) -> List[str]:
    if value is None:
        return []

    if isinstance(value, (list, tuple, set)):
        tokens: List[str] = []
        for item in value:
            tokens.extend(_extract_phone_tokens(item))
        return tokens

    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return []

    # Some sheets store contacts as JSON/list string, e.g. ["965..."]
    if (text.startswith("[") and text.endswith("]")) or (text.startswith("{") and text.endswith("}")):
        try:
            parsed = ast.literal_eval(text)
            return _extract_phone_tokens(parsed)
        except Exception:
            pass

    return re.findall(r"\d+", text)


def _phone_identity(value: Any, normalized: Optional[str]) -> Optional[str]:
    """Stable unique key for a phone cell (prefer E.164 when available)."""
    if normalized:
        return normalized
    tokens = _extract_phone_tokens(value)
    if not tokens:
        return None
    digits = max(("".join(ch for ch in t if ch.isdigit()) for t in tokens), key=len, default="")
    return digits or None


def _collect_phone_classifications(
    value: Any,
    *,
    all_phones: Set[str],
    valid_phones: Set[str],
    invalid_phones: Set[str],
    outside_country_phones: Set[str],
) -> None:
    """
    Classify one phone cell into valid / invalid-fake / outside-country buckets.

    Valid requires Kuwait 965 + 8 digits with local first digit in 2/4/5/6/9.
    Wrong country codes go to outside_country; other bad shapes go to invalid.
    """
    status, normalized = classify_phone(value)
    if status in (STATUS_MISSING, STATUS_HIDDEN):
        return

    identity = _phone_identity(value, normalized)
    if not identity:
        return

    all_phones.add(identity)
    if status == STATUS_VALID:
        valid_phones.add(identity)
    elif status == STATUS_WRONG_COUNTRY:
        outside_country_phones.add(identity)
    else:
        invalid_phones.add(identity)


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


def _hour_from_date_published(value: Any) -> Optional[int]:
    if value is None:
        return None

    if isinstance(value, datetime) or isinstance(value, pd.Timestamp):
        return int(value.hour)

    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return None

    try:
        dt = pd.to_datetime(text, errors="coerce")
    except Exception:
        return None

    if pd.isna(dt):
        return None

    return int(dt.hour)


def _count_date_published_hours_in_excel(raw: bytes) -> Dict[int, int]:
    hour_counts: Dict[int, int] = {}
    try:
        xl = pd.ExcelFile(io.BytesIO(raw), engine="openpyxl")
    except Exception as exc:
        log.debug(f"Date-published hour counts skipped: {exc}")
        return hour_counts

    for sheet_name in xl.sheet_names:
        if sheet_name.strip().lower() in SKIP_SHEETS:
            continue
        try:
            df = pd.read_excel(xl, sheet_name=sheet_name, engine="openpyxl")
        except Exception as exc:
            log.debug(f"Sheet '{sheet_name}' skipped for date_published hours: {exc}")
            continue
        if df.empty:
            continue

        date_col = next(
            (c for c in df.columns if str(c).strip().lower() in ("date_published", "date published")),
            None,
        )
        if date_col is None:
            continue

        for value in df[date_col].dropna():
            hour = _hour_from_date_published(value)
            if hour is None:
                continue
            hour_counts[hour] = hour_counts.get(hour, 0) + 1

    return hour_counts


def count_ads_from_downloads(downloads: List[Tuple[str, bytes]]) -> Dict[str, Any]:
    """Aggregate ad counts from in-memory Excel downloads."""
    combined_ids: Set[Any] = set()
    combined_phones: Set[str] = set()
    valid_phones: Set[str] = set()
    invalid_phones: Set[str] = set()
    outside_country_phones: Set[str] = set()
    total_rows = 0
    found_id = False
    date_published_hour_counts: Dict[int, int] = {}

    def _scan_workbook(raw: bytes, *, collect_ids: bool) -> None:
        xl = pd.ExcelFile(io.BytesIO(raw), engine="openpyxl")
        for sheet_name in xl.sheet_names:
            if sheet_name.strip().lower() in SKIP_SHEETS:
                continue
            df = pd.read_excel(xl, sheet_name=sheet_name, engine="openpyxl")
            if df.empty:
                continue

            phone_cols = _find_phone_columns(df.columns)
            for phone_col in phone_cols:
                for value in df[phone_col].dropna():
                    _collect_phone_classifications(
                        value,
                        all_phones=combined_phones,
                        valid_phones=valid_phones,
                        invalid_phones=invalid_phones,
                        outside_country_phones=outside_country_phones,
                    )

            if not collect_ids:
                continue
            id_col = _find_id_column(df.columns)
            if id_col is None:
                continue
            for value in df[id_col].dropna().astype(str).str.strip():
                if value and value.lower() not in ("nan", "none"):
                    combined_ids.add(value)

    for _key, raw in downloads:
        unique_ads, rows, has_id = count_ads_from_excel_bytes(raw)
        total_rows += rows
        file_hour_counts = _count_date_published_hours_in_excel(raw)
        for hour, count in file_hour_counts.items():
            date_published_hour_counts[hour] = date_published_hour_counts.get(hour, 0) + count

        if has_id and unique_ads is not None:
            found_id = True
            # Re-read IDs for cross-file dedup (small daily files)
            try:
                _scan_workbook(raw, collect_ids=True)
            except Exception:
                pass

        # Even if no ID columns exist, still capture unique phones from phone column.
        if not has_id:
            try:
                _scan_workbook(raw, collect_ids=False)
            except Exception:
                pass

    phone_stats = {
        "unique_phones": len(combined_phones),
        "valid_phones": len(valid_phones),
        "invalid_phones": len(invalid_phones),
        "outside_country_phones": len(outside_country_phones),
    }

    if found_id:
        return {
            "unique_ads": len(combined_ids),
            **phone_stats,
            "total_rows": total_rows,
            "ads_source": "excel_ids",
            "date_published_hour_counts": date_published_hour_counts,
        }

    if total_rows > 0:
        return {
            "unique_ads": total_rows,
            **phone_stats,
            "total_rows": total_rows,
            "ads_source": "excel_rows",
            "date_published_hour_counts": date_published_hour_counts,
        }

    return {
        "unique_ads": 0,
        **phone_stats,
        "total_rows": 0,
        "ads_source": "none",
        "date_published_hour_counts": date_published_hour_counts,
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
) -> Tuple[Optional[int], Optional[str], List[Dict[str, Any]]]:
    """
    List json-files/ under the scraper partition and return (total, source_key).

    When multiple JSON files exist, uses the largest total_listings value
    (handles upload-summary vs summary files).
    """
    best_total: Optional[int] = None
    best_key: Optional[str] = None
    best_breakdown: List[Dict[str, Any]] = []

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
                    breakdown = extract_subcategory_breakdown(data)
                    if total is None:
                        if breakdown and not best_breakdown:
                            best_breakdown = breakdown
                        continue
                    if best_total is None or total > best_total:
                        best_total = total
                        best_key = key
                        best_breakdown = breakdown
        except Exception as exc:
            log.debug(f"JSON listing under {prefix}: {exc}")

    return best_total, best_key, best_breakdown


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

    json_total, json_key, json_breakdown = load_json_summaries(client, bucket, r2_base, partition_dt)

    phone_fields = {
        "unique_phones": excel_stats.get("unique_phones", 0),
        "valid_phones": excel_stats.get("valid_phones", 0),
        "invalid_phones": excel_stats.get("invalid_phones", 0),
        "outside_country_phones": excel_stats.get("outside_country_phones", 0),
    }

    if excel_stats["ads_source"] == "excel_ids":
        result = dict(excel_stats)
        result["json_summary_key"] = json_key
        result["json_total_listings"] = json_total
        result["subcategory_breakdown"] = json_breakdown
        return result

    if json_total is not None:
        return {
            "unique_ads": json_total,
            **phone_fields,
            "total_rows": excel_stats["total_rows"] or json_total,
            "ads_source": "json_summary",
            "json_summary_key": json_key,
            "json_total_listings": json_total,
            "subcategory_breakdown": json_breakdown,
            "date_published_hour_counts": excel_stats.get("date_published_hour_counts", {}),
        }

    return {
        **excel_stats,
        "json_summary_key": json_key,
        "json_total_listings": json_total,
        "subcategory_breakdown": json_breakdown,
    }
