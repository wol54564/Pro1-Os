"""
Ad / listing ID validation rules derived from CFl_data scrapers.

Observed ID columns:
  id, listing id, listing_id, user_adv_id, ad_id
Typical values: integers like 20476856 embedded in slugs as well.
"""

from __future__ import annotations

import re
from typing import Any, List, Optional, Tuple

ID_COLUMN_NAMES = frozenset({
    "id",
    "listing id",
    "listing_id",
    "user_adv_id",
    "user adv id",
    "ad_id",
    "ad id",
})
ID_COLUMN_CANONICAL = frozenset({
    "id",
    "listingid",
    "useradvid",
    "adid",
})

STATUS_VALID = "valid"
STATUS_MISSING = "missing"
STATUS_MALFORMED = "malformed"
STATUS_DUPLICATE = "duplicate"


def normalize_header(name: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(name).strip().lower())


def find_id_columns(columns) -> List[str]:
    out: List[str] = []
    for col in columns:
        raw = str(col).strip().lower()
        canon = normalize_header(col)
        if raw in ID_COLUMN_NAMES or canon in ID_COLUMN_CANONICAL:
            out.append(col)
    return out


def find_id_column(columns) -> Optional[str]:
    cols = find_id_columns(columns)
    return cols[0] if cols else None


def normalize_ad_id(value: Any) -> Optional[str]:
    if value is None:
        return None

    if isinstance(value, float):
        if value != value:  # NaN
            return None
        if value == int(value):
            value = int(value)

    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return None

    # Prefer pure digit IDs; otherwise extract trailing numeric segment from slug-like values.
    if text.isdigit():
        return text.lstrip("0") or "0"

    digits = re.findall(r"\d+", text)
    if not digits:
        return None
    # Prefer the longest numeric run (listing ids are typically 7–9 digits).
    return max(digits, key=len)


def classify_ad_id(value: Any) -> Tuple[str, Optional[str]]:
    """Return (status, normalized_id). Duplicate detection is done by the caller."""
    if value is None:
        return STATUS_MISSING, None

    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return STATUS_MISSING, None

    normalized = normalize_ad_id(value)
    if normalized is None:
        return STATUS_MALFORMED, None

    # Guard against nonsense short IDs (single digit placeholders).
    if len(normalized) < 3:
        return STATUS_MALFORMED, normalized

    return STATUS_VALID, normalized
