"""
Kuwait / 4sale phone validation rules derived from CFl_data scrapers.

Observed shapes in Excel / JSON:
  - phone / user_phone / whatsapp_phone: "96565555210", "96551594994"
  - contact_no / contacts: "96596962567" or '["96596962567"]'
  - empty when is_hide_my_number is true
"""

from __future__ import annotations

import ast
import re
from typing import Any, List, Optional, Tuple

# Column headers seen across CFl_data scrapers (raw + normalized forms).
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

HIDE_COLUMN_NAMES = frozenset({
    "is_hide_my_number",
    "is hide my number",
    "hide_my_number",
    "hide my number",
})
HIDE_COLUMN_CANONICAL = frozenset({
    "ishideminumber",
    "hideminumber",
})

# Kuwait: country code 965 + 8-digit national number.
COUNTRY_CODE = "965"
NATIONAL_LEN = 8
E164_LEN = len(COUNTRY_CODE) + NATIONAL_LEN  # 11

# Local first digit: mobiles 5/6/9, landlines commonly 2.
VALID_LOCAL_PREFIXES = frozenset("2569")

# Status codes returned by classify_phone
STATUS_VALID = "valid"
STATUS_MISSING = "missing"
STATUS_HIDDEN = "hidden"
STATUS_TOO_SHORT = "too_short"
STATUS_TOO_LONG = "too_long"
STATUS_WRONG_COUNTRY = "wrong_country_code"
STATUS_BAD_PREFIX = "invalid_local_prefix"
STATUS_MALFORMED = "malformed"


def normalize_header(name: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(name).strip().lower())


def find_phone_columns(columns) -> List[str]:
    out: List[str] = []
    for col in columns:
        raw = str(col).strip().lower()
        canon = normalize_header(col)
        if raw in PHONE_COLUMN_NAMES or canon in PHONE_COLUMN_CANONICAL:
            out.append(col)
    return out


def find_hide_column(columns) -> Optional[str]:
    for col in columns:
        raw = str(col).strip().lower()
        canon = normalize_header(col)
        if raw in HIDE_COLUMN_NAMES or canon in HIDE_COLUMN_CANONICAL:
            return col
    return None


def _truthy_hide(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, float) and value != value:  # NaN
        return False
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y", "t"}


def extract_phone_tokens(value: Any) -> List[str]:
    """Pull digit tokens from scalars, lists, or JSON/list string cells."""
    if value is None:
        return []

    if isinstance(value, (list, tuple, set)):
        tokens: List[str] = []
        for item in value:
            tokens.extend(extract_phone_tokens(item))
        return tokens

    # pandas / Excel empty cells often arrive as float NaN
    if isinstance(value, float):
        if value != value:  # NaN
            return []
        if value == int(value):
            value = int(value)

    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return []

    if (text.startswith("[") and text.endswith("]")) or (
        text.startswith("{") and text.endswith("}")
    ):
        try:
            parsed = ast.literal_eval(text)
            return extract_phone_tokens(parsed)
        except Exception:
            pass

    # Keep leading + for country detection before digit strip.
    cleaned = text.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    if cleaned.startswith("+"):
        cleaned = cleaned[1:]
    digits = re.sub(r"\D", "", cleaned)
    return [digits] if digits else re.findall(r"\d+", text)


def normalize_to_e164(digits: str) -> Optional[str]:
    """
    Normalize to 965XXXXXXXX when possible.
    Accepts: 8-digit local, 11-digit 965…, or 00/0 prefixes stripped to digits.
    """
    d = "".join(ch for ch in digits if ch.isdigit())
    if not d:
        return None

    # Strip leading international dial prefix remnants already digit-only.
    if d.startswith("00") and len(d) > 2:
        d = d[2:]

    if len(d) == NATIONAL_LEN:
        return COUNTRY_CODE + d

    if len(d) == E164_LEN and d.startswith(COUNTRY_CODE):
        return d

    # Rare: 9650XXXXXXXX → drop trunk 0 after country code
    if len(d) == E164_LEN + 1 and d.startswith(COUNTRY_CODE + "0"):
        return COUNTRY_CODE + d[len(COUNTRY_CODE) + 1 :]

    return None


def classify_phone(value: Any, *, is_hidden: bool = False) -> Tuple[str, Optional[str]]:
    """
    Return (status, normalized_e164_or_none).

    Hidden + empty → STATUS_HIDDEN (not counted as invalid).
    """
    tokens = extract_phone_tokens(value)
    if not tokens:
        if is_hidden:
            return STATUS_HIDDEN, None
        return STATUS_MISSING, None

    # Use the longest digit token (handles "call 965… / alt …").
    digits = max(tokens, key=len)
    digits = "".join(ch for ch in digits if ch.isdigit())

    if len(digits) < NATIONAL_LEN:
        return STATUS_TOO_SHORT, None
    if len(digits) > E164_LEN + 2:
        return STATUS_TOO_LONG, None

    normalized = normalize_to_e164(digits)
    if normalized is None:
        if len(digits) >= 10 and not digits.startswith(COUNTRY_CODE):
            return STATUS_WRONG_COUNTRY, None
        return STATUS_MALFORMED, None

    local = normalized[len(COUNTRY_CODE) :]
    if not local or local[0] not in VALID_LOCAL_PREFIXES:
        return STATUS_BAD_PREFIX, normalized

    return STATUS_VALID, normalized


def is_valid_phone(value: Any, *, is_hidden: bool = False) -> bool:
    status, _ = classify_phone(value, is_hidden=is_hidden)
    return status == STATUS_VALID
