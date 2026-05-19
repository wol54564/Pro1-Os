"""Backfill runner for Commercials — runs as if it were TARGET_DATE.

Usage:
    TARGET_DATE=2026-05-15 python main.py

The scraper will fetch listings from TARGET_DATE - 1 day (the day before the
TARGET_DATE you supply).  R2 data is stored under the TARGET_DATE partition.
"""

import os
import sys

# ── 1. Read and validate TARGET_DATE ─────────────────────────────────────────
_TARGET_DATE_STR = os.environ.get("TARGET_DATE", "").strip()
if not _TARGET_DATE_STR:
    raise SystemExit("ERROR: TARGET_DATE environment variable is required (e.g. 2026-05-15)")

# ── 2. Monkey-patch datetime.now() BEFORE any scraper code is imported ────────
import datetime as _dt_module
from datetime import timedelta as _timedelta

_OriginalDatetime = _dt_module.datetime

try:
    _target_dt = _OriginalDatetime.strptime(_TARGET_DATE_STR, "%Y-%m-%d")
except ValueError:
    raise SystemExit(
        f"ERROR: Invalid TARGET_DATE '{_TARGET_DATE_STR}'. Expected format: YYYY-MM-DD"
    )

_scrape_dt = _target_dt - _timedelta(days=1)
print(f"[BACKFILL] Save date   (TARGET_DATE) : {_target_dt.strftime('%Y-%m-%d')}")
print(f"[BACKFILL] Scrape date (day before)  : {_scrape_dt.strftime('%Y-%m-%d')}")


class _PatchedDatetime(_OriginalDatetime):
    """Drop-in replacement for datetime that returns TARGET_DATE from .now()."""
    @classmethod
    def now(cls, tz=None):
        if tz is not None:
            return _target_dt.replace(tzinfo=tz)
        return _target_dt


_dt_module.datetime = _PatchedDatetime  # Apply patch before downstream imports

# ── 3. Resolve paths ─────────────────────────────────────────────────────────
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_CATEGORY_DIR = os.path.abspath(os.path.join(_THIS_DIR, "..", "..", "Commercials"))
_CFL_ROOT = os.path.abspath(os.path.join(_THIS_DIR, "..", ".."))

for _p in (_CATEGORY_DIR, _CFL_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ── 4. Delegate to the original main() ───────────────────────────────────────
import asyncio
import main as _orig_main  # resolves from _CATEGORY_DIR

asyncio.run(_orig_main.main())
