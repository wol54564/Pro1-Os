"""Backfill runner for Automotive-Cars-and-Trucks — runs as if it were TARGET_DATE.

Usage:
    TARGET_DATE=2026-05-15 python main.py

The scraper will fetch listings from TARGET_DATE - 1 day (the day before the
TARGET_DATE you supply).  R2 data is stored under the TARGET_DATE partition.
"""

import os
import sys
from datetime import datetime, timedelta

# ── 1. Read and validate TARGET_DATE ─────────────────────────────────────────
_TARGET_DATE_STR = os.environ.get("TARGET_DATE", "").strip()
if not _TARGET_DATE_STR:
    raise SystemExit("ERROR: TARGET_DATE environment variable is required (e.g. 2026-05-15)")

try:
    _target_dt = datetime.strptime(_TARGET_DATE_STR, "%Y-%m-%d")
except ValueError:
    raise SystemExit(
        f"ERROR: Invalid TARGET_DATE '{_TARGET_DATE_STR}'. Expected format: YYYY-MM-DD"
    )

_scrape_dt = _target_dt - timedelta(days=1)
print(f"[BACKFILL] Save date   (TARGET_DATE) : {_target_dt.strftime('%Y-%m-%d')}")
print(f"[BACKFILL] Scrape date (day before)  : {_scrape_dt.strftime('%Y-%m-%d')}")

# ── 2. Set environment variables for scraper to read (if needed) ──────────────
os.environ['BACKFILL_TARGET_DATE'] = _target_dt.strftime('%Y-%m-%d')
os.environ['BACKFILL_SCRAPE_DATE'] = _scrape_dt.strftime('%Y-%m-%d')

# ── 3. Resolve paths ─────────────────────────────────────────────────────────
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_CATEGORY_DIR = os.path.abspath(os.path.join(_THIS_DIR, "..", "..", "Automotive-Cars-and-Trucks"))
_CFL_ROOT = os.path.abspath(os.path.join(_THIS_DIR, "..", ".."))

for _p in (_CATEGORY_DIR, _CFL_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ── 4. Delegate to the original main() ───────────────────────────────────────
import asyncio
import main as _orig_main  # resolves from _CATEGORY_DIR

asyncio.run(_orig_main.main())
