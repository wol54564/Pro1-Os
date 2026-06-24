"""
push_to_motherduck.py
=====================
Upsert flat hub Parquet tables from R2 into MotherDuck for the Evidence dashboard.

Reads:
  monitor-sites/hub/tables/{table}/{partition-date}.parquet

Requires env:
  MOTHERDUCK_TOKEN
  MOTHERDUCK_DATABASE   (e.g. monitor_hub)

Usage
-----
  python monitor/push_to_motherduck.py --from-r2
  python monitor/push_to_motherduck.py --date 2026-06-16 --from-r2
  python monitor/push_to_motherduck.py --partition 2026-06-17 --from-r2
  python monitor/push_to_motherduck.py --from-local ./out --partition 2026-06-17
  python monitor/push_to_motherduck.py --backfill --days 30 --from-r2
  python monitor/push_to_motherduck.py --init-schema
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import duckdb
import pandas as pd

from export_hub_tables import (
    ALERTS_COLS,
    HUB_DAILY_COLS,
    SCRAPER_DAILY_COLS,
    SITE_DAILY_COLS,
)
from monitor_r2 import (
    MONITOR_SITES_ROOT,
    build_r2_client,
    hub_table_parquet_key,
    hub_tables_base,
    list_hub_partition_dates,
    load_registry_from_r2,
    partition_date_for_listing,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("push-motherduck")

TABLE_COLUMNS: Dict[str, List[str]] = {
    "hub_daily": HUB_DAILY_COLS,
    "site_daily": SITE_DAILY_COLS,
    "scraper_daily": SCRAPER_DAILY_COLS,
    "alerts": ALERTS_COLS,
}

SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS hub_daily (
      hub_partition_date   DATE NOT NULL,
      inspect_date         DATE,
      generated_at         TIMESTAMP,
      sites_total          INTEGER,
      sites_ok             INTEGER,
      sites_failed         INTEGER,
      sites_missing        INTEGER,
      total_alerts         INTEGER,
      total_unique_ads     INTEGER,
      hub_prefix           VARCHAR,
      PRIMARY KEY (hub_partition_date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS site_daily (
      hub_partition_date   DATE NOT NULL,
      site_id              VARCHAR NOT NULL,
      folder               VARCHAR,
      display_name         VARCHAR,
      website              VARCHAR,
      country              VARCHAR,
      repo                 VARCHAR,
      github_username      VARCHAR,
      run_place            VARCHAR,
      workflow_name        VARCHAR,
      workflow_run_number  INTEGER,
      workflow_run_id      VARCHAR,
      workflow_status      VARCHAR,
      workflow_duration_sec INTEGER,
      schedule             VARCHAR,
      status               VARCHAR,
      scrapers_total       INTEGER,
      scrapers_passed      INTEGER,
      alert_count          INTEGER,
      unique_ads           INTEGER,
      run_date             DATE,
      inspect_date         DATE,
      report_fallback      BOOLEAN DEFAULT FALSE,
      PRIMARY KEY (hub_partition_date, site_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS scraper_daily (
      hub_partition_date   DATE NOT NULL,
      site_id              VARCHAR NOT NULL,
      scraper              VARCHAR NOT NULL,
      files_found          INTEGER,
      checks_passed        INTEGER,
      checks_total         INTEGER,
      all_passed           BOOLEAN,
      files_optional       BOOLEAN DEFAULT FALSE,
      unique_ads           INTEGER,
      total_rows           INTEGER,
      ads_source           VARCHAR,
      PRIMARY KEY (hub_partition_date, site_id, scraper)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS alerts (
      hub_partition_date   DATE NOT NULL,
      site_id              VARCHAR NOT NULL,
      scraper              VARCHAR,
      severity             VARCHAR,
      alert_type           VARCHAR,
      check_name           VARCHAR,
      detail               VARCHAR,
      file_key             VARCHAR,
      alert_id             VARCHAR,
      PRIMARY KEY (alert_id)
    )
    """,
]

MIGRATION_STATEMENTS = [
    "ALTER TABLE site_daily ADD COLUMN IF NOT EXISTS run_place VARCHAR",
    "ALTER TABLE site_daily ADD COLUMN IF NOT EXISTS workflow_name VARCHAR",
    "ALTER TABLE site_daily ADD COLUMN IF NOT EXISTS workflow_run_number INTEGER",
    "ALTER TABLE site_daily ADD COLUMN IF NOT EXISTS workflow_run_id VARCHAR",
    "ALTER TABLE site_daily ADD COLUMN IF NOT EXISTS workflow_status VARCHAR",
    "ALTER TABLE site_daily ADD COLUMN IF NOT EXISTS workflow_duration_sec INTEGER",
    "ALTER TABLE site_daily ADD COLUMN IF NOT EXISTS unique_ads INTEGER",
    "ALTER TABLE hub_daily ADD COLUMN IF NOT EXISTS total_unique_ads INTEGER",
    "ALTER TABLE scraper_daily ADD COLUMN IF NOT EXISTS unique_ads INTEGER",
    "ALTER TABLE scraper_daily ADD COLUMN IF NOT EXISTS total_rows INTEGER",
    "ALTER TABLE scraper_daily ADD COLUMN IF NOT EXISTS ads_source VARCHAR",
]


def connect_motherduck() -> duckdb.DuckDBPyConnection:
    token = os.environ.get("MOTHERDUCK_TOKEN", "").strip()
    if not token:
        raise EnvironmentError(
            "MOTHERDUCK_TOKEN is required. Add it to GitHub Actions secrets or your shell env."
        )
    database = (os.environ.get("MOTHERDUCK_DATABASE") or "monitor_hub").strip()
    log.info(f"Connecting to MotherDuck database: {database}")
    return duckdb.connect(f"md:{database}?motherduck_token={token}")


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    for stmt in SCHEMA_STATEMENTS:
        con.execute(stmt)
    for stmt in MIGRATION_STATEMENTS:
        con.execute(stmt)
    log.info("MotherDuck schema ready (hub_daily, site_daily, scraper_daily, alerts)")


def _align_df(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    out = df.copy()
    for col in columns:
        if col not in out.columns:
            out[col] = None
    return out[columns]


def upsert_table(
    con: duckdb.DuckDBPyConnection,
    table: str,
    partition_date: str,
    df: pd.DataFrame,
) -> int:
    columns = TABLE_COLUMNS[table]
    con.execute(
        f"DELETE FROM {table} WHERE hub_partition_date = ?::DATE",
        [partition_date],
    )
    if df.empty:
        log.info(f"  {table}: 0 rows (deleted stale partition {partition_date})")
        return 0

    batch = _align_df(df, columns)
    con.register("batch_df", batch)
    cols = ", ".join(columns)
    con.execute(f"INSERT INTO {table} ({cols}) SELECT {cols} FROM batch_df")
    con.unregister("batch_df")
    n = len(batch)
    log.info(f"  {table}: inserted {n} row(s) for {partition_date}")
    return n


def load_parquet_from_r2(client, bucket: str, key: str) -> Optional[pd.DataFrame]:
    try:
        resp = client.get_object(Bucket=bucket, Key=key)
        return pd.read_parquet(io.BytesIO(resp["Body"].read()))
    except client.exceptions.NoSuchKey:
        return None


def load_parquet_local(base_dir: Path, table: str, partition_date: str) -> Optional[pd.DataFrame]:
    path = base_dir / table / f"{partition_date}.parquet"
    if not path.is_file():
        return None
    return pd.read_parquet(path)


def load_partition_tables_from_r2(
    client,
    bucket: str,
    partition_date: str,
    root: str,
) -> Dict[str, pd.DataFrame]:
    tables: Dict[str, pd.DataFrame] = {}
    for table in TABLE_COLUMNS:
        key = hub_table_parquet_key(table, partition_date, root)
        df = load_parquet_from_r2(client, bucket, key)
        if df is None:
            log.warning(f"  missing r2://{bucket}/{key}")
            tables[table] = pd.DataFrame({c: [] for c in TABLE_COLUMNS[table]})
        else:
            tables[table] = df
    return tables


def load_partition_tables_local(base_dir: Path, partition_date: str) -> Dict[str, pd.DataFrame]:
    tables: Dict[str, pd.DataFrame] = {}
    for table in TABLE_COLUMNS:
        df = load_parquet_local(base_dir, table, partition_date)
        if df is None:
            log.warning(f"  missing local {base_dir / table / partition_date}.parquet")
            tables[table] = pd.DataFrame({c: [] for c in TABLE_COLUMNS[table]})
        else:
            tables[table] = df
    return tables


def push_partition(
    con: duckdb.DuckDBPyConnection,
    partition_date: str,
    tables: Dict[str, pd.DataFrame],
) -> int:
    total = 0
    log.info(f"Pushing partition {partition_date} to MotherDuck …")
    for table in TABLE_COLUMNS:
        total += upsert_table(con, table, partition_date, tables[table])
    return total


def resolve_partition_date(partition: Optional[str], listing_date: Optional[str]) -> str:
    if partition:
        return partition
    if listing_date:
        listing_dt = datetime.strptime(listing_date, "%Y-%m-%d")
        return partition_date_for_listing(listing_dt).strftime("%Y-%m-%d")
    listing_dt = datetime.utcnow() - timedelta(days=1)
    return partition_date_for_listing(listing_dt).strftime("%Y-%m-%d")


def backfill_partitions(
    con: duckdb.DuckDBPyConnection,
    client,
    bucket: str,
    root: str,
    days: int,
    from_local: Optional[Path],
) -> int:
    if from_local:
        # Local backfill: scan hub_daily folder
        hub_dir = from_local / "hub_daily"
        if not hub_dir.is_dir():
            log.error(f"No hub_daily folder under {from_local}")
            return 0
        all_dates = sorted(
            p.stem for p in hub_dir.glob("*.parquet")
            if len(p.stem) == 10
        )
    else:
        all_dates = list_hub_partition_dates(client, bucket, root)

    if not all_dates:
        log.warning("No hub partitions found for backfill")
        return 0

    if days > 0:
        max_dt = datetime.strptime(all_dates[-1], "%Y-%m-%d")
        min_dt = max_dt - timedelta(days=days - 1)
        targets = [
            d for d in all_dates
            if min_dt <= datetime.strptime(d, "%Y-%m-%d") <= max_dt
        ]
    else:
        targets = all_dates

    log.info(f"Backfill push: {len(targets)} partition(s) {targets[0]} … {targets[-1]}")

    ok = 0
    for partition_date in targets:
        try:
            if from_local:
                tables = load_partition_tables_local(from_local, partition_date)
            else:
                tables = load_partition_tables_from_r2(client, bucket, partition_date, root)
            if tables["hub_daily"].empty:
                log.warning(f"  skip {partition_date}: hub_daily parquet empty or missing")
                continue
            push_partition(con, partition_date, tables)
            ok += 1
        except Exception as exc:
            log.error(f"  skip {partition_date}: {exc}")

    return ok


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Push hub Parquet tables from R2 into MotherDuck.")
    p.add_argument("--date", help="Listing date (YYYY-MM-DD) → partition = date + 1 day")
    p.add_argument("--partition", help="Hub partition date (YYYY-MM-DD)")
    p.add_argument("--from-r2", action="store_true", help="Read Parquet from R2")
    p.add_argument("--from-local", type=Path, help="Read Parquet from local directory")
    p.add_argument("--backfill", action="store_true", help="Push all partitions in range")
    p.add_argument("--days", type=int, default=30, help="Backfill window (default 30)")
    p.add_argument(
        "--init-schema",
        action="store_true",
        help="Create tables only (no data push)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    con = connect_motherduck()
    ensure_schema(con)

    if args.init_schema:
        log.info("Schema initialized — no data pushed")
        con.close()
        return

    client = None
    bucket: Optional[str] = None
    root = MONITOR_SITES_ROOT

    if args.from_r2 or args.backfill:
        client, env_bucket = build_r2_client()
        bucket = os.environ.get("CF_R2_BUCKET_NAME") or env_bucket
        try:
            registry = load_registry_from_r2(client, bucket, root)
            hub_cfg = registry.get("hub", {})
            root = hub_cfg.get("monitor_sites_prefix", root)
            bucket = os.environ.get("CF_R2_BUCKET_NAME") or hub_cfg.get("r2_bucket") or bucket
        except FileNotFoundError as exc:
            log.warning(f"Registry not loaded ({exc})")

    if args.backfill:
        if not args.from_local and client is None:
            log.error("--backfill requires --from-r2 (CF_R2_* env) or --from-local")
            sys.exit(1)
        count = backfill_partitions(
            con,
            client,
            bucket or "",
            root,
            args.days,
            args.from_local,
        )
        log.info(f"Backfill push complete: {count} partition(s)")
        con.close()
        return

    partition_date = resolve_partition_date(args.partition, args.date)

    if args.from_local:
        tables = load_partition_tables_local(args.from_local, partition_date)
    elif args.from_r2 or client is not None:
        if client is None or bucket is None:
            log.error("--from-r2 requires CF_R2_* env vars")
            sys.exit(1)
        tables = load_partition_tables_from_r2(client, bucket, partition_date, root)
    else:
        log.error("Specify --from-r2 or --from-local")
        sys.exit(1)

    if tables["hub_daily"].empty:
        log.error(
            f"No hub_daily data for {partition_date}. "
            f"Run export_hub_tables.py first (r2://…/{hub_tables_base(root)}/hub_daily/{partition_date}.parquet)"
        )
        sys.exit(1)

    rows = push_partition(con, partition_date, tables)
    log.info(f"Done — partition {partition_date}, {rows} total row(s) inserted")
    con.close()


if __name__ == "__main__":
    main()
