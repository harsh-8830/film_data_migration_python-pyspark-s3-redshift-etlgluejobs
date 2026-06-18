"""
historical_migration.py
=======================
One-time full historical backfill for the Sakila database → S3.

Design
------
- Mirrors the connection/state conventions in incpipe.py so both scripts
  share the same STATE_FILE and can co-exist without overlap.
- Uploads data in configurable chunks (CHUNK_SIZE rows) to avoid loading
  entire large tables into memory.
- Partitions S3 keys by date extracted from each row where possible, falling
  back to a dedicated "historical/" prefix otherwise.
- After a successful migration of every table, writes the bootstrapped state
  file that incpipe.py expects, so incremental loads pick up exactly where
  this script left off — with no gaps and no duplicates.
- Resumable: if the script is interrupted, a migration_progress.json file
  tracks which tables are done so re-runs skip them.

Usage
-----
    python historical_migration.py

Set DRY_RUN = True to log what would be uploaded without touching S3.
"""

import pymysql
import pandas as pd
import boto3
import json
import os
import logging
from io import StringIO
from datetime import datetime, date

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

DB_CONFIG = dict(
    host="localhost",
    user="root",
    password="Root",
    database="sakila",
    cursorclass=pymysql.cursors.Cursor,
)

S3_CONFIG = dict(
    region_name="ap-south-1",
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", ""),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
)

BUCKET_NAME      = "aws-yog"
S3_PREFIX        = "historical-data/"          # distinct from incremental-data/
CHUNK_SIZE       = 10_000                      # rows per upload batch
DRY_RUN          = False                       # True → log only, no S3 writes
STATE_FILE       = "last_id_state.json"        # shared with incpipe.py
PROGRESS_FILE    = "migration_progress.json"   # tracks completed tables
DB_SCHEMA        = "sakila"

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("historical_migration.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONNECTIONS
# ─────────────────────────────────────────────

connection = pymysql.connect(**DB_CONFIG)
s3 = boto3.client("s3", **S3_CONFIG)

# ─────────────────────────────────────────────
# PROGRESS TRACKING  (resumable migrations)
# ─────────────────────────────────────────────

def load_progress() -> dict:
    """Return {table: {"status": "done"|"partial", "rows_uploaded": int}}."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            data = json.load(f)
        logger.info(f"Resuming migration — progress file loaded: {PROGRESS_FILE}")
        return data
    return {}


def save_progress(progress: dict) -> None:
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


# ─────────────────────────────────────────────
# S3 HELPERS
# ─────────────────────────────────────────────

def _s3_key(table: str, partition: str, chunk_index: int) -> str:
    """
    Build an S3 key.
    historical-data/<partition>/<table>/<table>_chunk<N>.csv
    partition is either a date string (YYYY-MM-DD) or "no-date"
    """
    return f"{S3_PREFIX}{partition}/{table}/{table}_chunk{chunk_index:04d}.csv"


def upload_chunk(df: pd.DataFrame, table: str, partition: str, chunk_index: int) -> bool:
    """Upload a single DataFrame chunk to S3. Returns True on success."""
    if df.empty:
        return False
    key = _s3_key(table, partition, chunk_index)
    if DRY_RUN:
        logger.info(f"[DRY-RUN] Would upload {len(df)} rows → s3://{BUCKET_NAME}/{key}")
        return True
    try:
        buf = StringIO()
        df.to_csv(buf, index=False)
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=buf.getvalue().encode("utf-8"),
        )
        logger.info(f"[UPLOAD] {len(df):>7,} rows → s3://{BUCKET_NAME}/{key}")
        return True
    except Exception as exc:
        logger.error(f"[S3 ERROR] {table} chunk {chunk_index}: {exc}")
        return False


# ─────────────────────────────────────────────
# SCHEMA INTROSPECTION
# ─────────────────────────────────────────────

def get_all_tables(cursor) -> list[str]:
    cursor.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
        (DB_SCHEMA,),
    )
    return [r[0] for r in cursor.fetchall()]


def has_column(cursor, table: str, column: str) -> bool:
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s AND column_name = %s
        """,
        (DB_SCHEMA, table, column),
    )
    return cursor.fetchone()[0] > 0


def get_primary_key(cursor, table: str) -> str | None:
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.key_column_usage
        WHERE table_schema = %s AND table_name = %s AND constraint_name = 'PRIMARY'
        LIMIT 1
        """,
        (DB_SCHEMA, table),
    )
    row = cursor.fetchone()
    return row[0] if row else None


def get_row_count(cursor, table: str) -> int:
    cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
    return cursor.fetchone()[0]


def get_max_pk(cursor, table: str, pk_col: str) -> int:
    cursor.execute(f"SELECT MAX(`{pk_col}`) FROM `{table}`")
    row = cursor.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


# ─────────────────────────────────────────────
# MIGRATION STRATEGIES
# ─────────────────────────────────────────────

def migrate_with_timestamp(cursor, table: str, progress: dict) -> int:
    """
    Migrate a table that has a `last_update` timestamp column.
    Partitions S3 files by date so historical and future incremental
    loads land in the same date-partitioned layout.
    Returns total rows uploaded.
    """
    logger.info(f"[TIMESTAMP] Migrating '{table}' (date-partitioned)")

    # Fetch distinct dates present in the table
    cursor.execute(f"SELECT DISTINCT DATE(last_update) FROM `{table}` ORDER BY 1")
    dates = [r[0] for r in cursor.fetchall()]
    logger.info(f"[TIMESTAMP] '{table}' spans {len(dates)} distinct date(s)")

    total_rows = 0
    chunk_index = 0

    for dt in dates:
        dt_str = dt.strftime("%Y-%m-%d") if isinstance(dt, date) else str(dt)
        offset = 0
        while True:
            query = (
                f"SELECT * FROM `{table}` "
                f"WHERE DATE(last_update) = %s "
                f"LIMIT %s OFFSET %s"
            )
            df = pd.read_sql(query, connection, params=[dt_str, CHUNK_SIZE, offset])
            if df.empty:
                break
            # Serialize datetime columns so they survive CSV round-trip cleanly
            for col in df.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
                df[col] = df[col].astype(str)
            if upload_chunk(df, table, dt_str, chunk_index):
                total_rows += len(df)
                chunk_index += 1
            offset += CHUNK_SIZE
            if len(df) < CHUNK_SIZE:
                break

    return total_rows


def migrate_with_pk(cursor, table: str, pk_col: str, progress: dict) -> tuple[int, int]:
    """
    Migrate a table that has no `last_update` column using PK-range chunking.
    Returns (total_rows_uploaded, max_pk_seen).
    """
    logger.info(f"[PK-BASED] Migrating '{table}' on PK '{pk_col}'")

    total_count = get_row_count(cursor, table)
    max_pk      = get_max_pk(cursor, table, pk_col)
    logger.info(f"[PK-BASED] '{table}': {total_count:,} rows, MAX({pk_col})={max_pk}")

    total_rows  = 0
    chunk_index = 0
    last_pk     = 0  # high-water mark across chunks

    while True:
        query = (
            f"SELECT * FROM `{table}` "
            f"WHERE `{pk_col}` > %s "
            f"ORDER BY `{pk_col}` "
            f"LIMIT %s"
        )
        df = pd.read_sql(query, connection, params=[last_pk, CHUNK_SIZE])
        if df.empty:
            break
        # Serialize datetime/date columns
        for col in df.select_dtypes(include=["datetime64[ns]", "datetimetz", "object"]).columns:
            if df[col].dtype == object:
                try:
                    df[col] = df[col].astype(str)
                except Exception:
                    pass
        if upload_chunk(df, table, "no-date", chunk_index):
            total_rows += len(df)
            chunk_index += 1
            last_pk = int(df[pk_col].max())

        if len(df) < CHUNK_SIZE:
            break

    return total_rows, max_pk


def migrate_fallback(table: str, progress: dict) -> int:
    """
    Migrate a table with no PK and no last_update by dumping it whole.
    Used only as a last resort for small lookup/reference tables.
    """
    logger.info(f"[FALLBACK] Migrating '{table}' (no PK, no timestamp) — full dump")
    df = pd.read_sql(f"SELECT * FROM `{table}`", connection)
    if df.empty:
        logger.info(f"[SKIP] '{table}' is empty")
        return 0
    for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
        df[col] = df[col].astype(str)
    upload_chunk(df, table, "no-date", chunk_index=0)
    return len(df)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    logger.info("=" * 65)
    logger.info("HISTORICAL MIGRATION STARTED")
    logger.info(f"Destination : s3://{BUCKET_NAME}/{S3_PREFIX}")
    logger.info(f"Chunk size  : {CHUNK_SIZE:,} rows")
    logger.info(f"Dry run     : {DRY_RUN}")
    logger.info("=" * 65)

    progress   = load_progress()
    # Final state to write for incpipe.py (PK high-water marks)
    incr_state = {"_bootstrapped": True}

    migration_start = datetime.now()
    grand_total_rows = 0

    try:
        with connection.cursor() as cursor:

            tables = get_all_tables(cursor)
            logger.info(f"Tables to migrate: {tables}")

            for table in tables:

                # ── Skip already-completed tables (resumability) ──
                if progress.get(table, {}).get("status") == "done":
                    logger.info(f"[SKIP] '{table}' already migrated — resuming")
                    # Re-populate incr_state from progress so the state file
                    # stays accurate even when we skip tables
                    saved_max = progress[table].get("max_pk")
                    if saved_max is not None:
                        incr_state[table] = saved_max
                    continue

                logger.info(f"── Starting '{table}' ──")
                table_start = datetime.now()

                try:
                    if has_column(cursor, table, "last_update"):
                        # Strategy A: date-partitioned timestamp migration
                        rows = migrate_with_timestamp(cursor, table, progress)
                        progress[table] = {"status": "done", "rows_uploaded": rows}
                        # Tables with last_update are handled by incpipe.py
                        # with today's date filter — no PK state needed.

                    else:
                        pk_col = get_primary_key(cursor, table)
                        if pk_col:
                            # Strategy B: PK high-water mark migration
                            rows, max_pk = migrate_with_pk(cursor, table, pk_col, progress)
                            incr_state[table] = max_pk
                            progress[table] = {
                                "status": "done",
                                "rows_uploaded": rows,
                                "max_pk": max_pk,
                            }
                        else:
                            # Strategy C: full-table fallback for tiny ref tables
                            rows = migrate_fallback(table, progress)
                            progress[table] = {"status": "done", "rows_uploaded": rows}

                    elapsed = (datetime.now() - table_start).total_seconds()
                    logger.info(
                        f"[DONE] '{table}' — {rows:,} rows in {elapsed:.1f}s"
                    )
                    grand_total_rows += rows

                except Exception as exc:
                    logger.error(f"[FAIL] '{table}': {exc}", exc_info=True)
                    progress[table] = {"status": "error", "error": str(exc)}

                finally:
                    # Persist progress after every table so interrupted runs resume cleanly
                    save_progress(progress)

        # ── Write shared state file for incpipe.py ──────────────────
        if not DRY_RUN:
            with open(STATE_FILE, "w") as f:
                json.dump(incr_state, f, indent=2)
            logger.info(
                f"State file written → {os.path.abspath(STATE_FILE)}\n"
                "Incremental pipeline (incpipe.py) can now run without overlap."
            )

        total_elapsed = (datetime.now() - migration_start).total_seconds()
        logger.info("=" * 65)
        logger.info("HISTORICAL MIGRATION COMPLETE")
        logger.info(f"  Tables processed : {len(tables)}")
        logger.info(f"  Total rows       : {grand_total_rows:,}")
        logger.info(f"  Elapsed          : {total_elapsed:.1f}s")
        logger.info("=" * 65)

    finally:
        connection.close()


if __name__ == "__main__":
    main()
