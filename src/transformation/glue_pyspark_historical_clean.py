"""
glue_pyspark_historical_clean.py
=================================
AWS Glue PySpark Job — Historical Data Cleaning & Deduplication

PURPOSE:
    Reads raw CSVs written by historical_migration.py from the
    historical-data/ prefix, applies PySpark cleaning + deduplication,
    and writes clean output to the final 'final-data' bucket.

RAW INPUT STRUCTURE (written by historical_migration.py):
    s3://<SOURCE_BUCKET>/historical-data/<partition>/<table>/<table>_chunk<N>.csv
    partition is either:
        - a date string  e.g. "2024-06-14"   (tables with last_update)
        - "no-date"                           (tables without last_update)

CLEAN OUTPUT STRUCTURE:
    s3://final-data/
        └── clean-historical/
            └── <table_name>/          ← one folder per table (named after CSV)
                └── <table_name>.csv   ← all chunks merged, cleaned, deduped

GLUE JOB PARAMETERS (Glue Console → Job details → Job parameters):
    --SOURCE_BUCKET  : Raw data bucket              e.g. aws-yog
    --S3_PREFIX      : Historical raw prefix        e.g. historical-data/
    --OUTPUT_PREFIX  : Clean output prefix          e.g. clean-historical/

NOTE:
    FINAL_BUCKET is hardcoded as "final-data" per requirement.
    Change FINAL_BUCKET below if the bucket name differs.

REQUIREMENTS:
    - AWS Glue 4.0+ (Spark 3.3, Python 3.10)
    - IAM role: s3:GetObject on SOURCE_BUCKET, s3:PutObject on final-data
    - Worker type: G.1X, minimum 2 workers (increase for large history sets)
"""

import sys
import re
import boto3
import logging
from collections import defaultdict

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DoubleType
)

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job


# ============================================================
# SECTION 1 — LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("GlueHistoricalClean")


# ============================================================
# SECTION 2 — GLUE / SPARK CONTEXT
# ============================================================

sc           = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark        = glue_context.spark_session
job          = Job(glue_context)


# ============================================================
# SECTION 3 — JOB PARAMETERS
# ============================================================
# FINAL_BUCKET is hardcoded to "final-data" as required.
# All other values come from Glue job parameters so the job
# can be re-used across environments without code changes.

FINAL_BUCKET = "final-data"   # ← destination bucket (hardcoded per requirement)

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SOURCE_BUCKET",   # bucket where historical_migration.py wrote raw chunks
        "S3_PREFIX",       # e.g. "historical-data/"
        "OUTPUT_PREFIX",   # e.g. "clean-historical/"
    ]
)

SOURCE_BUCKET = args["SOURCE_BUCKET"]
S3_PREFIX     = args["S3_PREFIX"]       # e.g. "historical-data/"
OUTPUT_PREFIX = args["OUTPUT_PREFIX"]   # e.g. "clean-historical/"

job.init(args["JOB_NAME"], args)

logger.info("=" * 65)
logger.info(f"Job name      : {args['JOB_NAME']}")
logger.info(f"Source        : s3://{SOURCE_BUCKET}/{S3_PREFIX}")
logger.info(f"Destination   : s3://{FINAL_BUCKET}/{OUTPUT_PREFIX}")
logger.info("=" * 65)


# ============================================================
# SECTION 4 — DISCOVER ALL CHUNK FILES, GROUPED BY TABLE
# ============================================================
# historical_migration.py writes chunks like:
#   historical-data/2024-06-01/payment/payment_chunk0000.csv
#   historical-data/2024-06-02/payment/payment_chunk0001.csv
#   historical-data/no-date/rental/rental_chunk0000.csv
#
# We group ALL chunk URIs by table name so PySpark can union them
# into a single DataFrame per table — cleaning and deduplication
# then happens across the full history, not per-chunk.

def discover_chunks_by_table(bucket: str, prefix: str) -> dict[str, list[str]]:
    """
    Paginate S3 under <prefix> and return a dict:
        { table_name: [s3_uri, s3_uri, ...] }

    Key structure:
        historical-data/<partition>/<table>/<table>_chunk<N>.csv
        ↓
        relative  = "<partition>/<table>/<table>_chunk<N>.csv"
        parts[0]  = partition  (date string or "no-date")
        parts[1]  = table_name
    """
    s3        = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    # defaultdict so we can append without checking key existence
    table_chunks: dict[str, list[str]] = defaultdict(list)

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if not key.endswith(".csv"):
                continue  # skip folder markers or non-CSV files

            # Strip the prefix to get the relative path
            # e.g. "2024-06-01/payment/payment_chunk0000.csv"
            relative = key[len(prefix):]
            parts    = relative.split("/")

            # We need at least partition + table_folder + filename
            if len(parts) < 3:
                logger.warning(f"[DISCOVER] Unexpected key structure, skipping: {key}")
                continue

            # parts[1] is the folder named after the table
            table_name = parts[1]
            s3_uri     = f"s3://{bucket}/{key}"

            table_chunks[table_name].append(s3_uri)

    # Log discovery summary
    for table, uris in table_chunks.items():
        logger.info(f"[DISCOVER] '{table}' — {len(uris)} chunk file(s) found")

    if not table_chunks:
        logger.warning(f"[DISCOVER] No CSV chunks found under s3://{bucket}/{prefix}")

    return dict(table_chunks)


# ============================================================
# SECTION 5 — COLUMN NAME NORMALISATION
# ============================================================

def snake_case(name: str) -> str:
    """
    Convert any column name to snake_case.
        "Last Update"  → "last_update"
        "CustomerID"   → "customerid"
        "  Film Title" → "film_title"
    """
    name = name.strip().lower()
    name = re.sub(r"[\s\-]+", "_", name)   # spaces / hyphens → underscore
    name = re.sub(r"[^\w]", "", name)       # remove remaining special chars
    return name


# ============================================================
# SECTION 6 — CORE CLEANING FUNCTION
# ============================================================

def clean_spark_df(df, table_name: str):
    """
    Apply standardised PySpark cleaning transforms to a historical DataFrame.

    Historical data needs all the same cleaning as incremental, PLUS:
        - Chunk-level duplicates: the same PK may appear in multiple date-
          partition chunks if the table was re-queried during a failed run.
          We use a Window-based PK dedup in addition to exact-row dedup.

    Steps:
        6.1  Normalise column names → snake_case
        6.2  Trim whitespace from all string columns
        6.3  Cast date/timestamp columns
        6.4  Replace empty string with NULL
        6.5  Fill NULL in numeric columns → 0
        6.6  Fill NULL in string columns → ""
        6.7  Drop all-NULL rows (zombie rows)
        6.8  Exact-row deduplication (dropDuplicates)
        6.9  PK-based deduplication (Window, keep latest by last_update or row order)
        6.10 Table-specific rules
    """

    original_count = df.count()
    logger.info(f"[CLEAN] '{table_name}' — input rows across all chunks: {original_count:,}")

    # ── 6.1  Normalise column names ──────────────────────────────────
    for col in df.columns:
        safe = snake_case(col)
        if safe != col:
            df = df.withColumnRenamed(col, safe)

    all_cols     = df.columns
    string_cols  = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    numeric_cols = [
        f.name for f in df.schema.fields
        if isinstance(f.dataType, (IntegerType, LongType, DoubleType))
    ]
    # Detect timestamp-like columns by name convention
    ts_cols = [c for c in all_cols if "date" in c or "update" in c or "time" in c]

    # ── 6.2  Trim whitespace from string columns ──────────────────────
    for col in string_cols:
        df = df.withColumn(col, F.trim(F.col(col)))

    logger.info(f"[CLEAN] '{table_name}' — trimmed {len(string_cols)} string column(s)")

    # ── 6.3  Parse date / timestamp columns ──────────────────────────
    # Historical data often contains serialised datetime strings
    # (historical_migration.py casts them with .astype(str) before CSV export).
    # to_timestamp() handles most ISO-8601 formats; bad values become NULL.
    for col in ts_cols:
        if col in string_cols:  # only recast if it arrived as a string
            df = df.withColumn(col, F.to_timestamp(F.col(col)))
            logger.info(f"[CLEAN] '{table_name}' — cast '{col}' → TimestampType")

    # ── 6.4  Empty string → NULL ──────────────────────────────────────
    # Normalise "" and NULL so aggregations are consistent
    for col in string_cols:
        df = df.withColumn(
            col,
            F.when(F.col(col) == "", None).otherwise(F.col(col))
        )

    # ── 6.5  Fill NULLs in numeric columns → 0 ───────────────────────
    if numeric_cols:
        df = df.fillna({col: 0 for col in numeric_cols})
        logger.info(f"[CLEAN] '{table_name}' — filled {len(numeric_cols)} numeric NULLs → 0")

    # ── 6.6  Fill NULLs in string columns → "" ───────────────────────
    if string_cols:
        df = df.fillna({col: "" for col in string_cols})
        logger.info(f"[CLEAN] '{table_name}' — filled string NULLs → empty string")

    # ── 6.7  Drop all-NULL zombie rows ────────────────────────────────
    # A row where every non-PK column is NULL has no business value.
    if len(all_cols) > 1:
        non_pk_cols = df.columns[1:]   # treat column[0] as PK
        df = df.dropna(how="all", subset=non_pk_cols)

    # ── 6.8  Exact-row deduplication ─────────────────────────────────
    # Removes rows that are 100% identical across all columns.
    # This catches chunk overlaps where the same row was exported twice.
    before_exact = df.count()
    df = df.dropDuplicates()
    after_exact  = df.count()
    logger.info(
        f"[DEDUP] '{table_name}' — exact dedup: "
        f"{before_exact:,} → {after_exact:,} "
        f"({before_exact - after_exact:,} duplicates removed)"
    )

    # ── 6.9  PK-based deduplication (keep latest row per PK) ─────────
    # Historical backfills can produce the same PK in multiple chunk
    # files when a migration was interrupted and re-run.
    # Strategy: within each PK group, keep the row with the latest
    # timestamp (last_update). If no timestamp column exists, keep the
    # row with the highest value in the PK column itself (last seen).
    pk_col = df.columns[0]   # first column assumed to be primary key

    # Choose the ordering column: prefer last_update, fall back to PK
    ts_col_exists = "last_update" in df.columns
    order_col     = "last_update" if ts_col_exists else pk_col
    order_dir     = F.col(order_col).desc() if ts_col_exists else F.col(order_col).desc()

    window = Window.partitionBy(pk_col).orderBy(order_dir)

    before_pk = df.count()
    df = (
        df.withColumn("_row_rank", F.row_number().over(window))
          .filter(F.col("_row_rank") == 1)   # keep only the latest row per PK
          .drop("_row_rank")                  # remove the helper column
    )
    after_pk = df.count()
    logger.info(
        f"[DEDUP] '{table_name}' — PK-based dedup on '{pk_col}' "
        f"(ordered by '{order_col}'): "
        f"{before_pk:,} → {after_pk:,} "
        f"({before_pk - after_pk:,} duplicate PKs removed)"
    )

    # ── 6.10  Table-specific rules ────────────────────────────────────
    # Add per-table logic below. Examples:
    #
    # if table_name == "payment":
    #     df = df.filter(F.col("amount") > 0)          # drop zero-amount rows
    #     df = df.withColumn("amount", F.round(F.col("amount"), 2))
    #
    # if table_name == "customer":
    #     df = df.withColumn("email", F.lower(F.col("email")))
    #
    # if table_name == "film":
    #     df = df.filter(F.col("rental_rate") >= 0)    # no negative rental rates

    final_count = df.count()
    logger.info(
        f"[CLEAN] '{table_name}' — finished: "
        f"{original_count:,} in → {final_count:,} out "
        f"({original_count - final_count:,} total rows removed)"
    )
    return df


# ============================================================
# SECTION 7 — WRITE CLEANED DATA TO final-data BUCKET
# ============================================================

def write_clean(df, table_name: str) -> bool:
    """
    Write a cleaned Spark DataFrame to:
        s3://final-data/<OUTPUT_PREFIX><table_name>/<table_name>.csv

    Strategy:
        - coalesce(1) merges all partitions into a single part file
          (appropriate for datasets that fit in driver memory after dedup).
        - boto3 renames the Spark part file to <table_name>.csv so
          downstream consumers get a stable, human-readable path.
        - The temp staging prefix is deleted after the rename.

    For very large historical tables (millions of rows), replace
    coalesce(1) with repartition(N) and drop the rename step —
    downstream tools like AWS Athena handle multi-part prefixes natively.

    Returns True on success, False on any error.
    """

    if df.rdd.isEmpty():
        logger.warning(f"[SKIP] '{table_name}' — empty after cleaning, nothing to write.")
        return False

    try:
        # ── Staging path (Spark writes part-00000-*.csv here) ────────
        temp_prefix = f"s3://{FINAL_BUCKET}/{OUTPUT_PREFIX}{table_name}/_temp/"
        final_key   = f"{OUTPUT_PREFIX}{table_name}/{table_name}.csv"

        # ── Write single-partition CSV with header ────────────────────
        (
            df.coalesce(1)              # merge all partitions before write
              .write
              .mode("overwrite")        # safe to re-run; previous output replaced
              .option("header", "true")
              .option("quote", '"')     # RFC-4180 quoting for cells with commas
              .csv(temp_prefix)
        )
        logger.info(f"[WRITE] '{table_name}' — staged to: {temp_prefix}")

        # ── Rename part file to <table_name>.csv via boto3 ───────────
        s3_client = boto3.client("s3")

        # List the temp prefix to find the single part file Spark produced
        resp = s3_client.list_objects_v2(
            Bucket=FINAL_BUCKET,
            Prefix=f"{OUTPUT_PREFIX}{table_name}/_temp/"
        )

        part_key = None
        for obj in resp.get("Contents", []):
            if obj["Key"].endswith(".csv"):
                part_key = obj["Key"]
                break

        if not part_key:
            logger.error(f"[WRITE] '{table_name}' — no part file found in temp staging!")
            return False

        # Copy the part file to the clean destination
        s3_client.copy_object(
            Bucket=FINAL_BUCKET,
            CopySource={"Bucket": FINAL_BUCKET, "Key": part_key},
            Key=final_key
        )

        # Delete all objects under the temp prefix (_temp/ + _SUCCESS marker)
        for obj in resp.get("Contents", []):
            s3_client.delete_object(Bucket=FINAL_BUCKET, Key=obj["Key"])

        logger.info(
            f"[UPLOAD] '{table_name}' — clean file saved: "
            f"s3://{FINAL_BUCKET}/{final_key}"
        )
        return True

    except Exception as exc:
        logger.error(f"[S3 ERROR] '{table_name}' — write failed: {exc}", exc_info=True)
        return False


# ============================================================
# SECTION 8 — MAIN ORCHESTRATION
# ============================================================

def main():
    """
    End-to-end pipeline:
        1. Discover all chunk files grouped by table name
        2. For each table:
              a. Union ALL chunk files into one Spark DataFrame
              b. Clean and deduplicate across the full history
              c. Write the single clean CSV to final-data/<table>/
        3. Log summary and fail job if any table errored
    """

    # 8.1 — Discover all raw chunk files, grouped by table
    table_chunks = discover_chunks_by_table(SOURCE_BUCKET, S3_PREFIX)

    if not table_chunks:
        logger.warning("No chunk files discovered. Committing and exiting.")
        job.commit()
        return

    # 8.2 — Track results for the end-of-job summary
    results = {"success": [], "skipped": [], "failed": []}

    # 8.3 — Process each table
    for table_name, chunk_uris in table_chunks.items():

        logger.info("─" * 55)
        logger.info(f"[START] Table: '{table_name}'  ({len(chunk_uris)} chunk file(s))")

        try:
            # ── Step A: Read ALL chunks for this table into one DataFrame ──
            # spark.read.csv accepts a list of URIs and unions them automatically.
            # inferSchema=True detects numeric/date types across the full dataset.
            raw_df = (
                spark.read
                     .option("header", "true")
                     .option("inferSchema", "true")    # auto-detect column types
                     .option("multiLine", "true")       # support quoted newlines
                     .option("escape", '"')             # RFC-4180 quote escaping
                     .csv(chunk_uris)                   # list → automatic UNION ALL
            )

            row_count = raw_df.count()
            logger.info(
                f"[READ] '{table_name}' — {row_count:,} total rows "
                f"across {len(chunk_uris)} chunk(s), "
                f"{len(raw_df.columns)} columns"
            )

            # ── Step B: Clean and deduplicate ───────────────────────────
            clean_df = clean_spark_df(raw_df, table_name)

            # ── Step C: Write to final-data bucket ──────────────────────
            uploaded = write_clean(clean_df, table_name)

            if uploaded:
                results["success"].append(table_name)
            else:
                results["skipped"].append(table_name)

        except Exception as exc:
            # Log and continue — one bad table must not abort the others
            logger.error(f"[ERROR] '{table_name}' — {exc}", exc_info=True)
            results["failed"].append(table_name)

    # 8.4 — Job summary
    logger.info("=" * 65)
    logger.info("HISTORICAL GLUE CLEAN JOB SUMMARY")
    logger.info(f"  ✓ Successful : {results['success']}")
    logger.info(f"  ⚠ Skipped    : {results['skipped']}")
    logger.info(f"  ✗ Failed     : {results['failed']}")
    logger.info(f"  Output       : s3://{FINAL_BUCKET}/{OUTPUT_PREFIX}")
    logger.info("=" * 65)

    # 8.5 — Commit Glue job bookmarks
    job.commit()

    # Raise to mark the Glue job as FAILED in the console if any table errored
    if results["failed"]:
        raise RuntimeError(
            f"Historical clean job finished with errors. "
            f"Failed tables: {results['failed']}"
        )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
