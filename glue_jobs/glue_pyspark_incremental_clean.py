"""
glue_pyspark_clean.py
=====================
AWS Glue PySpark Job — Data Cleaning & Deduplication

PURPOSE:
    Reads raw CSVs from the incremental-data/ prefix written by incremental_migration.py,
    performs comprehensive PySpark-based cleaning and deduplication,
    and writes the cleaned output to a final S3 bucket.

OUTPUT FOLDER STRUCTURE:
    s3://<FINAL_BUCKET>/
        └── clean-data/
            └── <table_name>/          ← one folder per table (named after the CSV file)
                └── <table_name>.csv   ← cleaned, deduplicated single CSV file

GLUE JOB PARAMETERS (set in Glue Console → Job details → Job parameters):
    --SOURCE_BUCKET  : Raw data bucket        e.g. aws-yog
    --FINAL_BUCKET   : Clean data bucket      e.g. aws-yog-clean
    --S3_PREFIX      : Raw data prefix        e.g. incremental-data/
    --RUN_DATE       : Date folder to process e.g. 2024-06-14
    --OUTPUT_PREFIX  : Clean output prefix    e.g. clean-data/

REQUIREMENTS:
    - AWS Glue 4.0+ (Spark 3.3, Python 3.10)
    - IAM role: s3:GetObject on SOURCE_BUCKET, s3:PutObject on FINAL_BUCKET
    - Worker type: G.1X (2 DPU minimum recommended)
"""

import sys
import re
import boto3
import logging
from io import StringIO

import pandas as pd

# PySpark core imports
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StringType, IntegerType, LongType,
    DoubleType, TimestampType, DateType
)

# AWS Glue SDK
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

# ============================================================
# SECTION 1 — LOGGING
# ============================================================
# Glue streams all Python logging to CloudWatch Logs automatically.
# Use named logger (not root) to avoid duplicate Spark log noise.

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("GlueCleanJob")


# ============================================================
# SECTION 2 — GLUE / SPARK CONTEXT INITIALISATION
# ============================================================
# GlueContext wraps SparkContext and gives access to Glue DynamicFrames.
# SparkSession is used here for direct DataFrame operations (more flexible
# for schema inference and per-column transforms).

sc           = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark        = glue_context.spark_session
job          = Job(glue_context)

# ============================================================
# SECTION 3 — RESOLVE JOB PARAMETERS
# ============================================================
# All runtime config comes from Glue job parameters — nothing hardcoded.

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SOURCE_BUCKET",
        "FINAL_BUCKET",
        "S3_PREFIX",       # e.g. "incremental-data/"
        "RUN_DATE",        # e.g. "2024-06-14"
        "OUTPUT_PREFIX",   # e.g. "clean-data/"
    ]
)

SOURCE_BUCKET  = args["SOURCE_BUCKET"]
FINAL_BUCKET   = args["FINAL_BUCKET"]
S3_PREFIX      = args["S3_PREFIX"]
RUN_DATE       = args["RUN_DATE"]
OUTPUT_PREFIX  = args["OUTPUT_PREFIX"]

# Full S3 prefix for today's raw CSV files:
# s3://<SOURCE_BUCKET>/incremental-data/2024-06-14/<table>/<table>.csv
RAW_DATE_PREFIX = f"{S3_PREFIX}{RUN_DATE}/"

# Initialise the Glue job (required for job bookmarks and Glue metrics)
job.init(args["JOB_NAME"], args)

logger.info("=" * 65)
logger.info(f"Job name      : {args['JOB_NAME']}")
logger.info(f"Run date      : {RUN_DATE}")
logger.info(f"Source        : s3://{SOURCE_BUCKET}/{RAW_DATE_PREFIX}")
logger.info(f"Destination   : s3://{FINAL_BUCKET}/{OUTPUT_PREFIX}")
logger.info("=" * 65)


# ============================================================
# SECTION 4 — S3 HELPER: DISCOVER CSV FILES FOR TODAY
# ============================================================

def list_raw_tables(bucket: str, prefix: str) -> list[dict]:
    """
    Paginate through S3 objects under <prefix> and return one entry per
    CSV file found.

    Each dict contains:
        s3_uri     : full s3:// URI  (passed to spark.read.csv)
        table_name : folder name one level below the date prefix
                     e.g. "incremental-data/2024-06-14/payment/payment.csv"
                          → table_name = "payment"

    Only .csv keys are returned; folder markers (keys ending with /) are skipped.
    """
    s3        = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    tables    = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if not key.endswith(".csv"):
                continue  # skip folder markers or non-CSV objects

            # Derive table name from folder immediately under the date prefix
            # Example: "incremental-data/2024-06-14/rental/rental.csv"
            #   relative = "rental/rental.csv"  →  table_name = "rental"
            relative   = key[len(prefix):]
            table_name = relative.split("/")[0]

            s3_uri = f"s3://{bucket}/{key}"
            tables.append({"s3_uri": s3_uri, "table_name": table_name})
            logger.info(f"[DISCOVER] {s3_uri}  →  table='{table_name}'")

    if not tables:
        logger.warning(f"[DISCOVER] No CSV files found under s3://{bucket}/{prefix}")

    return tables


# ============================================================
# SECTION 5 — COLUMN NAME NORMALISATION HELPER
# ============================================================

def snake_case(name: str) -> str:
    """
    Convert any column name to snake_case.
        "Last Update"  → "last_update"
        "CustomerID"   → "customerid"
        "  Amount  "   → "amount"
    """
    name = name.strip().lower()
    name = re.sub(r"[\s\-]+", "_", name)   # spaces and hyphens → underscore
    name = re.sub(r"[^\w]", "", name)       # remove any other special characters
    return name


# ============================================================
# SECTION 6 — CORE CLEANING FUNCTION
# ============================================================

def clean_spark_df(df, table_name: str):
    """
    Apply a standardised set of PySpark cleaning transforms.

    Steps (in order):
        6.1  Normalise column names to snake_case
        6.2  Trim leading/trailing whitespace from all string columns
        6.3  Cast columns whose names contain 'date' or 'update' to TimestampType
        6.4  Replace empty strings ("") in string columns with NULL
        6.5  Fill NULL in numeric columns with 0
        6.6  Fill NULL in string columns with empty string ""
        6.7  Drop rows where ALL non-first columns are NULL (zombie rows)
        6.8  Deduplicate — exact duplicate rows removed via dropDuplicates()
        6.9  Table-specific rules section (extend here per table)

    Returns the cleaned Spark DataFrame.
    """

    original_count = df.count()
    logger.info(f"[CLEAN] '{table_name}' — input rows: {original_count}")

    # ----------------------------------------------------------------
    # 6.1  Normalise column names → snake_case
    # ----------------------------------------------------------------
    # Rename every column so downstream SQL and joins are predictable.
    # "Last Update" → "last_update", "customerID" → "customerid"
    for col in df.columns:
        safe = snake_case(col)
        if safe != col:
            df = df.withColumnRenamed(col, safe)
            logger.info(f"[CLEAN] '{table_name}' — renamed column: '{col}' → '{safe}'")

    # Capture the refreshed column list after renaming
    all_cols        = df.columns
    string_cols     = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    numeric_cols    = [
        f.name for f in df.schema.fields
        if isinstance(f.dataType, (IntegerType, LongType, DoubleType))
    ]
    timestamp_cols  = [c for c in all_cols if "date" in c or "update" in c]

    # ----------------------------------------------------------------
    # 6.2  Trim whitespace from all string columns
    # ----------------------------------------------------------------
    # Trailing spaces cause false duplicates and break equality joins.
    for col in string_cols:
        df = df.withColumn(col, F.trim(F.col(col)))

    logger.info(f"[CLEAN] '{table_name}' — trimmed {len(string_cols)} string column(s)")

    # ----------------------------------------------------------------
    # 6.3  Parse date / timestamp columns
    # ----------------------------------------------------------------
    # Columns whose names contain "date" or "update" are assumed temporal.
    # to_timestamp() with try_cast semantics; unparseable values become NULL
    # rather than crashing the job.
    for col in timestamp_cols:
        if col not in string_cols:
            continue  # already typed correctly; skip
        df = df.withColumn(col, F.to_timestamp(F.col(col)))
        logger.info(f"[CLEAN] '{table_name}' — cast '{col}' → TimestampType")

    # ----------------------------------------------------------------
    # 6.4  Convert empty strings "" to NULL in string columns
    # ----------------------------------------------------------------
    # Empty string and NULL are semantically identical for our pipeline;
    # normalising to NULL makes aggregations and CASE logic consistent.
    for col in string_cols:
        df = df.withColumn(
            col,
            F.when(F.col(col) == "", None).otherwise(F.col(col))
        )

    logger.info(f"[CLEAN] '{table_name}' — replaced empty strings with NULL")

    # ----------------------------------------------------------------
    # 6.5  Fill NULL in numeric columns → 0
    # ----------------------------------------------------------------
    if numeric_cols:
        fill_numeric = {col: 0 for col in numeric_cols}
        df = df.fillna(fill_numeric)
        logger.info(f"[CLEAN] '{table_name}' — filled numeric NULLs with 0")

    # ----------------------------------------------------------------
    # 6.6  Fill NULL in string columns → ""
    # ----------------------------------------------------------------
    if string_cols:
        fill_strings = {col: "" for col in string_cols}
        df = df.fillna(fill_strings)
        logger.info(f"[CLEAN] '{table_name}' — filled string NULLs with empty string")

    # ----------------------------------------------------------------
    # 6.7  Drop all-NULL rows  (zombie rows with no meaningful data)
    # ----------------------------------------------------------------
    # A row where every non-PK column is NULL contributes nothing.
    # We treat the first column as the primary key and check the rest.
    if len(all_cols) > 1:
        non_pk_cols = all_cols[1:]  # skip column[0] which is assumed PK
        # dropna(how="all") drops a row only if ALL specified columns are NULL
        df = df.dropna(how="all", subset=non_pk_cols)
        logger.info(f"[CLEAN] '{table_name}' — dropped all-NULL (zombie) rows")

    # ----------------------------------------------------------------
    # 6.8  Deduplication — remove exact duplicate rows
    # ----------------------------------------------------------------
    # dropDuplicates() without arguments compares ALL columns.
    # If you want PK-level dedup (keep latest by a timestamp column),
    # use the Window-based approach in the commented block below.
    before_dedup = df.count()
    df = df.dropDuplicates()
    after_dedup  = df.count()

    logger.info(
        f"[DEDUP] '{table_name}' — removed {before_dedup - after_dedup} "
        f"exact duplicate rows  ({before_dedup} → {after_dedup})"
    )

    # ---- ALTERNATIVE: PK-based dedup — keep the LATEST row per primary key ----
    # Uncomment this block if the same PK can appear multiple times in the CSV
    # (e.g. an UPSERT scenario where only the most recent record should survive).
    #
    # PK_COL        = all_cols[0]          # first column treated as primary key
    # TIMESTAMP_COL = "last_update"        # pick the column that marks recency
    # if TIMESTAMP_COL in df.columns:
    #     window = Window.partitionBy(PK_COL).orderBy(F.col(TIMESTAMP_COL).desc())
    #     df = (
    #         df.withColumn("_row_rank", F.row_number().over(window))
    #           .filter(F.col("_row_rank") == 1)
    #           .drop("_row_rank")
    #     )
    #     logger.info(f"[DEDUP] '{table_name}' — applied PK-based dedup on '{PK_COL}'")

    # ----------------------------------------------------------------
    # 6.9  TABLE-SPECIFIC CLEANING RULES
    # ----------------------------------------------------------------
    # Add per-table logic here.  Examples:
    #
    # if table_name == "payment":
    #     # Drop payments with zero or negative amount — likely data errors
    #     df = df.filter(F.col("amount") > 0)
    #     # Round amount to 2 decimal places
    #     df = df.withColumn("amount", F.round(F.col("amount"), 2))
    #
    # if table_name == "customer":
    #     # Standardise email addresses to lowercase
    #     df = df.withColumn("email", F.lower(F.col("email")))

    final_count = df.count()
    logger.info(
        f"[CLEAN] '{table_name}' — finished: "
        f"{original_count} rows in → {final_count} rows out "
        f"({original_count - final_count} removed total)"
    )
    return df


# ============================================================
# SECTION 7 — WRITE CLEANED DATA TO FINAL BUCKET
# ============================================================

def write_clean(df, table_name: str) -> bool:
    """
    Write a cleaned Spark DataFrame to the final S3 bucket as a single CSV file.

    Output path:
        s3://<FINAL_BUCKET>/<OUTPUT_PREFIX><table_name>/<table_name>.csv

    Strategy:
        - Repartition to 1 so Spark writes exactly one part file (no _SUCCESS
          file fragmentation) — appropriate for datasets that fit in memory.
        - coalesce(1) is used instead of repartition(1) to avoid a full shuffle.
        - The single part file is renamed via boto3 to <table_name>.csv so
          downstream consumers get a stable, human-readable file name.
        - The temporary staging prefix is cleaned up after renaming.

    Returns True on success, False on error.
    """

    if df.rdd.isEmpty():
        logger.warning(f"[SKIP] '{table_name}' — empty after cleaning; nothing to write.")
        return False

    try:
        # Temporary staging path — Spark writes part-00000-*.csv here
        temp_prefix  = f"s3://{FINAL_BUCKET}/{OUTPUT_PREFIX}{table_name}/_temp/"
        # Final destination key for the clean, named CSV
        final_key    = f"{OUTPUT_PREFIX}{table_name}/{table_name}.csv"
        final_uri    = f"s3://{FINAL_BUCKET}/{final_key}"

        # ---- Write to temp staging (coalesce avoids full shuffle) ----
        (
            df.coalesce(1)            # merge all partitions into one before writing
              .write
              .mode("overwrite")      # overwrite any previous run's output
              .option("header", "true")
              .option("quote", '"')   # RFC-4180 quoting for values containing commas
              .csv(temp_prefix)
        )
        logger.info(f"[WRITE] '{table_name}' — staged to: {temp_prefix}")

        # ---- Rename the single part file to <table_name>.csv ----
        # Spark names its output file something like part-00000-<uuid>.csv.
        # We use boto3 to copy it to the final clean path and delete the temp prefix.
        s3_client = boto3.client("s3")

        # List objects in the temp prefix to find the part file
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

        # Copy the part file to the clean destination key
        s3_client.copy_object(
            Bucket=FINAL_BUCKET,
            CopySource={"Bucket": FINAL_BUCKET, "Key": part_key},
            Key=final_key
        )

        # Delete everything under the temp prefix (_temp/ folder + _SUCCESS marker)
        for obj in resp.get("Contents", []):
            s3_client.delete_object(Bucket=FINAL_BUCKET, Key=obj["Key"])

        logger.info(f"[UPLOAD] '{table_name}' — clean file saved: {final_uri}")
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
        1. Discover today's raw CSV files in the source bucket
        2. For each table:
              a. Read CSV into a Spark DataFrame
              b. Clean and deduplicate
              c. Write cleaned output to the final bucket under its own folder
        3. Print a summary and fail the job if any table errored
    """

    # 8.1 — Discover all raw CSV files for the current run date
    raw_tables = list_raw_tables(SOURCE_BUCKET, RAW_DATE_PREFIX)

    if not raw_tables:
        logger.warning("No tables to process. Committing job and exiting.")
        job.commit()
        return

    # 8.2 — Track results for the end-of-job summary
    results = {"success": [], "skipped": [], "failed": []}

    # 8.3 — Process each table
    for entry in raw_tables:
        s3_uri     = entry["s3_uri"]
        table_name = entry["table_name"]

        logger.info("─" * 55)
        logger.info(f"[START] Table: '{table_name}'")

        try:
            # ── Step A: Read raw CSV with header inference ──────────
            # inferSchema=True lets Spark auto-detect numeric and date types.
            # Set to False and provide an explicit schema if inference is slow
            # on very wide tables.
            raw_df = (
                spark.read
                     .option("header", "true")
                     .option("inferSchema", "true")    # auto-detect column types
                     .option("multiLine", "true")       # handle quoted newlines in cells
                     .option("escape", '"')             # RFC-4180 quote escaping
                     .csv(s3_uri)
            )
            logger.info(
                f"[READ] '{table_name}' — {raw_df.count()} rows, "
                f"{len(raw_df.columns)} columns"
            )

            # ── Step B: Clean & deduplicate ─────────────────────────
            clean_df = clean_spark_df(raw_df, table_name)

            # ── Step C: Write to final bucket ───────────────────────
            uploaded = write_clean(clean_df, table_name)

            if uploaded:
                results["success"].append(table_name)
            else:
                results["skipped"].append(table_name)

        except Exception as exc:
            # Log but continue — one bad table should not abort other tables
            logger.error(f"[ERROR] '{table_name}' — {exc}", exc_info=True)
            results["failed"].append(table_name)

    # 8.4 — Job summary
    logger.info("=" * 65)
    logger.info("GLUE PYSPARK JOB SUMMARY")
    logger.info(f"  ✓ Successful : {results['success']}")
    logger.info(f"  ⚠ Skipped    : {results['skipped']}")
    logger.info(f"  ✗ Failed     : {results['failed']}")
    logger.info("=" * 65)

    # 8.5 — Commit Glue job bookmarks; then raise if any table failed
    job.commit()

    if results["failed"]:
        raise RuntimeError(
            f"Glue job finished with errors. Failed tables: {results['failed']}"
        )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
