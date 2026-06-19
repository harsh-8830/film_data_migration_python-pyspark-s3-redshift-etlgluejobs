"""
glue_s3_to_redshift.py
=======================
AWS Glue PySpark Job — Read Dirty CSV from S3 → Clean → Write directly to Redshift

FLOW:
    s3://aws-yog/incremental-data/<date>/<table>/<table>.csv   (dirty raw data)
        ↓
    Glue PySpark Job
        ↓  Step 1: Read dirty CSV
        ↓  Step 2: Clean + Deduplicate
        ↓  Step 3: Write directly to Redshift table
    Redshift → sakila_db.<table_name>

GLUE JOB PARAMETERS (Glue Console → Job details → Job parameters):
    --SOURCE_BUCKET       : e.g. aws-yog
    --S3_PREFIX           : e.g. incremental-data/
    --RUN_DATE            : e.g. 2024-06-14
    --REDSHIFT_URL        : jdbc:redshift://your-cluster.xxxxxx.ap-south-1.redshift.amazonaws.com:5439/dev
    --REDSHIFT_USER       : admin
    --REDSHIFT_PASSWORD   : your-password
    --REDSHIFT_IAM_ROLE   : arn:aws:iam::<account-id>:role/RedshiftS3AccessRole
    --REDSHIFT_SCHEMA     : public       (Redshift schema where tables live)
    --TEMP_S3_PATH        : s3://aws-yog/redshift-temp/   (Glue needs temp S3 path for Redshift write)

REQUIREMENTS:
    - AWS Glue 4.0+
    - Add Redshift JDBC connector in Glue job → Connections
    - Worker type : G.1X, minimum 2 workers
    - IAM role    : s3:GetObject on SOURCE_BUCKET, s3:PutObject on TEMP_S3_PATH,
                    redshift:GetClusterCredentials
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
from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType

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
logger = logging.getLogger("GlueS3ToRedshift")


# ============================================================
# SECTION 2 — GLUE / SPARK CONTEXT
# ============================================================

sc           = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark        = glue_context.spark_session
job          = Job(glue_context)


# ============================================================
# SECTION 3 — RESOLVE JOB PARAMETERS
# ============================================================

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SOURCE_BUCKET",       # S3 bucket with dirty raw CSVs
        "S3_PREFIX",           # e.g. "incremental-data/"
        "RUN_DATE",            # e.g. "2024-06-14"
        "REDSHIFT_URL",        # JDBC URL for Redshift cluster
        "REDSHIFT_USER",       # Redshift username
        "REDSHIFT_PASSWORD",   # Redshift password
        "REDSHIFT_IAM_ROLE",   # IAM role ARN attached to Redshift for S3 access
        "REDSHIFT_SCHEMA",     # Target schema in Redshift e.g. "public"
        "TEMP_S3_PATH",        # Temp S3 path Glue uses internally to stage data for Redshift
    ]
)

SOURCE_BUCKET     = args["SOURCE_BUCKET"]
S3_PREFIX         = args["S3_PREFIX"]
RUN_DATE          = args["RUN_DATE"]
REDSHIFT_URL      = args["REDSHIFT_URL"]
REDSHIFT_USER     = args["REDSHIFT_USER"]
REDSHIFT_PASSWORD = args["REDSHIFT_PASSWORD"]
REDSHIFT_IAM_ROLE = args["REDSHIFT_IAM_ROLE"]
REDSHIFT_SCHEMA   = args["REDSHIFT_SCHEMA"]
TEMP_S3_PATH      = args["TEMP_S3_PATH"]

# Full S3 prefix for today's dirty raw files
# e.g. incremental-data/2024-06-14/
RAW_DATE_PREFIX = f"{S3_PREFIX}{RUN_DATE}/"

job.init(args["JOB_NAME"], args)

logger.info("=" * 65)
logger.info(f"Job name      : {args['JOB_NAME']}")
logger.info(f"Run date      : {RUN_DATE}")
logger.info(f"Source        : s3://{SOURCE_BUCKET}/{RAW_DATE_PREFIX}")
logger.info(f"Destination   : Redshift → {REDSHIFT_SCHEMA}.<table>")
logger.info("=" * 65)


# ============================================================
# SECTION 4 — DISCOVER DIRTY CSV FILES FOR TODAY
# ============================================================

def list_raw_tables(bucket: str, prefix: str) -> list[dict]:
    """
    List all dirty CSV files under today's S3 prefix.

    Returns a list of dicts:
        s3_uri     : full s3:// path passed to spark.read.csv
        table_name : folder name = table name
                     e.g. "incremental-data/2024-06-14/payment/payment.csv"
                          → table_name = "payment"
    """
    s3        = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    tables    = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if not key.endswith(".csv"):
                continue   # skip folder markers

            # Derive table name from folder name under the date prefix
            relative   = key[len(prefix):]       # e.g. "payment/payment.csv"
            table_name = relative.split("/")[0]   # e.g. "payment"

            s3_uri = f"s3://{bucket}/{key}"
            tables.append({"s3_uri": s3_uri, "table_name": table_name})
            logger.info(f"[DISCOVER] {s3_uri}  →  table='{table_name}'")

    if not tables:
        logger.warning(f"[DISCOVER] No CSV files found under s3://{bucket}/{prefix}")

    return tables


# ============================================================
# SECTION 5 — COLUMN NAME NORMALISATION
# ============================================================

def snake_case(name: str) -> str:
    """
    Normalise any column name to snake_case.
        "Last Update"  → "last_update"
        "CustomerID"   → "customerid"
    """
    name = name.strip().lower()
    name = re.sub(r"[\s\-]+", "_", name)
    name = re.sub(r"[^\w]", "", name)
    return name


# ============================================================
# SECTION 6 — CLEANING + DEDUPLICATION
# ============================================================

def clean_dataframe(df, table_name: str):
    """
    Apply full cleaning pipeline to the dirty raw DataFrame.

    Steps:
        6.1  Normalise column names → snake_case
        6.2  Trim whitespace from all string columns
        6.3  Cast date / timestamp columns
        6.4  Replace empty strings with NULL
        6.5  Fill NULL in numeric columns → 0
        6.6  Fill NULL in string columns → ""
        6.7  Drop all-NULL zombie rows
        6.8  Exact-row deduplication
        6.9  PK-based deduplication (keep latest row per PK)
        6.10 Table-specific rules
    """

    original_count = df.count()
    logger.info(f"[CLEAN] '{table_name}' — dirty input rows: {original_count:,}")

    # ── 6.1  Normalise column names ──────────────────────────────────
    for col in df.columns:
        safe = snake_case(col)
        if safe != col:
            df = df.withColumnRenamed(col, safe)

    # Classify columns by data type
    all_cols     = df.columns
    string_cols  = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    numeric_cols = [
        f.name for f in df.schema.fields
        if isinstance(f.dataType, (IntegerType, LongType, DoubleType))
    ]
    ts_cols = [c for c in all_cols if "date" in c or "update" in c or "time" in c]

    # ── 6.2  Trim whitespace ─────────────────────────────────────────
    for col in string_cols:
        df = df.withColumn(col, F.trim(F.col(col)))

    logger.info(f"[CLEAN] '{table_name}' — trimmed whitespace on {len(string_cols)} column(s)")

    # ── 6.3  Parse timestamp columns ─────────────────────────────────
    # Dirty CSV data often has timestamps as plain strings.
    # to_timestamp() handles most ISO-8601 formats; bad values → NULL.
    for col in ts_cols:
        if col in string_cols:
            df = df.withColumn(col, F.to_timestamp(F.col(col)))
            logger.info(f"[CLEAN] '{table_name}' — cast '{col}' → TimestampType")

    # ── 6.4  Empty string → NULL ──────────────────────────────────────
    for col in string_cols:
        df = df.withColumn(
            col,
            F.when(F.col(col) == "", None).otherwise(F.col(col))
        )

    # ── 6.5  Fill numeric NULLs → 0 ──────────────────────────────────
    if numeric_cols:
        df = df.fillna({col: 0 for col in numeric_cols})

    # ── 6.6  Fill string NULLs → "" ───────────────────────────────────
    if string_cols:
        df = df.fillna({col: "" for col in string_cols})

    # ── 6.7  Drop all-NULL zombie rows ────────────────────────────────
    if len(all_cols) > 1:
        df = df.dropna(how="all", subset=df.columns[1:])

    # ── 6.8  Exact-row deduplication ─────────────────────────────────
    before_dedup = df.count()
    df = df.dropDuplicates()
    logger.info(
        f"[DEDUP] '{table_name}' — exact dedup: "
        f"{before_dedup:,} → {df.count():,} rows"
    )

    # ── 6.9  PK-based dedup — keep latest row per primary key ─────────
    # Handles cases where the same PK appears multiple times in the dirty CSV.
    pk_col     = df.columns[0]
    order_col  = "last_update" if "last_update" in df.columns else pk_col
    window     = Window.partitionBy(pk_col).orderBy(F.col(order_col).desc())

    before_pk = df.count()
    df = (
        df.withColumn("_row_rank", F.row_number().over(window))
          .filter(F.col("_row_rank") == 1)
          .drop("_row_rank")
    )
    logger.info(
        f"[DEDUP] '{table_name}' — PK dedup on '{pk_col}': "
        f"{before_pk:,} → {df.count():,} rows"
    )

    # ── 6.10  Table-specific rules ─────────────────────────────────────
    # Add per-table cleaning logic here. Examples:
    #
    # if table_name == "payment":
    #     df = df.filter(F.col("amount") > 0)
    #     df = df.withColumn("amount", F.round(F.col("amount"), 2))
    #
    # if table_name == "customer":
    #     df = df.withColumn("email", F.lower(F.col("email")))

    final_count = df.count()
    logger.info(
        f"[CLEAN] '{table_name}' — done: "
        f"{original_count:,} dirty → {final_count:,} clean rows "
        f"({original_count - final_count:,} removed)"
    )
    return df


# ============================================================
# SECTION 7 — WRITE CLEAN DATA DIRECTLY TO REDSHIFT
# ============================================================

def write_to_redshift(df, table_name: str) -> bool:
    """
    Write a cleaned Spark DataFrame directly into a Redshift table.

    Uses Glue's built-in Redshift JDBC writer with the COPY command
    internally — Glue stages data to TEMP_S3_PATH then issues a
    Redshift COPY, which is the fastest way to bulk load into Redshift.

    Write modes:
        "append"    → adds rows to existing table (used for incremental)
        "overwrite" → drops and recreates the table (use for full reload)

    The target table must already exist in Redshift with matching columns.
    If the table doesn't exist yet, use mode="overwrite" and Glue will
    create it automatically from the DataFrame schema.
    """

    if df.rdd.isEmpty():
        logger.warning(f"[SKIP] '{table_name}' — empty after cleaning, nothing to write.")
        return False

    # Full Redshift table reference: schema.table_name
    redshift_table = f"{REDSHIFT_SCHEMA}.{table_name}"

    try:
        logger.info(f"[REDSHIFT] Writing '{table_name}' → {redshift_table}")

        (
            df.write
              .format("jdbc")
              .option("url",              REDSHIFT_URL)
              .option("dbtable",          redshift_table)
              .option("user",             REDSHIFT_USER)
              .option("password",         REDSHIFT_PASSWORD)
              .option("driver",           "com.amazon.redshift.jdbc42.Driver")

              # aws_iam_role tells Redshift which IAM role to use for
              # the internal COPY command Glue issues when staging to S3
              .option("aws_iam_role",     REDSHIFT_IAM_ROLE)

              # Temp S3 path Glue uses to stage data before COPY into Redshift
              # Must be accessible by both Glue IAM role and Redshift IAM role
              .option("tempdir",          TEMP_S3_PATH)

              # append = add new rows to existing table
              # overwrite = drop + recreate table (use for first load)
              .mode("append")

              .save()
        )

        logger.info(f"[REDSHIFT] '{table_name}' — {df.count():,} rows written to {redshift_table}")
        return True

    except Exception as exc:
        logger.error(f"[REDSHIFT ERROR] '{table_name}': {exc}", exc_info=True)
        return False


# ============================================================
# SECTION 8 — MAIN ORCHESTRATION
# ============================================================

def main():
    """
    Full pipeline:
        1. Discover dirty CSV files for today's run date
        2. For each table:
              a. Read dirty CSV from S3
              b. Clean + deduplicate
              c. Write clean data directly to Redshift
        3. Log summary
    """

    # 8.1 — Discover today's dirty CSV files
    raw_tables = list_raw_tables(SOURCE_BUCKET, RAW_DATE_PREFIX)

    if not raw_tables:
        logger.warning("No raw files found. Committing and exiting.")
        job.commit()
        return

    # 8.2 — Track results
    results = {"success": [], "skipped": [], "failed": []}

    # 8.3 — Process each table
    for entry in raw_tables:
        s3_uri     = entry["s3_uri"]
        table_name = entry["table_name"]

        logger.info("─" * 55)
        logger.info(f"[START] Table: '{table_name}'")

        try:
            # ── Step 1: Read dirty CSV from S3 ──────────────────────
            # inferSchema=True auto-detects column types from the dirty CSV.
            # multiLine=True handles quoted fields that span multiple lines.
            dirty_df = (
                spark.read
                     .option("header",      "true")
                     .option("inferSchema", "true")
                     .option("multiLine",   "true")
                     .option("escape",      '"')
                     .csv(s3_uri)
            )
            logger.info(
                f"[READ] '{table_name}' — {dirty_df.count():,} dirty rows, "
                f"{len(dirty_df.columns)} columns"
            )

            # ── Step 2: Clean + deduplicate ──────────────────────────
            clean_df = clean_dataframe(dirty_df, table_name)

            # ── Step 3: Write directly to Redshift ───────────────────
            uploaded = write_to_redshift(clean_df, table_name)

            if uploaded:
                results["success"].append(table_name)
            else:
                results["skipped"].append(table_name)

        except Exception as exc:
            logger.error(f"[ERROR] '{table_name}': {exc}", exc_info=True)
            results["failed"].append(table_name)

    # 8.4 — Summary
    logger.info("=" * 65)
    logger.info("GLUE S3 → REDSHIFT JOB SUMMARY")
    logger.info(f"  ✓ Successful : {results['success']}")
    logger.info(f"  ⚠ Skipped    : {results['skipped']}")
    logger.info(f"  ✗ Failed     : {results['failed']}")
    logger.info("=" * 65)

    job.commit()

    if results["failed"]:
        raise RuntimeError(f"Job finished with errors. Failed tables: {results['failed']}")


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
