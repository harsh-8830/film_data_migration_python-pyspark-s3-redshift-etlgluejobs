"""
glue_snowflake_to_star.py
==========================
AWS Glue PySpark Job — Snowflake Schema → Star Schema → Redshift / S3

FLOW:
    S3 (dirty snowflake CSVs written by incremental_migration.py)
        ↓
    Glue PySpark
        ↓  Step 1 : Read all raw tables from S3
        ↓  Step 2 : Clean + deduplicate each table
        ↓  Step 3 : Build Star Schema dimensions + fact table by joining tables
        ↓  Step 4 : Write each dimension + fact table to Redshift
    Redshift → star schema tables

SNOWFLAKE TABLES (input from S3):
    customer, address, city, country
    film, film_category, category, language
    store, staff, inventory
    rental, payment

STAR SCHEMA OUTPUT (written to Redshift):
    dim_customer   — customer + address + city + country (flattened)
    dim_film       — film + category + language (flattened)
    dim_store      — store + staff + city (flattened)
    dim_date       — generated from payment dates
    fact_payment   — payment + rental joined with all dimension keys

GLUE JOB PARAMETERS:
    --SOURCE_BUCKET      : e.g. aws-yog
    --S3_PREFIX          : e.g. incremental-data/
    --RUN_DATE           : e.g. 2024-06-14
    --REDSHIFT_URL       : jdbc:redshift://your-cluster.xxxxxx.ap-south-1.redshift.amazonaws.com:5439/dev
    --REDSHIFT_USER      : admin
    --REDSHIFT_PASSWORD  : your-password
    --REDSHIFT_IAM_ROLE  : arn:aws:iam::<account-id>:role/RedshiftS3AccessRole
    --REDSHIFT_SCHEMA    : star             (separate schema for star tables)
    --TEMP_S3_PATH       : s3://aws-yog/redshift-temp/
"""

import sys
import re
import boto3
import logging

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
logger = logging.getLogger("GlueSnowflakeToStar")


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

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SOURCE_BUCKET",
        "S3_PREFIX",
        "RUN_DATE",
        "REDSHIFT_URL",
        "REDSHIFT_USER",
        "REDSHIFT_PASSWORD",
        "REDSHIFT_IAM_ROLE",
        "REDSHIFT_SCHEMA",    # e.g. "star" — keeps star schema separate from raw
        "TEMP_S3_PATH",
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

RAW_DATE_PREFIX = f"{S3_PREFIX}{RUN_DATE}/"

job.init(args["JOB_NAME"], args)

logger.info("=" * 65)
logger.info(f"Job           : {args['JOB_NAME']}")
logger.info(f"Run date      : {RUN_DATE}")
logger.info(f"Source        : s3://{SOURCE_BUCKET}/{RAW_DATE_PREFIX}")
logger.info(f"Destination   : Redshift schema '{REDSHIFT_SCHEMA}'")
logger.info("=" * 65)


# ============================================================
# SECTION 4 — READ ALL RAW TABLES FROM S3 INTO A DICT
# ============================================================

def read_all_tables(bucket: str, prefix: str) -> dict:
    """
    Read every CSV under today's S3 prefix into a dict of Spark DataFrames.

    Returns:
        { "payment": DataFrame, "rental": DataFrame, ... }

    All tables are loaded upfront so the transformation steps below
    can join them freely without repeated S3 reads.
    """
    s3        = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    tables    = {}

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".csv"):
                continue

            # Extract table name from folder: "incremental-data/date/payment/payment.csv"
            relative   = key[len(prefix):]
            table_name = relative.split("/")[0]
            s3_uri     = f"s3://{bucket}/{key}"

            df = (
                spark.read
                     .option("header",      "true")
                     .option("inferSchema", "true")
                     .option("multiLine",   "true")
                     .option("escape",      '"')
                     .csv(s3_uri)
            )

            # Normalise all column names to snake_case immediately on read
            df = normalise_columns(df)

            tables[table_name] = df
            logger.info(
                f"[READ] '{table_name}' — {df.count():,} rows, "
                f"{len(df.columns)} columns"
            )

    logger.info(f"[READ] Tables loaded: {list(tables.keys())}")
    return tables


# ============================================================
# SECTION 5 — COLUMN NAME NORMALISATION
# ============================================================

def normalise_columns(df):
    """Rename all columns to snake_case so joins work reliably."""
    for col in df.columns:
        safe = col.strip().lower()
        safe = re.sub(r"[\s\-]+", "_", safe)
        safe = re.sub(r"[^\w]", "", safe)
        if safe != col:
            df = df.withColumnRenamed(col, safe)
    return df


# ============================================================
# SECTION 6 — CLEANING FUNCTION (applied per raw table)
# ============================================================

def clean(df, table_name: str):
    """
    Standard cleaning pipeline applied to each raw snowflake table
    before it is used in the star schema joins.

    Steps: trim → parse timestamps → replace empty→NULL → fill NULLs → dedup
    """
    string_cols  = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    numeric_cols = [
        f.name for f in df.schema.fields
        if isinstance(f.dataType, (IntegerType, LongType, DoubleType))
    ]
    ts_cols = [c for c in df.columns if "date" in c or "update" in c or "time" in c]

    # Trim whitespace
    for col in string_cols:
        df = df.withColumn(col, F.trim(F.col(col)))

    # Parse timestamp strings
    for col in ts_cols:
        if col in string_cols:
            df = df.withColumn(col, F.to_timestamp(F.col(col)))

    # Empty string → NULL
    for col in string_cols:
        df = df.withColumn(col, F.when(F.col(col) == "", None).otherwise(F.col(col)))

    # Fill NULLs
    if numeric_cols:
        df = df.fillna({col: 0 for col in numeric_cols})
    if string_cols:
        df = df.fillna({col: "" for col in string_cols})

    # Exact-row deduplication
    df = df.dropDuplicates()

    logger.info(f"[CLEAN] '{table_name}' — {df.count():,} rows after cleaning")
    return df


# ============================================================
# SECTION 7 — BUILD STAR SCHEMA DIMENSIONS + FACT TABLE
# ============================================================

# ── 7A  dim_customer ─────────────────────────────────────────────────
def build_dim_customer(tables: dict):
    """
    Flatten the snowflake customer → address → city → country chain
    into a single wide dim_customer table.

    Snowflake:
        customer(address_id) → address(city_id) → city(country_id) → country

    Star output columns:
        customer_id, first_name, last_name, email, active,
        address, district, postal_code,
        city, country
    """
    logger.info("[DIM] Building dim_customer")

    customer = clean(tables["customer"], "customer")
    address  = clean(tables["address"],  "address")
    city     = clean(tables["city"],     "city")
    country  = clean(tables["country"],  "country")

    dim = (
        customer
        # Join customer → address
        .join(
            address.select("address_id", "address", "district", "postal_code", "city_id"),
            on="address_id",
            how="left"
        )
        # Join address → city
        .join(
            city.select("city_id", "city", "country_id"),
            on="city_id",
            how="left"
        )
        # Join city → country
        .join(
            country.select("country_id", "country"),
            on="country_id",
            how="left"
        )
        # Select only the columns needed in the dimension
        .select(
            F.col("customer_id"),
            F.col("first_name"),
            F.col("last_name"),
            F.col("email"),
            F.col("active"),
            F.col("address"),
            F.col("district"),
            F.col("postal_code"),
            F.col("city"),
            F.col("country"),
            F.col("create_date"),
            F.col("last_update")
        )
        .dropDuplicates(["customer_id"])   # one row per customer
    )

    logger.info(f"[DIM] dim_customer — {dim.count():,} rows")
    return dim


# ── 7B  dim_film ──────────────────────────────────────────────────────
def build_dim_film(tables: dict):
    """
    Flatten the snowflake film → film_category → category + language chain
    into a single wide dim_film table.

    Snowflake:
        film(language_id) → language
        film ← film_category → category

    Star output columns:
        film_id, title, description, release_year,
        rental_duration, rental_rate, length, rating,
        language, category
    """
    logger.info("[DIM] Building dim_film")

    film          = clean(tables["film"],          "film")
    film_category = clean(tables["film_category"], "film_category")
    category      = clean(tables["category"],      "category")
    language      = clean(tables["language"],      "language")

    dim = (
        film
        # Join film → language
        .join(
            language.select(
                F.col("language_id"),
                F.col("name").alias("language")   # rename to avoid collision
            ),
            on="language_id",
            how="left"
        )
        # Join film → film_category bridge table
        .join(
            film_category.select("film_id", "category_id"),
            on="film_id",
            how="left"
        )
        # Join film_category → category
        .join(
            category.select(
                F.col("category_id"),
                F.col("name").alias("category")   # rename to avoid collision
            ),
            on="category_id",
            how="left"
        )
        .select(
            F.col("film_id"),
            F.col("title"),
            F.col("description"),
            F.col("release_year"),
            F.col("rental_duration"),
            F.col("rental_rate"),
            F.col("length"),
            F.col("rating"),
            F.col("language"),
            F.col("category"),
            F.col("last_update")
        )
        .dropDuplicates(["film_id"])   # one row per film
    )

    logger.info(f"[DIM] dim_film — {dim.count():,} rows")
    return dim


# ── 7C  dim_store ─────────────────────────────────────────────────────
def build_dim_store(tables: dict):
    """
    Flatten store → staff (manager) → address → city into dim_store.

    Star output columns:
        store_id, manager_first_name, manager_last_name,
        store_address, store_city, store_country
    """
    logger.info("[DIM] Building dim_store")

    store   = clean(tables["store"],   "store")
    staff   = clean(tables["staff"],   "staff")
    address = clean(tables["address"], "address")
    city    = clean(tables["city"],    "city")
    country = clean(tables["country"], "country")

    dim = (
        store
        # Join store → manager (staff table)
        .join(
            staff.select(
                F.col("staff_id").alias("manager_staff_id"),
                F.col("first_name").alias("manager_first_name"),
                F.col("last_name").alias("manager_last_name"),
            ),
            store["manager_staff_id"] == F.col("manager_staff_id"),
            how="left"
        )
        # Join store → address
        .join(
            address.select(
                F.col("address_id"),
                F.col("address").alias("store_address"),
                F.col("city_id")
            ),
            on="address_id",
            how="left"
        )
        # Join address → city
        .join(
            city.select(
                F.col("city_id"),
                F.col("city").alias("store_city"),
                F.col("country_id")
            ),
            on="city_id",
            how="left"
        )
        # Join city → country
        .join(
            country.select(
                F.col("country_id"),
                F.col("country").alias("store_country")
            ),
            on="country_id",
            how="left"
        )
        .select(
            F.col("store_id"),
            F.col("manager_first_name"),
            F.col("manager_last_name"),
            F.col("store_address"),
            F.col("store_city"),
            F.col("store_country"),
            F.col("last_update")
        )
        .dropDuplicates(["store_id"])
    )

    logger.info(f"[DIM] dim_store — {dim.count():,} rows")
    return dim


# ── 7D  dim_date ──────────────────────────────────────────────────────
def build_dim_date(tables: dict):
    """
    Generate a date dimension from all distinct payment dates.

    No source table has a date dimension in Sakila — we derive it
    from the payment_date column in the payment table.

    Star output columns:
        date_id (YYYYMMDD int), full_date,
        day, month, month_name, quarter, year, day_of_week, is_weekend
    """
    logger.info("[DIM] Building dim_date")

    payment = clean(tables["payment"], "payment")

    dim = (
        payment
        # Extract just the date part from payment_date timestamp
        .select(F.to_date(F.col("payment_date")).alias("full_date"))
        .dropDuplicates()
        .filter(F.col("full_date").isNotNull())

        # Generate date attributes from the date value
        .withColumn("date_id",     F.date_format(F.col("full_date"), "yyyyMMdd").cast("int"))
        .withColumn("day",         F.dayofmonth(F.col("full_date")))
        .withColumn("month",       F.month(F.col("full_date")))
        .withColumn("month_name",  F.date_format(F.col("full_date"), "MMMM"))
        .withColumn("quarter",     F.quarter(F.col("full_date")))
        .withColumn("year",        F.year(F.col("full_date")))
        .withColumn("day_of_week", F.date_format(F.col("full_date"), "EEEE"))
        .withColumn("is_weekend",
            F.when(F.dayofweek(F.col("full_date")).isin(1, 7), True).otherwise(False)
        )
        .select(
            "date_id", "full_date", "day", "month",
            "month_name", "quarter", "year", "day_of_week", "is_weekend"
        )
        .orderBy("date_id")
    )

    logger.info(f"[DIM] dim_date — {dim.count():,} distinct dates")
    return dim


# ── 7E  fact_payment (central fact table) ────────────────────────────
def build_fact_payment(tables: dict):
    """
    Build the central fact table by joining payment → rental → inventory
    to resolve all foreign keys to dimension surrogate keys.

    Snowflake joins resolved:
        payment → rental → inventory → film  (gets film_id)
        payment → rental → customer          (gets customer_id)
        payment → staff  → store             (gets store_id)
        payment.payment_date                 (gets date_id)

    Star output columns (all dimension FKs + measures):
        payment_id   — PK
        customer_id  → dim_customer
        film_id      → dim_film
        store_id     → dim_store
        date_id      → dim_date
        amount       — measure (payment amount)
        rental_id    — degenerate dimension (kept for traceability)
        payment_date — full timestamp for time-of-day analysis
    """
    logger.info("[FACT] Building fact_payment")

    payment   = clean(tables["payment"],   "payment")
    rental    = clean(tables["rental"],    "rental")
    inventory = clean(tables["inventory"], "inventory")
    staff     = clean(tables["staff"],     "staff")

    fact = (
        payment
        # Join payment → rental to get inventory_id and customer_id
        .join(
            rental.select("rental_id", "inventory_id", "customer_id"),
            on="rental_id",
            how="left"
        )
        # Join rental → inventory to get film_id and store_id
        .join(
            inventory.select("inventory_id", "film_id", "store_id"),
            on="inventory_id",
            how="left"
        )
        # Join payment → staff to get store_id (fallback if inventory join misses)
        .join(
            staff.select(
                F.col("staff_id"),
                F.col("store_id").alias("staff_store_id")
            ),
            on="staff_id",
            how="left"
        )
        # Resolve store_id: prefer inventory store, fall back to staff store
        .withColumn(
            "store_id",
            F.coalesce(F.col("store_id"), F.col("staff_store_id"))
        )
        # Generate date_id from payment_date (matches dim_date.date_id)
        .withColumn(
            "date_id",
            F.date_format(F.to_date(F.col("payment_date")), "yyyyMMdd").cast("int")
        )
        # Select only the star schema columns — no snowflake FK chains
        .select(
            F.col("payment_id"),       # fact PK
            F.col("customer_id"),      # FK → dim_customer
            F.col("film_id"),          # FK → dim_film
            F.col("store_id"),         # FK → dim_store
            F.col("date_id"),          # FK → dim_date
            F.col("rental_id"),        # degenerate dimension
            F.col("amount"),           # measure
            F.col("payment_date"),     # full timestamp
            F.col("last_update")
        )
        .dropDuplicates(["payment_id"])
    )

    logger.info(f"[FACT] fact_payment — {fact.count():,} rows")
    return fact


# ============================================================
# SECTION 8 — WRITE STAR SCHEMA TABLE TO REDSHIFT
# ============================================================

def write_to_redshift(df, table_name: str, mode: str = "append") -> bool:
    """
    Write a star schema DataFrame to Redshift via JDBC.

    Glue internally stages the data to TEMP_S3_PATH and issues a
    Redshift COPY command — the fastest bulk load method.

    Args:
        df         : cleaned star schema DataFrame
        table_name : target Redshift table  (e.g. "dim_customer")
        mode       : "append" for incremental, "overwrite" for full reload
    """
    if df.rdd.isEmpty():
        logger.warning(f"[SKIP] '{table_name}' is empty — nothing to write.")
        return False

    redshift_table = f"{REDSHIFT_SCHEMA}.{table_name}"

    try:
        (
            df.write
              .format("jdbc")
              .option("url",          REDSHIFT_URL)
              .option("dbtable",      redshift_table)
              .option("user",         REDSHIFT_USER)
              .option("password",     REDSHIFT_PASSWORD)
              .option("driver",       "com.amazon.redshift.jdbc42.Driver")
              .option("aws_iam_role", REDSHIFT_IAM_ROLE)
              .option("tempdir",      TEMP_S3_PATH)
              .mode(mode)
              .save()
        )
        logger.info(f"[REDSHIFT] '{redshift_table}' — {df.count():,} rows written ({mode})")
        return True

    except Exception as exc:
        logger.error(f"[REDSHIFT ERROR] '{redshift_table}': {exc}", exc_info=True)
        return False


# ============================================================
# SECTION 9 — MAIN ORCHESTRATION
# ============================================================

def main():
    """
    End-to-end pipeline:
        1. Read all raw snowflake tables from S3
        2. Build each star schema dimension + fact table via joins
        3. Write every star schema table to Redshift
    """

    # 9.1 — Load all raw snowflake tables from S3 into memory
    tables = read_all_tables(SOURCE_BUCKET, RAW_DATE_PREFIX)

    if not tables:
        logger.warning("No raw tables found. Exiting.")
        job.commit()
        return

    # 9.2 — Build star schema tables
    # Dimensions first, then fact table (fact needs dimension keys)
    star_tables = {}

    logger.info("── Building Star Schema ──")

    # Build only if the required source tables exist in S3
    if all(t in tables for t in ["customer", "address", "city", "country"]):
        star_tables["dim_customer"] = build_dim_customer(tables)

    if all(t in tables for t in ["film", "film_category", "category", "language"]):
        star_tables["dim_film"] = build_dim_film(tables)

    if all(t in tables for t in ["store", "staff", "address", "city", "country"]):
        star_tables["dim_store"] = build_dim_store(tables)

    if "payment" in tables:
        star_tables["dim_date"] = build_dim_date(tables)

    if all(t in tables for t in ["payment", "rental", "inventory", "staff"]):
        star_tables["fact_payment"] = build_fact_payment(tables)

    logger.info(f"Star schema tables built: {list(star_tables.keys())}")

    # 9.3 — Write each star schema table to Redshift
    results = {"success": [], "failed": []}

    for table_name, df in star_tables.items():
        logger.info("─" * 55)

        # Dimensions: overwrite (always reflect latest state)
        # Fact table: append (accumulate new daily payments)
        write_mode = "append" if table_name.startswith("fact_") else "overwrite"

        success = write_to_redshift(df, table_name, mode=write_mode)
        (results["success"] if success else results["failed"]).append(table_name)

    # 9.4 — Summary
    logger.info("=" * 65)
    logger.info("SNOWFLAKE → STAR SCHEMA JOB SUMMARY")
    logger.info(f"  ✓ Successful : {results['success']}")
    logger.info(f"  ✗ Failed     : {results['failed']}")
    logger.info("=" * 65)

    job.commit()

    if results["failed"]:
        raise RuntimeError(f"Job finished with errors. Failed: {results['failed']}")


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
