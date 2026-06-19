import pymysql
import pandas as pd
import boto3
import json
import os
import logging
from io import StringIO
from datetime import datetime

# ---------- LOGGING SETUP ----------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ---------- CONNECTIONS ----------

connection = pymysql.connect(
    host="localhost",
    user="root",
    password="Root",
    database="sakila",
    cursorclass=pymysql.cursors.Cursor
)

s3 = boto3.client(
    "s3",
    region_name="ap-south-1",
    aws_access_key_id="AKIAS6HTGSZLGJZSAQPW",
    aws_secret_access_key="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"
)

bucket_name = "aws-yog"
s3_prefix = "incremental-data/"
today = datetime.now().strftime("%Y-%m-%d")

STATE_FILE = "last_id_state.json"

# ---------- LOAD STATE FILE ----------
# STATE FILE STRUCTURE:
# {
#   "_bootstrapped": true,           <- Set to true after first-time MAX ID seeding
#   "payment": 32098,                <- Last processed PK per table
#   "rental": 16049
# }

if os.path.exists(STATE_FILE):
    with open(STATE_FILE, "r") as f:
        state = json.load(f)
    logger.info(f"Loaded existing state: {state}")
else:
    state = {}
    logger.info("No state file found. Fresh start — will bootstrap MAX IDs before processing.")

tables_with_last_update = []
tables_without_last_update = []

# ---------- HELPER: Upload DataFrame to S3 ----------

def upload_to_s3(df, table):
    if df.empty:
        logger.info(f"[SKIP] No new data for '{table}'")
        return False
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_key = f"{s3_prefix}{today}/{table}/{table}.csv"
        s3.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=csv_buffer.getvalue().encode("utf-8")
        )
        logger.info(f"[UPLOAD] '{table}' -> s3://{bucket_name}/{s3_key} ({len(df)} rows)")
        return True
    except Exception as e:
        logger.error(f"[S3 ERROR] Upload failed for '{table}': {e}")
        return False

# ---------- MAIN ----------

try:
    with connection.cursor() as cursor:

        # -------------------------------------------------------
        # Step 1: Discover all base tables in 'sakila'
        # -------------------------------------------------------
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'sakila'
            AND table_type = 'BASE TABLE'
        """)
        tables = [row[0] for row in cursor.fetchall()]
        logger.info(f"Tables discovered: {tables}")

        # -------------------------------------------------------
        # Step 2: Categorize tables by presence of 'last_update'
        # -------------------------------------------------------
        for table in tables:
            cursor.execute("""
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_schema = 'sakila'
                AND table_name = %s
                AND column_name = 'last_update'
            """, (table,))
            count = cursor.fetchone()[0]
            if count > 0:
                tables_with_last_update.append(table)
            else:
                tables_without_last_update.append(table)

        logger.info(f"Tables WITH 'last_update'    : {tables_with_last_update}")
        logger.info(f"Tables WITHOUT 'last_update' : {tables_without_last_update}")

        # -------------------------------------------------------
        # Step 3: BOOTSTRAP — Seed MAX PKs on very first deploy
        # -------------------------------------------------------
        # If state file didn't exist (or _bootstrapped flag is missing/False),
        # we pre-populate the state with the current MAX primary key for every
        # PK-based table. This prevents the first run from pulling all historical
        # data and causing duplication in S3/downstream systems.
        # -------------------------------------------------------

        if not state.get("_bootstrapped", False):
            logger.info("=" * 60)
            logger.info("BOOTSTRAP MODE: Seeding current MAX PKs into state file.")
            logger.info("No data will be uploaded this run. Incremental picks up NEXT run.")
            logger.info("=" * 60)

            for table in tables_without_last_update:
                cursor.execute("""
                    SELECT column_name
                    FROM information_schema.key_column_usage
                    WHERE table_schema = 'sakila'
                    AND table_name = %s
                    AND constraint_name = 'PRIMARY'
                """, (table,))
                pk_row = cursor.fetchone()

                if not pk_row:
                    logger.warning(f"[BOOTSTRAP] Skipping '{table}': no primary key found")
                    continue

                pk_column = pk_row[0]
                cursor.execute(f"SELECT MAX(`{pk_column}`) FROM `{table}`")
                max_id_row = cursor.fetchone()
                max_id = int(max_id_row[0]) if max_id_row and max_id_row[0] is not None else 0

                state[table] = max_id
                logger.info(f"[BOOTSTRAP] '{table}' -> seeded MAX({pk_column}) = {max_id}")

            state["_bootstrapped"] = True

            # Save bootstrapped state and exit — no data upload on bootstrap run
            with open(STATE_FILE, "w") as f:
                json.dump(state, f, indent=2)
            logger.info(f"Bootstrap complete. State saved to: {os.path.abspath(STATE_FILE)}")
            logger.info("Re-run the pipeline to begin incremental loading from this point.")

        else:
            # -------------------------------------------------------
            # CASE 1: Tables WITH last_update — filter by today's date
            # -------------------------------------------------------
            for table in tables_with_last_update:
                logger.info(f"[TIMESTAMP] Processing '{table}'")
                query = f"""
                    SELECT * FROM `{table}`
                    WHERE last_update >= %s
                    AND last_update < DATE_ADD(%s, INTERVAL 1 DAY)
                """
                df = pd.read_sql(query, connection, params=[f"{today} 00:00:00", today])
                upload_to_s3(df, table)

            # -------------------------------------------------------
            # CASE 2: Tables WITHOUT last_update — PK high-water mark
            # -------------------------------------------------------
            for table in tables_without_last_update:
                logger.info(f"[PK-BASED] Processing '{table}'")

                cursor.execute("""
                    SELECT column_name
                    FROM information_schema.key_column_usage
                    WHERE table_schema = 'sakila'
                    AND table_name = %s
                    AND constraint_name = 'PRIMARY'
                """, (table,))
                pk_row = cursor.fetchone()

                if not pk_row:
                    logger.warning(f"[SKIP] '{table}': no primary key found")
                    continue

                pk_column = pk_row[0]
                last_id = state.get(table, 0)
                logger.info(f"[PK-BASED] '{table}' -> fetching rows where {pk_column} > {last_id}")

                query = f"SELECT * FROM `{table}` WHERE `{pk_column}` > %s"
                df = pd.read_sql(query, connection, params=[last_id])

                uploaded = upload_to_s3(df, table)

                if uploaded:
                    new_max_id = int(df[pk_column].max())
                    logger.info(f"[STATE] '{table}' -> updating last_id: {last_id} -> {new_max_id}")
                    state[table] = new_max_id

            # Save updated state after successful incremental run
            with open(STATE_FILE, "w") as f:
                json.dump(state, f, indent=2)
            logger.info(f"State file updated: {os.path.abspath(STATE_FILE)}")
            logger.info("Incremental load completed.")

finally:
    connection.close()