import pymysql
import pandas as pd
import boto3
import json
import os
from io import StringIO
from datetime import datetime

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
today = datetime.now().strftime("%Y-%m-%d")  # ✅ Fixed: 4-digit year

STATE_FILE = "last_id_state.json"

if os.path.exists(STATE_FILE):
    with open(STATE_FILE, "r") as f:
        state = json.load(f)
else:
    state = {}

tables_with_last_update = []
tables_without_last_update = []

# ---------- HELPER: upload dataframe to S3 ----------

def upload_to_s3(df, table):
    if df.empty:
        print(f"No new data for {table}")
        return False
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3.put_object(
            Bucket=bucket_name,
            Key=f"{s3_prefix}{today}/{table}/{table}.csv",
            Body=csv_buffer.getvalue().encode("utf-8")  # ✅ Fixed: encode to bytes
        )
        print(f"Uploaded {len(df)} rows for {table}")
        return True
    except Exception as e:
        print(f"S3 upload failed for {table}: {e}")
        return False

# ---------- MAIN ----------

try:
    with connection.cursor() as cursor:

        # Step 1: Get all base tables
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'sakila'
            AND table_type = 'BASE TABLE'
        """)
        tables = [row[0] for row in cursor.fetchall()]
        print("Tables found:", tables)

        # Step 2: Split tables into those WITH and WITHOUT 'last_update'
        for table in tables:
            cursor.execute("""
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_schema = 'sakila'
                AND table_name = %s
                AND column_name = 'last_update'
            """, (table,))  # ✅ Fixed: parameterized query
            count = cursor.fetchone()[0]

            if count > 0:
                tables_with_last_update.append(table)
            else:
                tables_without_last_update.append(table)

        print("\n✅ Tables WITH 'last_update':", tables_with_last_update)
        print("❌ Tables WITHOUT 'last_update':", tables_without_last_update)

        # ---------------------------------------------------
        # CASE 1: Tables WITH last_update -> filter by date
        # ---------------------------------------------------
        for table in tables_with_last_update:
            print(f"\nProcessing (timestamp-based): {table}")

            query = f"""
                SELECT * FROM `{table}`
                WHERE last_update >= %s
                AND last_update < DATE_ADD(%s, INTERVAL 1 DAY)
            """
            # ✅ Fixed: parameterized dates, backtick-quoted table name
            df = pd.read_sql(query, connection, params=[f"{today} 00:00:00", today])
            upload_to_s3(df, table)

        # ---------------------------------------------------
        # CASE 2: Tables WITHOUT last_update -> use primary key high-water mark
        # ---------------------------------------------------
        for table in tables_without_last_update:
            print(f"\nProcessing (PK-based): {table}")

            cursor.execute("""
                SELECT column_name
                FROM information_schema.key_column_usage
                WHERE table_schema = 'sakila'
                AND table_name = %s
                AND constraint_name = 'PRIMARY'
            """, (table,))  # ✅ Fixed: parameterized query
            pk_row = cursor.fetchone()

            if not pk_row:
                print(f"Skipping {table}: no primary key found")
                continue

            pk_column = pk_row[0]
            last_id = state.get(table, 0)

            # ✅ backtick-quote table and column names (they come from information_schema, safe)
            query = f"SELECT * FROM `{table}` WHERE `{pk_column}` > %s"
            df = pd.read_sql(query, connection, params=[last_id])

            uploaded = upload_to_s3(df, table)

            if uploaded:
                new_max_id = int(df[pk_column].max())
                state[table] = new_max_id

finally:
    connection.close()

# Save updated state
with open(STATE_FILE, "w") as f:
    json.dump(state, f)
    print("State file path:", os.path.abspath(STATE_FILE))

print("\nIncremental load completed.")