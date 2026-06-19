"""
lambda_trigger_glue.py
======================
AWS Lambda Function — Auto-trigger Glue Clean Job

TRIGGERS (choose one — configured in Lambda Console):
    A) S3 Event       : fires when incremental_migration.py writes first CSV to S3
    B) EventBridge    : fires on a daily cron schedule (e.g. 2 AM IST)
    C) Direct invoke  : called explicitly at end of incremental_migration.py

ENVIRONMENT VARIABLES (set in Lambda Console → Configuration → Environment variables):
    GLUE_JOB_NAME       : glue_pyspark_clean          (incremental job)
    HIST_GLUE_JOB_NAME  : glue_pyspark_historical_clean (historical job)
    SOURCE_BUCKET       : aws-yog
    FINAL_BUCKET        : final-data
    S3_PREFIX           : incremental-data/
    OUTPUT_PREFIX       : clean-data/
    GLUE_IAM_ROLE       : arn:aws:iam::<account-id>:role/GlueServiceRole
    SNS_TOPIC_ARN       : arn:aws:sns:ap-south-1:<account-id>:glue-alerts  (optional)
"""

import os
import json
import logging
import boto3
from datetime import datetime, timezone

# ── Logging ──────────────────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── AWS Clients ───────────────────────────────────────────────────────
glue = boto3.client("glue", region_name="ap-south-1")
sns  = boto3.client("sns",  region_name="ap-south-1")

# ── Environment Variables ─────────────────────────────────────────────
GLUE_JOB_NAME      = os.environ["GLUE_JOB_NAME"]           # incremental Glue job
HIST_GLUE_JOB_NAME = os.environ.get("HIST_GLUE_JOB_NAME")  # historical Glue job (optional)
SOURCE_BUCKET      = os.environ["SOURCE_BUCKET"]
FINAL_BUCKET       = os.environ["FINAL_BUCKET"]
S3_PREFIX          = os.environ["S3_PREFIX"]
OUTPUT_PREFIX      = os.environ["OUTPUT_PREFIX"]
SNS_TOPIC_ARN      = os.environ.get("SNS_TOPIC_ARN", "")   # optional alerting


# ============================================================
# SECTION 1 — DETECT TRIGGER TYPE
# ============================================================

def detect_trigger(event: dict) -> str:
    """
    Identify what triggered this Lambda invocation.

    Returns:
        "s3"          : S3 PUT event (migration wrote a new CSV)
        "schedule"    : EventBridge scheduled rule (daily cron)
        "historical"  : manual invoke for historical Glue job
        "direct"      : direct invoke from incremental_migration.py
        "unknown"     : unrecognised event shape
    """
    # S3 trigger — event has Records with s3 key
    if "Records" in event and event["Records"][0].get("eventSource") == "aws:s3":
        return "s3"

    # EventBridge scheduled rule
    if event.get("source") == "aws.events":
        detail_type = event.get("detail-type", "")
        if "historical" in detail_type.lower():
            return "historical"
        return "schedule"

    # Direct invoke from migration script or manual test
    if "trigger_type" in event:
        return event["trigger_type"]   # "direct" or "historical"

    return "unknown"


# ============================================================
# SECTION 2 — RESOLVE RUN DATE
# ============================================================

def resolve_run_date(event: dict, trigger: str) -> str:
    """
    Determine the RUN_DATE to pass to the Glue job.

    Priority:
        1. Explicit date in the event payload  (manual override)
        2. Date extracted from S3 key          (when triggered by S3 event)
        3. Today's date                         (schedule / direct triggers)
    """
    # 1. Explicit override in event
    if "run_date" in event:
        logger.info(f"[DATE] Using explicit run_date from event: {event['run_date']}")
        return event["run_date"]

    # 2. Extract date from S3 key written by incremental_migration.py
    # Key pattern: incremental-data/2024-06-14/<table>/<table>.csv
    if trigger == "s3":
        try:
            key   = event["Records"][0]["s3"]["object"]["key"]
            # Key parts: ["incremental-data", "2024-06-14", "<table>", "<table>.csv"]
            parts = key.split("/")
            date  = parts[1]   # index 1 is the date folder
            logger.info(f"[DATE] Extracted run_date from S3 key '{key}': {date}")
            return date
        except (IndexError, KeyError) as exc:
            logger.warning(f"[DATE] Could not extract date from S3 key: {exc}")

    # 3. Fall back to today's date in IST (UTC+5:30)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    logger.info(f"[DATE] Using today's date: {today}")
    return today


# ============================================================
# SECTION 3 — CHECK FOR DUPLICATE / ALREADY-RUNNING JOB
# ============================================================

def is_job_already_running(job_name: str) -> bool:
    """
    Check whether a Glue job run is currently RUNNING or STARTING.
    Prevents duplicate runs if Lambda fires twice for the same S3 event.

    Returns True if a run is in progress, False otherwise.
    """
    try:
        response = glue.get_job_runs(JobName=job_name, MaxResults=5)
        for run in response.get("JobRuns", []):
            if run["JobRunState"] in ("RUNNING", "STARTING", "STOPPING"):
                logger.warning(
                    f"[GUARD] '{job_name}' already has a run in state "
                    f"'{run['JobRunState']}' (RunId: {run['Id']}). Skipping."
                )
                return True
    except Exception as exc:
        logger.error(f"[GUARD] Could not check job runs for '{job_name}': {exc}")
    return False


# ============================================================
# SECTION 4 — START GLUE JOB
# ============================================================

def start_glue_job(job_name: str, job_args: dict) -> str | None:
    """
    Start a Glue job with the given arguments.

    job_args keys must be prefixed with "--" as required by Glue.
    Returns the JobRunId string on success, None on failure.
    """
    try:
        response = glue.start_job_run(
            JobName=job_name,
            Arguments=job_args,
            # Timeout in minutes — adjust based on table count and data volume
            Timeout=120,
        )
        run_id = response["JobRunId"]
        logger.info(f"[GLUE] Started '{job_name}' — RunId: {run_id}")
        return run_id

    except glue.exceptions.ConcurrentRunsExceededException:
        logger.warning(f"[GLUE] '{job_name}' hit concurrent run limit. Will retry later.")
        return None
    except Exception as exc:
        logger.error(f"[GLUE] Failed to start '{job_name}': {exc}", exc_info=True)
        return None


# ============================================================
# SECTION 5 — SNS NOTIFICATION (optional alerting)
# ============================================================

def notify(subject: str, message: str) -> None:
    """
    Publish a notification to the configured SNS topic.
    Silent no-op if SNS_TOPIC_ARN is not set.
    """
    if not SNS_TOPIC_ARN:
        return
    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message
        )
        logger.info(f"[SNS] Notification sent: {subject}")
    except Exception as exc:
        logger.warning(f"[SNS] Failed to send notification: {exc}")


# ============================================================
# SECTION 6 — LAMBDA HANDLER (entry point)
# ============================================================

def lambda_handler(event, context):
    """
    Main Lambda entry point.

    Handles three scenarios:
        A) Incremental trigger (S3 event / schedule / direct)
              → starts glue_pyspark_clean with today's RUN_DATE

        B) Historical trigger (manual invoke or EventBridge rule)
              → starts glue_pyspark_historical_clean

        C) Unknown event shape
              → logs a warning and returns 400
    """

    logger.info("=" * 60)
    logger.info("Lambda invoked — Glue auto-trigger")
    logger.info(f"Event: {json.dumps(event)}")
    logger.info("=" * 60)

    # ── Detect what triggered this Lambda ────────────────────────────
    trigger  = detect_trigger(event)
    run_date = resolve_run_date(event, trigger)

    logger.info(f"[TRIGGER] Type: '{trigger}'  |  Run date: '{run_date}'")

    # ── Route to the correct Glue job ────────────────────────────────

    # ── SCENARIO A: Incremental clean job ────────────────────────────
    if trigger in ("s3", "schedule", "direct"):

        job_name = GLUE_JOB_NAME

        # Guard: skip if already running (S3 events can fire multiple times)
        if is_job_already_running(job_name):
            return {
                "statusCode": 200,
                "body": f"Glue job '{job_name}' already running — skipped."
            }

        # Build Glue job arguments — all keys must start with "--"
        job_args = {
            "--SOURCE_BUCKET" : SOURCE_BUCKET,
            "--FINAL_BUCKET"  : FINAL_BUCKET,
            "--S3_PREFIX"     : S3_PREFIX,
            "--RUN_DATE"      : run_date,
            "--OUTPUT_PREFIX" : OUTPUT_PREFIX,
        }

        run_id = start_glue_job(job_name, job_args)

        if run_id:
            notify(
                subject=f"Glue Job Started — {job_name}",
                message=(
                    f"Job     : {job_name}\n"
                    f"RunId   : {run_id}\n"
                    f"RunDate : {run_date}\n"
                    f"Trigger : {trigger}"
                )
            )
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "job_name" : job_name,
                    "run_id"   : run_id,
                    "run_date" : run_date,
                    "trigger"  : trigger,
                })
            }
        else:
            notify(
                subject=f"⚠ Glue Job FAILED to start — {job_name}",
                message=f"Lambda could not start '{job_name}' for run_date={run_date}"
            )
            return {"statusCode": 500, "body": f"Failed to start '{job_name}'"}

    # ── SCENARIO B: Historical clean job ─────────────────────────────
    elif trigger == "historical":

        if not HIST_GLUE_JOB_NAME:
            logger.error("[HISTORICAL] HIST_GLUE_JOB_NAME env var not set!")
            return {"statusCode": 500, "body": "HIST_GLUE_JOB_NAME not configured."}

        job_name = HIST_GLUE_JOB_NAME

        if is_job_already_running(job_name):
            return {
                "statusCode": 200,
                "body": f"Historical Glue job '{job_name}' already running — skipped."
            }

        # Historical job does not need RUN_DATE — it processes all partitions
        hist_job_args = {
            "--SOURCE_BUCKET" : SOURCE_BUCKET,
            "--S3_PREFIX"     : "historical-data/",   # historical prefix
            "--OUTPUT_PREFIX" : "clean-historical/",
        }

        run_id = start_glue_job(job_name, hist_job_args)

        if run_id:
            notify(
                subject=f"Historical Glue Job Started — {job_name}",
                message=f"Job: {job_name}\nRunId: {run_id}\nTrigger: {trigger}"
            )
            return {
                "statusCode": 200,
                "body": json.dumps({"job_name": job_name, "run_id": run_id})
            }
        else:
            return {"statusCode": 500, "body": f"Failed to start '{job_name}'"}

    # ── SCENARIO C: Unknown trigger ───────────────────────────────────
    else:
        logger.warning(f"[UNKNOWN] Unrecognised trigger type: '{trigger}'")
        return {
            "statusCode": 400,
            "body": f"Unknown trigger type: '{trigger}'. No Glue job started."
        }
