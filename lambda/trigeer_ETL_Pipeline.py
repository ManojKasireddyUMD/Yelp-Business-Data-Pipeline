import os
import json
import logging
import requests
import boto3
from requests.exceptions import RequestException

#—— SETUP ———————————————————————————————————————————————————————————————
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Databricks settings
DATABRICKS_INSTANCE = os.getenv("DATABRICKS_INSTANCE")
DATABRICKS_JOB_ID   = int(os.getenv("DATABRICKS_JOB_ID", "0"))
DATABRICKS_TOKEN    = os.getenv("DATABRICKS_TOKEN")
if not (DATABRICKS_INSTANCE and DATABRICKS_JOB_ID and DATABRICKS_TOKEN):
    raise RuntimeError("Missing one of DATABRICKS_INSTANCE, DATABRICKS_JOB_ID, DATABRICKS_TOKEN")

DB_API_URL = f"https://{DATABRICKS_INSTANCE}/api/2.1/jobs/run-now"
HEADERS    = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type":  "application/json",
}

# Glue crawler name
CRAWLER_NAME = "UnifiedAnalyticsCrawler"
glue = boto3.client("glue")

def invoke_databricks_job(raw_key: str) -> int:
    """
    Fire off a run-now for your job, passing raw_key into notebook_params.
    Returns the run_id on success.
    """
    payload = {
        "job_id": DATABRICKS_JOB_ID,
        "notebook_params": { "raw_key": raw_key }
    }
    resp = requests.post(DB_API_URL, headers=HEADERS, json=payload, timeout=10)
    resp.raise_for_status()
    run_id = resp.json().get("run_id")
    if not run_id:
        raise RuntimeError(f"No run_id returned: {resp.text}")
    return run_id

def start_crawler():
    """
    Trigger the Glue crawler to refresh the table schema/data.
    """
    try:
        glue.start_crawler(Name=CRAWLER_NAME)
        logger.info(f"Triggered Glue crawler '{CRAWLER_NAME}'")
    except glue.exceptions.CrawlerRunningException:
        logger.info(f"Crawler '{CRAWLER_NAME}' already running, skipping")
    except Exception:
        logger.exception(f"Failed to start Glue crawler '{CRAWLER_NAME}'")
        raise

#—— HANDLER —————————————————————————————————————————————————————————————
def lambda_handler(event, context):
    """
    Processes an S3 event, triggers the Databricks job for each JSON file,
    then triggers the Glue crawler once at the end.
    """
    triggered = []
    for rec in event.get("Records", []):
        s3 = rec.get("s3", {})
        bucket = s3.get("bucket", {}).get("name")
        key    = s3.get("object", {}).get("key")
        
        if not key or not key.lower().endswith(".json"):
            logger.info("Skipping non-JSON object: %s", key)
            continue
        
        logger.info("Received new JSON: s3://%s/%s", bucket, key)
        try:
            run_id = invoke_databricks_job(key)
            logger.info("Started Databricks job %s run_id=%s", DATABRICKS_JOB_ID, run_id)
            triggered.append({"key": key, "run_id": run_id})
        except RequestException as e:
            logger.error("HTTP error triggering Databricks job for %s: %s", key, e)
            raise
        except Exception as e:
            logger.error("Error triggering Databricks job for %s: %s", key, e)
            raise
    
    # After all Databricks runs are kicked off, refresh the Glue catalog
    start_crawler()

    return {
        "statusCode": 200,
        "body": json.dumps({"triggered": triggered})
    }

