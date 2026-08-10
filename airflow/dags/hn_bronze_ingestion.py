from datetime import datetime, timezone
from pathlib import Path
import json

import boto3
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator


# =============================================================================
# Configuration
# =============================================================================

HN_NEW_STORIES_URL = (
    "https://hacker-news.firebaseio.com/v0/newstories.json"
)

HN_ITEM_URL = (
    "https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
)

DATA_DIR = Path("/opt/airflow/data/hn")

HN_BATCH_SIZE = 50

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
HN_BUCKET = "hn-raw"


# =============================================================================
# Task 1 — Fetch raw Hacker News stories
# =============================================================================

def fetch_hn_stories(ti):
    """
    Fetch latest Hacker News stories and store them locally
    as raw JSON Lines.
    """

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    response = requests.get(
        HN_NEW_STORIES_URL,
        timeout=30,
    )
    response.raise_for_status()

    story_ids = response.json()
    selected_ids = story_ids[:HN_BATCH_SIZE]

    timestamp = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ"
    )

    output_path = DATA_DIR / f"hn_{timestamp}.jsonl"

    with open(output_path, "w", encoding="utf-8") as file:

        for story_id in selected_ids:

            story_response = requests.get(
                HN_ITEM_URL.format(item_id=story_id),
                timeout=30,
            )

            story_response.raise_for_status()

            raw_story = story_response.json()

            if raw_story is not None:
                file.write(
                    json.dumps(
                        raw_story,
                        ensure_ascii=False,
                    )
                    + "\n"
                )

    print(
        f"Hacker News micro-batch written to: {output_path}"
    )

    ti.xcom_push(
        key="hn_file_path",
        value=str(output_path),
    )


# =============================================================================
# Task 2 — Upload raw file to MinIO
# =============================================================================

def upload_hn_to_minio(ti):
    """
    Upload the raw Hacker News JSONL micro-batch
    to MinIO Bronze layer.
    """

    file_path = ti.xcom_pull(
        task_ids="fetch_hn_stories",
        key="hn_file_path",
    )

    if not file_path:
        raise ValueError(
            "No Hacker News file path received from previous task."
        )

    local_path = Path(file_path)

    if not local_path.exists():
        raise FileNotFoundError(
            f"Hacker News file not found: {local_path}"
        )

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    s3.upload_file(
        str(local_path),
        HN_BUCKET,
        local_path.name,
    )

    print(
        f"Hacker News raw file uploaded successfully "
        f"to MinIO bucket: {HN_BUCKET}/{local_path.name}"
    )


# =============================================================================
# DAG
# =============================================================================

with DAG(
    dag_id="hn_bronze_ingestion",
    description=(
        "Micro-batch ingestion of raw Hacker News "
        "stories into MinIO Bronze layer"
    ),
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    tags=[
        "scipulse",
        "bronze",
        "hacker-news",
    ],
) as dag:

    fetch_hn = PythonOperator(
        task_id="fetch_hn_stories",
        python_callable=fetch_hn_stories,
    )

    upload_hn = PythonOperator(
        task_id="upload_hn_to_minio",
        python_callable=upload_hn_to_minio,
    )

    fetch_hn >> upload_hn