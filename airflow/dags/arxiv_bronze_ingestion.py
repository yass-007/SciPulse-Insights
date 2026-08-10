from datetime import datetime
from pathlib import Path
import zipfile

import boto3
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator


# =============================================================================
# Configuration
# =============================================================================

ARXIV_URL = (
    "https://www.kaggle.com/api/v1/datasets/download/"
    "Cornell-University/arxiv"
)

DOWNLOAD_DIR = Path("/opt/airflow/data/arxiv")

ZIP_PATH = DOWNLOAD_DIR / "arxiv-metadata.zip"

ARXIV_JSON_PATH = (
    DOWNLOAD_DIR / "arxiv-metadata-oai-snapshot.json"
)

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

ARXIV_BUCKET = "arxiv-raw"


# =============================================================================
# Task 1 — Download ArXiv dump
# =============================================================================

def download_arxiv_dump():
    """
    Download the raw ArXiv metadata archive from Kaggle.
    """

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    with requests.get(
        ARXIV_URL,
        stream=True,
        timeout=60,
    ) as response:

        response.raise_for_status()

        with open(ZIP_PATH, "wb") as file:

            for chunk in response.iter_content(
                chunk_size=1024 * 1024
            ):
                if chunk:
                    file.write(chunk)

    print(
        f"ArXiv archive downloaded successfully: {ZIP_PATH}"
    )


# =============================================================================
# Task 2 — Extract raw JSON
# =============================================================================

def extract_arxiv_dump():
    """
    Extract the original ArXiv JSON file from the downloaded archive.

    No data transformation is performed.
    """

    if not ZIP_PATH.exists():
        raise FileNotFoundError(
            f"ArXiv ZIP file not found: {ZIP_PATH}"
        )

    with zipfile.ZipFile(ZIP_PATH, "r") as archive:

        archive.extract(
            "arxiv-metadata-oai-snapshot.json",
            DOWNLOAD_DIR,
        )

    if not ARXIV_JSON_PATH.exists():
        raise FileNotFoundError(
            f"ArXiv JSON extraction failed: {ARXIV_JSON_PATH}"
        )

    print(
        f"ArXiv raw JSON extracted successfully: {ARXIV_JSON_PATH}"
    )


# =============================================================================
# Task 3 — Upload raw JSON to MinIO
# =============================================================================

def upload_arxiv_to_minio():
    """
    Upload the original ArXiv JSON file to the Bronze MinIO bucket.
    """

    if not ARXIV_JSON_PATH.exists():
        raise FileNotFoundError(
            f"ArXiv JSON file not found: {ARXIV_JSON_PATH}"
        )

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    s3.upload_file(
        str(ARXIV_JSON_PATH),
        ARXIV_BUCKET,
        "arxiv-metadata-oai-snapshot.json",
    )

    print(
        "ArXiv raw JSON uploaded successfully "
        f"to MinIO bucket: {ARXIV_BUCKET}"
    )


# =============================================================================
# DAG
# =============================================================================

with DAG(
    dag_id="arxiv_bronze_ingestion",
    description=(
        "Batch ingestion of raw ArXiv metadata "
        "into MinIO Bronze layer"
    ),
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=[
        "scipulse",
        "bronze",
        "arxiv",
    ],
) as dag:

    download_arxiv = PythonOperator(
        task_id="download_arxiv_dump",
        python_callable=download_arxiv_dump,
    )

    extract_arxiv = PythonOperator(
        task_id="extract_arxiv_dump",
        python_callable=extract_arxiv_dump,
    )

    upload_arxiv = PythonOperator(
        task_id="upload_arxiv_to_minio",
        python_callable=upload_arxiv_to_minio,
    )

    download_arxiv >> extract_arxiv >> upload_arxiv
    