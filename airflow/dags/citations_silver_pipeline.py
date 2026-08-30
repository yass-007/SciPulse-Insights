"""
SciPulse - Citations Silver Pipeline

This DAG orchestrates the OpenAlex citations workflow:

1. Check that Bronze citations data exists
2. Run Great Expectations validation
3. Clean and normalize citations
4. Upload Silver Parquet to MinIO citations-clean/
"""

from datetime import datetime
from pathlib import Path
import subprocess
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio


# =============================================================================
# Configuration
# =============================================================================

CITATIONS_BRONZE_DIR = Path(
    "/opt/airflow/data/bronze/citations"
)

CITATIONS_SILVER_FILE = Path(
    "/opt/airflow/data/silver/citations/citations_clean.parquet"
)

EXPECTATIONS_SCRIPT = (
    "/opt/airflow/src/quality/citations_expectations.py"
)

CLEANING_SCRIPT = (
    "/opt/airflow/src/silver/clean_citations.py"
)

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

MINIO_BUCKET = "citations-clean"
MINIO_OBJECT = "citations_clean.parquet"


# =============================================================================
# Task 1 - Check Bronze data
# =============================================================================

def check_citations_bronze():
    """
    Verify that at least one OpenAlex Bronze JSONL file exists.
    """

    files = sorted(
        CITATIONS_BRONZE_DIR.glob("*.jsonl")
    )

    if not files:
        raise FileNotFoundError(
            f"No citations Bronze files found in "
            f"{CITATIONS_BRONZE_DIR}"
        )

    total_size = sum(
        file.stat().st_size
        for file in files
    )

    print("Citations Bronze data found.")
    print("Files:", len(files))
    print(
        "Total size:",
        round(total_size / (1024 ** 2), 2),
        "MiB",
    )


# =============================================================================
# Generic Python script runner
# =============================================================================

def run_python_script(script_path):
    """
    Execute one SciPulse Python script.
    """

    print(
        f"Executing: {script_path}"
    )

    subprocess.run(
        [
            sys.executable,
            script_path,
        ],
        check=True,
    )

    print(
        f"Successfully completed: "
        f"{script_path}"
    )


# =============================================================================
# Task 2 - Great Expectations
# =============================================================================

def run_citations_expectations():
    """
    Validate citations Bronze data with Great Expectations.
    """

    run_python_script(
        EXPECTATIONS_SCRIPT
    )


# =============================================================================
# Task 3 - Silver cleaning
# =============================================================================

def run_citations_cleaning():
    """
    Clean and normalize OpenAlex citations.
    """

    run_python_script(
        CLEANING_SCRIPT
    )


# =============================================================================
# Task 4 - Upload Silver to MinIO
# =============================================================================

def upload_citations_silver():
    """
    Upload citations_clean.parquet to MinIO.
    """

    if not CITATIONS_SILVER_FILE.exists():
        raise FileNotFoundError(
            f"Citations Silver file not found: "
            f"{CITATIONS_SILVER_FILE}"
        )

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    if not client.bucket_exists(
        MINIO_BUCKET
    ):
        client.make_bucket(
            MINIO_BUCKET
        )

    client.fput_object(
        MINIO_BUCKET,
        MINIO_OBJECT,
        str(CITATIONS_SILVER_FILE),
    )

    print(
        "Citations Silver uploaded successfully."
    )

    print(
        f"Destination: "
        f"{MINIO_BUCKET}/{MINIO_OBJECT}"
    )


# =============================================================================
# DAG definition
# =============================================================================

with DAG(
    dag_id="citations_silver_pipeline",
    description=(
        "Validation, cleaning and Silver "
        "generation for OpenAlex citations"
    ),
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=[
        "scipulse",
        "silver",
        "citations",
        "openalex",
    ],
) as dag:

    check_bronze = PythonOperator(
        task_id="check_citations_bronze",
        python_callable=check_citations_bronze,
    )

    great_expectations = PythonOperator(
        task_id="great_expectations_validation",
        python_callable=run_citations_expectations,
    )

    clean_silver = PythonOperator(
        task_id="clean_citations_silver",
        python_callable=run_citations_cleaning,
    )

    upload_silver = PythonOperator(
        task_id="upload_citations_silver_to_minio",
        python_callable=upload_citations_silver,
    )

    (
        check_bronze
        >> great_expectations
        >> clean_silver
        >> upload_silver
    )