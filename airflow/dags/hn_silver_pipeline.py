"""
SciPulse - Hacker News Silver Pipeline

This DAG orchestrates the Hacker News Silver workflow:

1. Check that Bronze micro-batches are available
2. Run Bronze quality profiling
3. Run Great Expectations validations and generate Data Docs
4. Clean and deduplicate Hacker News data
5. Write the cleaned dataset as Parquet
6. Upload the Silver dataset to MinIO hn-clean/

Bronze data is never modified.
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

HN_BRONZE_DIR = Path(
    "/opt/airflow/data/bronze/hn"
)

HN_SILVER_FILE = Path(
    "/opt/airflow/data/silver/hn/hn_clean.parquet"
)

QUALITY_SCRIPT = (
    "/opt/airflow/src/quality/analyze_hn.py"
)

EXPECTATIONS_SCRIPT = (
    "/opt/airflow/src/quality/hn_expectations.py"
)

CLEANING_SCRIPT = (
    "/opt/airflow/src/silver/clean_hn.py"
)

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

MINIO_BUCKET = "hn-clean"
MINIO_OBJECT = "hn_clean.parquet"


# =============================================================================
# Task 1 - Check Bronze data
# =============================================================================

def check_hn_bronze():
    """
    Verify that Hacker News Bronze micro-batches exist.
    """

    files = sorted(
        HN_BRONZE_DIR.glob("*.jsonl")
    )

    if not files:
        raise FileNotFoundError(
            f"No Hacker News Bronze files found in {HN_BRONZE_DIR}"
        )

    total_size = sum(
        file.stat().st_size
        for file in files
    )

    print("Hacker News Bronze data found.")
    print(f"Micro-batches found: {len(files)}")
    print(f"Total size: {total_size / (1024 ** 2):.2f} MiB")


# =============================================================================
# Generic script runner
# =============================================================================

def run_python_script(script_path):
    """
    Execute one of the versioned SciPulse Python scripts.
    """

    print(f"Executing: {script_path}")

    subprocess.run(
        [
            sys.executable,
            script_path,
        ],
        check=True,
    )

    print(
        f"Successfully completed: {script_path}"
    )


# =============================================================================
# Task 2 - Quality profiling
# =============================================================================

def run_quality_profiling():
    """
    Run Hacker News Bronze quality profiling.
    """

    run_python_script(
        QUALITY_SCRIPT
    )


# =============================================================================
# Task 3 - Great Expectations
# =============================================================================

def run_great_expectations():
    """
    Execute Hacker News Great Expectations validations.

    The versioned validation script also regenerates Data Docs.
    """

    run_python_script(
        EXPECTATIONS_SCRIPT
    )


# =============================================================================
# Task 4 - Silver cleaning
# =============================================================================

def run_hn_cleaning():
    """
    Clean all available Hacker News Bronze micro-batches.

    Duplicate IDs are removed and timestamps / missing values
    are normalized before writing the Silver Parquet dataset.
    """

    run_python_script(
        CLEANING_SCRIPT
    )


# =============================================================================
# Task 5 - Upload to MinIO
# =============================================================================

def upload_hn_silver():
    """
    Upload the Hacker News Silver Parquet dataset to MinIO.
    """

    if not HN_SILVER_FILE.exists():
        raise FileNotFoundError(
            f"HN Silver file not found: {HN_SILVER_FILE}"
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
        str(HN_SILVER_FILE),
    )

    print(
        "Hacker News Silver dataset uploaded successfully."
    )

    print(
        f"Destination: "
        f"{MINIO_BUCKET}/{MINIO_OBJECT}"
    )


# =============================================================================
# DAG definition
# =============================================================================

with DAG(
    dag_id="hn_silver_pipeline",
    description=(
        "Quality validation, cleaning and Silver "
        "Parquet generation for Hacker News"
    ),
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=[
        "scipulse",
        "silver",
        "hacker-news",
        "quality",
    ],
) as dag:

    check_bronze = PythonOperator(
        task_id="check_hn_bronze",
        python_callable=check_hn_bronze,
    )

    quality_profiling = PythonOperator(
        task_id="quality_profiling",
        python_callable=run_quality_profiling,
    )

    great_expectations = PythonOperator(
        task_id="great_expectations_validation",
        python_callable=run_great_expectations,
    )

    clean_silver = PythonOperator(
        task_id="clean_hn_silver",
        python_callable=run_hn_cleaning,
    )

    upload_silver = PythonOperator(
        task_id="upload_hn_silver_to_minio",
        python_callable=upload_hn_silver,
    )

    (
        check_bronze
        >> quality_profiling
        >> great_expectations
        >> clean_silver
        >> upload_silver
    )