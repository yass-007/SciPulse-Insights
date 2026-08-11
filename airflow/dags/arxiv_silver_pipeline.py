"""
SciPulse - ArXiv Silver Pipeline

This DAG orchestrates the complete ArXiv Silver workflow:

1. Check that Bronze data is available
2. Run Bronze quality profiling
3. Run Great Expectations validations and generate Data Docs
4. Clean and normalize the complete ArXiv dataset
5. Write Silver data as partitioned Parquet files
6. Upload the Parquet dataset to MinIO arxiv-clean/full/

The Bronze dataset is never modified.
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

ARXIV_BRONZE_FILE = Path(
    "/opt/airflow/data/bronze/arxiv/"
    "arxiv-metadata-oai-snapshot.json"
)

ARXIV_SILVER_DIR = Path(
    "/opt/airflow/data/silver/arxiv/full"
)

QUALITY_SCRIPT = (
    "/opt/airflow/src/quality/analyze_arxiv.py"
)

EXPECTATIONS_SCRIPT = (
    "/opt/airflow/src/quality/arxiv_expectations.py"
)

CLEANING_SCRIPT = (
    "/opt/airflow/src/silver/clean_arxiv.py"
)

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

MINIO_BUCKET = "arxiv-clean"
MINIO_PREFIX = "full/"


# =============================================================================
# Task 1 - Check Bronze data
# =============================================================================

def check_arxiv_bronze():
    """
    Verify that the raw ArXiv Bronze dataset exists
    before starting Silver processing.
    """

    if not ARXIV_BRONZE_FILE.exists():
        raise FileNotFoundError(
            f"ArXiv Bronze file not found: {ARXIV_BRONZE_FILE}"
        )

    size_gb = (
        ARXIV_BRONZE_FILE.stat().st_size
        / (1024 ** 3)
    )

    print("ArXiv Bronze dataset found.")
    print(f"Path: {ARXIV_BRONZE_FILE}")
    print(f"Size: {size_gb:.2f} GiB")


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

    print(f"Successfully completed: {script_path}")


# =============================================================================
# Task 2 - Quality profiling
# =============================================================================

def run_quality_profiling():
    """
    Run ArXiv Bronze quality profiling.
    """

    run_python_script(
        QUALITY_SCRIPT
    )


# =============================================================================
# Task 3 - Great Expectations
# =============================================================================

def run_great_expectations():
    """
    Execute ArXiv Great Expectations suite.

    The script also regenerates the Great Expectations Data Docs.
    """

    run_python_script(
        EXPECTATIONS_SCRIPT
    )


# =============================================================================
# Task 4 - Clean complete dataset
# =============================================================================

def run_arxiv_cleaning():
    """
    Run the complete ArXiv Silver cleaning procedure.

    The cleaning script processes the large Bronze dataset
    in chunks and generates partitioned Parquet files.
    """

    run_python_script(
        CLEANING_SCRIPT
    )


# =============================================================================
# Task 5 - Upload Silver Parquet files to MinIO
# =============================================================================

def upload_arxiv_silver():
    """
    Upload all generated ArXiv Silver Parquet files
    into MinIO under arxiv-clean/full/.

    Existing objects under the prefix are removed first,
    making the upload idempotent.
    """

    if not ARXIV_SILVER_DIR.exists():
        raise FileNotFoundError(
            f"Silver directory not found: {ARXIV_SILVER_DIR}"
        )

    parquet_files = sorted(
        ARXIV_SILVER_DIR.glob("*.parquet")
    )

    if not parquet_files:
        raise FileNotFoundError(
            "No ArXiv Silver Parquet files were generated."
        )

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    # -------------------------------------------------------------------------
    # Ensure bucket exists
    # -------------------------------------------------------------------------

    if not client.bucket_exists(
        MINIO_BUCKET
    ):
        client.make_bucket(
            MINIO_BUCKET
        )

    # -------------------------------------------------------------------------
    # Remove previous Silver partition files
    # -------------------------------------------------------------------------

    existing_objects = list(
        client.list_objects(
            MINIO_BUCKET,
            prefix=MINIO_PREFIX,
            recursive=True,
        )
    )

    for obj in existing_objects:
        client.remove_object(
            MINIO_BUCKET,
            obj.object_name,
        )

    print(
        f"Previous objects removed: "
        f"{len(existing_objects)}"
    )

    # -------------------------------------------------------------------------
    # Upload new partitioned Parquet dataset
    # -------------------------------------------------------------------------

    for index, parquet_file in enumerate(
        parquet_files,
        start=1,
    ):

        object_name = (
            f"{MINIO_PREFIX}"
            f"{parquet_file.name}"
        )

        client.fput_object(
            MINIO_BUCKET,
            object_name,
            str(parquet_file),
        )

        print(
            f"[{index}/{len(parquet_files)}] "
            f"Uploaded: {object_name}"
        )

    print(
        "\nArXiv Silver upload completed."
    )

    print(
        f"Parquet files uploaded: "
        f"{len(parquet_files)}"
    )


# =============================================================================
# DAG definition
# =============================================================================

with DAG(
    dag_id="arxiv_silver_pipeline",
    description=(
        "Quality validation, cleaning and Silver "
        "Parquet generation for ArXiv"
    ),
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=[
        "scipulse",
        "silver",
        "arxiv",
        "quality",
    ],
) as dag:

    check_bronze = PythonOperator(
        task_id="check_arxiv_bronze",
        python_callable=check_arxiv_bronze,
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
        task_id="clean_arxiv_silver",
        python_callable=run_arxiv_cleaning,
    )

    upload_silver = PythonOperator(
        task_id="upload_arxiv_silver_to_minio",
        python_callable=upload_arxiv_silver,
    )

    (
        check_bronze
        >> quality_profiling
        >> great_expectations
        >> clean_silver
        >> upload_silver
    )