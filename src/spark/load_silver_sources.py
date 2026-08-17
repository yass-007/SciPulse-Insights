"""
SciPulse - Load Silver datasets with PySpark

This script loads the three Silver datasets directly from MinIO:

- ArXiv Silver
- Hacker News Silver
- OpenAlex citations Silver

The goal is to validate Spark access to all Silver layers
before implementing the multi-source joins.
"""

import os

from pyspark.sql import SparkSession


# =============================================================================
# Configuration
# =============================================================================

MINIO_ENDPOINT = "http://minio:9000"

MINIO_ACCESS_KEY = os.getenv(
    "AWS_ACCESS_KEY_ID",
    "minioadmin",
)

MINIO_SECRET_KEY = os.getenv(
    "AWS_SECRET_ACCESS_KEY",
    "minioadmin",
)

ARXIV_PATH = (
    "s3a://arxiv-clean/full/*.parquet"
)

HN_PATH = (
    "s3a://hn-clean/hn_clean.parquet"
)

CITATIONS_PATH = (
    "s3a://citations-clean/citations_clean.parquet"
)


# =============================================================================
# Spark Session
# =============================================================================

def create_spark_session():
    """
    Create the Spark session configured for MinIO / S3A.
    """

    spark = (
        SparkSession.builder
        .appName("SciPulse-Silver-Loader")
        .master("local[*]")
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            MINIO_ENDPOINT,
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            MINIO_ACCESS_KEY,
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            MINIO_SECRET_KEY,
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true",
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    return spark


# =============================================================================
# Load Silver datasets
# =============================================================================

def load_silver_sources(spark):
    """
    Load the three Silver datasets from MinIO.
    """

    print()
    print("=" * 70)
    print("LOADING ARXIV SILVER")
    print("=" * 70)

    arxiv_df = spark.read.parquet(
        ARXIV_PATH
    )

    arxiv_df.printSchema()

    arxiv_count = arxiv_df.count()

    print(
        f"ArXiv rows: {arxiv_count}"
    )

    print()
    print("=" * 70)
    print("LOADING HACKER NEWS SILVER")
    print("=" * 70)

    hn_df = spark.read.parquet(
        HN_PATH
    )

    hn_df.printSchema()

    hn_count = hn_df.count()

    print(
        f"Hacker News rows: {hn_count}"
    )

    print()
    print("=" * 70)
    print("LOADING CITATIONS SILVER")
    print("=" * 70)

    citations_df = spark.read.parquet(
        CITATIONS_PATH
    )

    citations_df.printSchema()

    citations_count = (
        citations_df.count()
    )

    print(
        f"Citations rows: {citations_count}"
    )

    return (
        arxiv_df,
        hn_df,
        citations_df,
    )


# =============================================================================
# Main
# =============================================================================

def main():

    spark = create_spark_session()

    try:

        (
            arxiv_df,
            hn_df,
            citations_df,
        ) = load_silver_sources(
            spark
        )

        print()
        print("=" * 70)
        print("SILVER DATASETS LOADED SUCCESSFULLY")
        print("=" * 70)

        print(
            f"ArXiv: {arxiv_df.count()} rows"
        )

        print(
            f"Hacker News: {hn_df.count()} rows"
        )

        print(
            f"Citations: {citations_df.count()} rows"
        )

    
    finally:

        input(
    "\nSpark UI available on port 4040. "
    "Press ENTER to stop Spark..."
      )

        spark.stop()


if __name__ == "__main__":
    main()