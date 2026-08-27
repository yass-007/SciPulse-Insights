"""
SciPulse - Hacker News Structured Streaming pipeline.

Optional extension A.

This job continuously monitors the Hacker News Bronze bucket in MinIO.

Each new JSON/JSONL file is:
- detected automatically by Spark Structured Streaming
- cleaned using rules equivalent to the existing HN Silver batch pipeline
- deduplicated by Hacker News item ID
- written incrementally as Parquet into the HN Silver streaming area

The job stays alive and processes new files without being restarted.

Architecture:

    MinIO hn-raw/
          |
          v
    Spark readStream
          |
          v
    Cleaning + deduplication
          |
          v
    Spark writeStream
          |
          v
    MinIO hn-clean/streaming/
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# =============================================================================
# Configuration
# =============================================================================

HN_BRONZE_PATH = "s3a://hn-raw/"

HN_STREAMING_SILVER_PATH = (
    "s3a://hn-clean/streaming/"
)

HN_CHECKPOINT_PATH = (
        "s3a://hn-clean/_checkpoints/hn_streaming_v2/"
)

# Process one newly discovered file per micro-batch.
MAX_FILES_PER_TRIGGER = 10


# =============================================================================
# Hacker News Bronze schema
# =============================================================================
#
# Structured Streaming requires an explicit schema when reading file sources.
#
# The schema below corresponds to the fields already used by clean_hn.py.
# =============================================================================

HN_SCHEMA = StructType(
    [
        StructField(
            "id",
            LongType(),
            True,
        ),
        StructField(
            "by",
            StringType(),
            True,
        ),
        StructField(
            "time",
            LongType(),
            True,
        ),
        StructField(
            "type",
            StringType(),
            True,
        ),
        StructField(
            "title",
            StringType(),
            True,
        ),
        StructField(
            "url",
            StringType(),
            True,
        ),
        StructField(
            "score",
            LongType(),
            True,
        ),
        StructField(
            "descendants",
            LongType(),
            True,
        ),
        StructField(
            "text",
            StringType(),
            True,
        ),
        StructField(
            "kids",
            ArrayType(
                LongType()
            ),
            True,
        ),
    ]
)


# =============================================================================
# Spark session
# =============================================================================

def create_spark():
    """
    Create the Spark session configured for MinIO / S3A.
    """

    return (
        SparkSession.builder
        .appName(
            "SciPulse-HN-Structured-Streaming"
        )
        .master(
            "local[*]"
        )

        # ---------------------------------------------------------------------
        # MinIO configuration
        # ---------------------------------------------------------------------

        .config(
            "spark.hadoop.fs.s3a.endpoint",
            "http://minio:9000",
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            "minioadmin",
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            "minioadmin",
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

        .config(
            "spark.sql.shuffle.partitions",
            "8",
        )

        .getOrCreate()
    )


# =============================================================================
# Cleaning
# =============================================================================

def clean_hn_stream(stream_df):
    """
    Apply Silver cleaning rules to the Hacker News stream.

    These transformations reproduce the main cleaning rules used by
    src/silver/clean_hn.py.
    """

    # =========================================================================
    # 1. Keep useful fields
    # =========================================================================

    df = stream_df.select(
        "id",
        "by",
        "time",
        "type",
        "title",
        "url",
        "score",
        "descendants",
        "text",
        "kids",
    )

    # =========================================================================
    # 2. Remove records without an ID
    # =========================================================================

    df = df.filter(
        F.col("id").isNotNull()
    )

    # =========================================================================
    # 3. Normalize Hacker News timestamp
    # =========================================================================
    #
    # Bronze:
    #
    #     1720000000
    #
    # becomes a Spark timestamp.
    #
    # We keep an actual timestamp internally because it is useful for
    # Structured Streaming watermarks.
    # =========================================================================

    df = df.withColumn(
        "event_time",
        F.to_timestamp(
            F.from_unixtime(
                F.col("time")
            )
        ),
    )

    # =========================================================================
    # 4. Normalize textual values
    # =========================================================================

    text_columns = [
        "by",
        "type",
        "title",
        "url",
        "text",
    ]

    for column in text_columns:

        df = df.withColumn(
            column,
            F.trim(
                F.coalesce(
                    F.col(column),
                    F.lit(""),
                )
            ),
        )

    # =========================================================================
    # 5. Normalize numeric values
    # =========================================================================

    df = (
        df
        .withColumn(
            "score",
            F.coalesce(
                F.col("score"),
                F.lit(0),
            ),
        )
        .withColumn(
            "descendants",
            F.coalesce(
                F.col("descendants"),
                F.lit(0),
            ),
        )
    )

    # =========================================================================
    # 6. Normalize kids
    # =========================================================================
    #
    # Missing kids:
    #
    #     NULL
    #
    # becomes:
    #
    #     []
    # =========================================================================

    df = df.withColumn(
        "kids",
        F.when(
            F.col("kids").isNull(),
            F.array().cast(
                ArrayType(
                    LongType()
                )
            ),
        ).otherwise(
            F.col("kids")
        ),
    )

    # =========================================================================
    # 7. Streaming deduplication
    # =========================================================================
    #
    # HN items appear repeatedly in consecutive Bronze micro-batches.
    #
    # A watermark limits the amount of state Spark must retain.
    #
    # Seven days is intentionally much longer than the Bronze ingestion
    # interval, so repeated HN items arriving in successive micro-batches
    # are deduplicated.
    # =========================================================================

    df = (
        df
        .withWatermark(
            "event_time",
            "7 days",
        )
        .dropDuplicates(
            ["id"]
        )
    )

    # =========================================================================
    # 8. Format timestamp like the existing Silver dataset
    # =========================================================================

    df = df.withColumn(
        "time",
        F.date_format(
            F.col("event_time"),
            "yyyy-MM-dd'T'HH:mm:ss'Z'",
        ),
    )

    # event_time was only required for streaming state / watermark handling.
    df = df.drop(
        "event_time"
    )

    return df


# =============================================================================
# Streaming pipeline
# =============================================================================

def main():

    spark = create_spark()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    print()
    print("=" * 70)
    print(
        "SCIPULSE HACKER NEWS STRUCTURED STREAMING"
    )
    print("=" * 70)

    print(
        "Bronze source:",
        HN_BRONZE_PATH,
    )

    print(
        "Silver destination:",
        HN_STREAMING_SILVER_PATH,
    )

    print(
        "Checkpoint:",
        HN_CHECKPOINT_PATH,
    )

    print(
        "maxFilesPerTrigger:",
        MAX_FILES_PER_TRIGGER,
    )

    # =========================================================================
    # 1. Streaming read from MinIO
    # =========================================================================

    bronze_stream = (
        spark.readStream

        # JSONL files are supported by the Spark JSON file source because
        # each line contains one complete JSON object.
        .format(
            "json"
        )

        # Structured Streaming cannot infer the schema automatically for
        # file streams, therefore the schema is explicitly defined above.
        .schema(
            HN_SCHEMA
        )

        # Limit the number of newly discovered files handled by each trigger.
        .option(
            "maxFilesPerTrigger",
            MAX_FILES_PER_TRIGGER,
        )


        .load(
            HN_BRONZE_PATH
        )
    )

    # =========================================================================
    # 2. Apply Silver cleaning
    # =========================================================================

    silver_stream = clean_hn_stream(
        bronze_stream
    )

    # =========================================================================
    # 3. Continuous Silver write
    # =========================================================================

    query = (
        silver_stream.writeStream

        .format(
            "parquet"
        )

        # New cleaned records are appended to Silver.
        .outputMode(
            "append"
        )

        # Required for exactly-once progress tracking of the file stream.
        .option(
            "checkpointLocation",
            HN_CHECKPOINT_PATH,
        )

        .option(
            "path",
            HN_STREAMING_SILVER_PATH,
        )

        # Spark remains alive and checks periodically for new files.
        .trigger(
            processingTime="30 seconds"
        )

        .start()
    )

    print()
    print("=" * 70)
    print(
        "STREAMING QUERY STARTED"
    )
    print("=" * 70)

    print(
        "Spark is now monitoring hn-raw/ for new files."
    )

    print(
        "A new micro-batch is evaluated every 30 seconds."
    )

    print(
        "Press CTRL+C to stop the streaming job."
    )

    # Keep the Spark application alive.
    query.awaitTermination()


# =============================================================================
# Entry point
# =============================================================================

if __name__ == "__main__":
    main()