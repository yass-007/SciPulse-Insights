from pyspark.sql import SparkSession
from pyspark.sql import functions as F


ARXIV_PATH = "s3a://arxiv-clean/full/*.parquet"
CITATIONS_PATH = "s3a://citations-clean/citations_clean.parquet"


def create_spark():
    return (
        SparkSession.builder
        .appName("SciPulse-Join-Strategy-Comparison")
        .master("local[*]")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def prepare_arxiv(df):
    return (
        df
        .withColumn(
            "join_arxiv_id",
            F.lower(F.trim(F.col("id")))
        )
        .select(
            "id",
            "title",
            "join_arxiv_id"
        )
    )


def prepare_citations(df):
    return (
        df
        .withColumn(
            "join_arxiv_id",
            F.lower(F.trim(F.col("arxiv_id")))
        )
        .select(
            "openalex_id",
            "cited_by_count",
            "join_arxiv_id"
        )
        .filter(
            F.col("join_arxiv_id").isNotNull()
        )
    )


def broadcast_join(arxiv, citations):

    print()
    print("=" * 80)
    print("BROADCAST JOIN PLAN")
    print("=" * 80)

    result = (
        arxiv.alias("a")
        .join(
            F.broadcast(
                citations.alias("c")
            ),
            F.col("a.join_arxiv_id")
            == F.col("c.join_arxiv_id"),
            "inner",
        )
    )

    result.explain("formatted")

    return result


def sort_merge_join(spark, arxiv, citations):

    print()
    print("=" * 80)
    print("SORT-MERGE JOIN PLAN")
    print("=" * 80)

    # Disable automatic broadcast so Spark cannot silently
    # choose BroadcastHashJoin.
    spark.conf.set(
        "spark.sql.autoBroadcastJoinThreshold",
        -1
    )

    result = (
        arxiv.alias("a")
        .join(
            citations.alias("c"),
            F.col("a.join_arxiv_id")
            == F.col("c.join_arxiv_id"),
            "inner",
        )
    )

    result.explain("formatted")

    return result


def main():

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:

        arxiv_raw = spark.read.parquet(
            ARXIV_PATH
        )

        citations_raw = spark.read.parquet(
            CITATIONS_PATH
        )

        arxiv = prepare_arxiv(
            arxiv_raw
        )

        citations = prepare_citations(
            citations_raw
        )

        print()
        print("ArXiv rows:", arxiv.count())
        print("Citation rows:", citations.count())

        # -------------------------------------------------------------
        # Broadcast Join
        # -------------------------------------------------------------

        broadcast_result = broadcast_join(
            arxiv,
            citations
        )

        broadcast_count = (
            broadcast_result.count()
        )

        print()
        print(
            "Broadcast Join matched rows:",
            broadcast_count
        )

        # -------------------------------------------------------------
        # Sort-Merge Join
        # -------------------------------------------------------------

        sort_merge_result = sort_merge_join(
            spark,
            arxiv,
            citations
        )

        sort_merge_count = (
            sort_merge_result.count()
        )

        print()
        print(
            "Sort-Merge Join matched rows:",
            sort_merge_count
        )

        print()
        print("=" * 80)
        print("RESULT CONSISTENCY")
        print("=" * 80)

        print(
            "Same result count:",
            broadcast_count
            == sort_merge_count
        )

        input(
            "\nSpark UI available on port 4040. "
            "Press ENTER to stop Spark..."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()