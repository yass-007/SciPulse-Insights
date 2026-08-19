"""
SciPulse - Temporal analytics with PySpark

This script computes temporal indicators required by Module 2:

1. Annual publication growth by ArXiv category.
2. Citation velocity using Spark window functions.

Sources are loaded directly from the Silver layer in MinIO.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# =============================================================================
# Paths
# =============================================================================

ARXIV_PATH = "s3a://arxiv-clean/full/*.parquet"

CITATIONS_PATH = (
    "s3a://citations-clean/citations_clean.parquet"
)


# =============================================================================
# Spark session
# =============================================================================

def create_spark():
    """
    Create a Spark session configured for MinIO.
    """

    return (
        SparkSession.builder
        .appName("SciPulse-Temporal-Metrics")
        .master("local[*]")
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
        .getOrCreate()
    )


# =============================================================================
# Publication growth
# =============================================================================

def calculate_publication_growth(arxiv_df):
    """
    Calculate annual publication growth by ArXiv category.

    Because one ArXiv paper can belong to multiple categories,
    categories are exploded before aggregation.
    """

    arxiv_temporal = (
        arxiv_df
        .withColumn(
            "publication_date",
            F.to_date(
                F.col("published_date"),
                "yyyy-MM-dd",
            ),
        )
        .withColumn(
            "year",
            F.year("publication_date"),
        )
        .filter(
            F.col("year").isNotNull()
        )
        .withColumn(
            "category",
            F.explode("categories"),
        )
        .filter(
            F.col("category").isNotNull()
        )
    )

    publications_by_year = (
        arxiv_temporal
        .groupBy(
            "category",
            "year",
        )
        .agg(
            F.countDistinct("id").alias(
                "publication_count"
            )
        )
    )

    # -------------------------------------------------------------------------
    # Window by category ordered chronologically
    # -------------------------------------------------------------------------

    category_window = (
        Window
        .partitionBy("category")
        .orderBy("year")
    )

    publications_growth = (
        publications_by_year
        .withColumn(
            "previous_year_count",
            F.lag(
                "publication_count",
                1,
            ).over(category_window),
        )
        .withColumn(
            "publication_growth_pct",
            F.when(
                (
                    F.col("previous_year_count").isNotNull()
                    & (
                        F.col("previous_year_count")
                        > 0
                    )
                ),
                (
                    (
                        F.col("publication_count")
                        - F.col("previous_year_count")
                    )
                    / F.col("previous_year_count")
                )
                * 100,
            ),
        )
    )

    return publications_growth


# =============================================================================
# Citation velocity
# =============================================================================

def calculate_citation_velocity(citations_df):
    """
    Calculate year-over-year citation velocity.

    counts_by_year contains one element per citation year:
        {
            year,
            cited_by_count
        }

    We explode the array and use lag() to compare citation activity
    with the previous available year.
    """

    citations_yearly = (
        citations_df
        .select(
            "openalex_id",
            "arxiv_id",
            "publication_year",
            F.explode_outer(
                "counts_by_year"
            ).alias("citation_year_data"),
        )
        .select(
            "openalex_id",
            "arxiv_id",
            "publication_year",
            F.col(
                "citation_year_data.year"
            ).alias("citation_year"),
            F.col(
                "citation_year_data.cited_by_count"
            ).alias("citations_in_year"),
        )
        .filter(
            F.col("citation_year").isNotNull()
        )
    )

    citation_window = (
        Window
        .partitionBy("openalex_id")
        .orderBy("citation_year")
    )

    citation_velocity = (
        citations_yearly
        .withColumn(
            "previous_year_citations",
            F.lag(
                "citations_in_year",
                1,
            ).over(citation_window),
        )
        .withColumn(
            "citation_velocity",
            (
                F.col("citations_in_year")
                - F.coalesce(
                    F.col(
                        "previous_year_citations"
                    ),
                    F.lit(0),
                )
            ),
        )
        .withColumn(
            "citation_growth_pct",
            F.when(
                (
                    F.col(
                        "previous_year_citations"
                    ).isNotNull()
                    & (
                        F.col(
                            "previous_year_citations"
                        )
                        > 0
                    )
                ),
                (
                    (
                        F.col("citations_in_year")
                        - F.col(
                            "previous_year_citations"
                        )
                    )
                    / F.col(
                        "previous_year_citations"
                    )
                )
                * 100,
            ),
        )
    )

    return citation_velocity


# =============================================================================
# Main
# =============================================================================

def main():

    spark = create_spark()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    try:

        print()
        print("=" * 80)
        print("LOADING SILVER DATA")
        print("=" * 80)

        arxiv_df = spark.read.parquet(
            ARXIV_PATH
        )

        citations_df = spark.read.parquet(
            CITATIONS_PATH
        )

        print(
            "ArXiv rows:",
            arxiv_df.count(),
        )

        print(
            "Citation rows:",
            citations_df.count(),
        )

        # =====================================================================
        # Publication growth
        # =====================================================================

        publication_growth = (
            calculate_publication_growth(
                arxiv_df
            )
        )

        print()
        print("=" * 80)
        print("ANNUAL PUBLICATION GROWTH BY CATEGORY")
        print("=" * 80)

        publication_growth.orderBy(
            F.desc("year"),
            F.desc("publication_growth_pct"),
        ).show(
            30,
            truncate=False,
        )

        # =====================================================================
        # Citation velocity
        # =====================================================================

        citation_velocity = (
            calculate_citation_velocity(
                citations_df
            )
        )

        print()
        print("=" * 80)
        print("CITATION VELOCITY")
        print("=" * 80)

        citation_velocity.orderBy(
            F.desc("citation_year"),
            F.desc("citation_velocity"),
        ).show(
            30,
            truncate=False,
        )

        print()
        print("=" * 80)
        print("TEMPORAL METRICS COMPUTED SUCCESSFULLY")
        print("=" * 80)

        input(
            "\nSpark UI available on port 4040. "
            "Press ENTER to stop Spark..."
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()