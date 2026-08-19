from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import StorageLevel

# MinIO Silver paths

ARXIV_PATH = "s3a://arxiv-clean/full/*.parquet"
HN_PATH = "s3a://hn-clean/hn_clean.parquet"
CITATIONS_PATH = "s3a://citations-clean/citations_clean.parquet"

# Spark session

def create_spark():
    """
    Create a Spark session configured for MinIO / S3A.
    """

    return (
        SparkSession.builder
        .appName("SciPulse-Impact-Score")
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

        # Smaller number of shuffle partitions for the local environment.
        .config(
            "spark.sql.shuffle.partitions",
            "64",
        )

        .getOrCreate()
    )
# ArXiv preparation

def prepare_arxiv(df):
    """
    Keep only the ArXiv columns needed for the impact score.
    """

    return (
        df
        .select(
            F.lower(
                F.trim(F.col("id"))
            ).alias("arxiv_id"),

            "title",

            F.to_date(
                F.col("published_date"),
                "yyyy-MM-dd",
            ).alias("published_date"),
        )
        .filter(
            F.col("arxiv_id").isNotNull()
        )
    )
# Citation preparation

def prepare_citations(df):
    """
    Create one academic citation signal per ArXiv paper.

    Several OpenAlex records can theoretically refer to the same ArXiv ID,
    so we keep the maximum citation count for each paper.

    The raw signal is logarithmic:
        log(1 + cited_by_count)
    """

    return (
        df
        .select(
            F.lower(
                F.trim(F.col("arxiv_id"))
            ).alias("arxiv_id"),

            F.coalesce(
                F.col("cited_by_count"),
                F.lit(0),
            ).alias("cited_by_count"),
        )

        # OpenAlex records without an ArXiv ID cannot participate
        # in the impact score.
        .filter(
            F.col("arxiv_id").isNotNull()
        )

        .groupBy(
            "arxiv_id"
        )

        .agg(
            F.max(
                "cited_by_count"
            ).alias(
                "cited_by_count"
            )
        )

        .withColumn(
            "citation_signal_raw",
            F.log1p(
                F.col("cited_by_count")
            ),
        )
    )

# Hacker News preparation

def prepare_hn(df):
    """
    Extract ArXiv IDs from Hacker News URLs and aggregate all HN activity
    related to the same paper.

    HN signal:
        log(1 + total_score + total_descendants)
    """

    hn = (
        df
        .select(
            "url",

            F.coalesce(
                F.col("score"),
                F.lit(0.0),
            ).alias("score"),

            F.coalesce(
                F.col("descendants"),
                F.lit(0.0),
            ).alias("descendants"),
        )

        # Extract ID from URLs such as:
        # https://arxiv.org/abs/2301.10226
        # https://arxiv.org/pdf/2301.10226.pdf
        .withColumn(
            "arxiv_id",
            F.lower(
                F.regexp_extract(
                    F.col("url"),
                    r"arxiv\.org/(?:abs|pdf)/([^?#]+)",
                    1,
                )
            ),
        )

        # Remove ".pdf".
        .withColumn(
            "arxiv_id",
            F.regexp_replace(
                F.col("arxiv_id"),
                r"\.pdf$",
                "",
            ),
        )

        # Remove optional versions such as v1, v2...
        .withColumn(
            "arxiv_id",
            F.regexp_replace(
                F.col("arxiv_id"),
                r"v\d+$",
                "",
            ),
        )

        # Only keep rows where an ArXiv ID was extracted.
        .filter(
            F.length(
                F.col("arxiv_id")
            ) > 0
        )
    )

    return (
        hn
        .groupBy(
            "arxiv_id"
        )

        .agg(
            F.sum(
                "score"
            ).alias(
                "hn_score_total"
            ),

            F.sum(
                "descendants"
            ).alias(
                "hn_descendants_total"
            ),
        )

        .withColumn(
            "hn_signal_raw",
            F.log1p(
                F.col("hn_score_total")
                + F.col("hn_descendants_total")
            ),
        )
    )

# Build eligible impact dataset

def build_impact_dataset(
    arxiv_df,
    citations_df,
    hn_df,
):
    """
    Build the score only for ArXiv papers that have an OpenAlex citation match.

    This is important because our OpenAlex ingestion contains only a subset
    of the complete OpenAlex catalogue.

    A paper absent from this sample must not automatically be interpreted
    as having zero citations.

    Hacker News remains optional:
        no HN match -> HN signal = 0
    """
    # Start from citation papers (~44k), not the complete 3.1M ArXiv corpus.

    df = (
        citations_df.alias("c")

        .join(
            arxiv_df.alias("a"),
            "arxiv_id",
            "inner",
        )

        # HN is very small, so broadcasting it is appropriate.
        .join(
            F.broadcast(
                hn_df
            ).alias("h"),
            "arxiv_id",
            "left",
        )

        .fillna(
            {
                "hn_score_total": 0.0,
                "hn_descendants_total": 0.0,
                "hn_signal_raw": 0.0,
            }
        )
    )
    # Recency signal

    df = (
        df

        .withColumn(
            "age_days",
            F.datediff(
                F.current_date(),
                F.col("published_date"),
            ),
        )

        # Protect against possible future dates.
        .withColumn(
            "age_days",
            F.when(
                F.col("age_days") < 0,
                F.lit(0),
            ).otherwise(
                F.col("age_days")
            ),
        )

        # Recent papers receive a larger signal.
        #
        # today      -> close to 1.0
        # 1 year old -> close to 0.5
        # 4 years    -> close to 0.2
        .withColumn(
            "recency_signal_raw",
            F.when(
                F.col("published_date").isNotNull(),

                1.0
                / (
                    1.0
                    + (
                        F.col("age_days")
                        / F.lit(365.0)
                    )
                ),

            ).otherwise(
                F.lit(0.0)
            ),
        )
    )

    return df

# Normalize signals + final impact score

def normalize_and_score(df):
    """
    Normalize all three signals to [0, 1] and calculate the composite score.

    All min/max values are calculated in ONE Spark aggregation to limit
    recomputation and memory usage.

    Weights:
        50% academic citations
        30% Hacker News
        20% recency

    Final score is expressed on a 0-100 scale.
    """
    # Calculate all normalization statistics in a single action.

    stats = (
        df
        .agg(
            F.min(
                "citation_signal_raw"
            ).alias(
                "citation_min"
            ),

            F.max(
                "citation_signal_raw"
            ).alias(
                "citation_max"
            ),

            F.min(
                "hn_signal_raw"
            ).alias(
                "hn_min"
            ),

            F.max(
                "hn_signal_raw"
            ).alias(
                "hn_max"
            ),

            F.min(
                "recency_signal_raw"
            ).alias(
                "recency_min"
            ),

            F.max(
                "recency_signal_raw"
            ).alias(
                "recency_max"
            ),
        )
        .first()
    )

    citation_min = stats["citation_min"]
    citation_max = stats["citation_max"]

    hn_min = stats["hn_min"]
    hn_max = stats["hn_max"]

    recency_min = stats["recency_min"]
    recency_max = stats["recency_max"]

    # Normalize citation component.

    df = (
        df
        .withColumn(
            "citation_signal_norm",

            F.when(
                F.lit(citation_max) > F.lit(citation_min),

                (
                    F.col("citation_signal_raw")
                    - F.lit(citation_min)
                )
                / (
                    F.lit(citation_max)
                    - F.lit(citation_min)
                ),

            ).otherwise(
                F.lit(0.0)
            ),
        )
    )

    # Normalize HN component.
    

    df = (
        df
        .withColumn(
            "hn_signal_norm",

            F.when(
                F.lit(hn_max) > F.lit(hn_min),

                (
                    F.col("hn_signal_raw")
                    - F.lit(hn_min)
                )
                / (
                    F.lit(hn_max)
                    - F.lit(hn_min)
                ),

            ).otherwise(
                F.lit(0.0)
            ),
        )
    )
    # Normalize recency component.

    df = (
        df
        .withColumn(
            "recency_signal_norm",

            F.when(
                F.lit(recency_max) > F.lit(recency_min),

                (
                    F.col("recency_signal_raw")
                    - F.lit(recency_min)
                )
                / (
                    F.lit(recency_max)
                    - F.lit(recency_min)
                ),

            ).otherwise(
                F.lit(0.0)
            ),
        )
    )

    # Composite impact score.
    #
    # Citations dominate because SciPulse primarily measures scientific impact.
    # HN captures public/technical attention.
    # Recency prevents older papers from automatically dominating.

    return (
        df
        .withColumn(
            "impact_score",

            (
                F.lit(0.50)
                * F.col("citation_signal_norm")

                + F.lit(0.30)
                * F.col("hn_signal_norm")

                + F.lit(0.20)
                * F.col("recency_signal_norm")
            )
            * F.lit(100.0),
        )
    )
# Main

def main():

    spark = create_spark()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    impact_df = None

    try:
        # Load and prepare sources

        print()
        print("=" * 70)
        print("LOADING IMPACT SCORE SOURCES")
        print("=" * 70)

        arxiv_df = prepare_arxiv(
            spark.read.parquet(
                ARXIV_PATH
            )
        )

        citations_df = prepare_citations(
            spark.read.parquet(
                CITATIONS_PATH
            )
        )

        hn_df = prepare_hn(
            spark.read.parquet(
                HN_PATH
            )
        )

        print(
            "ArXiv rows:",
            arxiv_df.count(),
        )

        print(
            "Citation papers:",
            citations_df.count(),
        )

        print(
            "HN papers:",
            hn_df.count(),
        )
        # Build joined score dataset

        print()
        print("=" * 70)
        print("BUILDING IMPACT DATASET")
        print("=" * 70)

        impact_df = build_impact_dataset(
            arxiv_df,
            citations_df,
            hn_df,
        )
        # Persist on DISK ONLY.
        #
        # Without this, Spark can recompute the complete ArXiv join several
        # times during normalization and ranking, which caused OOM failures
        # in our local Docker environment.

        impact_df = impact_df.persist(
            StorageLevel.DISK_ONLY
        )

        # Materialize the persisted dataset now.
        impact_count = impact_df.count()

        print(
            "Papers eligible for impact score:",
            impact_count,
        )
        # Normalize and calculate final score

        final_df = normalize_and_score(
            impact_df
        )

        # Top papers

        print()
        print("=" * 70)
        print("TOP IMPACT PAPERS")
        print("=" * 70)

        final_df.select(
            "arxiv_id",
            "title",
            "published_date",
            "cited_by_count",
            "hn_score_total",
            "hn_descendants_total",

            F.round(
                "citation_signal_norm",
                4,
            ).alias(
                "citation_norm"
            ),

            F.round(
                "hn_signal_norm",
                4,
            ).alias(
                "hn_norm"
            ),

            F.round(
                "recency_signal_norm",
                4,
            ).alias(
                "recency_norm"
            ),

            F.round(
                "impact_score",
                2,
            ).alias(
                "impact_score"
            ),

        ).orderBy(
            F.desc(
                "impact_score"
            )
        ).show(
            20,
            truncate=False,
        )

        print()
        print("=" * 70)
        print("IMPACT SCORE COMPUTED SUCCESSFULLY")
        print("=" * 70)

        print()
        print(
            "Weights: "
            "citations=50%, "
            "Hacker News=30%, "
            "recency=20%"
        )

    finally:

        # Remove persisted Spark blocks explicitly.
        if impact_df is not None:
            impact_df.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()