from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# =============================================================================
# Paths
# =============================================================================

ARXIV_PATH = "s3a://arxiv-clean/full/*.parquet"
HN_PATH = "s3a://hn-clean/hn_clean.parquet"
CITATIONS_PATH = "s3a://citations-clean/citations_clean.parquet"


# =============================================================================
# Spark session
# =============================================================================

def create_spark():
    return (
        SparkSession.builder
        .appName("SciPulse-Multi-Source-Joins")
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


# =============================================================================
# Normalization helpers
# =============================================================================

def prepare_arxiv(df):
    return (
        df
        .withColumn(
            "join_arxiv_id",
            F.lower(F.trim(F.col("id")))
        )
        .withColumn(
            "join_doi",
            F.lower(
                F.regexp_replace(
                    F.trim(F.col("doi")),
                    r"^https?://(dx\.)?doi\.org/",
                    ""
                )
            )
        )
        .withColumn(
            "join_doi",
            F.regexp_replace(
                F.col("join_doi"),
                r"^doi:",
                ""
            )
        )
    )


def prepare_citations(df):
    return (
        df
        .withColumn(
            "join_arxiv_id",
            F.lower(F.trim(F.col("arxiv_id")))
        )
        .withColumn(
            "join_doi",
            F.lower(F.trim(F.col("doi")))
        )
    )

def prepare_hn(df):
    """
    Extract a normalized ArXiv ID from Hacker News URLs.

    Supported examples:
    - https://arxiv.org/abs/2301.10226
    - https://arxiv.org/pdf/2301.10226
    - https://arxiv.org/pdf/2301.10226.pdf
    """

    return (
        df
        .withColumn(
            "join_arxiv_id",
            F.lower(
                F.regexp_extract(
                    F.col("url"),
                    r"arxiv\.org/(?:abs|pdf)/([^?#]+)",
                    1,
                )
            )
        )
        .withColumn(
            "join_arxiv_id",
            F.regexp_replace(
                F.col("join_arxiv_id"),
                r"\.pdf$",
                "",
            )
        )
        .withColumn(
            "join_arxiv_id",
            F.regexp_replace(
                F.col("join_arxiv_id"),
                r"v\d+$",
                "",
            )
        )
        .withColumn(
            "join_arxiv_id",
            F.when(
                F.length(F.col("join_arxiv_id")) > 0,
                F.col("join_arxiv_id"),
            )
        )
    )

def join_hn_arxiv(hn_df, arxiv_df):
    """
    Join Hacker News posts to ArXiv papers using ArXiv IDs
    extracted from HN URLs.
    """

    hn = prepare_hn(hn_df)
    arxiv = prepare_arxiv(arxiv_df)

    hn_with_arxiv = (
        hn
        .filter(F.col("join_arxiv_id").isNotNull())
        .alias("h")
        .join(
            arxiv.alias("a"),
            F.col("h.join_arxiv_id")
            == F.col("a.join_arxiv_id"),
            "inner",
        )
    )

    total_hn = hn.count()

    hn_with_arxiv_id = (
        hn
        .filter(F.col("join_arxiv_id").isNotNull())
        .count()
    )

    matched_hn = (
        hn_with_arxiv
        .select(F.col("h.id").alias("hn_id"))
        .distinct()
        .count()
    )

    matched_arxiv = (
        hn_with_arxiv
        .select(F.col("a.id").alias("arxiv_id"))
        .distinct()
        .count()
    )

    hn_coverage = (
        matched_hn / total_hn * 100
        if total_hn > 0
        else 0
    )

    direct_url_match_rate = (
        matched_hn / hn_with_arxiv_id * 100
        if hn_with_arxiv_id > 0
        else 0
    )

    print()
    print("=" * 70)
    print("HACKER NEWS ↔ ARXIV MATCH COVERAGE")
    print("=" * 70)

    print(f"Total HN rows: {total_hn}")
    print(f"HN rows with extracted ArXiv ID: {hn_with_arxiv_id}")
    print(f"Matched HN rows: {matched_hn}")
    print(f"Matched ArXiv papers: {matched_arxiv}")
    print(f"HN overall coverage (%): {hn_coverage:.4f}")
    print(
        "Coverage among HN rows containing an ArXiv URL (%): "
        f"{direct_url_match_rate:.4f}"
    )

    return (
        hn_with_arxiv
        .select(
            F.col("h.id").alias("hn_id"),
            F.col("a.id").alias("arxiv_id"),
            F.col("h.score").alias("hn_score"),
            F.col("h.descendants").alias("hn_descendants"),
            F.col("h.title").alias("hn_title"),
            F.col("h.url").alias("hn_url"),
        )
    )
# =============================================================================
# ArXiv ↔ Citations join
# =============================================================================

def join_arxiv_citations(arxiv_df, citations_df):

    arxiv = prepare_arxiv(arxiv_df)
    citations = prepare_citations(citations_df)

    print()
    print("=" * 70)
    print("ARXIV ↔ CITATIONS MATCH COVERAGE")
    print("=" * 70)

    # =====================================================================
    # 1. Match using ArXiv ID
    # =====================================================================

    arxiv_id_matches = (
        arxiv.alias("a")
        .join(
            citations.alias("c"),
            (
                F.col("a.join_arxiv_id").isNotNull()
                & F.col("c.join_arxiv_id").isNotNull()
                & (
                    F.col("a.join_arxiv_id")
                    == F.col("c.join_arxiv_id")
                )
            ),
            "inner",
        )
        .select(
            F.col("a.id").alias("arxiv_id"),
            F.col("c.openalex_id").alias("openalex_id"),
        )
    )

    # Materialize because we use this result more than once
    arxiv_id_matches = arxiv_id_matches.cache()

    matched_arxiv_by_id = (
        arxiv_id_matches
        .select("arxiv_id")
        .distinct()
        .count()
    )

    matched_citations_by_id = (
        arxiv_id_matches
        .select("openalex_id")
        .distinct()
        .count()
    )

    print(
        f"Matched by ArXiv ID - ArXiv rows: "
        f"{matched_arxiv_by_id}"
    )

    print(
        f"Matched by ArXiv ID - Citation rows: "
        f"{matched_citations_by_id}"
    )

    # =====================================================================
    # 2. Remove citations already matched by ArXiv ID
    # =====================================================================

    already_matched_citations = (
        arxiv_id_matches
        .select("openalex_id")
        .distinct()
    )

    citations_unmatched = (
        citations.alias("c")
        .join(
            already_matched_citations.alias("m"),
            F.col("c.openalex_id")
            == F.col("m.openalex_id"),
            "left_anti",
        )
    )

    # =====================================================================
    # 3. DOI fallback
    # =====================================================================

    doi_matches = (
        arxiv.alias("a")
        .join(
            citations_unmatched.alias("c"),
            (
                F.col("a.join_doi").isNotNull()
                & F.col("c.join_doi").isNotNull()
                & (
                    F.col("a.join_doi")
                    == F.col("c.join_doi")
                )
            ),
            "inner",
        )
        .select(
            F.col("a.id").alias("arxiv_id"),
            F.col("c.openalex_id").alias("openalex_id"),
        )
    )

    doi_matches = doi_matches.cache()

    matched_arxiv_by_doi = (
        doi_matches
        .select("arxiv_id")
        .distinct()
        .count()
    )

    matched_citations_by_doi = (
        doi_matches
        .select("openalex_id")
        .distinct()
        .count()
    )

    print()
    print(
        f"Additional matches by DOI - ArXiv rows: "
        f"{matched_arxiv_by_doi}"
    )

    print(
        f"Additional matches by DOI - Citation rows: "
        f"{matched_citations_by_doi}"
    )

    # =====================================================================
    # 4. Combine matches
    # =====================================================================

    all_matches = (
        arxiv_id_matches
        .unionByName(doi_matches)
        .dropDuplicates(
            ["arxiv_id", "openalex_id"]
        )
    )

    matched_arxiv = (
        all_matches
        .select("arxiv_id")
        .distinct()
        .count()
    )

    matched_citations = (
        all_matches
        .select("openalex_id")
        .distinct()
        .count()
    )

    total_arxiv = arxiv.count()
    total_citations = citations.count()

    arxiv_coverage = (
        matched_arxiv
        / total_arxiv
        * 100
    )

    citation_coverage = (
        matched_citations
        / total_citations
        * 100
    )

    print()
    print("-" * 70)
    print("FINAL MATCH COVERAGE")
    print("-" * 70)

    print(
        f"Total ArXiv rows: {total_arxiv}"
    )

    print(
        f"Matched ArXiv rows: {matched_arxiv}"
    )

    print(
        f"ArXiv coverage (%): "
        f"{arxiv_coverage:.4f}"
    )

    print()

    print(
        f"Total citation rows: {total_citations}"
    )

    print(
        f"Matched citation rows: "
        f"{matched_citations}"
    )

    print(
        f"Citation coverage (%): "
        f"{citation_coverage:.4f}"
    )

    return all_matches


def join_three_sources(citation_matches, hn_matches):
    """
    Join the ArXiv↔OpenAlex matches with the HN↔ArXiv matches.

    A row is considered a three-source match when the same ArXiv paper
    exists in:
    - ArXiv Silver
    - OpenAlex citations Silver
    - Hacker News Silver
    """

    three_source = (
        citation_matches.alias("c")
        .join(
            hn_matches.alias("h"),
            F.col("c.arxiv_id") == F.col("h.arxiv_id"),
            "inner",
        )
        .select(
            F.col("c.arxiv_id").alias("arxiv_id"),
            F.col("c.openalex_id").alias("openalex_id"),
            F.col("h.hn_id").alias("hn_id"),
            F.col("h.hn_score").alias("hn_score"),
            F.col("h.hn_descendants").alias("hn_descendants"),
            F.col("h.hn_title").alias("hn_title"),
            F.col("h.hn_url").alias("hn_url"),
        )
        .dropDuplicates(
            ["arxiv_id", "openalex_id", "hn_id"]
        )
        .cache()
    )

    total_rows = three_source.count()

    matched_arxiv = (
        three_source
        .select("arxiv_id")
        .distinct()
        .count()
    )

    matched_openalex = (
        three_source
        .select("openalex_id")
        .distinct()
        .count()
    )

    matched_hn = (
        three_source
        .select("hn_id")
        .distinct()
        .count()
    )

    print()
    print("=" * 70)
    print("THREE-SOURCE MATCH COVERAGE")
    print("=" * 70)

    print(f"Three-source joined rows: {total_rows}")
    print(f"Distinct ArXiv papers matched across all 3 sources: {matched_arxiv}")
    print(f"Distinct OpenAlex works matched across all 3 sources: {matched_openalex}")
    print(f"Distinct HN posts matched across all 3 sources: {matched_hn}")

    return three_source


# =============================================================================
# Main
# =============================================================================

def main():

    spark = create_spark()

    spark.sparkContext.setLogLevel("WARN")

    try:
        arxiv_df = spark.read.parquet(ARXIV_PATH)
        citations_df = spark.read.parquet(CITATIONS_PATH)
        hn_df = spark.read.parquet(HN_PATH)

        print("ArXiv rows:", arxiv_df.count())
        print("Citation rows:", citations_df.count())

        joined = join_arxiv_citations(
            arxiv_df,
            citations_df
        )

        hn_matches = join_hn_arxiv(
            hn_df,
            arxiv_df
        )


        three_source = join_three_sources(
            joined,
            hn_matches
        )

        print()
        print("Sample three-source matches:")

        three_source.show(
            10,
            truncate=False
)

        print()
        print("Sample HN ↔ ArXiv matches:")

        hn_matches.show(
            10,
            truncate=False
        )

        print()
        print("Sample matched pairs:")

        joined.show(
            10,
            truncate=False,
        )
        
        input(
            "\nSpark UI available on port 4040. "
            "Press ENTER to stop Spark..."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()