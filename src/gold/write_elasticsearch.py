import sys

from pyspark.sql import functions as F


# =============================================================================
# Allow imports from /opt/spark/work-dir/src
# =============================================================================

sys.path.insert(
    0,
    "/opt/spark/work-dir/src",
)


# =============================================================================
# Reuse the impact score pipeline
# =============================================================================

from spark.impact_score import (
    create_spark,
    prepare_arxiv,
    prepare_citations,
    prepare_hn,
    build_impact_dataset,
    normalize_and_score,
)


# =============================================================================
# Data sources
# =============================================================================

ARXIV_PATH = "s3a://arxiv-clean/full/*.parquet"
HN_PATH = "s3a://hn-clean/hn_clean.parquet"
CITATIONS_PATH = "s3a://citations-clean/citations_clean.parquet"

ES_INDEX = "arxiv-papers-enriched"


# =============================================================================
# Main Gold pipeline
# =============================================================================

def main():

    spark = create_spark()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    try:

        # =====================================================================
        # 1. Load Silver datasets from MinIO
        # =====================================================================

        print()
        print("=" * 70)
        print("LOADING SILVER DATA FOR GOLD")
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

        # =====================================================================
        # 2. Build enriched impact dataset
        # =====================================================================

        print()
        print("=" * 70)
        print("BUILDING GOLD DATAFRAME")
        print("=" * 70)

        impact_df = build_impact_dataset(
            arxiv_df,
            citations_df,
            hn_df,
        )

        final_df = normalize_and_score(
            impact_df
        )

        # =====================================================================
        # 3. Select the final Gold schema
        # =====================================================================

        gold_df = (
            final_df
            .select(
                "arxiv_id",
                "title",
                "categories",
                "published_date",
                "cited_by_count",
                "hn_score_total",
                "hn_descendants_total",
                "citation_signal_norm",
                "hn_signal_norm",
                "recency_signal_norm",
                "impact_score",
            )

            # Store the date in a simple ISO format for Elasticsearch.
            .withColumn(
                "published_date",
                F.date_format(
                    F.col("published_date"),
                    "yyyy-MM-dd",
                ),
            )
        )

        gold_count = gold_df.count()

        print(
            "Gold rows:",
            gold_count,
        )

        print()
        print("=" * 70)
        print("GOLD SAMPLE")
        print("=" * 70)

        gold_df.orderBy(
            F.desc("impact_score")
        ).show(
            10,
            truncate=False,
        )

        # =====================================================================
        # 4. Write the Gold layer to Elasticsearch
        # =====================================================================
        #
        # This must be the only final Elasticsearch write in the project.
        #
        # arxiv_id is used as the Elasticsearch document ID so that
        # each ArXiv paper has a stable identifier.
        # =====================================================================

        print()
        print("=" * 70)
        print("WRITING GOLD DATA TO ELASTICSEARCH")
        print("=" * 70)

        (
            gold_df.write
            .format(
                "org.elasticsearch.spark.sql"
            )

            # Elasticsearch service name inside Docker network.
            .option(
                "es.nodes",
                "elasticsearch",
            )

            .option(
                "es.port",
                "9200",
            )

            # Required when the connector communicates using a fixed node.
            .option(
                "es.nodes.wan.only",
                "true",
            )

            # Final Gold index.
            .option(
                "es.resource",
                ES_INDEX,
            )

            # Stable Elasticsearch document identifier.
            .option(
                "es.mapping.id",
                "arxiv_id",
            )

            # Replace the Gold index content when the pipeline is rerun.
            .mode(
                "overwrite"
            )

            .save()
        )

        print()
        print("=" * 70)
        print("GOLD WRITE COMPLETED SUCCESSFULLY")
        print("=" * 70)

        print(
            "Elasticsearch index:",
            ES_INDEX,
        )

        print(
            "Documents written:",
            gold_count,
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()