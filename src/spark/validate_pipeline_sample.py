from impact_score import (
    create_spark,
    prepare_arxiv,
    prepare_citations,
    prepare_hn,
    build_impact_dataset,
    normalize_and_score,
    ARXIV_PATH,
    CITATIONS_PATH,
    HN_PATH,
)

from pyspark.sql import functions as F


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        arxiv_df = prepare_arxiv(
            spark.read.parquet(ARXIV_PATH)
        )

        citations_df = prepare_citations(
            spark.read.parquet(CITATIONS_PATH)
        )

        hn_df = prepare_hn(
            spark.read.parquet(HN_PATH)
        )

        # Sous-ensemble de validation
        citations_test = citations_df.limit(1000)

        test_df = build_impact_dataset(
            arxiv_df,
            citations_test,
            hn_df,
        )

        final_df = normalize_and_score(test_df)

        count = final_df.count()

        null_scores = final_df.filter(
            F.col("impact_score").isNull()
        ).count()

        invalid_scores = final_df.filter(
            (F.col("impact_score") < 0)
            | (F.col("impact_score") > 100)
        ).count()

        print()
        print("=" * 60)
        print("PIPELINE VALIDATION TEST")
        print("=" * 60)

        print("Input citation sample:", citations_test.count())
        print("Output papers:", count)
        print("Null impact scores:", null_scores)
        print("Impact scores outside [0,100]:", invalid_scores)

        if count > 0 and null_scores == 0 and invalid_scores == 0:
            print("VALIDATION RESULT: SUCCESS")
        else:
            print("VALIDATION RESULT: FAILED")

        print("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()