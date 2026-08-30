from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark.ml.feature import (
    RegexTokenizer,
    StopWordsRemover,
    HashingTF,
    IDF,
)


ARXIV_PATH = "s3a://arxiv-clean/full/*.parquet"


def create_spark():
    return (
        SparkSession.builder
        .appName("SciPulse-TFIDF")
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


def main():

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:

        # -------------------------------------------------------------
        # Load ArXiv Silver
        # -------------------------------------------------------------

        arxiv_df = (
            spark.read.parquet(ARXIV_PATH)
            .select(
                "id",
                "title",
                "abstract",
            )
            .filter(
                F.col("abstract").isNotNull()
                & (F.length(F.trim("abstract")) > 0)
            )
        )

        print("ArXiv abstracts:", arxiv_df.count())

        # -------------------------------------------------------------
        # 1. Tokenization
        # -------------------------------------------------------------

        tokenizer = RegexTokenizer(
            inputCol="abstract",
            outputCol="tokens",
            pattern=r"\W+",
            minTokenLength=2,
            toLowercase=True,
        )

        tokenized = tokenizer.transform(arxiv_df)

        # -------------------------------------------------------------
        # 2. Stop words removal
        # -------------------------------------------------------------

        remover = StopWordsRemover(
            inputCol="tokens",
            outputCol="filtered_tokens",
        )

        cleaned = remover.transform(tokenized)

        # -------------------------------------------------------------
        # 3. Term Frequency
        # -------------------------------------------------------------

        hashing_tf = HashingTF(
            inputCol="filtered_tokens",
            outputCol="tf_features",
            numFeatures=1 << 18,
        )

        tf_df = hashing_tf.transform(cleaned)

        # -------------------------------------------------------------
        # 4. IDF
        # -------------------------------------------------------------

        idf = IDF(
            inputCol="tf_features",
            outputCol="tfidf_features",
        )

        idf_model = idf.fit(tf_df)

        tfidf_df = idf_model.transform(tf_df)

        # -------------------------------------------------------------
        # Validation
        # -------------------------------------------------------------

        print()
        print("=" * 70)
        print("TF-IDF PIPELINE RESULT")
        print("=" * 70)

        tfidf_df.select(
            "id",
            "title",
            "filtered_tokens",
            "tfidf_features",
        ).show(
            5,
            truncate=80,
        )

        print()
        print("TF-IDF rows:", tfidf_df.count())

        print()
        print("TF-IDF pipeline completed successfully.")

        input(
            "\nSpark UI available on port 4040. "
            "Press ENTER to stop Spark..."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()