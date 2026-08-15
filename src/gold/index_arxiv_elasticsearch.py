from pathlib import Path

import pandas as pd
from elasticsearch import Elasticsearch, helpers


# =============================================================================
# Configuration
# =============================================================================

ELASTICSEARCH_URL = "http://elasticsearch:9200"
INDEX_NAME = "arxiv-papers"

ARXIV_SILVER_DIR = Path(
    "/opt/airflow/data/silver/arxiv/full"
)

BULK_CHUNK_SIZE = 1000


# =============================================================================
# Elasticsearch connection
# =============================================================================

def get_elasticsearch_client():
    """
    Create and verify the Elasticsearch connection.
    """

    es = Elasticsearch(
        ELASTICSEARCH_URL,
        request_timeout=60,
    )

    if not es.ping():
        raise ConnectionError(
            "Unable to connect to Elasticsearch."
        )

    print("Connected to Elasticsearch.")

    return es


# =============================================================================
# Prepare Elasticsearch documents
# =============================================================================
def generate_actions(df):
    """
    Convert a pandas DataFrame into Elasticsearch bulk actions.

    The ArXiv ID is used as Elasticsearch _id so that re-running
    the indexing process updates the same document instead of
    creating duplicates.
    """

    for record in df.to_dict(orient="records"):

        # ---------------------------------------------------------------------
        # Normalize update_date
        # ---------------------------------------------------------------------

        update_date = record.get("update_date")

        if pd.notna(update_date):
            record["update_date"] = (
                pd.Timestamp(update_date)
                .strftime("%Y-%m-%d")
            )
        else:
            record["update_date"] = None

        # ---------------------------------------------------------------------
        # Normalize values for Elasticsearch
        # ---------------------------------------------------------------------

        for key, value in list(record.items()):

            # Lists are valid Elasticsearch values
            if isinstance(value, (list, tuple)):
                record[key] = list(value)
                continue

            # PyArrow / NumPy can return ndarray-like values
            if hasattr(value, "tolist") and not isinstance(
                value,
                (str, bytes),
            ):
                converted_value = value.tolist()

                if isinstance(converted_value, list):
                    record[key] = converted_value
                    continue

            # Scalar missing values
            if pd.isna(value):
                record[key] = None

        # ---------------------------------------------------------------------
        # Use ArXiv ID as Elasticsearch document ID
        # ---------------------------------------------------------------------

        document_id = str(record["id"])

        yield {
            "_index": INDEX_NAME,
            "_id": document_id,
            "_source": record,
        }


# =============================================================================
# Index full Silver dataset
# =============================================================================

def index_arxiv():
    """
    Read all ArXiv Silver Parquet partitions and index them
    progressively into Elasticsearch.
    """

    es = get_elasticsearch_client()

    parquet_files = sorted(
    ARXIV_SILVER_DIR.glob(
        "*.parquet"
    )
)

    if not parquet_files:
        raise FileNotFoundError(
            f"No Parquet files found in {ARXIV_SILVER_DIR}"
        )

    print(
        f"Parquet files found: "
        f"{len(parquet_files)}"
    )

    total_indexed = 0

    for file_number, parquet_file in enumerate(
        parquet_files,
        start=1,
    ):

        print(
            f"\nProcessing "
            f"{parquet_file.name} "
            f"({file_number}/{len(parquet_files)})"
        )

        df = pd.read_parquet(
            parquet_file
        )

        print(
            f"Rows loaded: {len(df)}"
        )

        success_count = 0
        failed_count = 0

        bulk_client = es.options(
            request_timeout=60
        )

        for success, result in helpers.streaming_bulk(
            bulk_client,
            generate_actions(df),
            chunk_size=BULK_CHUNK_SIZE,
            max_retries=3,
            initial_backoff=2,
            max_backoff=30,
            raise_on_error=False,
            raise_on_exception=False,
        ):

            if success:
                success_count += 1
            else:
                failed_count += 1
                print(
                    f"Indexing error: {result}"
                )

        total_indexed += success_count

        print(
            f"Indexed from partition: "
            f"{success_count}"
        )

        print(
            f"Failed from partition: "
            f"{failed_count}"
        )

        print(
            f"Total indexed so far: "
            f"{total_indexed}"
        )

    # Refresh so documents are immediately searchable
    es.indices.refresh(
        index=INDEX_NAME
    )

    count = es.count(
        index=INDEX_NAME
    )["count"]

    print("\n" + "=" * 70)
    print("ARXIV ELASTICSEARCH INDEXING COMPLETE")
    print("=" * 70)

    print(
        f"Documents indexed during run: "
        f"{total_indexed}"
    )

    print(
        f"Documents currently in index: "
        f"{count}"
    )


if __name__ == "__main__":
    index_arxiv()