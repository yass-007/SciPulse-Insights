"""
Full Silver cleaning pipeline for ArXiv.

The Bronze JSONL file is processed in chunks to avoid loading
the full ~5 GB dataset into memory.

Transformations:
- keep useful fields
- normalize missing values
- extract original publication date from versions[0].created
- normalize update_date
- encode categories as arrays
- remove duplicate IDs
- write cleaned data as multiple Parquet files

The Bronze dataset is never modified.
"""

import json
from pathlib import Path

import pandas as pd


# =============================================================================
# Configuration
# =============================================================================

ARXIV_BRONZE_FILE = Path(
    "/opt/airflow/data/bronze/arxiv/arxiv-metadata-oai-snapshot.json"
)

ARXIV_SILVER_DIR = Path(
    "/opt/airflow/data/silver/arxiv/full"
)

CHUNK_SIZE = 50_000


# =============================================================================
# Helpers
# =============================================================================

def extract_first_publication_date(versions):
    """
    Extract the initial ArXiv submission date.

    ArXiv stores submission history in the "versions" field.
    The first version corresponds to the initial publication/submission.
    """

    if not isinstance(versions, list) or not versions:
        return None

    first_version = versions[0]

    if not isinstance(first_version, dict):
        return None

    return first_version.get("created")


# =============================================================================
# Cleaning
# =============================================================================

def clean_chunk(df):
    """
    Apply Silver cleaning rules to one ArXiv chunk.
    """

    columns = [
        "id",
        "title",
        "abstract",
        "authors",
        "categories",
        "versions",
        "update_date",
        "doi",
        "journal-ref",
        "comments",
        "license",
    ]

    existing_columns = [
        column
        for column in columns
        if column in df.columns
    ]

    df = df[existing_columns].copy()

    # -------------------------------------------------------------------------
    # Normalize required text fields
    # -------------------------------------------------------------------------

    for column in [
        "title",
        "abstract",
        "authors",
    ]:
        if column in df.columns:
            df[column] = (
                df[column]
                .fillna("")
                .astype(str)
                .str.strip()
            )

    # -------------------------------------------------------------------------
    # Normalize optional fields
    # -------------------------------------------------------------------------

    for column in [
        "doi",
        "journal-ref",
        "comments",
        "license",
    ]:
        if column in df.columns:
            df[column] = (
                df[column]
                .fillna("")
                .astype(str)
                .str.strip()
            )

    # -------------------------------------------------------------------------
    # Categories
    #
    # "cs.AI cs.LG" -> ["cs.AI", "cs.LG"]
    # -------------------------------------------------------------------------

    if "categories" in df.columns:
        df["categories"] = (
            df["categories"]
            .fillna("")
            .astype(str)
            .apply(
                lambda value: [
                    category
                    for category in value.split()
                    if category
                ]
            )
        )

    # -------------------------------------------------------------------------
    # Extract original publication date
    #
    # versions[0]["created"] corresponds to the initial ArXiv submission.
    # -------------------------------------------------------------------------

    if "versions" in df.columns:

        df["published_date"] = (
            df["versions"]
            .apply(extract_first_publication_date)
        )

        df["published_date"] = (
            pd.to_datetime(
                df["published_date"],
                errors="coerce",
                utc=True,
            )
            .dt.strftime("%Y-%m-%d")
        )

        # Raw nested structure no longer needed in Silver
        df = df.drop(
            columns=["versions"]
        )

    # -------------------------------------------------------------------------
    # Normalize update date
    # -------------------------------------------------------------------------

    if "update_date" in df.columns:
        df["update_date"] = (
            pd.to_datetime(
                df["update_date"],
                errors="coerce",
            )
            .dt.strftime("%Y-%m-%d")
        )

    # -------------------------------------------------------------------------
    # Remove records without ID
    # -------------------------------------------------------------------------

    if "id" in df.columns:

        df = df.dropna(
            subset=["id"]
        )

        df = df[
            df["id"]
            .astype(str)
            .str.strip()
            != ""
        ]

    # -------------------------------------------------------------------------
    # Remove duplicates inside current chunk
    # -------------------------------------------------------------------------

    df = df.drop_duplicates(
        subset=["id"],
        keep="last",
    )

    return df.reset_index(
        drop=True
    )


# =============================================================================
# Full dataset processing
# =============================================================================

def process_full_dataset():
    """
    Process the complete ArXiv Bronze dataset in chunks.
    """

    if not ARXIV_BRONZE_FILE.exists():
        raise FileNotFoundError(
            f"Bronze file not found: {ARXIV_BRONZE_FILE}"
        )

    ARXIV_SILVER_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    records = []

    chunk_number = 0
    total_input_rows = 0
    total_output_rows = 0

    with open(
        ARXIV_BRONZE_FILE,
        "r",
        encoding="utf-8",
    ) as file:

        for line in file:

            try:
                record = json.loads(line)
                records.append(record)

            except json.JSONDecodeError:
                continue

            if len(records) >= CHUNK_SIZE:

                chunk_number += 1
                total_input_rows += len(records)

                df = pd.DataFrame(
                    records
                )

                clean_df = clean_chunk(
                    df
                )

                output_file = (
                    ARXIV_SILVER_DIR
                    / f"arxiv_clean_part_{chunk_number:04d}.parquet"
                )

                clean_df.to_parquet(
                    output_file,
                    index=False,
                    compression="snappy",
                )

                total_output_rows += len(
                    clean_df
                )

                print(
                    f"Chunk {chunk_number} written: "
                    f"{len(clean_df)} rows"
                )

                records = []

        # ---------------------------------------------------------------------
        # Last incomplete chunk
        # ---------------------------------------------------------------------

        if records:

            chunk_number += 1
            total_input_rows += len(
                records
            )

            df = pd.DataFrame(
                records
            )

            clean_df = clean_chunk(
                df
            )

            output_file = (
                ARXIV_SILVER_DIR
                / f"arxiv_clean_part_{chunk_number:04d}.parquet"
            )

            clean_df.to_parquet(
                output_file,
                index=False,
                compression="snappy",
            )

            total_output_rows += len(
                clean_df
            )

            print(
                f"Chunk {chunk_number} written: "
                f"{len(clean_df)} rows"
            )

    print()
    print("=" * 70)
    print("ARXIV FULL SILVER PROCESSING COMPLETE")
    print("=" * 70)

    print(
        f"Input rows processed: "
        f"{total_input_rows}"
    )

    print(
        f"Silver rows written: "
        f"{total_output_rows}"
    )

    print(
        f"Parquet files generated: "
        f"{chunk_number}"
    )

    print(
        f"Output directory: "
        f"{ARXIV_SILVER_DIR}"
    )


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    process_full_dataset()