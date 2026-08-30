"""
Full Silver cleaning pipeline for ArXiv.

The Bronze JSONL file is processed in small chunks to avoid loading
the full ~5 GB dataset into memory.

Transformations:
- keep only useful fields
- normalize missing values
- extract original publication date from versions[0].created
- normalize update_date
- encode categories as arrays
- remove duplicate IDs
- write cleaned data as multiple Parquet files

The Bronze dataset is never modified.
"""

import gc
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

# Smaller chunks reduce peak memory usage inside Airflow.
CHUNK_SIZE = 10_000


# Keep only the Bronze fields required for Silver.
# This avoids storing large unused structures such as authors_parsed.
ARXIV_COLUMNS = [
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


# =============================================================================
# Helpers
# =============================================================================

def extract_first_publication_date(versions):
    """
    Extract the initial ArXiv submission date.

    ArXiv stores the submission history in the "versions" field.
    The first version corresponds to the initial submission.
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

    # -------------------------------------------------------------------------
    # Keep only expected columns
    # -------------------------------------------------------------------------

    existing_columns = [
        column
        for column in ARXIV_COLUMNS
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
    # Normalize optional text fields
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
    # Example:
    # "cs.AI cs.LG"
    # ->
    # ["cs.AI", "cs.LG"]
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
    # Original ArXiv publication date
    #
    # versions[0]["created"] corresponds to the first submission.
    # -------------------------------------------------------------------------

    if "versions" in df.columns:

        df["published_date"] = (
            df["versions"]
            .apply(
                extract_first_publication_date
            )
        )

        df["published_date"] = (
            pd.to_datetime(
                df["published_date"],
                errors="coerce",
                utc=True,
            )
            .dt.strftime("%Y-%m-%d")
        )

        # Raw nested structure is no longer needed in Silver.
        df = df.drop(
            columns=["versions"]
        )

    # -------------------------------------------------------------------------
    # Normalize update_date
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
    # Remove records without an ArXiv ID
    # -------------------------------------------------------------------------

    if "id" in df.columns:

        df = df.dropna(
            subset=["id"]
        )

        df["id"] = (
            df["id"]
            .astype(str)
            .str.strip()
        )

        df = df[
            df["id"] != ""
        ]

    # -------------------------------------------------------------------------
    # Remove duplicates inside the current chunk
    # -------------------------------------------------------------------------

    if "id" in df.columns:
        df = df.drop_duplicates(
            subset=["id"],
            keep="last",
        )

    return df.reset_index(
        drop=True
    )


# =============================================================================
# Write one Silver chunk
# =============================================================================

def write_chunk(
    records,
    chunk_number,
):
    """
    Convert one Bronze chunk to Silver and write it as Parquet.
    """

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

    rows_written = len(
        clean_df
    )

    print(
        f"Chunk {chunk_number} written: "
        f"{rows_written} rows"
    )

    # Explicitly release memory before the next chunk.
    del df
    del clean_df

    gc.collect()

    return rows_written


# =============================================================================
# Full dataset processing
# =============================================================================

def process_full_dataset():
    """
    Process the complete ArXiv Bronze dataset in small chunks.
    """

    if not ARXIV_BRONZE_FILE.exists():
        raise FileNotFoundError(
            f"Bronze file not found: "
            f"{ARXIV_BRONZE_FILE}"
        )

    ARXIV_SILVER_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # -------------------------------------------------------------------------
    # Remove old Parquet files before rebuilding Silver
    # -------------------------------------------------------------------------

    old_files = list(
        ARXIV_SILVER_DIR.glob(
            "arxiv_clean_part_*.parquet"
        )
    )

    for old_file in old_files:
        old_file.unlink()

    print(
        f"Previous Silver files removed: "
        f"{len(old_files)}"
    )

    # -------------------------------------------------------------------------
    # Streaming Bronze processing
    # -------------------------------------------------------------------------

    records = []

    chunk_number = 0

    total_input_rows = 0
    total_output_rows = 0
    invalid_json_lines = 0

    with open(
        ARXIV_BRONZE_FILE,
        "r",
        encoding="utf-8",
    ) as file:

        for line in file:

            try:
                raw_record = json.loads(
                    line
                )

            except json.JSONDecodeError:
                invalid_json_lines += 1
                continue

            # -------------------------------------------------------------
            # Keep only useful fields immediately.
            #
            # This significantly reduces memory usage because the complete
            # raw ArXiv records contain additional nested structures.
            # -------------------------------------------------------------

            record = {
                column: raw_record.get(
                    column
                )
                for column in ARXIV_COLUMNS
            }

            records.append(
                record
            )

            total_input_rows += 1

            # -------------------------------------------------------------
            # Process a complete chunk
            # -------------------------------------------------------------

            if len(records) >= CHUNK_SIZE:

                chunk_number += 1

                rows_written = write_chunk(
                    records,
                    chunk_number,
                )

                total_output_rows += (
                    rows_written
                )

                # Release the current Bronze chunk.
                records.clear()

                gc.collect()

        # ---------------------------------------------------------------------
        # Last incomplete chunk
        # ---------------------------------------------------------------------

        if records:

            chunk_number += 1

            rows_written = write_chunk(
                records,
                chunk_number,
            )

            total_output_rows += (
                rows_written
            )

            records.clear()

            gc.collect()

    # =========================================================================
    # Final report
    # =========================================================================

    print()
    print("=" * 70)
    print(
        "ARXIV FULL SILVER PROCESSING COMPLETE"
    )
    print("=" * 70)

    print(
        "Input rows processed:",
        total_input_rows,
    )

    print(
        "Silver rows written:",
        total_output_rows,
    )

    print(
        "Invalid JSON lines:",
        invalid_json_lines,
    )

    print(
        "Parquet files generated:",
        chunk_number,
    )

    print(
        "Chunk size:",
        CHUNK_SIZE,
    )

    print(
        "Output directory:",
        ARXIV_SILVER_DIR,
    )
# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    process_full_dataset()