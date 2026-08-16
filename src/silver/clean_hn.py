"""
Hacker News Silver cleaning pipeline.

Reads all Hacker News Bronze JSONL micro-batches, applies the
cleaning rules identified during quality profiling, removes duplicate
items, normalizes timestamps and missing values, and writes the
result as Parquet.

Bronze data is never modified.
"""

import json
from pathlib import Path

import pandas as pd


# =============================================================================
# Configuration
# =============================================================================

HN_BRONZE_DIR = Path("/opt/airflow/data/bronze/hn")

HN_SILVER_DIR = Path("/opt/airflow/data/silver/hn")

HN_SILVER_FILE = HN_SILVER_DIR / "hn_clean.parquet"


# =============================================================================
# Load Bronze data
# =============================================================================

def load_hn_bronze():
    """
    Load all Hacker News Bronze micro-batch JSONL files.
    """

    files = sorted(HN_BRONZE_DIR.glob("*.jsonl"))

    if not files:
        raise FileNotFoundError(
            f"No Hacker News Bronze files found in {HN_BRONZE_DIR}"
        )

    records = []
    invalid_lines = 0

    print(f"Bronze files found: {len(files)}")

    for file in files:

        with open(file, "r", encoding="utf-8") as f:

            for line in f:

                try:
                    records.append(json.loads(line))

                except json.JSONDecodeError:
                    invalid_lines += 1

    print(f"Bronze rows loaded: {len(records)}")
    print(f"Invalid JSON lines: {invalid_lines}")

    return pd.DataFrame(records)


# =============================================================================
# Cleaning
# =============================================================================

def clean_hn(df):
    """
    Apply Silver cleaning rules to Hacker News data.
    """

    print("\nStarting Hacker News Silver cleaning...")

    original_rows = len(df)

    # -------------------------------------------------------------------------
    # Keep useful fields
    # -------------------------------------------------------------------------

    columns = [
        "id",
        "by",
        "time",
        "type",
        "title",
        "url",
        "score",
        "descendants",
        "text",
        "kids",
    ]

    existing_columns = [
        column
        for column in columns
        if column in df.columns
    ]

    df = df[existing_columns].copy()

    # -------------------------------------------------------------------------
    # Remove records without an ID
    # -------------------------------------------------------------------------

    df = df.dropna(subset=["id"])

    # -------------------------------------------------------------------------
    # Remove duplicate HN items
    #
    # The same item can appear in several 5-minute Bronze micro-batches.
    # This is expected in Bronze but must be deduplicated in Silver.
    # -------------------------------------------------------------------------

    before_duplicates = len(df)

    df = df.drop_duplicates(
        subset=["id"],
        keep="last",
    )

    duplicates_removed = before_duplicates - len(df)

    # -------------------------------------------------------------------------
    # Normalize timestamp
    #
    # Hacker News API stores time as Unix timestamp.
    # -------------------------------------------------------------------------

    df["time"] = (
    pd.to_datetime(
        df["time"],
        unit="s",
        errors="coerce",
        utc=True,
    )
    .dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    )

    # -------------------------------------------------------------------------
    # Normalize textual values
    # -------------------------------------------------------------------------

    text_columns = [
        "by",
        "type",
        "title",
        "url",
        "text",
    ]

    for column in text_columns:

        if column in df.columns:

            df[column] = (
                df[column]
                .fillna("")
                .astype(str)
                .str.strip()
            )

    # -------------------------------------------------------------------------
    # Normalize numeric values
    # -------------------------------------------------------------------------

    for column in ["score", "descendants"]:

        if column in df.columns:

            df[column] = pd.to_numeric(
                df[column],
                errors="coerce",
            ).fillna(0)

    # -------------------------------------------------------------------------
    # Normalize kids
    #
    # Missing kids means that the item currently has no known children.
    # -------------------------------------------------------------------------

    if "kids" in df.columns:

        df["kids"] = df["kids"].apply(
            lambda value:
            value if isinstance(value, list) else []
        )

    # -------------------------------------------------------------------------
    # Final statistics
    # -------------------------------------------------------------------------

    invalid_dates = df["time"].isna().sum()

    print(f"Silver rows: {len(df)}")
    print(f"Duplicate IDs removed: {duplicates_removed}")
    print(f"Invalid dates after normalization: {invalid_dates}")
    print(f"Rows removed in total: {original_rows - len(df)}")

    return df.reset_index(drop=True)


# =============================================================================
# Write Silver Parquet
# =============================================================================

def write_silver(df):
    """
    Write cleaned Hacker News data to Parquet.
    """

    HN_SILVER_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    df.to_parquet(
        HN_SILVER_FILE,
        index=False,
        compression="snappy",
    )

    print(
        f"\nSilver Parquet written to: {HN_SILVER_FILE}"
    )


# =============================================================================
# Main
# =============================================================================

def main():

    df = load_hn_bronze()

    clean_df = clean_hn(df)

    write_silver(clean_df)

    print(
        "\nHacker News Silver cleaning completed successfully."
    )


if __name__ == "__main__":
    main()