"""
SciPulse - OpenAlex citations Silver cleaning

This script transforms the raw OpenAlex Bronze citation dataset into
a normalized Silver dataset ready for Spark joins.

Main transformations:
- normalize DOI values
- extract and normalize ArXiv identifiers
- retain citation metrics required for analytics
- remove duplicate OpenAlex records
- write the cleaned dataset as Parquet
"""

import json
import re
from pathlib import Path

import pandas as pd


# =============================================================================
# Configuration
# =============================================================================

BRONZE_DIR = Path(
    "/opt/airflow/data/bronze/citations"
)

SILVER_DIR = Path(
    "/opt/airflow/data/silver/citations"
)

OUTPUT_FILE = SILVER_DIR / "citations_clean.parquet"


# =============================================================================
# Helpers
# =============================================================================

def find_latest_bronze_file():
    """
    Return the latest final OpenAlex ArXiv Bronze JSONL file.
    """

    files = sorted(
        BRONZE_DIR.glob(
            "openalex_arxiv_*.jsonl"
        )
    )

    if not files:
        raise FileNotFoundError(
            "No OpenAlex ArXiv Bronze JSONL file found."
        )

    return files[-1]


def normalize_doi(doi):
    """
    Normalize DOI values.

    Examples:
        https://doi.org/10.1234/ABC
        http://doi.org/10.1234/ABC
        doi:10.1234/ABC

    become:
        10.1234/abc
    """

    if doi is None:
        return None

    doi = str(doi).strip().lower()

    if not doi:
        return None

    prefixes = [
        "https://doi.org/",
        "http://doi.org/",
        "http://dx.doi.org/",
        "https://dx.doi.org/",
        "doi:",
    ]

    for prefix in prefixes:
        if doi.startswith(prefix):
            doi = doi[len(prefix):]
            break

    doi = doi.strip()

    return doi or None


def extract_arxiv_id(record):
    """
    Extract and normalize the ArXiv identifier from OpenAlex.

    Example:
        pmh:oai:arXiv.org:2308.10620
    becomes:
        2308.10620
    """

    primary_location = (
        record.get("primary_location")
        or {}
    )

    raw_id = (
        primary_location.get("id")
        or ""
    )

    marker = "arXiv.org:"

    if marker not in raw_id:
        return None

    arxiv_id = raw_id.split(
        marker,
        1,
    )[1]

    arxiv_id = arxiv_id.strip().lower()

    # Remove possible version suffix:
    # 2308.10620v2 -> 2308.10620
    arxiv_id = re.sub(
        r"v\d+$",
        "",
        arxiv_id,
    )

    return arxiv_id or None


# =============================================================================
# Load and clean Bronze citations
# =============================================================================

def clean_citations():

    bronze_file = find_latest_bronze_file()

    print("=" * 70)
    print("OPENALEX CITATIONS SILVER CLEANING")
    print("=" * 70)

    print(
        f"Input Bronze file: {bronze_file}"
    )

    records = []

    with open(
        bronze_file,
        "r",
        encoding="utf-8",
    ) as file:

        for line in file:

            raw = json.loads(
                line
            )

            records.append(
                {
                    "openalex_id": raw.get("id"),
                    "doi": normalize_doi(
                        raw.get("doi")
                    ),
                    "arxiv_id": extract_arxiv_id(
                        raw
                    ),
                    "title": (
                        raw.get("title")
                        or ""
                    ).strip(),
                    "publication_year": raw.get(
                        "publication_year"
                    ),
                    "publication_date": raw.get(
                        "publication_date"
                    ),
                    "cited_by_count": raw.get(
                        "cited_by_count"
                    ),
                    "counts_by_year": raw.get(
                        "counts_by_year"
                    ),
                    "referenced_works_count": raw.get(
                        "referenced_works_count"
                    ),
                    "type": raw.get(
                        "type"
                    ),
                }
            )

    df = pd.DataFrame(
        records
    )

    bronze_rows = len(df)

    print()
    print(
        f"Bronze rows loaded: {bronze_rows}"
    )

    # -------------------------------------------------------------------------
    # Normalize publication date
    # -------------------------------------------------------------------------

    df["publication_date"] = pd.to_datetime(
        df["publication_date"],
        errors="coerce",
    )

    # -------------------------------------------------------------------------
    # Normalize numeric citation fields
    # -------------------------------------------------------------------------

    df["cited_by_count"] = pd.to_numeric(
        df["cited_by_count"],
        errors="coerce",
    ).fillna(0).astype("int64")

    df["referenced_works_count"] = pd.to_numeric(
        df["referenced_works_count"],
        errors="coerce",
    ).fillna(0).astype("int64")

    # -------------------------------------------------------------------------
    # Remove duplicate OpenAlex records
    # -------------------------------------------------------------------------

    before_dedup = len(df)

    df = df.drop_duplicates(
        subset=["openalex_id"],
        keep="first",
    )

    duplicate_rows_removed = (
        before_dedup - len(df)
    )

    # -------------------------------------------------------------------------
    # Basic missing-value handling
    # -------------------------------------------------------------------------

    df["title"] = (
        df["title"]
        .fillna("")
        .astype(str)
        .str.strip()
    )

    df["type"] = (
        df["type"]
        .fillna("unknown")
        .astype(str)
    )

    # -------------------------------------------------------------------------
    # Silver statistics
    # -------------------------------------------------------------------------

    silver_rows = len(df)

    doi_count = df["doi"].notna().sum()
    arxiv_count = df["arxiv_id"].notna().sum()

    print()
    print("-" * 70)
    print("SILVER CLEANING RESULTS")
    print("-" * 70)

    print(
        f"Silver rows: {silver_rows}"
    )

    print(
        f"Duplicate OpenAlex rows removed: "
        f"{duplicate_rows_removed}"
    )

    print(
        f"Rows with normalized DOI: "
        f"{doi_count}"
    )

    print(
        "DOI coverage (%):",
        round(
            doi_count
            / silver_rows
            * 100,
            2,
        ),
    )

    print(
        f"Rows with normalized ArXiv ID: "
        f"{arxiv_count}"
    )

    print(
        "ArXiv ID coverage (%):",
        round(
            arxiv_count
            / silver_rows
            * 100,
            2,
        ),
    )

    # -------------------------------------------------------------------------
    # Write Silver Parquet
    # -------------------------------------------------------------------------

    SILVER_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    df.to_parquet(
        OUTPUT_FILE,
        index=False,
    )

    print()
    print(
        f"Silver Parquet written to: "
        f"{OUTPUT_FILE}"
    )

    print()
    print(
        "OpenAlex citations Silver cleaning "
        "completed successfully."
    )


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    clean_citations()