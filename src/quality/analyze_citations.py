"""
SciPulse - OpenAlex citation Bronze profiling

This script analyses the raw OpenAlex citation dataset before
Silver transformations.

It checks:
- record count
- missing values
- DOI availability
- ArXiv identifier availability
- citation count availability
- duplicate OpenAlex IDs
- duplicate DOI values
- duplicate ArXiv identifiers
- publication year consistency

No transformation is performed.
"""

import json
from collections import Counter
from pathlib import Path


# =============================================================================
# Configuration
# =============================================================================

BRONZE_DIR = Path(
    "/opt/airflow/data/bronze/citations"
)

# We only want the final 50k Bronze ingestion.
EXPECTED_MIN_RECORDS = 50000


# =============================================================================
# Helpers
# =============================================================================

def extract_arxiv_id(record):
    """
    Extract the raw ArXiv identifier from OpenAlex primary_location.

    Example:
        pmh:oai:arXiv.org:2308.10620
    becomes:
        2308.10620

    This is ONLY for profiling.
    Actual normalization will be implemented in the Silver pipeline.
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

    if marker in raw_id:
        return raw_id.split(marker, 1)[1]

    return None


def find_final_bronze_file():
    """
    Find the most recent OpenAlex ArXiv Bronze JSONL file
    containing the final large ingestion.
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

    # Latest ingestion file
    return files[-1]


# =============================================================================
# Profiling
# =============================================================================

def profile_citations():

    file_path = find_final_bronze_file()

    print("=" * 70)
    print("OPENALEX CITATIONS BRONZE PROFILING")
    print("=" * 70)

    print(
        f"Input file: {file_path}"
    )

    total = 0

    missing_openalex_id = 0
    missing_title = 0
    missing_doi = 0
    missing_arxiv_id = 0
    missing_citation_count = 0
    missing_publication_year = 0

    negative_citation_count = 0
    invalid_publication_year = 0

    openalex_ids = Counter()
    doi_values = Counter()
    arxiv_ids = Counter()

    publication_years = []

    with open(
        file_path,
        "r",
        encoding="utf-8",
    ) as file:

        for line in file:

            record = json.loads(
                line
            )

            total += 1

            # -------------------------------------------------------------
            # OpenAlex ID
            # -------------------------------------------------------------

            openalex_id = record.get(
                "id"
            )

            if openalex_id:
                openalex_ids[
                    openalex_id
                ] += 1
            else:
                missing_openalex_id += 1

            # -------------------------------------------------------------
            # Title
            # -------------------------------------------------------------

            title = (
                record.get("title")
                or ""
            ).strip()

            if not title:
                missing_title += 1

            # -------------------------------------------------------------
            # DOI
            # -------------------------------------------------------------

            doi = record.get(
                "doi"
            )

            if doi:
                doi_values[
                    doi
                ] += 1
            else:
                missing_doi += 1

            # -------------------------------------------------------------
            # ArXiv ID
            # -------------------------------------------------------------

            arxiv_id = extract_arxiv_id(
                record
            )

            if arxiv_id:
                arxiv_ids[
                    arxiv_id
                ] += 1
            else:
                missing_arxiv_id += 1

            # -------------------------------------------------------------
            # Citation count
            # -------------------------------------------------------------

            citation_count = record.get(
                "cited_by_count"
            )

            if citation_count is None:
                missing_citation_count += 1

            elif citation_count < 0:
                negative_citation_count += 1

            # -------------------------------------------------------------
            # Publication year
            # -------------------------------------------------------------

            year = record.get(
                "publication_year"
            )

            if year is None:
                missing_publication_year += 1

            elif not isinstance(
                year,
                int,
            ):
                invalid_publication_year += 1

            else:
                publication_years.append(
                    year
                )

    # =====================================================================
    # Duplicates
    # =====================================================================

    duplicate_openalex_ids = sum(
        1
        for count in openalex_ids.values()
        if count > 1
    )

    duplicate_dois = sum(
        1
        for count in doi_values.values()
        if count > 1
    )

    duplicate_arxiv_ids = sum(
        1
        for count in arxiv_ids.values()
        if count > 1
    )

    # =====================================================================
    # Results
    # =====================================================================

    print()
    print("-" * 70)
    print("RECORDS")
    print("-" * 70)

    print(
        f"Total records: {total}"
    )

    print()
    print("-" * 70)
    print("MISSING VALUES")
    print("-" * 70)

    print(
        f"Missing OpenAlex ID: {missing_openalex_id}"
    )

    print(
        f"Missing title: {missing_title}"
    )

    print(
        f"Missing DOI: {missing_doi}"
    )

    print(
        f"Missing ArXiv ID: {missing_arxiv_id}"
    )

    print(
        f"Missing citation count: {missing_citation_count}"
    )

    print(
        f"Missing publication year: {missing_publication_year}"
    )

    print()
    print("-" * 70)
    print("COVERAGE")
    print("-" * 70)

    print(
        "DOI coverage (%):",
        round(
            (total - missing_doi)
            / total
            * 100,
            2,
        ),
    )

    print(
        "ArXiv ID coverage (%):",
        round(
            (total - missing_arxiv_id)
            / total
            * 100,
            2,
        ),
    )

    print(
        "Citation count coverage (%):",
        round(
            (total - missing_citation_count)
            / total
            * 100,
            2,
        ),
    )

    print()
    print("-" * 70)
    print("DUPLICATES")
    print("-" * 70)

    print(
        f"Duplicate OpenAlex IDs: {duplicate_openalex_ids}"
    )

    print(
        f"Duplicate DOI values: {duplicate_dois}"
    )

    print(
        f"Duplicate ArXiv IDs: {duplicate_arxiv_ids}"
    )

    print()
    print("-" * 70)
    print("CONSISTENCY")
    print("-" * 70)

    print(
        f"Negative citation counts: {negative_citation_count}"
    )

    print(
        f"Invalid publication year types: {invalid_publication_year}"
    )

    if publication_years:

        print(
            f"Minimum publication year: {min(publication_years)}"
        )

        print(
            f"Maximum publication year: {max(publication_years)}"
        )

    print()
    print("=" * 70)
    print("PROFILING COMPLETE")
    print("=" * 70)


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    profile_citations()