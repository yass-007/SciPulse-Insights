"""
SciPulse - OpenAlex Bronze ingestion

Robust ingestion of OpenAlex works whose primary source is arXiv.

The script:
- uses cursor-based pagination
- filters OpenAlex works whose primary source is arXiv
- authenticates with OPENALEX_API_KEY from the environment
- retries transient HTTP/network errors
- handles HTTP 429 rate limits
- writes raw OpenAlex records as JSON Lines
- uploads the raw JSONL file to MinIO citations-raw

Bronze rules:
- raw source records only
- no cleaning
- no normalization
- no deduplication
"""

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from minio import Minio


# =============================================================================
# Configuration
# =============================================================================

OPENALEX_URL = "https://api.openalex.org/works"

# OpenAlex source ID corresponding to arXiv
ARXIV_SOURCE_ID = "S4306400194"

# API key is read from the environment.
# Never hardcode the key in the source code.
OPENALEX_API_KEY = os.getenv("OPENALEX_API_KEY")

# Local Bronze storage
LOCAL_OUTPUT_DIR = Path(
    "/opt/airflow/data/bronze/citations"
)

# MinIO
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

MINIO_BUCKET = "citations-raw"

# Pagination
PER_PAGE = 100

# 500 pages x 100 records = maximum 50,000 records
MAX_PAGES = 500

# HTTP configuration
REQUEST_TIMEOUT = 60

# Retry configuration
MAX_RETRIES = 5
INITIAL_BACKOFF_SECONDS = 2

# Small delay between successful requests
REQUEST_DELAY_SECONDS = 0.15

# Safety limit:
# If OpenAlex asks us to wait for several hours, fail instead of sleeping forever.
MAX_RETRY_AFTER_SECONDS = 120


# =============================================================================
# Configuration validation
# =============================================================================

def validate_configuration():
    """
    Validate required runtime configuration before starting ingestion.
    """

    if not OPENALEX_API_KEY:
        raise RuntimeError(
            "OPENALEX_API_KEY environment variable is not set."
        )


# =============================================================================
# HTTP session
# =============================================================================

def create_session():
    """
    Create a reusable HTTP session for OpenAlex requests.
    """

    session = requests.Session()

    session.headers.update(
        {
            "User-Agent": (
                "SciPulse-Academic-Project/1.0 "
                "(OpenAlex citation ingestion)"
            )
        }
    )

    return session


# =============================================================================
# OpenAlex request with retry
# =============================================================================

def fetch_page(session, params):
    """
    Retrieve one OpenAlex page.

    Retries are performed for:
    - HTTP 429 rate limiting
    - HTTP 5xx server errors
    - timeouts
    - connection errors
    - interrupted/chunked HTTP responses
    """

    backoff = INITIAL_BACKOFF_SECONDS

    for attempt in range(
        1,
        MAX_RETRIES + 1,
    ):

        try:
            response = session.get(
                OPENALEX_URL,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            # -----------------------------------------------------------------
            # Rate limiting
            # -----------------------------------------------------------------

            if response.status_code == 429:

                retry_after_header = response.headers.get(
                    "Retry-After"
                )

                if retry_after_header:
                    try:
                        wait_time = float(
                            retry_after_header
                        )
                    except ValueError:
                        wait_time = backoff
                else:
                    wait_time = backoff

                # Avoid accidentally sleeping for several hours.
                if wait_time > MAX_RETRY_AFTER_SECONDS:
                    raise RuntimeError(
                        "OpenAlex rate limit reached. "
                        f"Requested Retry-After is {wait_time:.0f} seconds, "
                        "which exceeds the configured safety limit. "
                        "Check the API key and remaining quota."
                    )

                print(
                    f"HTTP 429 - rate limited. "
                    f"Waiting {wait_time:.1f}s "
                    f"(attempt {attempt}/{MAX_RETRIES})..."
                )

                time.sleep(
                    wait_time
                )

                backoff *= 2
                continue

            # -----------------------------------------------------------------
            # Temporary server errors
            # -----------------------------------------------------------------

            if 500 <= response.status_code < 600:

                print(
                    f"HTTP {response.status_code} "
                    f"on attempt {attempt}/{MAX_RETRIES}. "
                    f"Retrying in {backoff}s..."
                )

                time.sleep(
                    backoff
                )

                backoff *= 2
                continue

            response.raise_for_status()

            return response.json()

        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
        ) as error:

            print(
                f"Network/protocol error on attempt "
                f"{attempt}/{MAX_RETRIES}: {error}"
            )

            if attempt == MAX_RETRIES:
                raise

            print(
                f"Retrying in {backoff}s..."
            )

            time.sleep(
                backoff
            )

            backoff *= 2

    raise RuntimeError(
        "OpenAlex request failed after maximum retries."
    )


# =============================================================================
# OpenAlex Bronze ingestion
# =============================================================================

def download_openalex():
    """
    Download raw OpenAlex works associated with the arXiv source.

    Cursor pagination is used to retrieve up to MAX_PAGES pages.

    Records are written exactly as returned by OpenAlex.
    No cleaning or normalization is performed.
    """

    LOCAL_OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    timestamp = datetime.now(
        timezone.utc
    ).strftime("%Y%m%dT%H%M%SZ")

    output_file = (
        LOCAL_OUTPUT_DIR
        / f"openalex_arxiv_{timestamp}.jsonl"
    )

    session = create_session()

    cursor = "*"

    page_number = 0
    total_records = 0

    print("=" * 70)
    print("OPENALEX ARXIV BRONZE INGESTION")
    print("=" * 70)

    print(
        f"Source filter: {ARXIV_SOURCE_ID}"
    )

    print(
        f"Output file: {output_file}"
    )

    print(
        f"Maximum pages: {MAX_PAGES}"
    )

    print(
        f"Maximum expected records: "
        f"{MAX_PAGES * PER_PAGE}"
    )

    with open(
        output_file,
        "w",
        encoding="utf-8",
    ) as file:

        while (
            cursor
            and page_number < MAX_PAGES
        ):

            page_number += 1

            print(
                f"\nFetching page "
                f"{page_number}/{MAX_PAGES}..."
            )

            params = {
                "filter": (
                    f"primary_location.source.id:"
                    f"{ARXIV_SOURCE_ID}"
                ),
                "per-page": PER_PAGE,
                "cursor": cursor,
                "api_key": OPENALEX_API_KEY,
            }

            payload = fetch_page(
                session,
                params,
            )

            results = payload.get(
                "results",
                []
            )

            meta = payload.get(
                "meta",
                {}
            )

            if page_number == 1:

                print(
                    "Total matching works available: "
                    f"{meta.get('count')}"
                )

            print(
                f"Records received: "
                f"{len(results)}"
            )

            # -----------------------------------------------------------------
            # Raw Bronze JSONL write
            # -----------------------------------------------------------------

            for record in results:

                file.write(
                    json.dumps(
                        record,
                        ensure_ascii=False,
                    )
                    + "\n"
                )

            # Persist the current progress to disk after every page.
            file.flush()

            total_records += len(results)

            print(
                f"Total records written so far: "
                f"{total_records}"
            )

            cursor = meta.get(
                "next_cursor"
            )

            if not results:
                print(
                    "No more OpenAlex results."
                )
                break

            time.sleep(
                REQUEST_DELAY_SECONDS
            )

    session.close()

    print()
    print("=" * 70)
    print("OPENALEX DOWNLOAD COMPLETE")
    print("=" * 70)

    print(
        f"Pages downloaded: "
        f"{page_number}"
    )

    print(
        f"Records written: "
        f"{total_records}"
    )

    print(
        f"Local Bronze file: "
        f"{output_file}"
    )

    return output_file


# =============================================================================
# MinIO upload
# =============================================================================

def upload_to_minio(file_path):
    """
    Upload the raw OpenAlex JSONL file to citations-raw.
    """

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    # -------------------------------------------------------------------------
    # Ensure Bronze bucket exists
    # -------------------------------------------------------------------------

    if not client.bucket_exists(
        MINIO_BUCKET
    ):

        client.make_bucket(
            MINIO_BUCKET
        )

        print(
            f"Created bucket: "
            f"{MINIO_BUCKET}"
        )

    # -------------------------------------------------------------------------
    # Upload JSONL
    # -------------------------------------------------------------------------

    object_name = file_path.name

    client.fput_object(
        MINIO_BUCKET,
        object_name,
        str(file_path),
        content_type="application/x-ndjson",
    )

    print()
    print(
        "Uploaded to MinIO:"
    )

    print(
        f"{MINIO_BUCKET}/{object_name}"
    )


# =============================================================================
# Main
# =============================================================================

def main():
    """
    Run the complete OpenAlex Bronze ingestion workflow.
    """

    validate_configuration()

    bronze_file = download_openalex()

    upload_to_minio(
        bronze_file
    )

    print()
    print(
        "OpenAlex Bronze ingestion "
        "completed successfully."
    )


if __name__ == "__main__":
    main()