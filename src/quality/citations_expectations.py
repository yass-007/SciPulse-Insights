"""
SciPulse - Great Expectations validation for OpenAlex citations.

This script validates a sample of the raw OpenAlex Bronze citation data
before the Silver cleaning and normalization stage.

The validation focuses on:
- OpenAlex identifier integrity
- citation count integrity
- publication year consistency

Validation results are stored in Great Expectations and Data Docs
are updated after each run.
"""

import json
from pathlib import Path

import pandas as pd
import great_expectations as gx


# =============================================================================
# Configuration
# =============================================================================

CITATIONS_DIR = Path(
    "/opt/airflow/data/bronze/citations"
)

SAMPLE_SIZE = 10000

GX_CONTEXT_DIR = "/opt/airflow/great_expectations"

SUITE_NAME = "citations_bronze_suite"

CHECKPOINT_NAME = "citations_bronze_checkpoint"


# =============================================================================
# Find latest Citations Bronze file
# =============================================================================

def find_latest_citations_file():
    """
    Find the latest OpenAlex ArXiv Bronze JSONL file.
    """

    files = sorted(
        CITATIONS_DIR.glob(
            "openalex_arxiv_*.jsonl"
        )
    )

    if not files:
        raise FileNotFoundError(
            "No OpenAlex ArXiv Bronze JSONL file found."
        )

    return files[-1]


# =============================================================================
# Load Citations Bronze sample
# =============================================================================

def load_citations_sample():
    """
    Load a sample of the OpenAlex Bronze citation dataset.

    Bronze data is only read here.
    No cleaning or normalization is performed.
    """

    citations_file = (
        find_latest_citations_file()
    )

    print(
        f"Citations Bronze file: {citations_file}"
    )

    records = []

    with open(
        citations_file,
        "r",
        encoding="utf-8",
    ) as file:

        for index, line in enumerate(file):

            if index >= SAMPLE_SIZE:
                break

            records.append(
                json.loads(line)
            )

    df = pd.DataFrame(
        records
    )

    print(
        f"Citation rows loaded for validation: {len(df)}"
    )

    return df


# =============================================================================
# Great Expectations validation
# =============================================================================

def run_citations_expectations():
    """
    Validate the OpenAlex Bronze citation sample.

    The workflow:
    1. Loads a Bronze sample.
    2. Creates / updates the pandas datasource.
    3. Builds the citations expectation suite.
    4. Creates a Checkpoint.
    5. Executes the Checkpoint.
    6. Stores the Validation Result.
    7. Updates the Data Docs.
    """

    # -------------------------------------------------------------------------
    # Load Bronze sample
    # -------------------------------------------------------------------------

    df = load_citations_sample()

    # -------------------------------------------------------------------------
    # Load Great Expectations Data Context
    # -------------------------------------------------------------------------

    context = gx.get_context(
        context_root_dir=GX_CONTEXT_DIR
    )

    # -------------------------------------------------------------------------
    # Datasource
    # -------------------------------------------------------------------------

    datasource = context.sources.add_or_update_pandas(
        name="citations_pandas_source"
    )

    # -------------------------------------------------------------------------
    # Data Asset
    # -------------------------------------------------------------------------

    asset = datasource.add_dataframe_asset(
        name="citations_sample"
    )

    # -------------------------------------------------------------------------
    # Runtime Batch Request
    # -------------------------------------------------------------------------

    batch_request = asset.build_batch_request(
        dataframe=df
    )

    # -------------------------------------------------------------------------
    # Create / update expectation suite
    # -------------------------------------------------------------------------

    context.add_or_update_expectation_suite(
        expectation_suite_name=SUITE_NAME
    )

    # -------------------------------------------------------------------------
    # Validator
    # -------------------------------------------------------------------------

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=SUITE_NAME,
    )

    # =========================================================================
    # Expectations
    # =========================================================================

    # 1. OpenAlex ID must always exist
    validator.expect_column_values_to_not_be_null(
        column="id"
    )

    # 2. OpenAlex ID must be unique
    validator.expect_column_values_to_be_unique(
        column="id"
    )

    # 3. Citation count must always exist
    validator.expect_column_values_to_not_be_null(
        column="cited_by_count"
    )

    # 4. Citation count cannot be negative
    validator.expect_column_values_to_be_between(
        column="cited_by_count",
        min_value=0,
    )

    # 5. Publication year must always exist
    validator.expect_column_values_to_not_be_null(
        column="publication_year"
    )

    # 6. Publication year must remain in the observed valid range
    validator.expect_column_values_to_be_between(
        column="publication_year",
        min_value=1984,
        max_value=2026,
    )

    # -------------------------------------------------------------------------
    # Save expectation suite
    # -------------------------------------------------------------------------

    validator.save_expectation_suite(
        discard_failed_expectations=False
    )

    # =========================================================================
    # Checkpoint
    # =========================================================================

    checkpoint = context.add_or_update_checkpoint(
        name=CHECKPOINT_NAME,
        expectation_suite_name=SUITE_NAME,
        batch_request=batch_request,
        action_list=[
            {
                "name": "store_validation_result",
                "action": {
                    "class_name": "StoreValidationResultAction",
                    "target_store_name": "validations_store",
                },
            },
            {
                "name": "update_data_docs",
                "action": {
                    "class_name": "UpdateDataDocsAction",
                },
            },
        ],
    )

    # -------------------------------------------------------------------------
    # Run checkpoint
    # -------------------------------------------------------------------------

    checkpoint_result = checkpoint.run(
        run_name="citations_validation_run"
    )

    # -------------------------------------------------------------------------
    # Read validation result
    # -------------------------------------------------------------------------

    validation_result = list(
        checkpoint_result.run_results.values()
    )[0]["validation_result"]

    statistics = validation_result.statistics

    # =========================================================================
    # Results
    # =========================================================================

    print()
    print("=" * 70)
    print("OPENALEX CITATIONS GREAT EXPECTATIONS VALIDATION")
    print("=" * 70)

    print(
        f"Success: {validation_result.success}"
    )

    print(
        "Evaluated expectations:",
        statistics.get(
            "evaluated_expectations"
        ),
    )

    print(
        "Successful expectations:",
        statistics.get(
            "successful_expectations"
        ),
    )

    print(
        "Unsuccessful expectations:",
        statistics.get(
            "unsuccessful_expectations"
        ),
    )

    print(
        "Success percent:",
        statistics.get(
            "success_percent"
        ),
    )

    print()
    print(
        "Validation result stored successfully."
    )

    print(
        "Data Docs updated successfully."
    )


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    run_citations_expectations()