import json
from pathlib import Path

import pandas as pd
import great_expectations as gx


# =============================================================================
# Configuration
# =============================================================================

ARXIV_FILE = Path(
    "/opt/airflow/data/bronze/arxiv/"
    "arxiv-metadata-oai-snapshot.json"
)

SAMPLE_SIZE = 10000

GX_CONTEXT_DIR = "/opt/airflow/great_expectations"

SUITE_NAME = "arxiv_bronze_suite"

CHECKPOINT_NAME = "arxiv_bronze_checkpoint"


# =============================================================================
# Load ArXiv Bronze sample
# =============================================================================

def load_arxiv_sample():
    """
    Load a sample of the ArXiv Bronze dataset into a pandas DataFrame.

    The Bronze source is only read here.
    No cleaning or transformation is performed.
    """

    records = []

    with open(
        ARXIV_FILE,
        "r",
        encoding="utf-8",
    ) as file:

        for index, line in enumerate(file):

            if index >= SAMPLE_SIZE:
                break

            records.append(
                json.loads(line)
            )

    df = pd.DataFrame(records)

    print(
        f"ArXiv rows loaded for validation: {len(df)}"
    )

    return df


# =============================================================================
# Great Expectations validation
# =============================================================================

def run_arxiv_expectations():
    """
    Validate the ArXiv Bronze sample with Great Expectations.

    The workflow:
    1. Creates / updates the pandas datasource.
    2. Builds the ArXiv expectation suite.
    3. Creates a Checkpoint.
    4. Executes the Checkpoint.
    5. Stores the Validation Result.
    6. Updates the Data Docs.
    """

    # -------------------------------------------------------------------------
    # Load Bronze sample
    # -------------------------------------------------------------------------

    df = load_arxiv_sample()

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
        name="arxiv_pandas_source"
    )

    # -------------------------------------------------------------------------
    # Data Asset
    # -------------------------------------------------------------------------

    asset = datasource.add_dataframe_asset(
        name="arxiv_sample"
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

    # 1. ArXiv ID must always exist
    validator.expect_column_values_to_not_be_null(
        column="id"
    )

    # 2. ArXiv ID must be unique
    validator.expect_column_values_to_be_unique(
        column="id"
    )

    # 3. Title must exist
    validator.expect_column_values_to_not_be_null(
        column="title"
    )

    # 4. Abstract must exist
    validator.expect_column_values_to_not_be_null(
        column="abstract"
    )

    # 5. Categories must exist
    validator.expect_column_values_to_not_be_null(
        column="categories"
    )

    # 6. Update date must exist
    validator.expect_column_values_to_not_be_null(
        column="update_date"
    )

    # 7. Update date must follow YYYY-MM-DD format
    validator.expect_column_values_to_match_strftime_format(
        column="update_date",
        strftime_format="%Y-%m-%d",
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
    #
    # StoreValidationResultAction:
    #     persists the result in the GX Validations Store.
    #
    # UpdateDataDocsAction:
    #     updates the generated Data Docs after the validation.
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
        run_name="arxiv_validation_run"
    )

    # -------------------------------------------------------------------------
    # Extract Validation Result
    # -------------------------------------------------------------------------

    run_results = list(
        checkpoint_result.run_results.values()
    )

    if not run_results:
        raise RuntimeError(
            "No Great Expectations validation result was produced."
        )

    validation_result = (
        run_results[0]["validation_result"]
    )

    # =========================================================================
    # Console summary
    # =========================================================================

    print()
    print("=" * 70)
    print("ARXIV GREAT EXPECTATIONS VALIDATION")
    print("=" * 70)

    print(
        f"Success: "
        f"{validation_result.success}"
    )

    print(
        "Evaluated expectations: "
        f"{validation_result.statistics['evaluated_expectations']}"
    )

    print(
        "Successful expectations: "
        f"{validation_result.statistics['successful_expectations']}"
    )

    print(
        "Unsuccessful expectations: "
        f"{validation_result.statistics['unsuccessful_expectations']}"
    )

    print(
        "Success percent: "
        f"{validation_result.statistics['success_percent']:.2f}%"
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
    run_arxiv_expectations()