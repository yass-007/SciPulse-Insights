import glob
import json

import pandas as pd
import great_expectations as gx


# =============================================================================
# Configuration
# =============================================================================

HN_DATA_PATH = "/opt/airflow/data/bronze/hn/*.jsonl"

SAMPLE_SIZE = 10000

GX_CONTEXT_DIR = "/opt/airflow/great_expectations"

SUITE_NAME = "hn_bronze_suite"

CHECKPOINT_NAME = "hn_bronze_checkpoint"


# =============================================================================
# Load Hacker News Bronze sample
# =============================================================================

def load_hn_sample():
    """
    Load Hacker News Bronze micro-batches into a pandas DataFrame.

    No cleaning or deduplication is performed here.
    """

    records = []

    files = sorted(
        glob.glob(HN_DATA_PATH)
    )

    if not files:
        raise FileNotFoundError(
            "No Hacker News Bronze files were found."
        )

    for file_path in files:

        with open(
            file_path,
            "r",
            encoding="utf-8",
        ) as file:

            for line in file:

                if len(records) >= SAMPLE_SIZE:
                    break

                try:
                    records.append(
                        json.loads(line)
                    )

                except json.JSONDecodeError:
                    continue

        if len(records) >= SAMPLE_SIZE:
            break

    df = pd.DataFrame(records)

    print(
        f"Hacker News rows loaded for validation: {len(df)}"
    )

    return df


# =============================================================================
# Great Expectations validation
# =============================================================================

def run_hn_expectations():
    """
    Validate the Hacker News Bronze sample with Great Expectations.

    Workflow:
    1. Load Bronze data.
    2. Create/update the pandas datasource.
    3. Create/update the expectation suite.
    4. Execute the expectations.
    5. Run a Checkpoint.
    6. Persist the validation result.
    7. Update the Data Docs.
    """

    # -------------------------------------------------------------------------
    # Load Bronze sample
    # -------------------------------------------------------------------------

    df = load_hn_sample()

    # -------------------------------------------------------------------------
    # Great Expectations context
    # -------------------------------------------------------------------------

    context = gx.get_context(
        context_root_dir=GX_CONTEXT_DIR
    )

    # -------------------------------------------------------------------------
    # Datasource
    # -------------------------------------------------------------------------

    datasource = context.sources.add_or_update_pandas(
        name="hn_pandas_source"
    )

    # -------------------------------------------------------------------------
    # Data Asset
    # -------------------------------------------------------------------------

    asset = datasource.add_dataframe_asset(
        name="hn_sample"
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
    # Expectations based on Hacker News Bronze profiling
    # =========================================================================

    # 1. ID must exist
    validator.expect_column_values_to_not_be_null(
        column="id"
    )

    # 2. ID must be an integer
    validator.expect_column_values_to_be_of_type(
        column="id",
        type_="int64",
    )

    # 3. Title must exist
    validator.expect_column_values_to_not_be_null(
        column="title"
    )

    # 4. Time must exist
    validator.expect_column_values_to_not_be_null(
        column="time"
    )

    # 5. Time must be an integer Unix timestamp
    validator.expect_column_values_to_be_of_type(
        column="time",
        type_="int64",
    )

    # 6. Type must exist
    validator.expect_column_values_to_not_be_null(
        column="type"
    )

    # 7. This Bronze pipeline ingests stories
    validator.expect_column_values_to_be_in_set(
        column="type",
        value_set=["story"],
    )

    # 8. Author must exist
    validator.expect_column_values_to_not_be_null(
        column="by"
    )

    # 9. Score must exist
    validator.expect_column_values_to_not_be_null(
        column="score"
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
        run_name="hn_validation_run"
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
    print("HACKER NEWS GREAT EXPECTATIONS VALIDATION")
    print("=" * 70)

    print(
        f"Success: {validation_result.success}"
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
    run_hn_expectations()