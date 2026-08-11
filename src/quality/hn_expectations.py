import glob
import json

import pandas as pd
import great_expectations as gx


HN_DATA_PATH = "/opt/airflow/data/bronze/hn/*.jsonl"

SAMPLE_SIZE = 10000


def load_hn_sample():
    """
    Load Hacker News Bronze micro-batches into a pandas DataFrame.

    No cleaning or deduplication is performed here.
    """

    records = []

    files = glob.glob(HN_DATA_PATH)

    for file_path in files:
        with open(file_path, "r", encoding="utf-8") as file:
            for line in file:
                if len(records) >= SAMPLE_SIZE:
                    break

                records.append(json.loads(line))

        if len(records) >= SAMPLE_SIZE:
            break

    return pd.DataFrame(records)


def run_hn_expectations():
    """
    Run Great Expectations validations on Hacker News Bronze data.
    """

    df = load_hn_sample()

    context = gx.get_context(
        context_root_dir="/opt/airflow/great_expectations"
    )

    datasource = context.sources.add_or_update_pandas(
        name="hn_pandas_source"
    )

    asset = datasource.add_dataframe_asset(
        name="hn_sample"
    )

    batch_request = asset.build_batch_request(
        dataframe=df
    )

    suite_name = "hn_bronze_suite"

    context.add_or_update_expectation_suite(
        expectation_suite_name=suite_name
    )

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )

    # -------------------------------------------------------------------------
    # Expectations based on Hacker News Bronze profiling
    # -------------------------------------------------------------------------

    validator.expect_column_values_to_not_be_null(
        column="id"
    )

    validator.expect_column_values_to_be_of_type(
        column="id",
        type_="int64",
    )

    validator.expect_column_values_to_not_be_null(
        column="title"
    )

    validator.expect_column_values_to_not_be_null(
        column="time"
    )

    validator.expect_column_values_to_be_of_type(
        column="time",
        type_="int64",
    )

    validator.expect_column_values_to_not_be_null(
        column="type"
    )

    validator.expect_column_values_to_be_in_set(
        column="type",
        value_set=["story"],
    )

    validator.expect_column_values_to_not_be_null(
        column="by"
    )

    validator.expect_column_values_to_not_be_null(
        column="score"
    )

    validator.save_expectation_suite(
        discard_failed_expectations=False
    )

    validation_result = validator.validate()

    print("=" * 70)
    print("HACKER NEWS GREAT EXPECTATIONS VALIDATION")
    print("=" * 70)

    print(
        f"Success: {validation_result.success}"
    )

    print(
        f"Evaluated expectations: "
        f"{validation_result.statistics['evaluated_expectations']}"
    )

    print(
        f"Successful expectations: "
        f"{validation_result.statistics['successful_expectations']}"
    )

    print(
        f"Unsuccessful expectations: "
        f"{validation_result.statistics['unsuccessful_expectations']}"
    )

    print(
        f"Success percent: "
        f"{validation_result.statistics['success_percent']:.2f}%"
    )

    context.build_data_docs()

    print("\nData Docs generated successfully.")


if __name__ == "__main__":
    run_hn_expectations()