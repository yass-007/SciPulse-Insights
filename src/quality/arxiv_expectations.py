import json
from pathlib import Path

import pandas as pd
import great_expectations as gx


ARXIV_FILE = Path(
    "/opt/airflow/data/bronze/arxiv/arxiv-metadata-oai-snapshot.json"
)

SAMPLE_SIZE = 10000


def load_arxiv_sample():
    """
    Load a sample of the ArXiv Bronze dataset into a pandas DataFrame.
    No cleaning or transformation is performed.
    """

    records = []

    with open(ARXIV_FILE, "r", encoding="utf-8") as file:
        for index, line in enumerate(file):
            if index >= SAMPLE_SIZE:
                break

            records.append(json.loads(line))

    return pd.DataFrame(records)


def run_arxiv_expectations():
    """
    Run Great Expectations validations on the ArXiv Bronze sample.
    """

    df = load_arxiv_sample()

    context = gx.get_context(
        context_root_dir="/opt/airflow/great_expectations"
    )

    datasource = context.sources.add_or_update_pandas(
        name="arxiv_pandas_source"
    )

    asset = datasource.add_dataframe_asset(
        name="arxiv_sample"
    )

    batch_request = asset.build_batch_request(
        dataframe=df
    )

    suite_name = "arxiv_bronze_suite"

    context.add_or_update_expectation_suite(
        expectation_suite_name=suite_name
    )

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )

    # -------------------------------------------------------------------------
    # Expectations based on Bronze profiling
    # -------------------------------------------------------------------------

    validator.expect_column_values_to_not_be_null(
        column="id"
    )

    validator.expect_column_values_to_be_unique(
        column="id"
    )

    validator.expect_column_values_to_not_be_null(
        column="title"
    )

    validator.expect_column_values_to_not_be_null(
        column="abstract"
    )

    validator.expect_column_values_to_not_be_null(
        column="categories"
    )

    validator.expect_column_values_to_not_be_null(
        column="update_date"
    )

    validator.expect_column_values_to_match_strftime_format(
        column="update_date",
        strftime_format="%Y-%m-%d",
    )

    validator.save_expectation_suite(
        discard_failed_expectations=False
    )

    validation_result = validator.validate()

    print("=" * 70)
    print("ARXIV GREAT EXPECTATIONS VALIDATION")
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
    run_arxiv_expectations()