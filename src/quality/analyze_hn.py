"""
Quality profiling for Hacker News Bronze data.

This script analyses the raw Hacker News JSONL files stored in the
Bronze layer. No transformation is performed here.

Checks:
- Number of records
- Invalid JSON lines
- Fields detected
- Missing values
- Observed data types
- Duplicate IDs
- Distribution of Hacker News item types
"""

import json
import glob
from collections import Counter, defaultdict


# Location of Hacker News Bronze files inside the Airflow container
HN_DATA_PATH = "/opt/airflow/data/bronze/hn/*.jsonl"

# Limit the profiling to avoid unnecessary memory consumption
MAX_ROWS = 10_000


def analyze_hn():
    """Profile Hacker News Bronze data."""

    files = glob.glob(HN_DATA_PATH)

    if not files:
        print("No Hacker News Bronze files found.")
        return

    print("=" * 70)
    print("HACKER NEWS BRONZE QUALITY PROFILING")
    print("=" * 70)

    rows = []
    invalid_json = 0

    # ---------------------------------------------------------
    # 1. Read Bronze JSONL files
    # ---------------------------------------------------------

    for file_path in files:
        print(f"Reading: {file_path}")

        with open(file_path, "r", encoding="utf-8") as file:

            for line in file:

                if len(rows) >= MAX_ROWS:
                    break

                try:
                    record = json.loads(line)
                    rows.append(record)

                except json.JSONDecodeError:
                    invalid_json += 1

            if len(rows) >= MAX_ROWS:
                break

    print(f"\nRows analyzed: {len(rows)}")
    print(f"Invalid JSON lines: {invalid_json}")

    if not rows:
        print("No valid Hacker News records found.")
        return

    # ---------------------------------------------------------
    # 2. Detect fields
    # ---------------------------------------------------------

    fields = sorted(
        set().union(*(record.keys() for record in rows))
    )

    print("\nFields detected:")

    for field in fields:
        print(f"- {field}")

    # ---------------------------------------------------------
    # 3. Missing values
    # ---------------------------------------------------------

    missing_values = Counter()

    for field in fields:

        for record in rows:

            if (
                field not in record
                or record[field] is None
                or record[field] == ""
            ):
                missing_values[field] += 1

    print("\nMissing values:")

    missing_found = False

    for field, count in missing_values.most_common():

        if count > 0:

            percentage = count / len(rows) * 100

            print(
                f"- {field}: {count} "
                f"({percentage:.2f}%)"
            )

            missing_found = True

    if not missing_found:
        print("- No missing values detected")

    # ---------------------------------------------------------
    # 4. Detect observed Python types
    # ---------------------------------------------------------

    observed_types = defaultdict(set)

    for record in rows:

        for field in fields:

            if field in record:
                observed_types[field].add(
                    type(record[field]).__name__
                )

    print("\nObserved types:")

    for field in fields:

        types = ", ".join(
            sorted(observed_types[field])
        )

        print(f"- {field}: {types}")

    # ---------------------------------------------------------
    # 5. Detect duplicate Hacker News IDs
    # ---------------------------------------------------------

    ids = [
        record.get("id")
        for record in rows
        if record.get("id") is not None
    ]

    duplicate_ids = len(ids) - len(set(ids))

    print(f"\nDuplicate IDs: {duplicate_ids}")

    # ---------------------------------------------------------
    # 6. Distribution of HN item types
    # ---------------------------------------------------------

    item_types = Counter(
        record.get("type", "missing")
        for record in rows
    )

    print("\nHacker News item types:")

    for item_type, count in item_types.most_common():
        print(f"- {item_type}: {count}")

    print("\n" + "=" * 70)
    print("END OF HACKER NEWS QUALITY PROFILING")
    print("=" * 70)


if __name__ == "__main__":
    analyze_hn()