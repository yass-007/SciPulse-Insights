import json
from collections import Counter
from pathlib import Path


ARXIV_FILE = Path(
    "/opt/airflow/data/bronze/arxiv/arxiv-metadata-oai-snapshot.json"
)

SAMPLE_SIZE = 10000


def is_missing(value):
    return (
        value is None
        or value == ""
        or value == []
        or value == {}
    )


def analyze_arxiv():
    """
    Profile a sample of the ArXiv Bronze dataset.

    No cleaning or transformation is performed.
    """

    if not ARXIV_FILE.exists():
        raise FileNotFoundError(
            f"ArXiv Bronze file not found: {ARXIV_FILE}"
        )

    total_rows = 0

    field_presence = Counter()
    missing_values = Counter()
    observed_types = {}

    ids = []
    categories = Counter()

    invalid_json_lines = 0

    with open(ARXIV_FILE, "r", encoding="utf-8") as file:

        for line in file:

            if total_rows >= SAMPLE_SIZE:
                break

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                invalid_json_lines += 1
                continue

            total_rows += 1

            for field, value in record.items():

                field_presence[field] += 1

                if is_missing(value):
                    missing_values[field] += 1

                observed_types.setdefault(field, set()).add(
                    type(value).__name__
                )

            if record.get("id"):
                ids.append(record["id"])

            if record.get("categories"):
                for category in record["categories"].split():
                    categories[category] += 1

    duplicate_ids = len(ids) - len(set(ids))

    print("=" * 70)
    print("ARXIV BRONZE QUALITY PROFILING")
    print("=" * 70)

    print(f"\nRows analyzed: {total_rows}")
    print(f"Invalid JSON lines: {invalid_json_lines}")

    print("\nFields detected:")
    for field in sorted(field_presence):
        print(f"- {field}")

    print("\nMissing values:")
    if not missing_values:
        print("- None detected")
    else:
        for field, count in missing_values.most_common():
            percentage = (count / total_rows) * 100
            print(
                f"- {field}: {count} "
                f"({percentage:.2f}%)"
            )

    print("\nObserved types:")
    for field in sorted(observed_types):
        types = ", ".join(sorted(observed_types[field]))
        print(f"- {field}: {types}")

    print(f"\nDuplicate IDs: {duplicate_ids}")

    print("\nTop 15 categories:")
    for category, count in categories.most_common(15):
        print(f"- {category}: {count}")


if __name__ == "__main__":
    analyze_arxiv()