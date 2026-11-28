"""The seed of metric concepts (blueprint 8.7; week 3 plan 3.3 and change C13).

dbt reads the seed in WP-08. The recorder of the SEC test files reads the same file: a facts
document is shortened to the concepts of the seed. The rules here keep the file usable for
both readers.
"""

from __future__ import annotations

import csv
from collections import Counter, defaultdict
from pathlib import Path

import yaml

DBT = Path(__file__).resolve().parents[2] / "dbt"
SEED = DBT / "seeds" / "metric_concept_map.csv"
COLUMNS = ["metric", "taxonomy", "concept", "priority", "flow_or_stock"]
#: The seven metrics of the blueprint (8.7), each as a flow or as a stock.
METRICS = {
    "revenue": "flow",
    "net_income": "flow",
    "operating_income": "flow",
    "total_assets": "stock",
    "equity": "stock",
    "cash": "stock",
    "employees": "stock",
}


def rows() -> list[dict[str, str]]:
    with SEED.open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        assert reader.fieldnames == COLUMNS
        return list(reader)


def test_the_seed_lists_candidate_concepts_for_the_seven_metrics() -> None:
    found = rows()

    assert {row["metric"]: row["flow_or_stock"] for row in found} == METRICS
    assert {row["taxonomy"] for row in found} == {"us-gaap", "ifrs-full", "dei"}
    assert all(value == value.strip() and value for row in found for value in row.values())
    # A metric of money has candidates in both accounting vocabularies (finding F7).
    for metric in ("revenue", "net_income", "operating_income", "total_assets", "equity", "cash"):
        taxonomies = {row["taxonomy"] for row in found if row["metric"] == metric}
        assert taxonomies == {"us-gaap", "ifrs-full"}


def test_the_priorities_of_a_metric_and_taxonomy_count_from_1_without_a_gap() -> None:
    priorities: dict[tuple[str, str], list[int]] = defaultdict(list)
    for row in rows():
        priorities[row["metric"], row["taxonomy"]].append(int(row["priority"]))

    for key, values in priorities.items():
        assert values == list(range(1, len(values) + 1)), key


def test_a_concept_belongs_to_one_metric() -> None:
    concepts = Counter((row["taxonomy"], row["concept"]) for row in rows())

    assert max(concepts.values()) == 1


def test_the_dbt_project_gives_the_seed_its_schema_and_its_column_types() -> None:
    project = yaml.safe_load((DBT / "dbt_project.yml").read_text(encoding="utf-8"))

    seeds = project["seeds"]["gbmi"]
    assert seeds["+schema"] == "seeds"
    assert list(seeds["metric_concept_map"]["+column_types"]) == COLUMNS
