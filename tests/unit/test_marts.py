"""The check after a mart build: the five SEC marts come from the pinned tables of the run."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from gbmi.lake.tables import SEC_SCHEMAS
from gbmi.warehouse.marts import SEC_MARTS, build_problems

MODELS = Path(__file__).resolve().parents[2] / "dbt" / "models" / "marts"
SNAPSHOTS = dict(zip(SEC_SCHEMAS, range(11, 16), strict=True))
VERSION = "0123456789abcdef0123456789abcdef01234567"


def pinned() -> dict[str, Any]:
    return {
        "tables": {name: {"rows": 7, "created": False} for name in SEC_SCHEMAS},
        "snapshot_ids": dict(SNAPSHOTS),
        "pipeline_version": VERSION,
    }


def states(**changes: dict[str, Any]) -> dict[str, dict[str, Any]]:
    sound = {
        mart: {
            "rows": 7,
            "silver_snapshot_ids": [SNAPSHOTS[silver]],
            "pipeline_versions": [VERSION],
        }
        for mart, silver in SEC_MARTS.items()
    }
    return {mart: {**state, **changes.get(mart, {})} for mart, state in sound.items()}


def test_a_build_from_the_pinned_tables_has_no_problem() -> None:
    assert build_problems(states(), pinned()) == []


def test_a_mart_from_another_snapshot_is_a_problem() -> None:
    problems = build_problems(states(dim_company={"silver_snapshot_ids": [99]}), pinned())

    assert problems == [
        "dim_company was built from the snapshots [99] of sec_company_snapshot, but 11 was pinned"
    ]


def test_a_mart_with_rows_of_two_builds_is_a_problem() -> None:
    problems = build_problems(states(fct_filing={"silver_snapshot_ids": [13, 14]}), pinned())

    assert len(problems) == 1
    assert problems[0].startswith("fct_filing was built from the snapshots [13, 14]")


def test_a_mart_of_another_version_of_the_code_is_a_problem() -> None:
    problems = build_problems(states(fct_filing_item={"pipeline_versions": ["unknown"]}), pinned())

    assert problems == [
        f"fct_filing_item carries the pipeline versions ['unknown'], but the run is {VERSION}"
    ]


def test_an_empty_mart_carries_no_provenance_and_passes() -> None:
    empty = {"rows": 0, "silver_snapshot_ids": [], "pipeline_versions": []}

    assert build_problems(states(**dict.fromkeys(SEC_MARTS, empty)), pinned()) == []


def test_a_table_without_a_snapshot_is_pinned_as_zero() -> None:
    # An empty table which no pipeline has filled: the models write 0 into the marts.
    never_filled = {**pinned(), "snapshot_ids": {**SNAPSHOTS, "sec_filing_index": 0}}

    assert build_problems(states(fct_filing={"silver_snapshot_ids": [0]}), never_filled) == []


def test_each_mart_names_the_same_silver_table_in_its_model() -> None:
    # The model writes the snapshot of one silver table, and the check reads the same table.
    assert set(SEC_MARTS.values()) <= set(SEC_SCHEMAS)
    for mart, silver in SEC_MARTS.items():
        (model,) = MODELS.glob(f"*/{mart}.sql")
        named = re.findall(r"mart_provenance\('(\w+)'\)", model.read_text(encoding="utf-8"))
        assert named == [silver], mart
