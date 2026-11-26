"""The contracts of the silver tables against the tables themselves (blueprint 9.6).

``docs/contracts/<table>.yaml`` is what a reader of the lake may rely on. The schema in
``gbmi.lake.tables`` is what the writers produce. The two have to say the same thing, column
for column.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from gbmi.lake.tables import PARTITION_COLUMN, SILVER_SCHEMAS, TABLE_KEYS

CONTRACTS = Path(__file__).resolve().parents[2] / "docs" / "contracts"


def contract(table: str) -> dict[str, object]:
    with (CONTRACTS / f"{table}.yaml").open(encoding="utf-8") as stream:
        loaded = yaml.safe_load(stream)
    assert isinstance(loaded, dict)
    return loaded


def test_every_silver_table_has_a_contract_and_every_contract_a_table() -> None:
    assert {path.stem for path in CONTRACTS.glob("*.yaml")} == set(SILVER_SCHEMAS)


@pytest.mark.parametrize("table", sorted(SILVER_SCHEMAS))
def test_the_contract_lists_the_columns_of_the_table_in_order(table: str) -> None:
    schema = SILVER_SCHEMAS[table]

    columns = contract(table)["columns"]

    assert isinstance(columns, list)
    assert [(column["name"], column["type"], column["required"]) for column in columns] == [
        (field.name, str(field.field_type), field.required) for field in schema.fields
    ]


@pytest.mark.parametrize("table", sorted(SILVER_SCHEMAS))
def test_the_contract_names_the_key_the_partition_and_a_meaning_for_each_column(
    table: str,
) -> None:
    document = contract(table)
    schema = SILVER_SCHEMAS[table]

    assert document["table"] == table
    assert tuple(document["logical_key"]) == TABLE_KEYS[table]  # type: ignore[arg-type]
    assert document["partition"] == PARTITION_COLUMN
    for text in ("source", "grain", "duplicates", "format"):
        assert isinstance(document[text], str) and len(document[text]) > 10  # type: ignore[arg-type]
    # A key column is always present, and so is the partition column.
    for name in (*TABLE_KEYS[table], PARTITION_COLUMN):
        assert schema.find_field(name).required, name
    for column in document["columns"]:  # type: ignore[attr-defined]
        assert isinstance(column["meaning"], str) and len(column["meaning"]) > 15, column["name"]
        assert set(column) == {"name", "type", "required", "meaning"}


def test_every_table_carries_the_provenance_columns_and_a_list_is_never_null() -> None:
    provenance = [
        "_source_file",
        "_source_md5",
        "_slot_ts",
        "_ingested_at",
        "_run_id",
        "_pipeline_version",
    ]
    for table, schema in SILVER_SCHEMAS.items():
        names = [field.name for field in schema.fields]
        assert names[-6:] == provenance, table
        assert len(set(names)) == len(names), table
        ids = [field.field_id for field in schema.fields]
        assert ids == list(range(1, len(ids) + 1)), table
        for field in schema.fields:
            if str(field.field_type).startswith("list"):
                # ClickHouse reads a NULL list as an empty array, so NULL is not allowed (SK2).
                assert field.required and field.field_type.element_required, field.name
