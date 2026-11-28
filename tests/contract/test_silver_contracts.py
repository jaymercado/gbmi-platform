"""The contracts of the silver tables against the tables themselves (blueprint 9.6).

``docs/contracts/<table>.yaml`` is what a reader of the lake may rely on. The schema in
``gbmi.lake.tables`` is what the writers produce. The two have to say the same thing, column
for column. The key, the partition and the provenance columns of a table come from the
definition of the table: a GDELT table is partitioned by ``slot_date`` and carries ``_slot_ts``;
an SEC table has its own partition, or none, and no ``_slot_ts`` (week 3 plan 3.3).
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from gbmi.lake.tables import (
    ALL_SCHEMAS,
    ALL_TABLE_KEYS,
    PARTITION_COLUMN,
    PARTITIONS,
    SEC_SCHEMAS,
    SILVER_SCHEMAS,
    provenance_names,
)

CONTRACTS = Path(__file__).resolve().parents[2] / "docs" / "contracts"


def contract(table: str) -> dict[str, object]:
    with (CONTRACTS / f"{table}.yaml").open(encoding="utf-8") as stream:
        loaded = yaml.safe_load(stream)
    assert isinstance(loaded, dict)
    return loaded


def test_every_silver_table_has_a_contract_and_every_contract_a_table() -> None:
    assert {path.stem for path in CONTRACTS.glob("*.yaml")} == set(ALL_SCHEMAS)
    assert set(ALL_SCHEMAS) == set(ALL_TABLE_KEYS) == set(PARTITIONS)
    assert len(SILVER_SCHEMAS) == 4 and len(SEC_SCHEMAS) == 5
    assert not set(SILVER_SCHEMAS) & set(SEC_SCHEMAS)


@pytest.mark.parametrize("table", sorted(ALL_SCHEMAS))
def test_the_contract_lists_the_columns_of_the_table_in_order(table: str) -> None:
    schema = ALL_SCHEMAS[table]

    columns = contract(table)["columns"]

    assert isinstance(columns, list)
    assert [(column["name"], column["type"], column["required"]) for column in columns] == [
        (field.name, str(field.field_type), field.required) for field in schema.fields
    ]


@pytest.mark.parametrize("table", sorted(ALL_SCHEMAS))
def test_the_contract_names_the_key_the_partition_and_a_meaning_for_each_column(
    table: str,
) -> None:
    document = contract(table)
    schema = ALL_SCHEMAS[table]
    partition = PARTITIONS[table]

    assert document["table"] == table
    assert tuple(document["logical_key"]) == ALL_TABLE_KEYS[table]  # type: ignore[arg-type]
    # A table without a partition says so: the key "partition" is present and null.
    assert "partition" in document and document["partition"] == partition
    for text in ("source", "grain", "duplicates", "format"):
        assert isinstance(document[text], str) and len(document[text]) > 10  # type: ignore[arg-type]
    # A key column is always present, and so is the partition column.
    for name in (*ALL_TABLE_KEYS[table], *([partition] if partition else [])):
        assert schema.find_field(name).required, name
    for column in document["columns"]:  # type: ignore[attr-defined]
        assert isinstance(column["meaning"], str) and len(column["meaning"]) > 15, column["name"]
        assert set(column) == {"name", "type", "required", "meaning"}


def test_the_partition_of_each_table() -> None:
    assert {PARTITIONS[table] for table in SILVER_SCHEMAS} == {PARTITION_COLUMN}
    assert {table: PARTITIONS[table] for table in SEC_SCHEMAS} == {
        "sec_company_snapshot": "snapshot_date",
        "sec_company_former_name": None,
        "sec_filing_index": "index_date",
        "sec_filing_detail": None,
        "sec_xbrl_fact": "cik",  # week 3 plan, change C15
    }


def test_every_table_carries_the_provenance_columns_and_a_list_is_never_null() -> None:
    # _slot_ts is the slot of a GDELT file: an SEC table does not carry the column.
    assert provenance_names() == [
        "_source_file",
        "_source_md5",
        "_slot_ts",
        "_ingested_at",
        "_run_id",
        "_pipeline_version",
    ]
    assert provenance_names(slot_ts=False) == [
        "_source_file",
        "_source_md5",
        "_ingested_at",
        "_run_id",
        "_pipeline_version",
    ]
    for table, schema in ALL_SCHEMAS.items():
        provenance = provenance_names(slot_ts=table in SILVER_SCHEMAS)
        names = [field.name for field in schema.fields]
        assert names[-len(provenance) :] == provenance, table
        assert ("_slot_ts" in names) == (table in SILVER_SCHEMAS), table
        assert len(set(names)) == len(names), table
        ids = [field.field_id for field in schema.fields]
        assert ids == list(range(1, len(ids) + 1)), table
        for field in schema.fields:
            assert not (field.name.startswith("_") and field.required), field.name
            if str(field.field_type).startswith("list"):
                # ClickHouse reads a NULL list as an empty array, so NULL is not allowed (SK2).
                assert field.required and field.field_type.element_required, field.name
