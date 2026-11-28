"""The pin step of marts_build: several tables at once, a missing table created empty first.

The first mart build of a new lake runs before any SEC pipeline has written a table. The pin
step then creates each missing table without a row, and ClickHouse has to read such a table:
an Iceberg table without a snapshot (week 3 plan, WP-08 step 8).
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator

import pytest
from clickhouse_connect.driver.client import Client

from gbmi.lake.tables import SEC_COMPANY_SNAPSHOT, SEC_SCHEMAS, SEC_XBRL_FACT
from gbmi.warehouse.pin import pin_silver_tables
from tests.integration.conftest import SHARED_LAKE, Lake

pytestmark = pytest.mark.integration


@pytest.fixture
def shared(local_lake: Lake) -> Lake:
    if not local_lake.settings.lake_uri.startswith(f"file://{SHARED_LAKE}/"):
        pytest.skip("needs the lake mount of the Compose stack (run with make integration)")
    return local_lake


@pytest.fixture
def pinned_names(clickhouse: Client) -> Iterator[dict[str, str]]:
    names = {name: f"{name}_{uuid.uuid4().hex[:8]}" for name in SEC_SCHEMAS}
    try:
        yield names
    finally:
        for name in names.values():
            clickhouse.command(f"DROP TABLE IF EXISTS lake.{name}")


def test_a_missing_table_is_created_empty_and_clickhouse_reads_no_row(
    shared: Lake, clickhouse: Client, pinned_names: dict[str, str]
) -> None:
    names = list(SEC_SCHEMAS)

    first = pin_silver_tables(
        clickhouse, shared.catalog, shared.namespace, names, pinned_names=pinned_names
    )

    assert list(first) == names
    for name, entry in first.items():
        assert entry["created"] is True, name
        assert (entry["rows"], entry["snapshot_id"]) == (0, None), name
        assert entry["table"] == f"lake.{pinned_names[name]}"
        # ClickHouse reads the columns of the schema from a table without a snapshot.
        columns = [
            row[0]
            for row in clickhouse.query(f"DESCRIBE TABLE lake.{pinned_names[name]}").result_rows
        ]
        assert columns == [field.name for field in SEC_SCHEMAS[name].fields], name
    # The created table has the partition of the pipelines: a pipeline which runs later
    # loads the same table and does not create another one.
    facts = shared.catalog.load_table((shared.namespace, SEC_XBRL_FACT))
    snapshots = shared.catalog.load_table((shared.namespace, SEC_COMPANY_SNAPSHOT))
    assert [field.name for field in facts.spec().fields] == ["cik"]
    assert [field.name for field in snapshots.spec().fields] == ["snapshot_date"]

    again = pin_silver_tables(
        clickhouse, shared.catalog, shared.namespace, names, pinned_names=pinned_names
    )

    assert [entry["created"] for entry in again.values()] == [False] * len(names)
    assert {name: entry["metadata_file"] for name, entry in again.items()} == {
        name: entry["metadata_file"] for name, entry in first.items()
    }


def test_a_name_which_is_no_silver_table_is_refused(
    shared: Lake, clickhouse: Client, pinned_names: dict[str, str]
) -> None:
    with pytest.raises(ValueError, match="no silver table is named sec_typo"):
        pin_silver_tables(
            clickhouse,
            shared.catalog,
            shared.namespace,
            [SEC_XBRL_FACT, "sec_typo"],
            pinned_names=pinned_names,
        )

    # The check comes first: no table was created and none was pinned.
    assert not shared.catalog.table_exists((shared.namespace, SEC_XBRL_FACT))
