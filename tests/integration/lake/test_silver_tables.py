"""The four silver tables of GDELT as real Iceberg tables (week 2 plan, WP-05).

The parsers are tested without a lake. This test writes the recorded fixture hour through the
commit helper and reads the rows back, and ClickHouse reads the list columns of
``gdelt_gkg_doc``: a list is declared as required, and the test shows that PyIceberg and
ClickHouse accept the declaration (the fallback of WP-05 is not needed).
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import date

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from clickhouse_connect.driver.client import Client

from gbmi.core.checksum import content_checksum
from gbmi.core.gdelt import Feed
from gbmi.lake.commit import Unit, overwrite_unit
from gbmi.lake.tables import (
    GDELT_GKG_DOC,
    SILVER_SCHEMAS,
    TABLE_KEYS,
    create_table_if_missing,
)
from gbmi.transforms.duckdb.gdelt_event import parse_events
from gbmi.transforms.duckdb.gdelt_gkg import parse_gkg
from gbmi.transforms.duckdb.gdelt_mention import parse_mentions
from gbmi.warehouse.pin import pin_table
from tests.integration.conftest import Lake
from tests.transforms.helpers import INGESTED_AT, recorded

pytestmark = pytest.mark.integration

UNIT = Unit(date(2026, 11, 25), 8)
PARSERS = {Feed.EXPORT: parse_events, Feed.MENTIONS: parse_mentions, Feed.GKG: parse_gkg}


@pytest.fixture(scope="module")
def fixture_hour() -> dict[str, pa.Table]:
    """The four tables of the recorded hour, as the parsers give them."""
    tables: dict[str, pa.Table] = {}
    for feed, parse in PARSERS.items():
        parsed = parse(
            recorded(feed), run_id="integration", pipeline_version="test", ingested_at=INGESTED_AT
        )
        tables.update(parsed.tables)
    return tables


def test_each_silver_table_takes_the_fixture_hour_and_gives_the_same_rows_back(
    local_lake: Lake, fixture_hour: dict[str, pa.Table]
) -> None:
    assert set(fixture_hour) == set(SILVER_SCHEMAS)
    expected_rows = {
        "gdelt_event": 4023,
        "gdelt_mention": 7762,
        "gdelt_gkg_doc": 240,
        "gdelt_gkg_org": fixture_hour["gdelt_gkg_org"].num_rows,
    }
    for name, data in fixture_hour.items():
        table = create_table_if_missing(
            local_lake.catalog, local_lake.namespace, name, SILVER_SCHEMAS[name]
        )

        first = overwrite_unit(table, data, UNIT, run_id="integration")
        stored = table.scan().to_arrow()

        assert stored.num_rows == data.num_rows == expected_rows[name], name
        assert content_checksum(stored) == content_checksum(data), name
        # The key is unique and never NULL in what the table holds.
        keys = set(zip(*(stored[column].to_pylist() for column in TABLE_KEYS[name]), strict=True))
        assert len(keys) == stored.num_rows, name

        # A second commit of the same unit: the same rows, a new snapshot.
        second = overwrite_unit(table, data, UNIT, run_id="integration")
        assert content_checksum(table.scan().to_arrow()) == content_checksum(data), name
        assert second.snapshot_id != first.snapshot_id
        assert second.previous_snapshot_id == first.snapshot_id


def test_a_list_is_stored_as_a_list_and_an_empty_list_stays_empty(
    local_lake: Lake, fixture_hour: dict[str, pa.Table]
) -> None:
    data = fixture_hour[GDELT_GKG_DOC]
    table = create_table_if_missing(
        local_lake.catalog, local_lake.namespace, GDELT_GKG_DOC, SILVER_SCHEMAS[GDELT_GKG_DOC]
    )
    overwrite_unit(table, data, UNIT)

    stored = table.scan().to_arrow().sort_by("gkg_record_id")

    for column in ("themes", "countries"):
        assert stored[column].null_count == 0
        assert stored[column].to_pylist() == data.sort_by("gkg_record_id")[column].to_pylist()
    empty = pc.sum(pc.equal(pc.list_value_length(stored["countries"]), 0)).as_py()
    assert empty > 0  # the hour holds documents without a place


@pytest.fixture
def pinned_name(clickhouse: Client) -> Iterator[str]:
    name = f"silver_tables_{uuid.uuid4().hex[:10]}"
    try:
        yield name
    finally:
        clickhouse.command(f"DROP TABLE IF EXISTS lake.{name}")


def test_clickhouse_reads_the_lists_of_gdelt_gkg_doc(
    shared_lake: Lake, clickhouse: Client, pinned_name: str, fixture_hour: dict[str, pa.Table]
) -> None:
    if shared_lake.backend != "local":
        pytest.skip("the list columns are read on the local backend; S3 reads are spike SK2")
    data = fixture_hour[GDELT_GKG_DOC]
    table = create_table_if_missing(
        shared_lake.catalog,
        shared_lake.track(GDELT_GKG_DOC)[0],
        GDELT_GKG_DOC,
        SILVER_SCHEMAS[GDELT_GKG_DOC],
    )
    overwrite_unit(table, data, UNIT)
    pin_table(clickhouse, shared_lake.catalog.load_table(table.name()), name=pinned_name)

    row = clickhouse.query(
        f"SELECT count(), countIf(empty(countries)), sum(length(themes)), "
        f"sum(length(countries)), toTypeName(themes), sum(org_count), uniqExact(story_id) "
        f"FROM lake.{pinned_name}"
    ).result_rows[0]

    assert row[0] == data.num_rows == 240
    assert row[1] == pc.sum(pc.equal(pc.list_value_length(data["countries"]), 0)).as_py()
    assert row[2] == pc.sum(pc.list_value_length(data["themes"])).as_py()
    assert row[3] == pc.sum(pc.list_value_length(data["countries"])).as_py()
    assert row[4] == "Array(String)"
    assert row[5] == fixture_hour["gdelt_gkg_org"].num_rows
    assert row[6] == pc.count_distinct(data["story_id"]).as_py()
