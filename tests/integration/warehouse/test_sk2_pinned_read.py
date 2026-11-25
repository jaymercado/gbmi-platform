"""Spike SK2 - ClickHouse reads a pinned Iceberg table (week 1 plan, WP-07; risk R1).

Can a mart build read exactly one Iceberg metadata version (blueprint 5.12)? The tests write a
table with PyIceberg, pin it in ClickHouse with ``gbmi.warehouse.pin`` and compare what
ClickHouse returns with a PyIceberg scan of the same snapshot, row for row.

They need ClickHouse and a lake that it can read, so they run in the toolbox container
(``make integration``) and are skipped elsewhere. With GBMI_TEST_AWS=1 the same tests run
against S3 with short-lived credentials (question Q6).
"""

from __future__ import annotations

import os
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, date, datetime
from typing import Any

import pytest
from clickhouse_connect.driver.client import Client
from pyiceberg.table import Table

from gbmi.lake.commit import Unit, overwrite_day_streaming, overwrite_unit
from gbmi.warehouse.pin import S3Credentials, pin_table
from tests.integration.conftest import Lake
from tests.integration.lake import sk1_table as sk1
from tests.integration.lake.sk1_table import DAY_1, DAY_2

pytestmark = pytest.mark.integration

COLUMNS = ("id", "slot_date", "slot_hour", "event_ts", "score", "tags", "_source_file", "_slot_ts")
Row = tuple[Any, ...]


def note(lake: Lake, question: str, text: str) -> None:
    """An observation for the spike report (shown with pytest -s)."""
    print(f"\nSK2 {question} [{lake.backend}] {text}")


def _normalise(row: dict[str, Any]) -> Row:
    """One row in a form that is the same whether it came from PyIceberg or from ClickHouse."""

    def instant(value: datetime | None) -> int | None:
        if value is None:
            return None
        aware = value if value.tzinfo else value.replace(tzinfo=UTC)
        epoch = datetime(1970, 1, 1, tzinfo=UTC)
        delta = aware - epoch
        return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds

    score = row["score"]
    slot_date: date = row["slot_date"]
    return (
        row["id"],
        slot_date.isoformat(),
        row["slot_hour"],
        instant(row["event_ts"]),
        None if score is None else round(score, 9),
        # An Iceberg list that is NULL arrives in ClickHouse as an empty array (see Q3).
        tuple(row["tags"] or ()),
        row["_source_file"],
        instant(row["_slot_ts"]),
    )


def iceberg_rows(table: Table, snapshot_id: int | None = None) -> list[Row]:
    scan = table.scan(selected_fields=COLUMNS, snapshot_id=snapshot_id)
    return sorted(_normalise(row) for row in scan.to_arrow().to_pylist())


def clickhouse_rows(client: Client, name: str, settings: str = "") -> list[Row]:
    result = client.query(f"SELECT {', '.join(COLUMNS)} FROM lake.{name} {settings}")
    return sorted(_normalise(dict(zip(COLUMNS, row, strict=True))) for row in result.result_rows)


def credentials_for(lake: Lake) -> S3Credentials | None:
    if lake.backend != "aws":
        return None
    return S3Credentials(
        os.environ["AWS_ACCESS_KEY_ID"],
        os.environ["AWS_SECRET_ACCESS_KEY"],
        os.environ.get("AWS_SESSION_TOKEN"),
    )


@pytest.fixture
def pinned_name(clickhouse: Client) -> Iterator[str]:
    """A unique name in the ``lake`` database, dropped when the test ends."""
    name = f"sk2_{uuid.uuid4().hex[:10]}"
    try:
        yield name
    finally:
        clickhouse.command(f"DROP TABLE IF EXISTS lake.{name}")


def three_commits(lake: Lake, name: str) -> tuple[Table, list[tuple[str, int, list[Row]]]]:
    """A table with three commits; for each, its metadata file, snapshot id and rows."""
    table = sk1.create(lake, name)
    states: list[tuple[str, int, list[Row]]] = []
    for data, unit in (
        (sk1.rows(DAY_1, 8), Unit(DAY_1, 8)),
        (sk1.rows(DAY_1, 9), Unit(DAY_1, 9)),
        (sk1.rows(DAY_1, 8, count=3, version=2), Unit(DAY_1, 8)),
    ):
        result = overwrite_unit(table, data, unit)
        assert result.snapshot_id is not None
        states.append((table.metadata_location, result.snapshot_id, iceberg_rows(table)))
    return table, states


# --- Q1 ------------------------------------------------------------------------------------


def test_q1_engine_level_pin_reads_exactly_the_named_metadata_file(
    shared_lake: Lake, clickhouse: Client, pinned_name: str
) -> None:
    table, states = three_commits(shared_lake, "sk2_q1")
    (first_metadata, _, first_rows), _, (_, latest_snapshot, latest_rows) = states
    credentials = credentials_for(shared_lake)

    started = time.perf_counter()
    pinned = pin_table(clickhouse, table, name=pinned_name, credentials=credentials)
    from_clickhouse = clickhouse_rows(clickhouse, pinned_name)
    elapsed = time.perf_counter() - started

    # The pinned table returns exactly the rows of the catalog's current snapshot ...
    assert pinned.snapshot_id == latest_snapshot
    assert from_clickhouse == latest_rows
    assert len(latest_rows) == 8
    # ... and pinning an older metadata file returns the older rows.
    pin_table(
        clickhouse,
        table,
        name=pinned_name,
        metadata_location=first_metadata,
        credentials=credentials,
    )
    assert clickhouse_rows(clickhouse, pinned_name) == first_rows
    assert first_rows != latest_rows
    note(
        shared_lake, "Q1", f"pin and first read of {len(latest_rows)} rows: {elapsed * 1000:.0f} ms"
    )


# --- Q2 ------------------------------------------------------------------------------------


def test_q2_pin_holds_until_the_table_is_pinned_again(
    shared_lake: Lake, clickhouse: Client, pinned_name: str
) -> None:
    table, states = three_commits(shared_lake, "sk2_q2")
    credentials = credentials_for(shared_lake)
    pin_table(clickhouse, table, name=pinned_name, credentials=credentials)
    pinned_rows = clickhouse_rows(clickhouse, pinned_name)
    assert pinned_rows == states[-1][2]

    # New commits to the lake table: one more hour, and one hour replaced.
    overwrite_unit(table, sk1.rows(DAY_1, 10), Unit(DAY_1, 10))
    overwrite_unit(table, sk1.rows(DAY_1, 9, count=1, version=5), Unit(DAY_1, 9))
    current_rows = iceberg_rows(table)
    assert current_rows != pinned_rows

    # The pinned table does not move, however often it is read.
    assert clickhouse_rows(clickhouse, pinned_name) == pinned_rows
    assert clickhouse_rows(clickhouse, pinned_name) == pinned_rows

    # Pinning again takes effect at once, although the metadata cache is on by default.
    reloaded = shared_lake.catalog.load_table(table.name())
    pin_table(clickhouse, reloaded, name=pinned_name, credentials=credentials)
    assert clickhouse_rows(clickhouse, pinned_name) == current_rows
    note(
        shared_lake,
        "Q2",
        "pin held across two later commits; re-pinning showed the new rows at once",
    )


# --- Q3 ------------------------------------------------------------------------------------


def test_q3_types_are_mapped_as_documented(
    shared_lake: Lake, clickhouse: Client, pinned_name: str
) -> None:
    table, _ = three_commits(shared_lake, "sk2_q3")
    pin_table(clickhouse, table, name=pinned_name, credentials=credentials_for(shared_lake))

    described = clickhouse.query(f"DESCRIBE TABLE lake.{pinned_name}").result_rows
    types = {row[0]: row[1] for row in described}

    # Required Iceberg fields are plain types; optional fields are Nullable.
    assert types["slot_date"] == "Date32"
    assert types["slot_hour"] == "Int32"
    assert types["id"] == "Int64"
    assert types["event_ts"] == "Nullable(DateTime64(6, 'UTC'))"
    assert types["_slot_ts"] == "Nullable(DateTime64(6, 'UTC'))"
    assert types["score"] == "Nullable(Float64)"
    assert types["_source_file"] == "Nullable(String)"
    # A list is an Array, which cannot itself be NULL in ClickHouse.
    assert types["tags"] == "Array(Nullable(String))"

    # So an Iceberg list that is NULL reads as an empty array, while a NULL double stays NULL.
    nulls = clickhouse.query(
        f"SELECT countIf(score IS NULL), countIf(empty(tags)) FROM lake.{pinned_name}"
    ).result_rows[0]
    in_iceberg = table.scan(selected_fields=("score", "tags")).to_arrow().to_pylist()
    assert nulls[0] == sum(row["score"] is None for row in in_iceberg) > 0
    assert nulls[1] == sum(row["tags"] is None for row in in_iceberg) > 0
    note(shared_lake, "Q3", "; ".join(f"{name} {kind}" for name, kind in types.items()))


# --- Q4 ------------------------------------------------------------------------------------


def test_q4_pipeline_user_pins_and_dbt_user_reads(
    shared_lake: Lake, clickhouse: Client, clickhouse_dbt: Client, pinned_name: str
) -> None:
    """The gbmi user (role pipeline_rw) creates the pinned table; the dbt user only reads it."""
    table, states = three_commits(shared_lake, "sk2_q4")
    pinned = pin_table(
        clickhouse, table, name=pinned_name, credentials=credentials_for(shared_lake)
    )

    statement = clickhouse.command(f"SHOW CREATE TABLE lake.{pinned_name}")
    assert "iceberg_metadata_file_path" in str(statement)
    assert pinned.metadata_file in str(statement)
    if shared_lake.backend == "local":
        # The lake mount is read as mounted, with no extra server setting (finding F7).
        assert "IcebergLocal" in str(statement)
        assert pinned.table_location.startswith("file:///lake/")
    assert clickhouse_rows(clickhouse_dbt, pinned_name) == states[-1][2]
    with pytest.raises(Exception, match=r"ACCESS_DENIED|Not enough privileges"):
        clickhouse_dbt.command(f"DROP TABLE lake.{pinned_name}")
    note(shared_lake, "Q4", str(statement).replace("\\n", " ")[:300])


# --- Q5 ------------------------------------------------------------------------------------


def test_q5_view_over_a_pinned_table_survives_repinning(
    shared_lake: Lake, clickhouse: Client, clickhouse_dbt: Client, pinned_name: str
) -> None:
    table, states = three_commits(shared_lake, "sk2_q5")
    (first_metadata, _, first_rows), _, (_, _, latest_rows) = states
    credentials = credentials_for(shared_lake)
    pin_table(
        clickhouse,
        table,
        name=pinned_name,
        metadata_location=first_metadata,
        credentials=credentials,
    )
    view = f"staging.v_{pinned_name}"
    clickhouse_dbt.command(f"CREATE VIEW {view} AS SELECT id, slot_hour FROM lake.{pinned_name}")
    try:
        through_view = clickhouse_dbt.query(f"SELECT id FROM {view} ORDER BY id").result_rows
        assert [row[0] for row in through_view] == [row[0] for row in first_rows]

        # Re-pin the table; the view is not touched.
        pin_table(clickhouse, table, name=pinned_name, credentials=credentials)

        through_view = clickhouse_dbt.query(f"SELECT id FROM {view} ORDER BY id").result_rows
        assert [row[0] for row in through_view] == [row[0] for row in latest_rows]
    finally:
        clickhouse_dbt.command(f"DROP VIEW IF EXISTS {view}")
    note(
        shared_lake,
        "Q5",
        "a plain view over the pinned table returned the new rows after re-pinning",
    )


# --- Cross-check with SK1 --------------------------------------------------------------------


def test_day_written_with_add_files_reads_like_any_other(
    shared_lake: Lake, clickhouse: Client, pinned_name: str
) -> None:
    """SK1 chose to stream a day into one file and add it; ClickHouse must read that file too."""
    table = sk1.create(shared_lake, "sk2_streamed")
    overwrite_unit(table, sk1.rows(DAY_2, 8), Unit(DAY_2, 8))
    overwrite_day_streaming(table, DAY_1, (sk1.rows(DAY_1, hour) for hour in range(6)))
    # An hourly overwrite on top rewrites the day's file (snapshots: overwrite, append).
    overwrite_unit(table, sk1.rows(DAY_1, 3, count=2, version=2), Unit(DAY_1, 3))

    pin_table(clickhouse, table, name=pinned_name, credentials=credentials_for(shared_lake))

    expected = iceberg_rows(table)
    assert clickhouse_rows(clickhouse, pinned_name) == expected
    assert len(expected) == 5 + 6 * 5 - 5 + 2
    note(
        shared_lake,
        "SK1/Q5b",
        f"ClickHouse read {len(expected)} rows, including a day added with add_files",
    )
