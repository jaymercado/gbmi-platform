"""The table and the rows used by spikes SK1 and SK2 (week 1 plan, WP-06).

The table has one column of each type the silver tables need: a date used as the identity
partition, an hour, an integer key, a timestamp with time zone, a double, a list of strings
and the provenance columns of blueprint 7.12.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import (
    DateType,
    DoubleType,
    IntegerType,
    ListType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

from gbmi.lake.tables import create_table_if_missing
from tests.integration.conftest import Lake

SCHEMA = Schema(
    NestedField(1, "slot_date", DateType(), required=True),
    NestedField(2, "slot_hour", IntegerType(), required=True),
    NestedField(3, "id", LongType(), required=True),
    NestedField(4, "event_ts", TimestamptzType(), required=False),
    NestedField(5, "score", DoubleType(), required=False),
    NestedField(
        6,
        "tags",
        ListType(element_id=7, element_type=StringType(), element_required=False),
        required=False,
    ),
    NestedField(8, "_source_file", StringType(), required=False),
    NestedField(9, "_source_md5", StringType(), required=False),
    NestedField(10, "_slot_ts", TimestamptzType(), required=False),
    NestedField(11, "_ingested_at", TimestamptzType(), required=False),
    NestedField(12, "_run_id", StringType(), required=False),
    NestedField(13, "_pipeline_version", StringType(), required=False),
)

ARROW_SCHEMA = pa.schema(
    [
        pa.field("slot_date", pa.date32(), nullable=False),
        pa.field("slot_hour", pa.int32(), nullable=False),
        pa.field("id", pa.int64(), nullable=False),
        pa.field("event_ts", pa.timestamp("us", tz="UTC")),
        pa.field("score", pa.float64()),
        pa.field("tags", pa.list_(pa.string())),
        pa.field("_source_file", pa.string()),
        pa.field("_source_md5", pa.string()),
        pa.field("_slot_ts", pa.timestamp("us", tz="UTC")),
        pa.field("_ingested_at", pa.timestamp("us", tz="UTC")),
        pa.field("_run_id", pa.string()),
        pa.field("_pipeline_version", pa.string()),
    ]
)

DAY_1 = date(2026, 11, 24)
DAY_2 = date(2026, 11, 25)


def create(lake: Lake, name: str) -> Table:
    lake.track(name)
    return create_table_if_missing(lake.catalog, lake.namespace, name, SCHEMA)


def rows(
    slot_date: date, slot_hour: int, count: int = 5, *, version: int = 1, run: str = "run-1"
) -> pa.Table:
    """``count`` rows for one slot-hour. ``version`` changes the business columns of every row;
    ``run`` changes only the columns that differ between two runs by design."""
    slot_ts = datetime(slot_date.year, slot_date.month, slot_date.day, slot_hour, tzinfo=UTC)
    base = int(slot_date.strftime("%Y%m%d")) * 10_000 + slot_hour * 100
    ingested = datetime(2026, 11, 26, tzinfo=UTC) + timedelta(seconds=sum(map(ord, run)))
    return pa.Table.from_pylist(
        [
            {
                "slot_date": slot_date,
                "slot_hour": slot_hour,
                "id": base + index,
                "event_ts": slot_ts + timedelta(minutes=15 * (index % 4), microseconds=index),
                "score": None if index == 3 else round(index * 1.1 + version / 7, 6),
                "tags": None if index == 4 else [f"tag-{index}", f"v{version}"],
                "_source_file": f"raw/test/dt={slot_date}/{slot_ts:%Y%m%d%H}0000.test.zip",
                "_source_md5": f"{base + index:032x}",
                "_slot_ts": slot_ts,
                "_ingested_at": ingested,
                "_run_id": run,
                "_pipeline_version": "test",
            }
            for index in range(count)
        ],
        schema=ARROW_SCHEMA,
    )


def empty() -> pa.Table:
    return ARROW_SCHEMA.empty_table()


def ids(table: Table, row_filter: str = "true", ref: str | None = None) -> list[int]:
    scan = table.scan(row_filter=row_filter, selected_fields=("id",))
    if ref is not None:
        scan = scan.use_ref(ref)
    return sorted(scan.to_arrow()["id"].to_pylist())


def data_files(table: Table) -> dict[str, int]:
    """Path and record count of every data file of the current snapshot."""
    if table.current_snapshot() is None:
        return {}
    files = table.inspect.data_files()
    return dict(zip(files["file_path"].to_pylist(), files["record_count"].to_pylist(), strict=True))


def commits_so_far(table: Table) -> int:
    """How many catalog commits the table has seen: each one writes a new metadata file."""
    return len(table.metadata.metadata_log)
