"""Partition versions: one row in ``ops.silver_partition`` for each build of a unit and table.

The row is the evidence of a build (blueprint 8.4): which files went in, where every raw line
went, and the checksum of what the unit holds afterwards. The newest row for a table and a date
is the current version of the date.

A GDELT unit is a slot-hour or a slot-date. A unit of an SEC table can also be a set of key
values or the whole table (week 3 plan 3.3): such a row has the kind ``keys`` or ``table``, a
``unit_label`` and no ``slot_date``.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from psycopg.types.json import Jsonb

from gbmi.ledger.db import Connection

HOUR = "hour"
DAY = "day"
KEYS = "keys"
TABLE = "table"


@dataclass(frozen=True)
class PartitionVersion:
    """One build of one unit of one table."""

    feed: str
    table_name: str
    #: The date of an hour unit or a day unit; None for a unit of keys or of a whole table.
    slot_date: date | None
    slot_hour: int | None
    committed_rows: int
    file_count: int
    fileset_hash: str
    content_digest: str
    pipeline_version: str
    #: The Iceberg commit; all three are None when an empty unit needed no commit.
    commit_id: str | None = None
    snapshot_id: int | None = None
    previous_snapshot_id: int | None = None
    #: The line counts; None for a table whose rows are not lines of a source file.
    raw_lines: int | None = None
    parsed_rows: int | None = None
    quarantined_rows: int | None = None
    duplicate_rows: int | None = None
    run_id: str | None = None
    duration_ms: int | None = None
    detail: Mapping[str, Any] = field(default_factory=dict)
    #: ``keys`` or ``table`` for a unit without a date; None for a slot-hour and a slot-date.
    kind: str | None = None
    #: The label of the unit; a unit without a date needs the label.
    unit_label: str | None = None

    def __post_init__(self) -> None:
        if self.kind in (KEYS, TABLE):
            if self.slot_date is not None or self.slot_hour is not None or not self.unit_label:
                raise ValueError(
                    f"a unit of the kind {self.kind} has a label and neither a date nor an hour"
                )
        elif self.kind is not None:
            raise ValueError(f"the kind of a unit without a date is keys or table, got {self.kind}")
        elif self.slot_date is None:
            raise ValueError("an hour unit and a day unit need slot_date")

    @property
    def unit_kind(self) -> str:
        if self.kind is not None:
            return self.kind
        return DAY if self.slot_hour is None else HOUR


def record_versions(
    connection: Connection, versions: Sequence[PartitionVersion], *, source: str
) -> None:
    """Store the partition versions of one build."""
    with connection.cursor() as cursor:
        cursor.executemany(
            "INSERT INTO ops.silver_partition (source, feed, table_name, slot_date, unit_kind, "
            "slot_hour, commit_id, snapshot_id, previous_snapshot_id, raw_lines, parsed_rows, "
            "quarantined_rows, duplicate_rows, committed_rows, file_count, fileset_hash, "
            "content_digest, run_id, pipeline_version, duration_ms, detail, unit_label) VALUES "
            "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
            "%s)",
            [
                (
                    source,
                    version.feed,
                    version.table_name,
                    version.slot_date,
                    version.unit_kind,
                    version.slot_hour,
                    version.commit_id,
                    version.snapshot_id,
                    version.previous_snapshot_id,
                    version.raw_lines,
                    version.parsed_rows,
                    version.quarantined_rows,
                    version.duplicate_rows,
                    version.committed_rows,
                    version.file_count,
                    version.fileset_hash,
                    version.content_digest,
                    version.run_id,
                    version.pipeline_version,
                    version.duration_ms,
                    Jsonb(dict(version.detail)),
                    version.unit_label,
                )
                for version in versions
            ],
        )


def day_rows(
    connection: Connection, *, source: str, table_name: str, dates: Sequence[date]
) -> list[int | None]:
    """The row count of the current day-level version of each date; None where a date has none."""
    rows = connection.execute(
        "SELECT DISTINCT ON (slot_date) slot_date, committed_rows FROM ops.silver_partition "
        "WHERE source = %s AND table_name = %s AND unit_kind = 'day' AND slot_date = ANY(%s) "
        "ORDER BY slot_date, partition_version_id DESC",
        (source, table_name, list(dates)),
    ).fetchall()
    found = {row[0]: int(row[1]) for row in rows}
    return [found.get(day) for day in dates]


def versions_of_run(
    connection: Connection, *, source: str, slot_date: date, run_id: str | None
) -> dict[str, dict[str, Any]]:
    """The newest day-level version of each table which one run built for a date."""
    rows = connection.execute(
        "SELECT DISTINCT ON (table_name) table_name, feed, committed_rows, file_count, "
        "fileset_hash, content_digest, snapshot_id, duration_ms FROM ops.silver_partition "
        "WHERE source = %s AND slot_date = %s AND unit_kind = 'day' "
        "AND run_id IS NOT DISTINCT FROM %s ORDER BY table_name, partition_version_id DESC",
        (source, slot_date, run_id),
    ).fetchall()
    return {
        row[0]: {
            "feed": row[1],
            "rows": int(row[2]),
            "files": int(row[3]),
            "fileset_hash": row[4],
            "content_digest": row[5],
            "snapshot_id": row[6],
            "duration_ms": row[7],
        }
        for row in rows
    }


def label_rows(
    connection: Connection, *, source: str, table_name: str, unit_label: str
) -> int | None:
    """The row count of the current version of a labelled unit; None when the unit has none."""
    row = connection.execute(
        "SELECT committed_rows FROM ops.silver_partition WHERE source = %s AND table_name = %s "
        "AND unit_label = %s ORDER BY partition_version_id DESC LIMIT 1",
        (source, table_name, unit_label),
    ).fetchone()
    return int(row[0]) if row is not None else None
