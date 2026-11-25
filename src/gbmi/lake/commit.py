"""Stage, audit, commit: the only way rows reach a silver table (blueprint 5.6 and 8.4).

A *unit* is what one job replaces as a whole: a slot-hour for the incremental job, a slot-date
for the daily finalisation. A commit replaces exactly one unit, in one catalog commit, with an
overwrite that is filtered on the unit. The unit comes from the task, never from the rows, so an
empty result empties the unit instead of leaving stale rows behind (week 1 plan 3.3).

PyIceberg writes an overwrite as up to two snapshots - a delete and an append - inside that one
catalog commit (week 1 plan, D8). Every snapshot of a commit carries the same
``gbmi.commit-id``, which is how maintenance counts commits, not snapshots. The last
snapshot of a commit is the one a mart records as ``_silver_snapshot_id``; the snapshot before
the commit is the rollback target.
"""

from __future__ import annotations

import contextlib
import uuid
import warnings
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from datetime import date

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyiceberg.expressions import And, BooleanExpression, EqualTo, Reference
from pyiceberg.expressions.literals import literal
from pyiceberg.table import Table

COMMIT_ID_PROPERTY = "gbmi.commit-id"
UNIT_PROPERTY = "gbmi.unit"
RUN_ID_PROPERTY = "gbmi.run-id"
PIPELINE_VERSION_PROPERTY = "gbmi.pipeline-version"

SLOT_DATE = "slot_date"
SLOT_HOUR = "slot_hour"

Audit = Callable[[pa.Table], None]


class UnitMismatchError(ValueError):
    """The rows handed to a commit do not all belong to the unit being replaced."""


@dataclass(frozen=True)
class Unit:
    """One slot-hour (``slot_hour`` set) or one whole slot-date (``slot_hour`` is None)."""

    slot_date: date
    slot_hour: int | None = None

    def __post_init__(self) -> None:
        if self.slot_hour is not None and not 0 <= self.slot_hour <= 23:
            raise ValueError(f"slot_hour must be 0-23, got {self.slot_hour}")

    @property
    def label(self) -> str:
        day = self.slot_date.isoformat()
        return day if self.slot_hour is None else f"{day}T{self.slot_hour:02d}"

    @property
    def row_filter(self) -> BooleanExpression:
        """The overwrite filter: everything this unit owns, and nothing else."""
        day = _equals(SLOT_DATE, self.slot_date.isoformat())
        if self.slot_hour is None:
            return day
        return And(day, _equals(SLOT_HOUR, self.slot_hour))

    def check(self, data: pa.Table) -> None:
        """Raise unless every row of ``data`` belongs to this unit."""
        if data.num_rows == 0:
            return
        dates = pc.unique(data[SLOT_DATE]).to_pylist()
        if dates != [self.slot_date]:
            raise UnitMismatchError(
                f"unit {self.label} was given rows of slot_date {sorted(dates)}"
            )
        if self.slot_hour is not None:
            hours = pc.unique(data[SLOT_HOUR]).to_pylist()
            if hours != [self.slot_hour]:
                raise UnitMismatchError(
                    f"unit {self.label} was given rows of slot_hour {sorted(hours)}"
                )


@dataclass(frozen=True)
class CommitResult:
    """What one commit did to a table."""

    table: str
    unit: str
    commit_id: str
    #: Snapshots created by this commit, oldest first, with their operations.
    snapshot_ids: tuple[int, ...]
    operations: tuple[str, ...]
    #: The table's current snapshot after the commit (the commit's last snapshot, D8).
    snapshot_id: int | None
    #: The current snapshot before the commit: the rollback target.
    previous_snapshot_id: int | None
    metadata_location: str
    rows: int


def overwrite_unit(
    table: Table,
    data: pa.Table,
    unit: Unit,
    *,
    audit: Audit | None = None,
    run_id: str | None = None,
    pipeline_version: str | None = None,
) -> CommitResult:
    """Replace one unit with ``data``, which is held in memory, in one catalog commit.

    The audit runs first and raises to stop the commit; until the commit, nothing is visible
    and the previous version of the unit stays live (gate G3, blueprint 5.7).
    """
    unit.check(data)
    if audit is not None:
        audit(data)
    commit_id, properties = _commit_properties(unit, run_id, pipeline_version)
    previous = table.current_snapshot()
    with _expected_empty_delete(), table.transaction() as transaction:
        transaction.overwrite(
            data.select([field.name for field in table.schema().fields]),
            overwrite_filter=unit.row_filter,
            snapshot_properties=properties,
        )
    return _result(
        table, unit, commit_id, previous.snapshot_id if previous else None, data.num_rows
    )


def overwrite_day_streaming(
    table: Table,
    slot_date: date,
    batches: Iterable[pa.Table],
    *,
    audit_totals: Callable[[int], None] | None = None,
    run_id: str | None = None,
    pipeline_version: str | None = None,
) -> CommitResult:
    """Replace one whole day from a stream of batches, with memory bounded by one batch.

    PyIceberg 0.12 cannot stream into a partitioned table. Instead, the batches are written
    one row group at a time into a single Parquet file that carries the table's field ids, and
    the day is then replaced in one catalog commit: delete the day, add the file. A failure
    before the commit leaves an unreferenced file and no visible change. Because the day ends
    as one file, this also compacts its hourly files (blueprint 8.4).

    ``audit_totals`` receives the number of rows written and raises to stop the commit.
    """
    unit = Unit(slot_date)
    commit_id, properties = _commit_properties(unit, run_id, pipeline_version)
    arrow_schema = table.schema().as_arrow()
    path = f"{table.location()}/data/{SLOT_DATE}={slot_date.isoformat()}/{commit_id}-day.parquet"
    codec = table.properties.get("write.parquet.compression-codec", "zstd")

    rows = 0
    output = table.io.new_output(path)
    with (
        output.create(overwrite=True) as stream,
        pq.ParquetWriter(stream, arrow_schema, compression=codec) as writer,
    ):
        for batch in batches:
            if batch.num_rows == 0:
                continue
            unit.check(batch)
            writer.write_table(batch.select(arrow_schema.names).cast(arrow_schema))
            rows += batch.num_rows
    if audit_totals is not None:
        try:
            audit_totals(rows)
        except Exception:
            table.io.delete(path)
            raise

    previous = table.current_snapshot()
    with _expected_empty_delete(), table.transaction() as transaction:
        if previous is not None:
            transaction.delete(delete_filter=unit.row_filter, snapshot_properties=properties)
        if rows:
            transaction.add_files([path], snapshot_properties=properties)
    if not rows:
        table.io.delete(path)
    return _result(table, unit, commit_id, previous.snapshot_id if previous else None, rows)


@contextlib.contextmanager
def _expected_empty_delete() -> Iterator[None]:
    """The first load of a unit deletes nothing; PyIceberg's warning about that is expected."""
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Delete operation did not match any records")
        yield


def _equals(column: str, value: str | int) -> BooleanExpression:
    # Keyword form: the positional form works at run time but does not type-check, because
    # the predicate classes are pydantic models.
    return EqualTo(term=Reference(column), value=literal(value))


def _commit_properties(
    unit: Unit, run_id: str | None, pipeline_version: str | None
) -> tuple[str, dict[str, str]]:
    commit_id = uuid.uuid4().hex
    properties = {COMMIT_ID_PROPERTY: commit_id, UNIT_PROPERTY: unit.label}
    if run_id:
        properties[RUN_ID_PROPERTY] = run_id
    if pipeline_version:
        properties[PIPELINE_VERSION_PROPERTY] = pipeline_version
    return commit_id, properties


def _result(
    table: Table, unit: Unit, commit_id: str, previous_snapshot_id: int | None, rows: int
) -> CommitResult:
    created = sorted(
        (
            snapshot
            for snapshot in table.metadata.snapshots
            if snapshot.summary is not None and snapshot.summary[COMMIT_ID_PROPERTY] == commit_id
        ),
        key=lambda snapshot: snapshot.sequence_number or 0,
    )
    current = table.current_snapshot()
    return CommitResult(
        table=".".join(table.name()),
        unit=unit.label,
        commit_id=commit_id,
        snapshot_ids=tuple(snapshot.snapshot_id for snapshot in created),
        operations=tuple(
            snapshot.summary.operation.value for snapshot in created if snapshot.summary
        ),
        snapshot_id=current.snapshot_id if current else None,
        previous_snapshot_id=previous_snapshot_id,
        metadata_location=table.metadata_location,
        rows=rows,
    )
