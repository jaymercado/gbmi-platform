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
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass
from datetime import date

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyiceberg.exceptions import CommitFailedException, ValidationException
from pyiceberg.expressions import And, BooleanExpression, EqualTo, Reference
from pyiceberg.expressions.literals import literal
from pyiceberg.io import OutputStream
from pyiceberg.table import Table

COMMIT_ID_PROPERTY = "gbmi.commit-id"
UNIT_PROPERTY = "gbmi.unit"
RUN_ID_PROPERTY = "gbmi.run-id"
PIPELINE_VERSION_PROPERTY = "gbmi.pipeline-version"

SLOT_DATE = "slot_date"
SLOT_HOUR = "slot_hour"

#: A commit which loses a conflict is tried again up to three times (blueprint 8.4).
COMMIT_RETRIES = 3

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


class DayFile:
    """One Parquet file with a whole day of a table, written batch by batch, then committed.

    PyIceberg 0.12 cannot stream into a partitioned table. Instead, the batches are written one
    row group at a time into a single Parquet file which carries the table's field ids, and
    the day is then replaced in one catalog commit: delete the day, add the file. Memory is
    bounded by one batch, and the day ends as one file, which also compacts its hourly files
    (blueprint 8.4).

    Staging and committing are two steps, so a caller can stage the files of several tables in
    one pass over the source and audit the totals before anything is committed. Until the
    commit nothing is visible; a staged file which is never committed is an unreferenced file.
    """

    def __init__(self, table: Table, slot_date: date) -> None:
        self.table = table
        self.unit = Unit(slot_date)
        self.commit_id = uuid.uuid4().hex
        directory = f"{table.location()}/data/{SLOT_DATE}={slot_date.isoformat()}"
        self.path = f"{directory}/{self.commit_id}-day.parquet"
        self.rows = 0
        self._schema = table.schema().as_arrow()
        self._codec = table.properties.get("write.parquet.compression-codec", "zstd")
        # The file is created by the first batch with rows: a day without rows leaves no file.
        self._stream: OutputStream | None = None
        self._writer: pq.ParquetWriter | None = None
        self._closed = False
        self._on_storage = False

    def write(self, batch: pa.Table) -> None:
        """Append one batch as one row group. Rows of another day are refused."""
        if self._closed:
            raise RuntimeError("the day file is closed")
        if batch.num_rows == 0:
            return
        self.unit.check(batch)
        if self._writer is None:
            self._stream = self.table.io.new_output(self.path).create(overwrite=True)
            self._writer = pq.ParquetWriter(self._stream, self._schema, compression=self._codec)
            self._on_storage = True
        self._writer.write_table(batch.select(self._schema.names).cast(self._schema))
        self.rows += batch.num_rows

    def close(self) -> None:
        """Finish the file. Calling it twice is harmless."""
        if self._writer is not None and self._stream is not None and not self._closed:
            self._writer.close()
            self._stream.close()
        self._closed = True

    def read(self, columns: Sequence[str]) -> pa.Table:
        """Columns of the staged file, read back: an audit checks what the commit will add."""
        self.close()
        if not self._on_storage:
            return self._schema.empty_table().select(list(columns))
        with self.table.io.new_input(self.path).open() as stream:
            return pq.read_table(stream, columns=list(columns))

    def discard(self) -> None:
        """Remove the staged file: nothing of it was ever visible. Only before a commit."""
        self.close()
        if self._on_storage:
            self.table.io.delete(self.path)
            self._on_storage = False

    def commit(
        self,
        table: Table | None = None,
        *,
        run_id: str | None = None,
        pipeline_version: str | None = None,
    ) -> CommitResult:
        """Replace the day with the staged file in one catalog commit.

        ``table`` is a freshly loaded copy of the table for a further attempt after a commit
        conflict; the staged file stays the same. A day without rows is emptied.
        """
        self.close()
        table = table or self.table
        properties = _properties(self.commit_id, self.unit, run_id, pipeline_version)
        previous = table.current_snapshot()
        with _expected_empty_delete(), table.transaction() as transaction:
            if previous is not None:
                transaction.delete(
                    delete_filter=self.unit.row_filter, snapshot_properties=properties
                )
            if self.rows:
                transaction.add_files([self.path], snapshot_properties=properties)
        return _result(
            table, self.unit, self.commit_id, previous.snapshot_id if previous else None, self.rows
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

    The batches are staged in a ``DayFile`` and committed in one catalog commit. A failure
    before the commit removes the staged file and leaves no visible change.

    ``audit_totals`` receives the number of rows written and raises to stop the commit.
    """
    day = DayFile(table, slot_date)
    try:
        for batch in batches:
            day.write(batch)
        day.close()
        if audit_totals is not None:
            audit_totals(day.rows)
    except BaseException:
        day.discard()
        raise
    return day.commit(run_id=run_id, pipeline_version=pipeline_version)


def unit_rows(table: Table, unit: Unit) -> int:
    """The rows which the table holds for a unit now."""
    if table.current_snapshot() is None:
        return 0
    return int(table.scan(row_filter=unit.row_filter).count())


def commit_with_retry(
    load: Callable[[], Table],
    commit: Callable[[Table], CommitResult],
    *,
    retries: int = COMMIT_RETRIES,
) -> CommitResult:
    """Run a commit on a freshly loaded table; after a commit conflict, load and commit again.

    PyIceberg 0.12 repeats a commit by itself when another commit came first and touched other
    data. PyIceberg gives up with CommitFailedException after its own attempts, and with
    ValidationException when the other commit changed the same unit. A transaction which failed
    cannot be used again. So each attempt here starts from the catalog: a new copy of the table
    and a new transaction (blueprint 8.4). Both exceptions say: the commit did not land.
    """
    for attempt in range(retries + 1):
        try:
            return commit(load())
        except (CommitFailedException, ValidationException):
            if attempt == retries:
                raise
    raise RuntimeError("unreachable: the loop returns or raises")


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


def _properties(
    commit_id: str, unit: Unit, run_id: str | None, pipeline_version: str | None
) -> dict[str, str]:
    properties = {COMMIT_ID_PROPERTY: commit_id, UNIT_PROPERTY: unit.label}
    if run_id:
        properties[RUN_ID_PROPERTY] = run_id
    if pipeline_version:
        properties[PIPELINE_VERSION_PROPERTY] = pipeline_version
    return properties


def _commit_properties(
    unit: Unit, run_id: str | None, pipeline_version: str | None
) -> tuple[str, dict[str, str]]:
    commit_id = uuid.uuid4().hex
    return commit_id, _properties(commit_id, unit, run_id, pipeline_version)


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
    # PyIceberg retries a commit which lost a race against another unit, on the refreshed
    # table. The snapshot before the commit is therefore read from the commit itself: the parent
    # of its first snapshot. A commit without a snapshot changed nothing.
    if created:
        previous_snapshot_id = created[0].parent_snapshot_id
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
