"""What the three SEC pipelines share: the run, the tables, one commit and its record.

A pipeline builds a unit of a silver table in the order of phase P1 (blueprint 8.4): rows,
gate G3, one catalog commit, then one PostgreSQL transaction with the partition version, the
accounting of each source object and the results of the checks. Until the commit nothing is
visible. Between the commit and the transaction the table holds the new rows and the objects
are still ``verified``: the next run builds the unit again and then records the build.
"""

from __future__ import annotations

import time
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from functools import cached_property
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import AlwaysTrue, BooleanExpression, In, Reference
from pyiceberg.expressions.literals import literal
from pyiceberg.table import Table

from gbmi.config import Settings
from gbmi.core.checksum import content_checksum
from gbmi.core.clock import utc_now
from gbmi.lake.catalog import ensure_silver_namespace, load_lake_catalog
from gbmi.lake.commit import (
    KIND_DAY,
    MAX_SCOPE_KEYS,
    CommitResult,
    Scope,
    commit_with_retry,
    overwrite_scope,
    unit_rows,
)
from gbmi.lake.tables import SEC_SCHEMAS, create_sec_table_if_missing
from gbmi.ledger.objects import ObjectAccounting, ObjectKey, ObjectLedger, ObjectRecord
from gbmi.ledger.repository import fileset_hash
from gbmi.ops.partitions import PartitionVersion, record_versions
from gbmi.quality.audit import UnitCounts
from gbmi.quality.results import CheckResult, record_results
from gbmi.sources.gdelt.land import Bronze
from gbmi.sources.sec.client import SecClient
from gbmi.sources.sec.land import Lander, bronze_key
from gbmi.transforms.sec import RunFacts, SourceFile

CIK = "cik"


class PipelineError(Exception):
    """A pipeline cannot go on: an object which the run needs is missing or was refused."""


@dataclass
class SecRun:
    """What every step of one run of a pipeline needs."""

    as_of: datetime
    settings: Settings
    ledger: ObjectLedger
    lander: Lander
    facts: RunFacts
    #: The note of an operator who accepts a file outside a volume band.
    accept_volume: str | None = None
    started: float = field(default_factory=time.monotonic)

    @property
    def day(self) -> date:
        """The UTC day of the run: the date of every object which the run asks for."""
        return self.as_of.astimezone(UTC).date()

    @property
    def source(self) -> str:
        return self.ledger.source

    @cached_property
    def catalog(self) -> Catalog:
        return load_lake_catalog(self.settings)

    @cached_property
    def namespace(self) -> str:
        return ensure_silver_namespace(self.catalog, self.settings)

    def table(self, name: str) -> Table:
        """One of the five SEC tables; the table is created on first use."""
        return create_sec_table_if_missing(self.catalog, self.namespace, name)

    def load(self, name: str) -> Table:
        return self.catalog.load_table((self.namespace, name))

    def source_file(self, record: ObjectRecord) -> SourceFile:
        return SourceFile(key=bronze_key(record.key), md5=record.md5 or "")

    def seconds(self) -> float:
        return round(time.monotonic() - self.started, 3)


def new_run(
    *,
    as_of: datetime,
    settings: Settings,
    ledger: ObjectLedger,
    client: SecClient,
    accept_volume: str | None = None,
) -> SecRun:
    """The run of one pipeline on one ledger and one client."""
    lander = Lander(
        client=client,
        ledger=ledger,
        bronze=Bronze(settings.lake_uri),
        lake_uri=settings.lake_uri,
        as_of=as_of,
    )
    facts = RunFacts(
        ingested_at=utc_now(), run_id=ledger.run_id, pipeline_version=settings.pipeline_version
    )
    return SecRun(
        as_of=as_of,
        settings=settings,
        ledger=ledger,
        lander=lander,
        facts=facts,
        accept_volume=accept_volume,
    )


def _in(column: str, values: Collection[int]) -> BooleanExpression:
    return In(term=Reference(column), literals={literal(value) for value in values})


def stored_rows(
    run: SecRun,
    name: str,
    *,
    ciks: Collection[int] | None = None,
    without: Collection[int] | None = None,
    columns: Sequence[str] | None = None,
) -> pa.Table:
    """The rows which a table holds now, for some companies or without some companies.

    The rows come back in the Arrow schema of the table definition, so the rows can be joined
    with new rows.
    """
    schema = SEC_SCHEMAS[name].as_arrow()
    names = list(columns) if columns is not None else schema.names
    wanted = pa.schema([schema.field(column) for column in names])
    table = run.table(name)
    if table.current_snapshot() is None or (ciks is not None and not ciks):
        return wanted.empty_table()
    # Above 200 values PyIceberg reads every file anyway (week 3 plan, change C15).
    pushed = ciks is not None and len(ciks) <= MAX_SCOPE_KEYS
    row_filter = _in(CIK, ciks or ()) if pushed else AlwaysTrue()
    selected = tuple({*names, CIK})
    rows = table.scan(row_filter=row_filter, selected_fields=selected).to_arrow()
    if ciks is not None and not pushed:
        rows = rows.filter(pc.is_in(rows[CIK], value_set=pa.array(sorted(ciks), pa.int64())))
    if without:
        inside = pc.is_in(rows[CIK], value_set=pa.array(sorted(without), pa.int64()))
        rows = rows.filter(pc.invert(inside))
    return rows.select(names).cast(wanted)


def company_unit(
    run: SecRun, name: str, data: pa.Table, ciks: Collection[int]
) -> tuple[Scope, pa.Table]:
    """The unit which replaces the rows of some companies, and the rows to write.

    Up to 200 companies are a unit of keys. More companies replace the whole table: the rows
    of every other company are read and written again (week 3 plan, change C15).
    """
    if len(ciks) <= MAX_SCOPE_KEYS:
        return Scope.for_keys(CIK, ciks), data
    others = stored_rows(run, name, without=ciks)
    return Scope.for_table(), pa.concat_tables([others, data])


def commit(run: SecRun, name: str, data: pa.Table, unit: Scope) -> CommitResult | None:
    """Replace one unit of one table. None: the unit is empty and was empty before."""
    table = run.table(name)
    if data.num_rows == 0 and unit_rows(table, unit) == 0:
        return None

    def write(fresh: Table) -> CommitResult:
        return overwrite_scope(
            fresh,
            data,
            unit,
            run_id=run.facts.run_id,
            pipeline_version=run.facts.pipeline_version,
        )

    return commit_with_retry(lambda: run.load(name), write)


def version(
    run: SecRun,
    *,
    name: str,
    feed: str,
    unit: Scope,
    data: pa.Table,
    result: CommitResult | None,
    objects: Sequence[ObjectRecord],
    started: float,
    counts: UnitCounts | None = None,
    detail: Mapping[str, Any] | None = None,
) -> PartitionVersion:
    """The partition version of one build of one unit.

    ``counts`` is given for a table whose rows are the records of the source files; a table
    which also keeps stored rows records its committed rows only.
    """
    dated = unit.kind == KIND_DAY
    return PartitionVersion(
        feed=feed,
        table_name=name,
        slot_date=unit.day if dated else None,
        slot_hour=None,
        committed_rows=data.num_rows,
        file_count=len(objects),
        fileset_hash=fileset_hash((record.key.label, record.md5 or "") for record in objects),
        content_digest=content_checksum(data).digest,
        pipeline_version=run.facts.pipeline_version,
        commit_id=result.commit_id if result else None,
        snapshot_id=result.snapshot_id if result else None,
        previous_snapshot_id=result.previous_snapshot_id if result else None,
        raw_lines=counts.raw_lines if counts else None,
        parsed_rows=counts.parsed if counts else None,
        quarantined_rows=counts.quarantined if counts else None,
        duplicate_rows=counts.duplicates if counts else None,
        run_id=run.facts.run_id,
        duration_ms=round((time.monotonic() - started) * 1000),
        detail={
            "operations": list(result.operations) if result else [],
            # The load of the table and the catalog commit (week 3 plan, exit-gate row 12).
            "commit_ms": (
                round(result.seconds * 1000) if result and result.seconds is not None else None
            ),
            **(detail or {}),
        },
        kind=None if dated else unit.kind,
        unit_label=unit.label,
    )


def record(
    run: SecRun,
    versions: Sequence[PartitionVersion],
    entries: Sequence[ObjectAccounting],
    results: Sequence[CheckResult],
) -> set[ObjectKey]:
    """One transaction: the versions, the accounting of the objects and the check results."""
    connection = run.ledger.connection
    with connection.transaction():
        record_versions(connection, versions, source=run.source)
        moved = run.ledger.mark_loaded(entries, as_of=run.as_of)
        record_results(connection, results, source=run.source, run_id=run.facts.run_id)
    return moved


def record_results_only(run: SecRun, results: Sequence[CheckResult]) -> None:
    connection = run.ledger.connection
    with connection.transaction():
        record_results(connection, results, source=run.source, run_id=run.facts.run_id)


def table_summary(data: pa.Table, result: CommitResult | None, unit: Scope) -> dict[str, Any]:
    """What one build did to one table, in built-in types."""
    checksum = content_checksum(data)
    return {
        "unit": unit.label,
        "rows": data.num_rows,
        "committed": result is not None,
        "snapshot_id": result.snapshot_id if result else None,
        "snapshot_operations": list(result.operations) if result else [],
        "checksum": {"rows": checksum.rows, "digest": checksum.digest},
    }
