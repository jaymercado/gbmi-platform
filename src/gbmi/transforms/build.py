"""Build one unit of silver from the verified files of the ledger (pipeline PL-2, blueprint 8.4).

A *unit* is what one commit replaces: one slot-hour or one whole slot-date of one feed (week 2
plan 3.3). ``build_unit`` runs the stages of the blueprint for one unit and does not depend on
Airflow:

1. The file set: the slots of the unit in state ``verified`` or ``loaded`` without a claim. Each
   file is read from bronze, and the MD5 of the file is compared with the ledger.
2. Parse, gate G2 and deduplication (``gbmi.transforms.duckdb``).
3. Gate G3 (``gbmi.quality.audit``). A failed audit commits nothing and changes no slot.
4. One catalog commit for each table of the feed.
5. One PostgreSQL transaction: the partition versions, the accounting of each file, the change
   ``verified → loaded`` and the results of the checks.

An hour unit is built in memory. A day unit is parsed hour by hour and staged in one Parquet
file for each table, so memory follows one hour; the commit replaces the hourly files of the
day with the one file (compaction).

What a crash leaves behind: until the commit nothing is visible. Between the commit and the
transaction in PostgreSQL the table holds the new rows and the slots are still ``verified``;
the next run builds the same unit again and then records the build.
"""

from __future__ import annotations

import itertools
import time
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import partial
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.table import Table

from gbmi.config import Settings, get_settings
from gbmi.core.checksum import ContentChecksum, combine_checksums, content_checksum
from gbmi.core.clock import utc_now
from gbmi.core.failpoint import SILVER_AFTER_COMMIT, SILVER_BEFORE_COMMIT, failpoint
from gbmi.core.gdelt import Feed, parse_file_name
from gbmi.core.positional import FILE_THRESHOLD
from gbmi.lake.catalog import ensure_silver_namespace, load_lake_catalog
from gbmi.lake.commit import (
    SLOT_DATE,
    CommitResult,
    DayFile,
    Unit,
    commit_with_retry,
    overwrite_unit,
    unit_rows,
)
from gbmi.lake.tables import (
    FEED_TABLES,
    GDELT_GKG_DOC,
    GDELT_GKG_ORG,
    SILVER_SCHEMAS,
    TABLE_KEYS,
    create_table_if_missing,
)
from gbmi.ledger.repository import Accounting, Ledger, SlotKey, SlotRecord, fileset_hash
from gbmi.ledger.states import State
from gbmi.logging import get_logger
from gbmi.ops.partitions import PartitionVersion, day_rows, record_versions
from gbmi.quality.audit import (
    VOLUME_HISTORY,
    UnitCounts,
    check_accounting,
    check_keys,
    check_lists,
    check_org_rows,
    check_quarantine,
    check_rows,
    check_volume,
    list_columns,
    null_lists,
    problems,
    raise_if_failed,
)
from gbmi.quality.quarantine import (
    QUARANTINE_SCHEMA,
    clear_hour_quarantine,
    write_quarantine,
    write_quarantined_file,
)
from gbmi.quality.results import FAILED, G1, G2, PASSED, CheckResult, record_results
from gbmi.sources.gdelt.land import Bronze, md5_hex
from gbmi.transforms.duckdb.gdelt_event import parse_events
from gbmi.transforms.duckdb.gdelt_gkg import parse_gkg
from gbmi.transforms.duckdb.gdelt_mention import parse_mentions
from gbmi.transforms.duckdb.positional import BronzeFile, FileCounts, Parsed, arrow
from gbmi.transforms.duckdb.runner import connect

log = get_logger(__name__)

PARSERS: dict[Feed, Callable[..., Parsed]] = {
    Feed.EXPORT: parse_events,
    Feed.MENTIONS: parse_mentions,
    Feed.GKG: parse_gkg,
}
#: A key which holds the slot cannot repeat in another hour of the day.
SLOT_TS = "slot_ts"
BAD_ROWS_RULE = f"at most {FILE_THRESHOLD:.0%} of the rows of a file fail gate G2"


class BuildError(Exception):
    """The unit cannot be built. Nothing was committed."""


@dataclass(frozen=True)
class SetAside:
    """A verified file which gives no row to silver, and the check which the file failed."""

    record: SlotRecord
    gate: str
    check: str
    reason: str
    threshold: str
    observed: float | None = None
    detail: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class _Run:
    """What every step of one build needs."""

    feed: Feed
    unit: Unit
    bronze: Bronze
    run_id: str | None
    pipeline_version: str
    ingested_at: datetime


@dataclass
class _Output:
    """The rows of one table, ready for the audit and the commit."""

    name: str
    rows: int
    checksum: ContentChecksum
    #: The results of gate G3 for the rows themselves: the key, the unit, the lists.
    results: list[CheckResult]
    commit: Callable[[Table], CommitResult]
    #: Removes what the stage wrote. Only before a commit.
    discard: Callable[[], None] = lambda: None


@dataclass
class _Staged:
    """A parsed unit before the audit: the accepted files, the outputs and what was set aside."""

    files: list[FileCounts]
    duplicates: int
    quarantine: pa.Table
    aside: list[SetAside]
    #: The outputs in the order of the commits.
    outputs: list[_Output]
    #: The sum of ``org_count`` over the documents to commit (GKG only).
    org_count_total: int | None
    notes: dict[str, int]

    def discard(self) -> None:
        for output in self.outputs:
            output.discard()


class _SeenKeys:
    """The keys which the earlier hours of a day gave, in a DuckDB table (blueprint 8.4).

    The daily finalisation removes duplicates across hours: of the rows with one key, the row
    of the earliest hour stays, which is the first occurrence in slot order.
    """

    def __init__(self, key: Sequence[str]) -> None:
        self._key = list(key)
        self._connection = connect()
        self._created = False

    def keep(self, data: pa.Table) -> pa.Table:
        """The rows whose key no earlier call has seen. The keys of the rows are remembered."""
        if data.num_rows == 0:
            return data
        columns = ", ".join(self._key)
        numbered = data.select(self._key).append_column(
            "_row", pa.array(range(data.num_rows), pa.int64())
        )
        self._connection.register("batch", numbered)
        try:
            if not self._created:
                self._connection.execute(f"create table seen as select {columns} from batch")  # noqa: S608
                self._created = True
                return data
            match = " and ".join(f"s.{name} = b.{name}" for name in self._key)
            self._connection.execute(
                f"create or replace temp table fresh as select b.* from batch b "  # noqa: S608
                f"where not exists (select 1 from seen s where {match})"
            )
            rows = arrow(self._connection.sql("select _row from fresh order by _row"))["_row"]
            self._connection.execute(f"insert into seen select {columns} from fresh")  # noqa: S608
        finally:
            self._connection.unregister("batch")
        return data if len(rows) == data.num_rows else data.take(rows.combine_chunks())

    def close(self) -> None:
        self._connection.close()


def _slot_key(feed: Feed, bronze_key: str) -> SlotKey:
    slot, _ = parse_file_name(bronze_key.rsplit("/", 1)[-1])
    return SlotKey(feed, slot)


def _org_count(documents: pa.Table | None) -> int | None:
    if documents is None:
        return None
    return int(pc.sum(documents["org_count"]).as_py() or 0)


def _read_files(
    run: _Run, records: Sequence[SlotRecord]
) -> tuple[list[BronzeFile], list[SetAside]]:
    """Read the files of the slots from bronze and compare each MD5 with the ledger."""
    files: list[BronzeFile] = []
    aside: list[SetAside] = []
    for record in records:
        key = record.key.slot.bronze_key(run.feed)
        content = run.bronze.read(key)
        found = md5_hex(content) if content is not None else None
        if content is not None and found is not None and found == record.expected_md5:
            files.append(BronzeFile(key=key, md5=found, content=content))
            continue
        problem = (
            "the file is not in bronze"
            if found is None
            else f"the file in bronze has the MD5 {found}, the ledger has {record.expected_md5}"
        )
        if record.state is State.LOADED:
            # No transition leads out of loaded (blueprint 8.2): an operator has to look.
            raise BuildError(f"slot {record.key.label} is loaded, but {problem}")
        aside.append(
            SetAside(
                record,
                gate=G1,
                check="bronze_md5",
                reason=problem,
                threshold="the MD5 of the file in bronze equals the MD5 of the ledger",
                detail={"file": key},
            )
        )
    return files, aside


def _parse(run: _Run, records: Sequence[SlotRecord]) -> tuple[Parsed, list[SetAside]]:
    """Parse the files of the slots. A file which the 1% rule rejects is set aside."""
    files, aside = _read_files(run, records)
    parsed = PARSERS[run.feed](
        files,
        run_id=run.run_id,
        pipeline_version=run.pipeline_version,
        ingested_at=run.ingested_at,
        file_threshold=FILE_THRESHOLD,
    )
    states = {record.key.slot.bronze_key(run.feed): record for record in records}
    for rejected in parsed.rejected:
        record = states[rejected.key]
        reason = (
            f"{rejected.quarantined_rows} of {rejected.raw_lines} rows failed gate G2, "
            f"more than {FILE_THRESHOLD:.0%} of the file"
        )
        if record.state is State.LOADED:
            raise BuildError(f"slot {record.key.label} is loaded, but {reason}")
        aside.append(
            SetAside(
                record,
                gate=G2,
                check="file_bad_row_share",
                reason=reason,
                threshold=BAD_ROWS_RULE,
                observed=rejected.quarantined_rows / rejected.raw_lines,
                detail={
                    "file": rejected.key,
                    "raw_lines": rejected.raw_lines,
                    "failed_rows": rejected.quarantined_rows,
                },
            )
        )
    return parsed, aside


def _commit_hour(table: Table, *, data: pa.Table, run: _Run) -> CommitResult:
    return overwrite_unit(
        table, data, run.unit, run_id=run.run_id, pipeline_version=run.pipeline_version
    )


def _commit_day(table: Table, *, day: DayFile, run: _Run) -> CommitResult:
    return day.commit(table, run_id=run.run_id, pipeline_version=run.pipeline_version)


def _stage_hour(run: _Run, records: Sequence[SlotRecord]) -> _Staged:
    """An hour unit: the four files of the hour at most, parsed in one pass and held in memory."""
    parsed, aside = _parse(run, records)
    outputs = [
        _Output(
            name=name,
            rows=parsed.tables[name].num_rows,
            checksum=content_checksum(parsed.tables[name]),
            results=check_rows(
                parsed.tables[name],
                run.unit,
                key=TABLE_KEYS[name],
                table=name,
                feed=run.feed.value,
            ),
            commit=partial(_commit_hour, data=parsed.tables[name], run=run),
        )
        for name in FEED_TABLES[run.feed]
    ]
    return _Staged(
        files=list(parsed.files),
        duplicates=parsed.counts.duplicates,
        quarantine=parsed.quarantine,
        aside=aside,
        outputs=outputs,
        org_count_total=_org_count(parsed.tables.get(GDELT_GKG_DOC)),
        notes=dict(parsed.notes),
    )


def _day_output(
    run: _Run, name: str, day: DayFile, checksum: ContentChecksum, missing_lists: int
) -> _Output:
    """The audit of a staged day reads the key columns back from the file which will be added."""
    key = TABLE_KEYS[name]
    staged = day.read([*key, SLOT_DATE])
    results = check_keys(staged, run.unit, key=key, table=name, feed=run.feed.value)
    lists = list_columns(SILVER_SCHEMAS[name].as_arrow())
    if lists:
        results.append(check_lists(missing_lists, lists, run.unit, table=name, feed=run.feed.value))
    return _Output(
        name=name,
        rows=staged.num_rows,
        checksum=checksum,
        results=results,
        commit=partial(_commit_day, day=day, run=run),
        discard=day.discard,
    )


def _stage_day(run: _Run, records: Sequence[SlotRecord], tables: Mapping[str, Table]) -> _Staged:
    """A day unit: parsed hour by hour, one staged file for each table of the feed.

    One pass over the files of the Global Knowledge Graph stages the documents and the
    organisations. A row whose key an earlier hour gave is dropped; with a document go the
    organisation rows of the document.
    """
    names = FEED_TABLES[run.feed]
    main, main_key = names[0], TABLE_KEYS[names[0]]
    days = {name: DayFile(tables[name], run.unit.slot_date) for name in names}
    checksums = {
        name: [content_checksum(SILVER_SCHEMAS[name].as_arrow().empty_table())] for name in names
    }
    missing_lists = dict.fromkeys(names, 0)
    seen = _SeenKeys(main_key) if SLOT_TS not in main_key else None
    files: list[FileCounts] = []
    quarantine = [QUARANTINE_SCHEMA.empty_table()]
    aside: list[SetAside] = []
    notes: Counter[str] = Counter()
    duplicates = 0
    org_count_total = 0
    try:
        hours = itertools.groupby(records, key=lambda record: record.key.slot.slot_hour)
        for _, group in hours:
            parsed, set_aside = _parse(run, list(group))
            files += parsed.files
            aside += set_aside
            quarantine.append(parsed.quarantine)
            notes.update(parsed.notes)
            duplicates += parsed.counts.duplicates
            batches = dict(parsed.tables)
            if seen is not None:
                kept = seen.keep(batches[main])
                if kept.num_rows != batches[main].num_rows:
                    duplicates += batches[main].num_rows - kept.num_rows
                    parents = kept[main_key[0]].combine_chunks()
                    for name in names[1:]:
                        child = batches[name]
                        batches[name] = child.filter(
                            pc.is_in(child[main_key[0]], value_set=parents)
                        )
                    batches[main] = kept
            for name in names:
                batch = batches[name]
                if batch.num_rows:
                    days[name].write(batch)
                    checksums[name].append(content_checksum(batch))
                    missing_lists[name] += null_lists(batch)
            org_count_total += _org_count(batches.get(GDELT_GKG_DOC)) or 0
        outputs = [
            _day_output(
                run, name, days[name], combine_checksums(checksums[name]), missing_lists[name]
            )
            for name in names
        ]
    except BaseException:
        for day in days.values():
            day.discard()
        raise
    finally:
        if seen is not None:
            seen.close()
    return _Staged(
        files=files,
        duplicates=duplicates,
        quarantine=pa.concat_tables(quarantine),
        aside=aside,
        outputs=outputs,
        org_count_total=org_count_total if GDELT_GKG_DOC in names else None,
        notes=dict(notes),
    )


def _bad_rows(run: _Run, files: Sequence[FileCounts]) -> CheckResult:
    """Gate G2 for the unit: the largest share of failed rows among the accepted files."""
    shares = [counts.quarantined_rows / counts.raw_lines for counts in files if counts.raw_lines]
    return CheckResult(
        gate=G2,
        check_name="file_bad_row_share",
        status=PASSED,
        observed_value=max(shares, default=0.0),
        threshold=BAD_ROWS_RULE,
        feed=run.feed.value,
        unit=run.unit.label,
        detail={
            "files": len(files),
            "raw_lines": sum(counts.raw_lines for counts in files),
            "quarantined_rows": sum(counts.quarantined_rows for counts in files),
        },
    )


def _audit(
    run: _Run,
    staged: _Staged,
    counts: UnitCounts,
    ledger: Ledger,
    accept_volume: str | None,
) -> list[CheckResult]:
    """Gate G3 for every table of the unit (week 2 plan 3.3)."""
    feed, unit = run.feed.value, run.unit
    lines = staged.outputs[0]
    results = [
        _bad_rows(run, staged.files),
        *check_accounting(counts, lines.rows, unit, table=lines.name, feed=feed),
        check_quarantine(counts, staged.quarantine.num_rows, unit, table=lines.name, feed=feed),
    ]
    for output in staged.outputs:
        results += output.results
        if output.name == GDELT_GKG_ORG:
            results.append(
                check_org_rows(
                    output.rows, staged.org_count_total or 0, unit, table=output.name, feed=feed
                )
            )
        if unit.slot_hour is None:
            weekdays = [
                unit.slot_date - timedelta(weeks=weeks) for weeks in range(1, VOLUME_HISTORY + 1)
            ]
            history = day_rows(
                ledger.connection, source=ledger.source, table_name=output.name, dates=weekdays
            )
            results.append(
                check_volume(
                    output.rows,
                    history,
                    unit,
                    table=output.name,
                    feed=feed,
                    accept_note=accept_volume,
                )
            )
    return results


def _record(ledger: Ledger, results: Sequence[CheckResult], run_id: str | None) -> None:
    with ledger.connection.transaction():
        record_results(ledger.connection, results, source=ledger.source, run_id=run_id)


def _set_aside(
    run: _Run, aside: Sequence[SetAside], *, ledger: Ledger, lake_uri: str, as_of: datetime
) -> list[dict[str, Any]]:
    """Copy each file to the quarantine area and move the slot ``verified → quarantined``."""
    results: list[CheckResult] = []
    summary: list[dict[str, Any]] = []
    for item in aside:
        key = item.record.key
        content = run.bronze.read(key.slot.bronze_key(key.feed))
        kept = (
            write_quarantined_file(
                lake_uri,
                ledger.source,
                key.feed.value,
                key.slot.slot_date,
                key.slot.file_name(key.feed),
                content,
            )
            if content is not None
            else None
        )
        ledger.quarantine(key, from_state=State.VERIFIED, reason=item.reason, as_of=as_of)
        log.error("slot_quarantined", slot=key.label, gate=item.gate, reason=item.reason)
        results.append(
            CheckResult(
                gate=item.gate,
                check_name=item.check,
                status=FAILED,
                observed_value=item.observed,
                threshold=item.threshold,
                feed=key.feed.value,
                unit=key.slot.stamp,
                detail={**item.detail, "reason": item.reason, "kept": kept},
            )
        )
        summary.append({"slot": key.label, "gate": item.gate, "reason": item.reason, "kept": kept})
    if results:
        _record(ledger, results, run.run_id)
    return summary


def _table_summary(
    output: _Output, commit: CommitResult | None, snapshot_id: int | None
) -> dict[str, Any]:
    """What one build did to one table. Without a commit the table kept its snapshot."""
    return {
        "rows": output.rows,
        "committed": commit is not None,
        "snapshot_id": snapshot_id,
        "previous_snapshot_id": commit.previous_snapshot_id if commit else snapshot_id,
        "snapshot_operations": list(commit.operations) if commit else [],
        "checksum": {"rows": output.checksum.rows, "digest": output.checksum.digest},
    }


def build_unit(
    feed: Feed,
    unit: Unit,
    *,
    as_of: datetime,
    ledger: Ledger,
    settings: Settings | None = None,
    run_id: str | None = None,
    accept_volume: str | None = None,
) -> dict[str, Any]:
    """Build one unit of one feed and record the build; returns a summary of built-in types.

    ``accept_volume`` is the note of an operator who accepts a day outside the volume band.

    Raises AuditError when gate G3 fails: nothing is committed, no slot changes, and the
    results of the checks are stored. Raises BuildError when a loaded file changed in bronze
    or now fails the 1% rule.
    """
    settings = settings or get_settings()
    clock = time.monotonic()
    names = FEED_TABLES[feed]
    catalog = load_lake_catalog(settings)
    namespace = ensure_silver_namespace(catalog, settings)
    tables = {
        name: create_table_if_missing(catalog, namespace, name, SILVER_SCHEMAS[name])
        for name in names
    }

    def load(name: str) -> Table:
        return catalog.load_table((namespace, name))

    run = _Run(
        feed=feed,
        unit=unit,
        bronze=Bronze(settings.lake_uri),
        run_id=run_id if run_id is not None else ledger.run_id,
        pipeline_version=settings.pipeline_version,
        ingested_at=utc_now(),
    )
    records = ledger.unit_slots(feed, unit.slot_date, unit.slot_hour)
    whole_day = unit.slot_hour is None
    staged = _stage_day(run, records, tables) if whole_day else _stage_hour(run, records)
    try:
        counts = UnitCounts(
            raw_lines=sum(file.raw_lines for file in staged.files),
            parsed=sum(file.parsed_rows for file in staged.files),
            quarantined=sum(file.quarantined_rows for file in staged.files),
            duplicates=staged.duplicates,
            committed=staged.outputs[0].rows,
        )
        results = _audit(run, staged, counts, ledger, accept_volume)
        if problems(results):
            _record(ledger, results, run.run_id)
            raise_if_failed(results, unit)
        set_aside = _set_aside(
            run, staged.aside, ledger=ledger, lake_uri=settings.lake_uri, as_of=as_of
        )
        quarantine_file = write_quarantine(
            settings.lake_uri, ledger.source, feed.value, unit, staged.quarantine
        )
        if whole_day:
            # The file of the day holds the rejected rows of every hour (review point R8).
            clear_hour_quarantine(settings.lake_uri, ledger.source, feed.value, unit.slot_date)
    except BaseException:
        staged.discard()
        raise

    # From here on no staged file is removed: a commit may have landed.
    failpoint(SILVER_BEFORE_COMMIT)
    commits: dict[str, CommitResult | None] = {}
    snapshots: dict[str, int | None] = {}
    for output in staged.outputs:
        if output.rows == 0:
            table = load(output.name)
            if unit_rows(table, unit) == 0:
                # An empty unit which is empty already needs no snapshot (week 2 plan 3.3).
                output.discard()
                current = table.current_snapshot()
                commits[output.name] = None
                snapshots[output.name] = current.snapshot_id if current else None
                continue
        result = commit_with_retry(partial(load, output.name), output.commit)
        commits[output.name] = result
        snapshots[output.name] = result.snapshot_id
    failpoint(SILVER_AFTER_COMMIT)

    slots = [_slot_key(feed, file.key) for file in staged.files]
    hash_of_files = fileset_hash(
        (key.slot.stamp, file.md5) for key, file in zip(slots, staged.files, strict=True)
    )
    duration_ms = round((time.monotonic() - clock) * 1000)
    versions = []
    for index, output in enumerate(staged.outputs):
        commit = commits[output.name]
        versions.append(
            PartitionVersion(
                feed=feed.value,
                table_name=output.name,
                slot_date=unit.slot_date,
                slot_hour=unit.slot_hour,
                committed_rows=output.rows,
                file_count=len(staged.files),
                fileset_hash=hash_of_files,
                content_digest=output.checksum.digest,
                pipeline_version=settings.pipeline_version,
                commit_id=commit.commit_id if commit else None,
                snapshot_id=commit.snapshot_id if commit else None,
                previous_snapshot_id=commit.previous_snapshot_id if commit else None,
                # Only the first table of a feed holds one row for each parsed line.
                raw_lines=counts.raw_lines if index == 0 else None,
                parsed_rows=counts.parsed if index == 0 else None,
                quarantined_rows=counts.quarantined if index == 0 else None,
                duplicate_rows=counts.duplicates if index == 0 else None,
                run_id=run.run_id,
                duration_ms=duration_ms,
                detail={
                    "operations": list(commit.operations) if commit else [],
                    "quarantine_file": quarantine_file,
                    "set_aside": [item["slot"] for item in set_aside],
                    **({"notes": staged.notes} if staged.notes and index == 0 else {}),
                },
            )
        )
    entries = [
        Accounting(key, file.raw_lines, file.parsed_rows, file.quarantined_rows)
        for key, file in zip(slots, staged.files, strict=True)
    ]
    with ledger.connection.transaction():
        record_versions(ledger.connection, versions, source=ledger.source)
        moved = ledger.mark_loaded(entries, snapshot_id=snapshots[names[0]], as_of=as_of)
        record_results(ledger.connection, results, source=ledger.source, run_id=run.run_id)

    log.info(
        "silver_unit_built",
        feed=feed.value,
        unit=unit.label,
        files=len(staged.files),
        loaded=len(moved),
        duration_ms=duration_ms,
        **counts.as_dict(),
    )
    first = commits[names[0]]
    return {
        "feed": feed.value,
        "unit": unit.label,
        "unit_kind": "day" if whole_day else "hour",
        "files": [file.key for file in staged.files],
        "fileset_hash": hash_of_files,
        "counts": counts.as_dict(),
        "quarantine_file": quarantine_file,
        "set_aside": set_aside,
        "loaded": len(moved),
        "tables": {
            output.name: _table_summary(output, commits[output.name], snapshots[output.name])
            for output in staged.outputs
        },
        # The first table of the feed once more, for a task which reads one table.
        "table": f"{namespace}.{names[0]}",
        "snapshot_id": snapshots[names[0]],
        "snapshot_operations": list(first.operations) if first else [],
        "checks": dict(Counter(result.status for result in results)),
        "notes": staged.notes,
        "pipeline_version": settings.pipeline_version,
        "duration_ms": duration_ms,
        # The partitions of monthly marts which the unit makes stale (blueprint 7.7).
        "mart_partitions": [unit.slot_date.year * 100 + unit.slot_date.month],
    }


def build_pending(
    *,
    as_of: datetime,
    ledger: Ledger,
    settings: Settings | None = None,
    run_id: str | None = None,
    max_units: int | None = None,
) -> dict[str, Any]:
    """Build the pending units, oldest first, ``max_units`` at most (blueprint 8.1).

    A unit which fails does not stop the units after it. The summary names each failed unit;
    the caller decides how to report the failure.
    """
    settings = settings or get_settings()
    pending = ledger.pending_units(as_of)
    selected = pending[: max_units or settings.silver_max_units]
    built: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    for item in selected:
        unit = Unit(item.slot_date, item.slot_hour)
        try:
            summary = build_unit(
                item.feed, unit, as_of=as_of, ledger=ledger, settings=settings, run_id=run_id
            )
        except Exception as error:  # one unit must not stop the units beside it
            log.error("silver_unit_failed", feed=item.feed.value, unit=unit.label, error=str(error))
            failed.append(
                {
                    "feed": item.feed.value,
                    "unit": unit.label,
                    "error": f"{type(error).__name__}: {error}"[:500],
                }
            )
            continue
        built.append(
            {
                "feed": summary["feed"],
                "unit": summary["unit"],
                "committed": summary["counts"]["committed"],
                "loaded": summary["loaded"],
                "duration_ms": summary["duration_ms"],
            }
        )
    return {
        "as_of": as_of.isoformat(),
        "pending": len(pending),
        "selected": len(selected),
        "built": built,
        "failed": failed,
        "rows": sum(item["committed"] for item in built),
        "mart_partitions": sorted(
            {unit.slot_date.year * 100 + unit.slot_date.month for unit in selected}
        ),
    }
