"""Bronze export files to the silver table ``gdelt_event`` (pipeline PL-2 in miniature).

Each file is read as text. Every row is checked against the positional schema - exactly 61
columns - and every typed field is validated before it is cast, so that nothing is coerced
silently: a row that fails goes to quarantine with the reason (gate G2). Rows that pass are
*parsed* rows; they are then deduplicated on ``global_event_id``, keeping the first occurrence
in slot and line order.

The SQL is generated from ``gbmi.core.gdelt_export.EVENT_COLUMNS``, the single definition of
the positional schema.
"""

from __future__ import annotations

import io
import zipfile
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

import pyarrow as pa

from gbmi.config import Settings, get_settings
from gbmi.core.checksum import content_checksum
from gbmi.core.gdelt import Feed, parse_file_name, parse_hour, slots_of_hour
from gbmi.core.gdelt_export import EVENT_COLUMNS, EVENT_KEY, EXPORT_COLUMN_COUNT, Kind, SilverColumn
from gbmi.lake.catalog import ensure_silver_namespace, load_lake_catalog
from gbmi.lake.commit import Unit, overwrite_unit
from gbmi.lake.tables import GDELT_EVENT, GDELT_EVENT_SCHEMA, create_table_if_missing
from gbmi.logging import get_logger
from gbmi.quality.audit import UnitCounts, audit_unit
from gbmi.quality.quarantine import QUARANTINE_SCHEMA, write_quarantine
from gbmi.sources.gdelt.land import Bronze, md5_hex
from gbmi.transforms.duckdb.runner import connect

log = get_logger(__name__)

_INTEGER = "-?[0-9]+"
_DOUBLE = r"-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?"

RAW_LINES_SCHEMA = pa.schema(
    [
        pa.field("source_file", pa.string()),
        pa.field("source_md5", pa.string()),
        pa.field("slot_date", pa.date32()),
        pa.field("slot_hour", pa.int32()),
        pa.field("slot_ts", pa.timestamp("us", tz="UTC")),
        pa.field("line_number", pa.int64()),
        pa.field("line", pa.string()),
    ]
)


@dataclass(frozen=True)
class BronzeFile:
    """One landed export zip: its bronze key, its MD5 and its bytes."""

    key: str
    md5: str
    content: bytes


@dataclass(frozen=True)
class ParsedEvents:
    """The outcome of parsing a unit's files."""

    events: pa.Table
    quarantine: pa.Table
    counts: UnitCounts


def raw_lines(files: Sequence[BronzeFile]) -> pa.Table:
    """One row per line of every file, with the file it came from and its line number.

    Bytes that are not valid UTF-8 are replaced, never dropped, so the line count is exact.
    """
    rows: dict[str, list[Any]] = {name: [] for name in RAW_LINES_SCHEMA.names}
    for file in files:
        slot, _ = parse_file_name(file.key.rsplit("/", 1)[-1])
        with zipfile.ZipFile(io.BytesIO(file.content)) as archive:
            (member,) = archive.namelist()
            text = archive.read(member).decode("utf-8", errors="replace")
        lines = text.split("\n")
        if lines and lines[-1] == "":  # the file ends with a newline
            lines.pop()
        for number, line in enumerate(lines, start=1):
            rows["source_file"].append(file.key)
            rows["source_md5"].append(file.md5)
            rows["slot_date"].append(slot.slot_date)
            rows["slot_hour"].append(slot.slot_hour)
            rows["slot_ts"].append(slot.start)
            rows["line_number"].append(number)
            rows["line"].append(line)
    return pa.table(rows, schema=RAW_LINES_SCHEMA)


def _field(column: SilverColumn) -> str:
    return f"f[{column.position}]"


def _failure(column: SilverColumn) -> tuple[str, str] | None:
    """SQL that is true when the field cannot become its type, and the reason to record."""
    field = _field(column)
    label = f"{column.name} ({column.source})"
    match column.kind:
        case Kind.KEY:
            return (
                f"not regexp_full_match({field}, '{_INTEGER}') "
                f"or try_cast({field} as bigint) is null",
                f"{label}: not an integer",
            )
        case Kind.INTEGER:
            return (
                f"{field} <> '' and (not regexp_full_match({field}, '{_INTEGER}') "
                f"or try_cast({field} as integer) is null)",
                f"{label}: not an integer",
            )
        case Kind.DOUBLE:
            return (
                f"{field} <> '' and (not regexp_full_match({field}, '{_DOUBLE}') "
                f"or try_cast({field} as double) is null)",
                f"{label}: not a number",
            )
        case Kind.FLAG:
            return f"{field} not in ('', '0', '1')", f"{label}: not 0 or 1"
        case Kind.DATE8:
            return (
                f"{field} <> '' and (not regexp_full_match({field}, '[0-9]{{8}}') "
                f"or try_strptime({field}, '%Y%m%d') is null)",
                f"{label}: not a date YYYYMMDD",
            )
        case Kind.TIMESTAMP14:
            return (
                f"{field} <> '' and (not regexp_full_match({field}, '[0-9]{{14}}') "
                f"or try_strptime({field}, '%Y%m%d%H%M%S') is null)",
                f"{label}: not a timestamp YYYYMMDDHHMMSS",
            )
        case Kind.TEXT:
            return None


def _typed(column: SilverColumn) -> str:
    """SQL that turns a validated field into its typed value. An empty field is NULL."""
    field = _field(column)
    match column.kind:
        case Kind.KEY:
            return f"cast({field} as bigint)"
        case Kind.INTEGER:
            return f"cast(nullif({field}, '') as integer)"
        case Kind.DOUBLE:
            return f"cast(nullif({field}, '') as double)"
        case Kind.FLAG:
            return f"case {field} when '1' then true when '0' then false end"
        case Kind.DATE8:
            return f"cast(strptime(nullif({field}, ''), '%Y%m%d') as date)"
        case Kind.TIMESTAMP14:
            return f"strptime(nullif({field}, ''), '%Y%m%d%H%M%S') at time zone 'UTC'"
        case Kind.TEXT:
            return f"nullif({field}, '')"


def check_sql() -> str:
    """Split every raw line and give it a reason if it fails gate G2, else NULL."""
    failures = [failure for column in EVENT_COLUMNS if (failure := _failure(column)) is not None]
    cases = "\n".join(f"            when {test} then '{reason}'" for test, reason in failures)
    return f"""
    create or replace temp table checked as
    with split as (
        select *, string_split(line, chr(9)) as f from raw_lines
    )
    select
        *,
        case
            when len(f) <> {EXPORT_COLUMN_COUNT}
                then 'column_count: expected {EXPORT_COLUMN_COUNT}, found '
                    || cast(len(f) as varchar)
{cases}
        end as reason
    from split
    """  # noqa: S608


def typed_sql() -> str:
    """Typed rows of the lines that passed, numbered within each key in slot and line order."""
    columns = ",\n        ".join(f"{_typed(column)} as {column.name}" for column in EVENT_COLUMNS)
    return f"""
    create or replace temp table typed as
    select
        {columns},
        slot_date,
        slot_hour,
        slot_ts,
        source_file as _source_file,
        source_md5 as _source_md5,
        slot_ts as _slot_ts,
        row_number() over (
            partition by cast({_field(EVENT_COLUMNS[0])} as bigint) order by slot_ts, line_number
        ) as occurrence
    from checked
    where reason is null
    """  # noqa: S608


def parse_events(
    files: Sequence[BronzeFile],
    *,
    run_id: str | None,
    pipeline_version: str,
    ingested_at: datetime | None = None,
) -> ParsedEvents:
    """Parse export files into typed, deduplicated rows and quarantined rows."""
    ingested_at = ingested_at or datetime.now(UTC)
    connection = connect()
    try:
        connection.register("raw_lines", raw_lines(files))
        connection.execute(check_sql())
        connection.execute(typed_sql())
        names = ", ".join(
            field.name for field in GDELT_EVENT_SCHEMA.fields if not _is_run_column(field.name)
        )
        events = _arrow(
            connection.sql(
                f"select {names}, $ingested_at as _ingested_at, $run_id as _run_id, "  # noqa: S608
                f"$pipeline_version as _pipeline_version from typed where occurrence = 1 "
                f"order by {EVENT_KEY}",
                params={
                    "ingested_at": ingested_at,
                    "run_id": run_id,
                    "pipeline_version": pipeline_version,
                },
            )
        )
        quarantine = _arrow(
            connection.sql(
                "select source_file, source_md5, slot_ts, line_number, reason, line as raw_line, "
                "$run_id as _run_id, $ingested_at as _ingested_at from checked "
                "where reason is not null order by source_file, line_number",
                params={"ingested_at": ingested_at, "run_id": run_id},
            )
        )
        row = connection.execute(
            "select (select count(*) from raw_lines), (select count(*) from typed), "
            "(select count(*) from checked where reason is not null)"
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        raise RuntimeError("count query returned no row")
    total, parsed, quarantined = (int(value) for value in row)
    events = events.select([field.name for field in GDELT_EVENT_SCHEMA.fields]).cast(
        GDELT_EVENT_SCHEMA.as_arrow()
    )
    counts = UnitCounts(
        raw_lines=total,
        parsed=parsed,
        quarantined=quarantined,
        duplicates=parsed - events.num_rows,
        committed=events.num_rows,
    )
    return ParsedEvents(events=events, quarantine=quarantine.cast(QUARANTINE_SCHEMA), counts=counts)


def _arrow(result: Any) -> pa.Table:
    """The Arrow table of a DuckDB result (its stubs allow None, which a SELECT never gives)."""
    table: pa.Table | None = result.to_arrow_table()
    if table is None:
        raise RuntimeError("the query returned no result set")
    return table


def _is_run_column(name: str) -> bool:
    return name in ("_ingested_at", "_run_id", "_pipeline_version")


def build_event_hour(
    slot_date: date, slot_hour: int, *, run_id: str | None = None, settings: Settings | None = None
) -> dict[str, Any]:
    """Build one slot-hour of ``gdelt_event``: parse, quarantine, audit, commit.

    The unit is replaced with an overwrite filtered on (slot_date, slot_hour), so a rerun gives
    the same rows and a new snapshot (blueprint 5.6). Returns a summary of built-in types.
    """
    settings = settings or get_settings()
    unit = Unit(slot_date, slot_hour)
    bronze = Bronze(settings.lake_uri)
    files = [
        BronzeFile(key=key, md5=md5_hex(content), content=content)
        for slot in slots_of_hour(slot_date, slot_hour)
        if (content := bronze.read(key := slot.bronze_key(Feed.EXPORT))) is not None
    ]
    if not files:
        # Without a ledger an empty hour cannot be told from a land step that did not run.
        raise FileNotFoundError(
            f"no export files in bronze for {unit.label}: run the land step first"
        )

    parsed = parse_events(files, run_id=run_id, pipeline_version=settings.pipeline_version)
    quarantine_file = write_quarantine(
        settings.lake_uri, "gdelt", Feed.EXPORT.value, unit, parsed.quarantine
    )

    catalog = load_lake_catalog(settings)
    namespace = ensure_silver_namespace(catalog, settings)
    table = create_table_if_missing(catalog, namespace, GDELT_EVENT, GDELT_EVENT_SCHEMA)
    result = overwrite_unit(
        table,
        parsed.events,
        unit,
        audit=lambda data: audit_unit(parsed.counts, data, unit, key=EVENT_KEY),
        run_id=run_id,
        pipeline_version=settings.pipeline_version,
    )
    checksum = content_checksum(table.scan(row_filter=unit.row_filter).to_arrow())
    log.info("silver_unit_committed", table=GDELT_EVENT, unit=unit.label, **parsed.counts.as_dict())
    return {
        "table": result.table,
        "unit": unit.label,
        "files": [file.key for file in files],
        "counts": parsed.counts.as_dict(),
        "quarantine_file": quarantine_file,
        "snapshot_id": result.snapshot_id,
        "previous_snapshot_id": result.previous_snapshot_id,
        "snapshot_operations": list(result.operations),
        "checksum": {"rows": checksum.rows, "digest": checksum.digest},
        "pipeline_version": settings.pipeline_version,
        # The partitions of monthly marts that this unit makes stale (blueprint 7.7).
        "mart_partitions": [slot_date.year * 100 + slot_date.month],
    }


def build_event_hour_task(hour: str, run_id: str | None = None) -> dict[str, Any]:
    """Task entry point: build the slot-hour ``YYYY-MM-DDTHH`` of ``gdelt_event``."""
    slot_date, slot_hour = parse_hour(hour)
    return build_event_hour(slot_date, slot_hour, run_id=run_id)
