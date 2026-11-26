"""Bronze export files to the silver table ``gdelt_event`` (pipeline PL-2).

Rows which pass gate G2 are *parsed* rows; they are then deduplicated on ``global_event_id``,
keeping the first occurrence in slot and line order. The generic steps - raw lines, the row
check, the typed cast - are in ``gbmi.transforms.duckdb.positional``, generated from the feed
definition ``gbmi.core.gdelt_export.EXPORT``.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

from gbmi.core.gdelt import Feed
from gbmi.core.gdelt_export import EVENT_KEY, EXPORT
from gbmi.lake.tables import (
    GDELT_EVENT,
    GDELT_EVENT_SCHEMA,
    business_names,
)
from gbmi.transforms.duckdb.positional import (
    BronzeFile,
    Parsed,
    arrow,
    check_files,
    duplicate_rows,
    line_counts,
    quarantine_rows,
    raw_lines,
    typed_columns,
)
from gbmi.transforms.duckdb.runner import connect

__all__ = ["BronzeFile", "ParsedEvents", "parse_events", "raw_lines"]

#: The outcome of parsing export files; the events are ``parsed.events``.
ParsedEvents = Parsed


def typed_sql() -> str:
    """Typed rows of the lines that passed, numbered within each key in slot and line order."""
    return f"""
    create or replace temp table typed as
    select
        {typed_columns(EXPORT)},
        slot_date,
        slot_hour,
        slot_ts,
        source_file as _source_file,
        source_md5 as _source_md5,
        slot_ts as _slot_ts,
        row_number() over (
            partition by cast(f[{EXPORT.position("GLOBALEVENTID")}] as bigint)
            order by slot_ts, line_number
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
    file_threshold: float | None = None,
) -> Parsed:
    """Parse export files into typed, deduplicated rows and quarantined rows.

    With ``file_threshold``, a file with a larger share of failed rows is rejected as a whole.
    """
    ingested_at = ingested_at or datetime.now(UTC)
    connection = connect()
    try:
        accepted, rejected = check_files(connection, files, EXPORT, file_threshold=file_threshold)
        connection.execute(typed_sql())
        names = ", ".join(business_names(GDELT_EVENT_SCHEMA))
        events = arrow(
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
        quarantine = quarantine_rows(connection, run_id=run_id, ingested_at=ingested_at)
        duplicates = duplicate_rows(connection)
    finally:
        connection.close()
    events = events.select([field.name for field in GDELT_EVENT_SCHEMA.fields]).cast(
        GDELT_EVENT_SCHEMA.as_arrow()
    )
    return Parsed(
        feed=Feed.EXPORT,
        tables={GDELT_EVENT: events},
        quarantine=quarantine,
        counts=line_counts(accepted, events.num_rows, duplicates),
        files=accepted,
        rejected=rejected,
    )
