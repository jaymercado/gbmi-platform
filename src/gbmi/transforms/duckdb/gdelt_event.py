"""Bronze export files to the silver table ``gdelt_event`` (pipeline PL-2).

Rows which pass gate G2 are *parsed* rows; they are then deduplicated on ``global_event_id``,
keeping the first occurrence in slot and line order. The generic steps - raw lines, the row
check, the typed cast - are in ``gbmi.transforms.duckdb.positional``, generated from the feed
definition ``gbmi.core.gdelt_export.EXPORT``.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime
from typing import Any

from gbmi.config import Settings, get_settings
from gbmi.core.checksum import content_checksum
from gbmi.core.gdelt import Feed, parse_hour, slots_of_hour
from gbmi.core.gdelt_export import EVENT_KEY, EXPORT
from gbmi.lake.catalog import ensure_silver_namespace, load_lake_catalog
from gbmi.lake.commit import Unit, overwrite_unit
from gbmi.lake.tables import (
    GDELT_EVENT,
    GDELT_EVENT_SCHEMA,
    business_names,
    create_table_if_missing,
)
from gbmi.logging import get_logger
from gbmi.quality.audit import audit_unit
from gbmi.quality.quarantine import write_quarantine
from gbmi.sources.gdelt.land import Bronze, md5_hex
from gbmi.transforms.duckdb.positional import (
    BronzeFile,
    Parsed,
    arrow,
    check_files,
    line_counts,
    quarantine_rows,
    raw_lines,
    typed_columns,
)
from gbmi.transforms.duckdb.runner import connect

log = get_logger(__name__)

__all__ = ["BronzeFile", "ParsedEvents", "build_event_hour", "parse_events", "raw_lines"]

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
    finally:
        connection.close()
    events = events.select([field.name for field in GDELT_EVENT_SCHEMA.fields]).cast(
        GDELT_EVENT_SCHEMA.as_arrow()
    )
    return Parsed(
        feed=Feed.EXPORT,
        tables={GDELT_EVENT: events},
        quarantine=quarantine,
        counts=line_counts(accepted, events.num_rows),
        files=accepted,
        rejected=rejected,
    )


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
