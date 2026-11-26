"""Bronze mentions files to the silver table ``gdelt_mention`` (pipeline PL-2).

One row is one mention of one event in one article. One file can hold the same pair of event
and article more than once: the rows differ in the character offsets and in the confidence
(week 2 plan, F5). The logical key is (``global_event_id``, ``mention_identifier``,
``slot_ts``); the row with the highest confidence stays, then the first line, and the other
rows count as duplicates.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

from gbmi.core.gdelt import Feed
from gbmi.core.gdelt_mentions import MENTION_KEY, MENTIONS
from gbmi.lake.tables import GDELT_MENTION, GDELT_MENTION_SCHEMA, business_names
from gbmi.transforms.duckdb.positional import (
    BronzeFile,
    Parsed,
    arrow,
    check_files,
    duplicate_rows,
    line_counts,
    quarantine_rows,
    typed_columns,
)
from gbmi.transforms.duckdb.runner import connect


def typed_sql() -> str:
    """Typed rows of the lines that passed, numbered within each key: best confidence first."""
    event = f"cast(f[{MENTIONS.position('GLOBALEVENTID')}] as bigint)"
    article = f"f[{MENTIONS.position('MentionIdentifier')}]"
    confidence = f"try_cast(nullif(f[{MENTIONS.position('Confidence')}], '') as integer)"
    return f"""
    create or replace temp table typed as
    select
        {typed_columns(MENTIONS)},
        slot_date,
        slot_hour,
        slot_ts,
        source_file as _source_file,
        source_md5 as _source_md5,
        slot_ts as _slot_ts,
        row_number() over (
            partition by {event}, {article}, slot_ts
            order by {confidence} desc nulls last, line_number
        ) as occurrence
    from checked
    where reason is null
    """  # noqa: S608


def parse_mentions(
    files: Sequence[BronzeFile],
    *,
    run_id: str | None,
    pipeline_version: str,
    ingested_at: datetime | None = None,
    file_threshold: float | None = None,
) -> Parsed:
    """Parse mentions files into typed, deduplicated rows and quarantined rows."""
    ingested_at = ingested_at or datetime.now(UTC)
    connection = connect()
    try:
        accepted, rejected = check_files(connection, files, MENTIONS, file_threshold=file_threshold)
        connection.execute(typed_sql())
        names = ", ".join(business_names(GDELT_MENTION_SCHEMA))
        mentions = arrow(
            connection.sql(
                f"select {names}, $ingested_at as _ingested_at, $run_id as _run_id, "  # noqa: S608
                f"$pipeline_version as _pipeline_version from typed where occurrence = 1 "
                f"order by {', '.join(MENTION_KEY)}",
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
    mentions = mentions.select([field.name for field in GDELT_MENTION_SCHEMA.fields]).cast(
        GDELT_MENTION_SCHEMA.as_arrow()
    )
    return Parsed(
        feed=Feed.MENTIONS,
        tables={GDELT_MENTION: mentions},
        quarantine=quarantine,
        counts=line_counts(accepted, mentions.num_rows, duplicates),
        files=accepted,
        rejected=rejected,
    )
