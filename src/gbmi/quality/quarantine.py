"""Rows rejected by gate G2, kept with the reason (blueprint 7.2).

A unit's rejected rows are one Parquet file under a deterministic key, so a rerun replaces it,
and a rerun that rejects nothing removes it:

    quarantine/<source>/<feed>/dt=<slot date>/hour=<HH>/rows.parquet

A whole file which a gate rejected is kept beside the rows, under its own name:

    quarantine/<source>/<feed>/dt=<slot date>/<file name>

Each file is written as one whole object (gbmi.lake.objects): one PUT request on S3. A file is
removed with one DELETE request, which leaves no marker object behind.
"""

from __future__ import annotations

from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq

from gbmi.lake.commit import Unit
from gbmi.lake.objects import delete_object, write_object

QUARANTINE_SCHEMA = pa.schema(
    [
        pa.field("source_file", pa.string()),
        pa.field("source_md5", pa.string()),
        pa.field("slot_ts", pa.timestamp("us", tz="UTC")),
        pa.field("line_number", pa.int64()),
        pa.field("reason", pa.string()),
        pa.field("raw_line", pa.string()),
        pa.field("_run_id", pa.string()),
        pa.field("_ingested_at", pa.timestamp("us", tz="UTC")),
    ]
)


def quarantine_key(source: str, feed: str, unit: Unit) -> str:
    """Key of a unit's rejected rows, relative to the lake root."""
    hour = "day" if unit.slot_hour is None else f"hour={unit.slot_hour:02d}"
    return f"quarantine/{source}/{feed}/dt={unit.slot_date.isoformat()}/{hour}/rows.parquet"


def write_quarantine(
    lake_uri: str, source: str, feed: str, unit: Unit, rows: pa.Table
) -> str | None:
    """Replace the unit's quarantine file; returns its key, or None when no row was rejected."""
    key = quarantine_key(source, feed, unit)
    if rows.num_rows == 0:
        delete_object(lake_uri, key)
        return None
    # The rejected rows of a unit are few: the file is built in memory and written whole.
    sink = pa.BufferOutputStream()
    pq.write_table(rows.cast(QUARANTINE_SCHEMA), sink, compression="zstd")
    write_object(lake_uri, key, sink.getvalue().to_pybytes())
    return key


def quarantined_file_key(source: str, feed: str, slot_date: date, file_name: str) -> str:
    """Key of a rejected file, relative to the lake root."""
    return f"quarantine/{source}/{feed}/dt={slot_date.isoformat()}/{file_name}"


def write_quarantined_file(
    lake_uri: str, source: str, feed: str, slot_date: date, file_name: str, content: bytes
) -> str:
    """Keep the bytes of a rejected file; returns the key. A later rejection replaces the bytes."""
    key = quarantined_file_key(source, feed, slot_date, file_name)
    write_object(lake_uri, key, content)
    return key


def clear_hour_quarantine(lake_uri: str, source: str, feed: str, slot_date: date) -> int:
    """Remove the quarantine files of the hour units of a day; returns the files removed.

    A day build writes one quarantine file for the whole day. The files of the hour builds of
    the same day would count the same rows a second time.
    """
    removed = 0
    for hour in range(24):
        if delete_object(lake_uri, quarantine_key(source, feed, Unit(slot_date, hour))):
            removed += 1
    return removed
