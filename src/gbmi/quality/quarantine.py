"""Rows rejected by gate G2, kept with the reason (blueprint 7.2).

A unit's rejected rows are one Parquet file under a deterministic key, so a rerun replaces it,
and a rerun that rejects nothing removes it:

    quarantine/<source>/<feed>/dt=<slot date>/hour=<HH>/rows.parquet
"""

from __future__ import annotations

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from gbmi.lake.commit import Unit

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
    filesystem, root = pafs.FileSystem.from_uri(lake_uri)
    key = quarantine_key(source, feed, unit)
    path = f"{root.rstrip('/')}/{key}"
    if rows.num_rows == 0:
        if filesystem.get_file_info(path).type != pafs.FileType.NotFound:
            filesystem.delete_file(path)
        return None
    filesystem.create_dir(path.rsplit("/", 1)[0], recursive=True)
    pq.write_table(rows.cast(QUARANTINE_SCHEMA), path, filesystem=filesystem, compression="zstd")
    return key
