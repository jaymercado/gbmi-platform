"""Build one whole day of GDELT GKG into a scratch Iceberg table (week 1 plan, WP-10).

The question: does a daily silver build fit the task-process budget (blueprint 9.1)? The
probe runs inside the scheduler container, where a task process would run, and uses the daily
pattern chosen in spike SK1: each file is read as text, split by DuckDB (with a memory limit
and a spill directory), written as one row group, and the day is committed with add_files.
Every column stays text and the GCAM column is dropped, as in silver (blueprint 4.2).

A probe, not a parser: nothing is typed, derived or audited. The GKG parser is phase P1.

Run inside the scheduler container (the benchmarks directory is not mounted there):
    docker compose exec -T -e PROBE_DAY=2026-11-24 airflow-scheduler \\
        /opt/venvs/gbmi/bin/python - < benchmarks/p0_memory_probe/gkg_day_probe.py
"""

from __future__ import annotations

import io
import json
import os
import resource
import time
import zipfile
from collections.abc import Iterator
from datetime import date

import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import DateType, IntegerType, NestedField, StringType

from gbmi.config import get_settings
from gbmi.core.gdelt import Feed, slots_of_hour
from gbmi.lake.catalog import ensure_silver_namespace, load_lake_catalog
from gbmi.lake.commit import overwrite_day_streaming
from gbmi.lake.tables import create_table_if_missing
from gbmi.sources.gdelt.land import Bronze
from gbmi.transforms.duckdb.runner import connect

TABLE = "probe_gkg_day"
MEMORY_LIMIT = os.environ.get("PROBE_DUCKDB_MEMORY_LIMIT", "1GB")
SPILL_DIRECTORY = "/tmp/gbmi-duckdb-spill"  # noqa: S108 - inside the container

# The 27 columns of GKG 2.1, in file order (GKG Codebook V2.1).
GKG_COLUMNS = (
    "gkg_record_id", "date", "source_collection_identifier", "source_common_name",
    "document_identifier", "counts", "v2_counts", "themes", "v2_themes", "locations",
    "v2_locations", "persons", "v2_persons", "organizations", "v2_organizations", "v2_tone",
    "dates", "v2_gcam", "sharing_image", "related_images", "social_image_embeds",
    "social_video_embeds", "quotations", "all_names", "amounts", "translation_info", "extras",
)  # fmt: skip
KEPT = tuple(name for name in GKG_COLUMNS if name != "v2_gcam")

SCHEMA = Schema(
    NestedField(1, "slot_date", DateType(), required=True),
    NestedField(2, "slot_hour", IntegerType(), required=True),
    *(
        NestedField(index, name, StringType(), required=False)
        for index, name in enumerate(KEPT, start=3)
    ),
)


def main() -> None:
    day = date.fromisoformat(os.environ["PROBE_DAY"])
    settings = get_settings()
    bronze = Bronze(settings.lake_uri)
    catalog = load_lake_catalog(settings)
    table = create_table_if_missing(
        catalog, ensure_silver_namespace(catalog, settings), TABLE, SCHEMA
    )
    arrow_schema = table.schema().as_arrow()
    connection = connect(memory_limit=MEMORY_LIMIT, temp_directory=SPILL_DIRECTORY)
    columns = ", ".join(f"f[{GKG_COLUMNS.index(name) + 1}] as {name}" for name in KEPT)
    totals = {"files": 0, "raw_lines": 0, "rows": 0, "raw_bytes": 0, "compressed_bytes": 0}

    def batches() -> Iterator[pa.Table]:
        for hour in range(24):
            for slot in slots_of_hour(day, hour):
                content = bronze.read(slot.bronze_key(Feed.GKG))
                if content is None:
                    continue
                with zipfile.ZipFile(io.BytesIO(content)) as archive:
                    raw = archive.read(archive.namelist()[0])
                lines = raw.decode("utf-8", errors="replace").split("\n")
                if lines and lines[-1] == "":
                    lines.pop()
                connection.register("raw", pa.table({"line": lines}))
                batch = connection.sql(
                    f"select date '{day.isoformat()}' as slot_date, {hour}::integer as slot_hour, "
                    f"{columns} from (select string_split(line, chr(9)) as f from raw) "
                    f"where len(f) = {len(GKG_COLUMNS)}"
                ).to_arrow_table()
                connection.unregister("raw")
                totals["files"] += 1
                totals["raw_lines"] += len(lines)
                totals["rows"] += batch.num_rows
                totals["raw_bytes"] += len(raw)
                totals["compressed_bytes"] += len(content)
                yield batch.cast(arrow_schema)

    started = time.perf_counter()
    result = overwrite_day_streaming(table, day, batches())
    seconds = time.perf_counter() - started
    connection.close()

    files = table.inspect.data_files()
    usage = resource.getrusage(resource.RUSAGE_SELF)
    print(
        json.dumps(
            {
                "day": day.isoformat(),
                **totals,
                "rows_not_27_columns": totals["raw_lines"] - totals["rows"],
                "seconds": round(seconds, 1),
                # ru_maxrss is in kilobytes on Linux: the peak resident set of this process.
                "peak_rss_mib": round(usage.ru_maxrss / 1024, 1),
                "cpu_seconds": round(usage.ru_utime + usage.ru_stime, 1),
                "duckdb_memory_limit": MEMORY_LIMIT,
                "snapshot_operations": list(result.operations),
                "data_files": files.num_rows,
                "data_file_bytes": sum(files["file_size_in_bytes"].to_pylist()),
                "table": result.table,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
