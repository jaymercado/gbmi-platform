"""Benchmark B4: storage efficiency of one day (blueprint 15.6; week 2 plan WP-10).

For one UTC day of the stack the script measures the bytes of each feed in each form:

- the zip files in bronze, as GDELT serves the files;
- the same files as text (the uncompressed size which each zip records);
- the silver tables: the data files of the day in each Iceberg table (Parquet, ZSTD);
- for the Global Knowledge Graph also four scratch Parquet files with ZSTD, one row group for
  each hour: the 27 source columns as text; the 26 source columns without V2GCAM; the document
  rows of silver with the text of V2GCAM beside the rows; the document rows of silver alone.
  Silver drops V2GCAM (blueprint 8.4); the scratch files show what the column costs.

The script only reads bronze, silver and the ledger. The scratch files lie under ``_test/`` in
the lake and are removed at the end. A size is exact and does not vary between runs, so one
measurement is enough; the duration of the scratch write is recorded for information.

Data: The GDELT Project, https://www.gdeltproject.org/

Run on the Compose network of the working stack:
    make toolbox CMD="python -m benchmarks.b4_storage.storage --day 2026-11-04"
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import time
import uuid
import zipfile
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from gbmi.config import get_settings
from gbmi.core.clock import parse_day, utc_now
from gbmi.core.gdelt import Feed, slots_of_hour
from gbmi.core.gdelt_gkg import GKG
from gbmi.lake.catalog import load_lake_catalog
from gbmi.lake.tables import FEED_TABLES, GDELT_GKG_DOC
from gbmi.transforms.duckdb.gdelt_gkg import parse_gkg
from gbmi.transforms.duckdb.positional import BronzeFile, arrow, raw_lines
from gbmi.transforms.duckdb.runner import connect

RESULTS = Path(__file__).resolve().parent / "results"


def lake_path() -> Path:
    return Path(get_settings().lake_uri.removeprefix("file://"))


def bronze_sizes(feed: Feed, day: date) -> dict[str, int]:
    """Files, zipped bytes and text bytes of one feed and day in bronze."""
    files = zipped = text = 0
    for hour in range(24):
        for slot in slots_of_hour(day, hour):
            path = lake_path() / slot.bronze_key(feed)
            if not path.is_file():
                continue
            files += 1
            zipped += path.stat().st_size
            with zipfile.ZipFile(path) as archive:
                text += sum(member.file_size for member in archive.infolist())
    return {"files": files, "zip_bytes": zipped, "text_bytes": text}


def silver_sizes(table_name: str, day: date) -> dict[str, int]:
    """Data files, bytes and rows of one day in one silver table."""
    settings = get_settings()
    table = load_lake_catalog(settings).load_table((settings.silver_namespace, table_name))
    files = table.inspect.data_files().to_pylist()
    mine = [entry for entry in files if entry["partition"].get("slot_date") == day]
    return {
        "data_files": len(mine),
        "bytes": sum(int(entry["file_size_in_bytes"]) for entry in mine),
        "rows": sum(int(entry["record_count"]) for entry in mine),
    }


def gkg_scratch_files(day: date) -> dict[str, Any]:
    """Write the GKG day as four scratch Parquet files, hour by hour, and return the sizes.

    Two files hold the source columns as text: all 27 columns, and the 26 columns without
    V2GCAM. Two files hold the document rows of silver: with the text of V2GCAM beside the
    rows, and without. Every file has one row group for each hour and the codec of the silver
    tables, so each pair differs by the column V2GCAM alone.
    """
    scratch = lake_path() / "_test" / f"b4-{uuid.uuid4().hex[:8]}"
    scratch.mkdir(parents=True)
    names = (
        "source_columns_with_v2gcam",
        "source_columns_without_v2gcam",
        "silver_documents_with_v2gcam",
        "silver_documents_without_v2gcam",
    )
    paths = {name: scratch / f"{name}.parquet" for name in names}
    gcam = f"f[{GKG.position('V2GCAM')}]"
    record = f"f[{GKG.position('GKGRECORDID')}]"
    source_columns = ", ".join(
        f'f[{index}] as "{name}"' for index, name in enumerate(GKG.source_columns, start=1)
    )
    writers: dict[str, pq.ParquetWriter] = {}
    rows = lines = 0
    started = time.perf_counter()
    try:
        for hour in range(24):
            files = []
            for slot in slots_of_hour(day, hour):
                path = lake_path() / slot.bronze_key(Feed.GKG)
                if path.is_file():
                    content = path.read_bytes()
                    md5 = hashlib.md5(content, usedforsecurity=False).hexdigest()
                    files.append(
                        BronzeFile(key=slot.bronze_key(Feed.GKG), md5=md5, content=content)
                    )
            if not files:
                continue
            documents = parse_gkg(files, run_id="b4", pipeline_version="b4").tables[GDELT_GKG_DOC]
            connection = connect()
            try:
                connection.register("raw_lines", raw_lines(files))
                connection.register("documents", documents)
                connection.execute(
                    "create temp table split as "
                    "select slot_ts, line_number, string_split(line, chr(9)) as f from raw_lines"
                )
                source = arrow(
                    connection.sql(
                        f"select {source_columns} from split "
                        f"where len(f) = {GKG.column_count} order by slot_ts, line_number"
                    )
                )
                # The first line of a record identifier is the line which silver keeps.
                kept = arrow(
                    connection.sql(
                        "with texts as ("
                        f"  select {record} as gkg_record_id, {gcam} as v2_gcam, "
                        f"  row_number() over (partition by {record} "
                        "  order by slot_ts, line_number) as n "
                        f"  from split where len(f) = {GKG.column_count}) "
                        "select d.*, t.v2_gcam from documents d "
                        "left join texts t on t.gkg_record_id = d.gkg_record_id and t.n = 1 "
                        "order by d.gkg_record_id"
                    )
                )
            finally:
                connection.close()
            tables = {
                "source_columns_with_v2gcam": source,
                "source_columns_without_v2gcam": source.drop_columns(["V2GCAM"]),
                "silver_documents_with_v2gcam": kept,
                "silver_documents_without_v2gcam": kept.drop_columns(["v2_gcam"]),
            }
            for name, table in tables.items():
                if name not in writers:
                    writers[name] = pq.ParquetWriter(paths[name], table.schema, compression="zstd")
                writers[name].write_table(pa.Table.from_batches(table.to_batches(), table.schema))
            rows += kept.num_rows
            lines += source.num_rows
        for writer in writers.values():
            writer.close()
        seconds = time.perf_counter() - started
        sizes = {name: path.stat().st_size if path.exists() else 0 for name, path in paths.items()}
        return {
            "lines": lines,
            "documents": rows,
            "bytes": sizes,
            "seconds_to_write_the_four_files": round(seconds, 1),
        }
    finally:
        for path in paths.values():
            path.unlink(missing_ok=True)
        scratch.rmdir()


def ratio(part: int, whole: int) -> float | None:
    return round(part / whole, 4) if whole else None


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument("--day", required=True, help="a UTC day of the stack, YYYY-MM-DD")
    parser.add_argument("--out", type=Path, default=None)
    arguments = parser.parse_args()
    day = parse_day(arguments.day)
    out = arguments.out or RESULTS / f"storage-{day.isoformat()}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    feeds: dict[str, Any] = {}
    for feed in Feed:
        bronze = bronze_sizes(feed, day)
        silver = {name: silver_sizes(name, day) for name in FEED_TABLES[feed]}
        silver_bytes = sum(table["bytes"] for table in silver.values())
        feeds[feed.value] = {
            "bronze": bronze,
            "silver": silver,
            "silver_bytes": silver_bytes,
            "zip_to_text": ratio(bronze["zip_bytes"], bronze["text_bytes"]),
            "silver_to_text": ratio(silver_bytes, bronze["text_bytes"]),
            "silver_to_zip": ratio(silver_bytes, bronze["zip_bytes"]),
        }
    scratch = gkg_scratch_files(day)
    gkg = feeds[Feed.GKG.value]
    sizes = scratch["bytes"]
    scratch["to_zip"] = {
        name: ratio(size, gkg["bronze"]["zip_bytes"]) for name, size in sizes.items()
    }
    scratch["to_text"] = {
        name: ratio(size, gkg["bronze"]["text_bytes"]) for name, size in sizes.items()
    }
    scratch["share_of_v2gcam"] = {
        "source_columns": ratio(
            sizes["source_columns_with_v2gcam"] - sizes["source_columns_without_v2gcam"],
            sizes["source_columns_with_v2gcam"],
        ),
        "silver_documents": ratio(
            sizes["silver_documents_with_v2gcam"] - sizes["silver_documents_without_v2gcam"],
            sizes["silver_documents_with_v2gcam"],
        ),
    }
    result = {
        "benchmark": "B4",
        "day": day.isoformat(),
        "weekday": day.strftime("%A"),
        "measured_at": utc_now().isoformat(timespec="seconds"),
        "machine": {"platform": platform.platform(), "python": platform.python_version()},
        "versions": {"pyarrow": pa.__version__},
        "feeds": feeds,
        "gkg_as_parquet_with_zstd": scratch,
    }
    out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))
    print(f"the result is in {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
