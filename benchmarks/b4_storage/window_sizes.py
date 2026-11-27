"""Benchmark B4, the totals of a window: bytes in bronze and in silver for a range of days.

For each feed the script sums the zip bytes of the loaded files (from the ledger) and the
bytes, files and rows of the silver data files of the days (from the Iceberg metadata of the
current snapshot). The script only reads.

Data: The GDELT Project, https://www.gdeltproject.org/

Run on the Compose network of the working stack:
    make toolbox CMD="python -m benchmarks.b4_storage.window_sizes \
        --from 2026-10-26 --to 2026-11-24"
"""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path
from typing import Any

from gbmi.config import get_settings
from gbmi.core.clock import parse_day, utc_now
from gbmi.core.gdelt import Feed
from gbmi.lake.catalog import load_lake_catalog
from gbmi.lake.tables import FEED_TABLES
from gbmi.ledger.repository import open_ledger

RESULTS = Path(__file__).resolve().parent / "results"


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument("--from", dest="first", required=True, help="first UTC day, YYYY-MM-DD")
    parser.add_argument("--to", dest="last", required=True, help="last UTC day, YYYY-MM-DD")
    parser.add_argument("--out", type=Path, default=None)
    arguments = parser.parse_args()
    first_day, last_day = parse_day(arguments.first), parse_day(arguments.last)
    out = arguments.out or RESULTS / f"window-sizes-{first_day}-to-{last_day}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    settings = get_settings()
    catalog = load_lake_catalog(settings)
    with open_ledger() as ledger:
        bronze = {
            row[0]: {"files": int(row[1]), "zip_bytes": int(row[2]), "raw_lines": int(row[3])}
            for row in ledger.connection.execute(
                "SELECT feed, count(*), sum(expected_bytes), sum(raw_lines) FROM ops.source_slot "
                "WHERE source = %s AND state = 'loaded' "
                "AND (slot_ts AT TIME ZONE 'UTC')::date BETWEEN %s AND %s GROUP BY feed",
                (ledger.source, first_day, last_day),
            ).fetchall()
        }
    feeds: dict[str, Any] = {}
    for feed in Feed:
        tables: dict[str, Any] = {}
        for name in FEED_TABLES[feed]:
            table = catalog.load_table((settings.silver_namespace, name))
            by_day: dict[str, int] = {}
            rows = files = 0
            for entry in table.inspect.data_files().to_pylist():
                day = entry["partition"].get("slot_date")
                if day is None or not first_day <= day <= last_day:
                    continue
                by_day[day.isoformat()] = by_day.get(day.isoformat(), 0) + int(
                    entry["file_size_in_bytes"]
                )
                rows += int(entry["record_count"])
                files += 1
            sizes = list(by_day.values())
            tables[name] = {
                "days": len(by_day),
                "data_files": files,
                "rows": rows,
                "bytes": sum(sizes),
                "bytes_of_a_day": {
                    "median": statistics.median(sizes),
                    "min": min(sizes),
                    "max": max(sizes),
                }
                if sizes
                else None,
            }
        silver_bytes = sum(table["bytes"] for table in tables.values())
        mine = bronze.get(feed.value, {"files": 0, "zip_bytes": 0, "raw_lines": 0})
        feeds[feed.value] = {
            "bronze": mine,
            "silver": tables,
            "silver_bytes": silver_bytes,
            "silver_to_zip": round(silver_bytes / mine["zip_bytes"], 4)
            if mine["zip_bytes"]
            else None,
        }
    result = {
        "benchmark": "B4",
        "window": {"from": first_day.isoformat(), "to": last_day.isoformat()},
        "measured_at": utc_now().isoformat(timespec="seconds"),
        "feeds": feeds,
        "bronze_bytes": sum(feed["bronze"]["zip_bytes"] for feed in feeds.values()),
        "silver_bytes": sum(feed["silver_bytes"] for feed in feeds.values()),
    }
    out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))
    print(f"the result is in {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
