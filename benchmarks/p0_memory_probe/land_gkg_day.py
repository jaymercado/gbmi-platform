"""Land one closed UTC day of GDELT GKG files in bronze (week 1 plan, WP-10).

96 slots, about 0.43 GB compressed. It uses the pipeline's own land step, so every file is
verified against the master file list and a second run downloads nothing.

A probe, not a pipeline: the ledger-driven ingestion of all three feeds is phase P1.

Run on the Compose network:
    make toolbox CMD="python benchmarks/p0_memory_probe/land_gkg_day.py --day 2026-11-24"
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, date, datetime

from gbmi.config import get_settings
from gbmi.core.clock import utc_now
from gbmi.core.gdelt import Feed, Slot
from gbmi.logging import configure_logging
from gbmi.sources.gdelt.index import lines_for_hour, read_master_list
from gbmi.sources.gdelt.land import Bronze, HttpFetcher, land_file


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--day", required=True, help="closed UTC day, YYYY-MM-DD")
    args = parser.parse_args()
    configure_logging(level="WARNING")

    day = date.fromisoformat(args.day)
    fetcher = HttpFetcher()
    bronze = Bronze(get_settings().lake_uri)
    started = time.perf_counter()
    first = Slot(datetime(day.year, day.month, day.day, tzinfo=UTC))
    index = read_master_list(fetcher, oldest=first, as_of=utc_now())
    landed = [
        land_file(bronze, fetcher, line)
        for hour in range(24)
        for line in lines_for_hour(index, day, hour, Feed.GKG)
    ]
    print(
        json.dumps(
            {
                "day": args.day,
                "slots_listed": len(landed),
                "slots_expected": 96,
                "files_written": sum(file.written for file in landed),
                "compressed_bytes": sum(file.size for file in landed),
                "seconds": round(time.perf_counter() - started, 1),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
