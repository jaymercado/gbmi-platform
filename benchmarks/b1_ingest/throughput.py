"""Benchmark B1: ingestion throughput on a fixed window of six hours (blueprint 15.6; WP-10).

The dataset is fixed and recorded: the 72 files of the three feeds for six hours of one UTC
day, as the ledger of the working stack lists the files (address, size and MD5 hash). Each run
downloads the 72 files from the GDELT servers into a scratch lake through the code of the
ingest job: download, gate G1, write to bronze, ledger update. The number of parallel
downloads changes between the runs.

The script reads the ledger of the stack and writes nothing to the stack: the ledger of a run
is a database which the script creates and drops, and the lake of a run is a directory under
``/lake/_test`` which the script removes. Every run opens new connections, so no run takes
an open connection over from the run before.

Data: The GDELT Project, https://www.gdeltproject.org/

Run on the Compose network of the working stack, with the three GDELT DAGs paused:
    make toolbox CMD="python -m benchmarks.b1_ingest.throughput --day 2026-11-04 --from-hour 12"
"""

from __future__ import annotations

import argparse
import json
import platform
import shutil
import statistics
import time
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

from gbmi.core.clock import parse_day, utc_now
from gbmi.core.gdelt import Feed, Slot
from gbmi.ledger.repository import SlotKey, open_ledger
from gbmi.ledger.scratch import scratch_database
from gbmi.ledger.states import State
from gbmi.logging import configure_logging
from gbmi.sources.gdelt.index import IndexLine
from gbmi.sources.gdelt.ingest import download_listed, listings
from gbmi.sources.gdelt.land import Bronze, HttpFetcher

RESULTS = Path(__file__).resolve().parent / "results"
SCRATCH = Path("/lake/_test")
PAUSE_SECONDS = 5.0


def window_lines(day: date, first_hour: int, hours: int) -> list[IndexLine]:
    """The files of the window, as the ledger of the stack lists the files."""
    start = datetime(day.year, day.month, day.day, first_hour, tzinfo=UTC)
    end = start + timedelta(hours=hours)
    with open_ledger() as ledger:
        rows = ledger.connection.execute(
            "SELECT feed, slot_ts, url, expected_bytes, expected_md5 FROM ops.source_slot "
            "WHERE source = %s AND slot_ts >= %s AND slot_ts < %s AND url IS NOT NULL "
            "ORDER BY slot_ts, feed",
            (ledger.source, start, end),
        ).fetchall()
    return [
        IndexLine(
            size=int(size), md5=md5, url=url, slot=Slot(slot_ts.astimezone(UTC)), feed=Feed(feed)
        )
        for feed, slot_ts, url, size, md5 in rows
    ]


def one_run(lines: list[IndexLine], concurrency: int, index: int) -> dict[str, Any]:
    """Download the window once with the given number of parallel downloads."""
    root = SCRATCH / f"b1-{uuid.uuid4().hex[:12]}"
    root.mkdir(parents=True)
    lake_uri = f"file://{root}"
    try:
        with scratch_database() as dsn, open_ledger(dsn, run_id=f"b1-run-{index}") as ledger:
            as_of = utc_now()
            ledger.ensure_expected([SlotKey(line.feed, line.slot) for line in lines])
            ledger.mark_listed(listings(lines), as_of=as_of)
            selected = ledger.pending_downloads(limit=len(lines) + 1)
            bronze = Bronze(lake_uri)
            started_at = utc_now()
            started = time.perf_counter()
            counts = download_listed(
                selected,
                ledger=ledger,
                bronze=bronze,
                fetcher=HttpFetcher(),
                lake_uri=lake_uri,
                as_of=as_of,
                concurrency=concurrency,
            )
            seconds = time.perf_counter() - started
            verified = len(ledger.find(states=[State.VERIFIED]))
            landed_bytes = sum(bronze.sizes("raw").values())
    finally:
        shutil.rmtree(root, ignore_errors=True)
    return {
        "run": index,
        "parallel_downloads": concurrency,
        "started_at": started_at.isoformat(timespec="seconds"),
        "seconds": round(seconds, 2),
        "selected": len(selected),
        "verified": verified,
        "bytes": landed_bytes,
        "megabytes_per_second": round(landed_bytes / 1e6 / seconds, 2),
        "files_per_minute": round(verified / seconds * 60, 1),
        "outcomes": dict(counts),
    }


def spread(values: list[float]) -> dict[str, float]:
    return {"median": statistics.median(values), "min": min(values), "max": max(values)}


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument("--day", required=True, help="a UTC day of the stack, YYYY-MM-DD")
    parser.add_argument("--from-hour", type=int, default=12, help="first hour of the window")
    parser.add_argument("--hours", type=int, default=6)
    parser.add_argument("--parallel", type=int, action="append", help="default: 1, 4 and 8")
    parser.add_argument("--runs", type=int, default=3, help="runs for each number of downloads")
    parser.add_argument("--out", type=Path, default=None)
    arguments = parser.parse_args()
    configure_logging(level="WARNING")
    day = parse_day(arguments.day)
    levels = arguments.parallel or [1, 4, 8]
    lines = window_lines(day, arguments.from_hour, arguments.hours)
    expected = arguments.hours * 4 * len(Feed)
    if len(lines) != expected:
        print(f"the ledger lists {len(lines)} files of the window; {expected} are needed")
        return 1
    name = f"throughput-{day.isoformat()}-h{arguments.from_hour:02d}-{arguments.hours}h"
    out = arguments.out or RESULTS / f"{name}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    runs: list[dict[str, Any]] = []
    index = 0
    # The numbers of downloads take turns, so a drift of the network touches each number alike.
    for _ in range(arguments.runs):
        for concurrency in levels:
            index += 1
            run = one_run(lines, concurrency, index)
            runs.append(run)
            print(json.dumps(run), flush=True)
            time.sleep(PAUSE_SECONDS)
    summary = {}
    for concurrency in levels:
        mine = [run for run in runs if run["parallel_downloads"] == concurrency]
        summary[str(concurrency)] = {
            "runs": len(mine),
            "seconds": spread([run["seconds"] for run in mine]),
            "megabytes_per_second": spread([run["megabytes_per_second"] for run in mine]),
            "files_per_minute": spread([run["files_per_minute"] for run in mine]),
            "every_file_verified": all(run["verified"] == len(lines) for run in mine),
        }
    result = {
        "benchmark": "B1",
        "dataset": {
            "day": day.isoformat(),
            "first_hour": arguments.from_hour,
            "hours": arguments.hours,
            "files": len(lines),
            "bytes": sum(line.size for line in lines),
            "bytes_by_feed": {
                feed.value: sum(line.size for line in lines if line.feed is feed) for feed in Feed
            },
            "file_list": [
                {"file": line.file_name, "bytes": line.size, "md5": line.md5} for line in lines
            ],
        },
        "measured_at": utc_now().isoformat(timespec="seconds"),
        "machine": {"platform": platform.platform(), "python": platform.python_version()},
        "versions": {"httpx": httpx.__version__},
        "method": (
            "each run: a new throwaway ledger, a new scratch lake, new connections; the timed "
            "part is download, gate G1, the write to bronze and the ledger update of the 72 files"
        ),
        "summary": summary,
        "runs": runs,
    }
    out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    print(f"the result is in {out}")
    return 0 if all(part["every_file_verified"] for part in summary.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
