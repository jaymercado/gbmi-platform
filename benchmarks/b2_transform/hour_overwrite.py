"""Benchmark B2, the comparison for difference D17 of the week 2 plan (WP-10, step 3).

The day build finalises a day: each table then holds one data file for the day. When a file
of a finalised day arrives later, the routine job builds the whole day again (D17). The
alternative is an overwrite of the one hour: Iceberg then writes the data file of the day
again without the rows of the hour, and adds the rows of the hour as a second file. The
script measures both ways on a scratch copy of one real day.

The copy lives in a *world* (``drills/common.py``): a throwaway database and a lake directory
under ``/lake/_test`` with a catalog of its own. The files of the day are read from bronze of
the working stack and pass through the ingest code, with gate G1, into the world. The working
stack is only read; the world is removed at the end.

For each feed the script builds the day once and then repeats two steps: (A) the day build
again, (B) the overwrite of one hour. Each step runs in a child process and gives wall-clock
seconds, CPU seconds and the peak resident memory of the child. After each hour overwrite the script
compares the content checksum of the day with the checksum before, and counts the data files.

Data: The GDELT Project, https://www.gdeltproject.org/

Run on the Compose network of the working stack:
    make toolbox CMD="python -m benchmarks.b2_transform.hour_overwrite --day 2026-11-04"
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any

from benchmarks.b1_ingest.throughput import window_lines
from drills.common import World, fixtures_dir, world
from gbmi.config import get_settings
from gbmi.core.clock import parse_day, utc_now
from gbmi.core.gdelt import Feed
from gbmi.lake.commit import Unit
from gbmi.lake.tables import FEED_TABLES
from gbmi.ledger.repository import SlotKey
from gbmi.sources.gdelt.index import IndexLine, IndexTail
from gbmi.sources.gdelt.ingest import download_listed, listings
from gbmi.sources.gdelt.land import Bronze, Fetched, FileUnavailableError

RESULTS = Path(__file__).resolve().parent / "results"

#: The child process of step B: parse the four files of the hour and overwrite the hour.
HOUR_OVERWRITE = """
import json, sys, time
from datetime import date
from gbmi.config import get_settings
from gbmi.core.gdelt import Feed, slots_of_hour
from gbmi.lake.catalog import load_lake_catalog
from gbmi.lake.commit import Unit, overwrite_unit
from gbmi.lake.tables import FEED_TABLES
from gbmi.sources.gdelt.land import Bronze, md5_hex
from gbmi.transforms.build import PARSERS
from gbmi.transforms.duckdb.positional import BronzeFile

day, hour, feed = date.fromisoformat(sys.argv[1]), int(sys.argv[2]), Feed(sys.argv[3])
settings = get_settings()
bronze = Bronze(settings.lake_uri)
files = []
for slot in slots_of_hour(day, hour):
    key = slot.bronze_key(feed)
    content = bronze.read(key)
    files.append(BronzeFile(key=key, md5=md5_hex(content), content=content))
started = time.perf_counter()
parsed = PARSERS[feed](files, run_id="d17-hour", pipeline_version=settings.pipeline_version)
parse_seconds = time.perf_counter() - started
catalog = load_lake_catalog(settings)
commits = {}
for name in FEED_TABLES[feed]:
    table = catalog.load_table((settings.silver_namespace, name))
    tick = time.perf_counter()
    overwrite_unit(
        table, parsed.tables[name], Unit(day, hour),
        run_id="d17-hour", pipeline_version=settings.pipeline_version,
    )
    commits[name] = {
        "commit_seconds": round(time.perf_counter() - tick, 3),
        "rows_of_the_hour": parsed.tables[name].num_rows,
    }
print(json.dumps({"parse_seconds": round(parse_seconds, 3), "commits": commits}))
"""


class BronzeCopy:
    """Serves the files of the working lake in place of the GDELT servers."""

    def __init__(self, bronze: Bronze) -> None:
        self._bronze = bronze

    def last_update(self) -> str:
        raise NotImplementedError

    def master_tail(self, tail_bytes: int) -> IndexTail:
        raise NotImplementedError

    def fetch(self, line: IndexLine) -> Fetched:
        content = self._bronze.read(line.slot.bronze_key(line.feed))
        if content is None:
            raise FileUnavailableError(f"{line.file_name} is not in bronze of the stack")
        return Fetched(content=content, published_at=None)


def environment(place: World) -> dict[str, str]:
    """The environment of a gbmi process which works on the database and the lake of a world."""
    return {
        **os.environ,
        "GBMI_PG_DSN": place.dsn,
        "GBMI_LAKE_BACKEND": "local",
        "GBMI_LAKE_URI": place.settings.lake_uri,
        "GBMI_CATALOG_URI": place.settings.catalog_uri,
        "GBMI_PIPELINE_VERSION": place.settings.pipeline_version,
        "GBMI_FIXTURES_DIR": str(fixtures_dir()),
    }


#: Linux counts the memory which a child holds between fork and exec, a copy of the memory of
#: the parent, in the peak of the child. The script itself grows large (the script holds the
#: rows of a day for the checksums), so each step runs under a small wrapper process, and
#: the wrapper measures the step.
WRAPPER = """
import json, os, subprocess, sys, time
started = time.perf_counter()
process = subprocess.Popen(sys.argv[1:], stdout=subprocess.PIPE)
output = process.stdout.read()
_, status, usage = os.wait4(process.pid, 0)
print(json.dumps({
    "exit_code": os.waitstatus_to_exitcode(status),
    "wall_seconds": round(time.perf_counter() - started, 2),
    "cpu_seconds": round(usage.ru_utime + usage.ru_stime, 2),
    "peak_resident_mib": round(usage.ru_maxrss / 1024, 1),
    "output": output.decode("utf-8", "replace"),
}))
"""


def child(place: World, arguments: list[str]) -> dict[str, Any]:
    """Run a Python child process in the world, under the wrapper which measures the child."""
    command = [sys.executable, "-c", WRAPPER, sys.executable, *arguments]
    done = subprocess.run(  # noqa: S603
        command, env=environment(place), capture_output=True, text=True, check=False
    )
    try:
        measured: dict[str, Any] = json.loads(done.stdout)
    except json.JSONDecodeError:
        measured = {"exit_code": done.returncode or 1, "wall_seconds": 0.0, "output": ""}
    measured["error"] = done.stderr[-800:] if measured["exit_code"] else None
    return measured


def day_files(place: World, feed: Feed, day: date) -> dict[str, dict[str, int]]:
    """Data files, bytes and rows which each table of the feed holds for the day."""
    sizes = {}
    for name in FEED_TABLES[feed]:
        entries = [
            entry
            for entry in place.table(name).inspect.data_files().to_pylist()
            if entry["partition"].get("slot_date") == day
        ]
        sizes[name] = {
            "data_files": len(entries),
            "bytes": sum(int(entry["file_size_in_bytes"]) for entry in entries),
            "rows": sum(int(entry["record_count"]) for entry in entries),
        }
    return sizes


def last_commit(place: World, feed: Feed) -> dict[str, dict[str, str]]:
    """What the newest snapshot of each table says about the files which the commit wrote."""
    wanted = ("operation", "added-data-files", "deleted-data-files", "added-files-size")
    summaries = {}
    for name in FEED_TABLES[feed]:
        snapshot = place.table(name).current_snapshot()
        summary = snapshot.summary if snapshot and snapshot.summary else None
        fields = (
            {"operation": summary.operation.value, **summary.additional_properties}
            if summary
            else {}
        )
        summaries[name] = {key: str(fields[key]) for key in wanted if key in fields}
    return summaries


def measure_feed(place: World, feed: Feed, day: date, hour: int, rounds: int) -> dict[str, Any]:
    build = ["-c", "from gbmi.cli import app; app()", "silver", "build-day"]
    build += ["--day", day.isoformat(), "--feed", feed.value]
    overwrite = ["-c", HOUR_OVERWRITE, day.isoformat(), str(hour), feed.value]
    unit = Unit(day)
    first = child(place, build)
    if first["exit_code"] != 0:
        raise RuntimeError(f"the first day build failed: {first['error']}")
    runs: list[dict[str, Any]] = []
    for index in range(1, rounds + 1):
        rebuild = child(place, build)
        rebuild.pop("output")
        before = {name: place.checksum(name, unit) for name in FEED_TABLES[feed]}
        runs.append(
            {
                "round": index,
                "step": "day build",
                **rebuild,
                "files_after": day_files(place, feed, day),
                "commit": last_commit(place, feed),
            }
        )
        hourly = child(place, overwrite)
        inner = json.loads(hourly.pop("output") or "{}") if hourly["exit_code"] == 0 else {}
        after = {name: place.checksum(name, unit) for name in FEED_TABLES[feed]}
        runs.append(
            {
                "round": index,
                "step": "hour overwrite",
                **hourly,
                **inner,
                "files_after": day_files(place, feed, day),
                "commit": last_commit(place, feed),
                "day_holds_the_same_rows": all(
                    before[name].digest == after[name].digest
                    and before[name].rows == after[name].rows
                    for name in FEED_TABLES[feed]
                ),
            }
        )
        print(
            json.dumps(
                {"feed": feed.value, **{k: v for k, v in runs[-2].items() if k != "commit"}}
            ),
            flush=True,
        )
        print(
            json.dumps(
                {"feed": feed.value, **{k: v for k, v in runs[-1].items() if k != "commit"}}
            ),
            flush=True,
        )

    def spread(step: str) -> dict[str, float]:
        values = [run["wall_seconds"] for run in runs if run["step"] == step]
        return {"median": statistics.median(values), "min": min(values), "max": max(values)}

    return {
        "runs": runs,
        "wall_seconds_of_the_day_build": spread("day build"),
        "wall_seconds_of_the_hour_overwrite": spread("hour overwrite"),
        "every_step_ended_with_exit_code_0": all(run["exit_code"] == 0 for run in runs),
        "every_hour_overwrite_kept_the_rows_of_the_day": all(
            run["day_holds_the_same_rows"] for run in runs if run["step"] == "hour overwrite"
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument("--day", required=True, help="a UTC day of the stack, YYYY-MM-DD")
    parser.add_argument("--hour", type=int, default=12, help="the hour which step B overwrites")
    parser.add_argument("--feed", action="append", choices=[feed.value for feed in Feed])
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--out", type=Path, default=None)
    arguments = parser.parse_args()
    day = parse_day(arguments.day)
    feeds = [Feed(name) for name in arguments.feed] if arguments.feed else [Feed.EXPORT, Feed.GKG]
    out = arguments.out or RESULTS / f"hour-overwrite-{day.isoformat()}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    lines = [line for line in window_lines(day, 0, 24) if line.feed in feeds]
    result: dict[str, Any] = {
        "benchmark": "B2, comparison for D17",
        "day": day.isoformat(),
        "hour": arguments.hour,
        "measured_at": utc_now().isoformat(timespec="seconds"),
        "machine": {"platform": platform.platform(), "python": platform.python_version()},
        "feeds": {},
    }
    with world("b2-d17") as place:
        as_of = utc_now()
        place.ledger.ensure_expected([SlotKey(line.feed, line.slot) for line in lines])
        place.ledger.mark_listed(listings(lines), as_of=as_of)
        selected = place.ledger.pending_downloads(limit=len(lines) + 1)
        landed = download_listed(
            selected,
            ledger=place.ledger,
            bronze=Bronze(place.settings.lake_uri),
            fetcher=BronzeCopy(Bronze(get_settings().lake_uri)),
            lake_uri=place.settings.lake_uri,
            as_of=as_of,
            concurrency=4,
        )
        result["files_copied_into_the_world"] = {"listed": len(lines), "outcomes": dict(landed)}
        for feed in feeds:
            result["feeds"][feed.value] = measure_feed(
                place, feed, day, arguments.hour, arguments.rounds
            )
    out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    short = {
        feed: {key: value for key, value in facts.items() if key != "runs"}
        for feed, facts in result["feeds"].items()
    }
    print(json.dumps(short, indent=2, sort_keys=True))
    print(f"the result is in {out}")
    passed = all(
        facts["every_step_ended_with_exit_code_0"]
        and facts["every_hour_overwrite_kept_the_rows_of_the_day"]
        for facts in result["feeds"].values()
    )
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
