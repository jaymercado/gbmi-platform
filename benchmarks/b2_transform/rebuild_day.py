"""Benchmark B2: the routine transformation of one day (blueprint 15.6; WP-10).

Each run rebuilds one real day of one feed on the working stack with the operator command
``gbmi silver build-day``, in a child process, and records:

- the wall-clock seconds of the command;
- the CPU seconds and the peak resident memory of the child process (from ``wait4``);
- the highest memory of the container while the child ran: ``memory.current`` of the control
  group, read every 0.1 seconds (the value includes the page cache), and the highest
  anonymous memory (``anon`` in ``memory.stat``), which the kernel cannot give back;
- raw lines and committed rows for each second, from the partition version of the run.

A rebuild adds one partition version for each table of the feed (the exception of the week 2
plan, section 3.3). The script compares the content checksum of the new version with the
checksum of the version before: the two have to be equal.

Data: The GDELT Project, https://www.gdeltproject.org/

Run on the Compose network of the working stack, with the three GDELT DAGs paused:
    make toolbox CMD="python -m benchmarks.b2_transform.rebuild_day --day 2026-11-04 --runs 3"

Or inside the scheduler container, where a task process runs and the memory limit of the
container applies (the folder benchmarks is not mounted there, so the script comes from
standard input and the result goes to the scratch area of the lake):
    docker exec -i gbmi-airflow-scheduler-1 /opt/venvs/gbmi/bin/python - --day 2026-11-04 \
        --feed gkg --label scheduler --out /lake/_test/rebuild.json \
        < benchmarks/b2_transform/rebuild_day.py
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import subprocess
import sys
import threading
import time
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pyarrow
import pyiceberg

from gbmi.core.clock import parse_day, utc_now
from gbmi.core.gdelt import Feed
from gbmi.lake.tables import FEED_TABLES
from gbmi.ledger.repository import open_ledger

#: The script also runs from standard input (inside the scheduler container, where the folder
#: benchmarks is not mounted); the option --out then names the result file.
RESULTS = Path(globals().get("__file__", "rebuild_day.py")).resolve().parent / "results"
CGROUP = Path("/sys/fs/cgroup")
SAMPLE_SECONDS = 0.1


class ContainerMemory:
    """The highest memory of the container between start() and stop()."""

    def __init__(self) -> None:
        self.current = 0
        self.anonymous = 0
        self.samples = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._watch, daemon=True)

    def _read(self) -> None:
        try:
            self.current = max(self.current, int((CGROUP / "memory.current").read_text()))
            for line in (CGROUP / "memory.stat").read_text().splitlines():
                if line.startswith("anon "):
                    self.anonymous = max(self.anonymous, int(line.split()[1]))
                    break
            self.samples += 1
        except (OSError, ValueError):
            pass

    def _watch(self) -> None:
        while not self._stop.is_set():
            self._read()
            self._stop.wait(SAMPLE_SECONDS)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join()
        self._read()


def versions(day: date, feed: Feed, limit: int = 2) -> dict[str, list[dict[str, Any]]]:
    """The newest day-level partition versions of each table of the feed, newest first."""
    found: dict[str, list[dict[str, Any]]] = {}
    with open_ledger() as ledger:
        for table in FEED_TABLES[feed]:
            rows = ledger.connection.execute(
                "SELECT partition_version_id, content_digest, raw_lines, parsed_rows, "
                "duplicate_rows, committed_rows, file_count, duration_ms, run_id "
                "FROM ops.silver_partition WHERE source = %s AND table_name = %s "
                "AND slot_date = %s AND unit_kind = 'day' "
                "ORDER BY partition_version_id DESC LIMIT %s",
                (ledger.source, table, day, limit),
            ).fetchall()
            found[table] = [
                {
                    "partition_version_id": row[0],
                    "content_digest": row[1],
                    "raw_lines": row[2],
                    "parsed_rows": row[3],
                    "duplicate_rows": row[4],
                    "committed_rows": row[5],
                    "files": row[6],
                    "build_milliseconds": row[7],
                    "run_id": row[8],
                }
                for row in rows
            ]
    return found


def one_run(day: date, feed: Feed, index: int, label: str) -> dict[str, Any]:
    before = versions(day, feed, limit=1)
    # The command of the same environment as the interpreter, whatever PATH holds.
    beside = Path(sys.executable).parent / "gbmi"
    program = str(beside) if beside.exists() else "gbmi"
    command = [program, "silver", "build-day", "--day", day.isoformat(), "--feed", feed.value]
    memory = ContainerMemory()
    started_at = utc_now()
    memory.start()
    started = time.perf_counter()
    child = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)  # noqa: S603
    errors = child.stderr.read() if child.stderr is not None else b""
    _, status, usage = os.wait4(child.pid, 0)
    seconds = time.perf_counter() - started
    memory.stop()
    child.returncode = os.waitstatus_to_exitcode(status)
    after = versions(day, feed, limit=1)
    tables = {}
    for table in FEED_TABLES[feed]:
        old = before[table][0] if before[table] else None
        new = after[table][0] if after[table] else None
        added = new is not None and (
            old is None or new["partition_version_id"] != old["partition_version_id"]
        )
        tables[table] = {
            "new_version": new["partition_version_id"] if added and new else None,
            "committed_rows": new["committed_rows"] if new else None,
            "raw_lines": new["raw_lines"] if new else None,
            "duplicate_rows": new["duplicate_rows"] if new else None,
            "build_milliseconds": new["build_milliseconds"] if added and new else None,
            "checksum_equals_the_version_before": bool(
                added and old and new and new["content_digest"] == old["content_digest"]
            ),
        }
    first = tables[FEED_TABLES[feed][0]]
    raw_lines = first["raw_lines"] or 0
    committed = sum(table["committed_rows"] or 0 for table in tables.values())
    return {
        "run": index,
        "label": label,
        "feed": feed.value,
        "day": day.isoformat(),
        "started_at": started_at.isoformat(timespec="seconds"),
        "exit_code": child.returncode,
        "error": errors.decode("utf-8", "replace")[-600:] if child.returncode else None,
        "wall_seconds": round(seconds, 2),
        "cpu_seconds": round(usage.ru_utime + usage.ru_stime, 2),
        "cpu_user_seconds": round(usage.ru_utime, 2),
        "cpu_system_seconds": round(usage.ru_stime, 2),
        # Linux gives the peak resident memory in KiB; the fields are MiB (1,048,576 bytes).
        "peak_resident_mib": round(usage.ru_maxrss / 1024, 1),
        "container_peak_mib": round(memory.current / 2**20, 1),
        "container_peak_anonymous_mib": round(memory.anonymous / 2**20, 1),
        "container_memory_samples": memory.samples,
        "raw_lines": raw_lines,
        "raw_lines_per_second": round(raw_lines / seconds) if seconds else None,
        "committed_rows": committed,
        "committed_rows_per_second": round(committed / seconds) if seconds else None,
        "tables": tables,
    }


def spread(values: list[float]) -> dict[str, float]:
    return {"median": statistics.median(values), "min": min(values), "max": max(values)}


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument("--day", required=True, help="a UTC day of the stack, YYYY-MM-DD")
    parser.add_argument("--feed", action="append", choices=[feed.value for feed in Feed])
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--label", default="warm", help="what kind of run, for the report")
    parser.add_argument("--out", type=Path, default=None)
    arguments = parser.parse_args()
    day = parse_day(arguments.day)
    feeds = [Feed(name) for name in arguments.feed] if arguments.feed else list(Feed)
    label = arguments.label
    name = f"rebuild-{day.isoformat()}-{label.replace(' ', '-')}"
    out = arguments.out or RESULTS / f"{name}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    runs: list[dict[str, Any]] = []
    index = 0
    for feed in feeds:
        for _ in range(arguments.runs):
            index += 1
            run = one_run(day, feed, index, label)
            runs.append(run)
            print(json.dumps({k: v for k, v in run.items() if k != "tables"}), flush=True)
    summary = {}
    for feed in feeds:
        mine = [run for run in runs if run["feed"] == feed.value]
        summary[feed.value] = {
            "runs": len(mine),
            "wall_seconds": spread([run["wall_seconds"] for run in mine]),
            "cpu_seconds": spread([run["cpu_seconds"] for run in mine]),
            "peak_resident_mib": spread([run["peak_resident_mib"] for run in mine]),
            "container_peak_mib": spread([run["container_peak_mib"] for run in mine]),
            "container_peak_anonymous_mib": spread(
                [run["container_peak_anonymous_mib"] for run in mine]
            ),
            "raw_lines_per_second": spread([run["raw_lines_per_second"] for run in mine]),
            "every_run_ended_with_exit_code_0": all(run["exit_code"] == 0 for run in mine),
            "every_checksum_equals_the_version_before": all(
                table["checksum_equals_the_version_before"]
                for run in mine
                for table in run["tables"].values()
            ),
        }
    limit = (
        (CGROUP / "memory.max").read_text().strip() if (CGROUP / "memory.max").exists() else None
    )
    result = {
        "benchmark": "B2",
        "day": day.isoformat(),
        "weekday": day.strftime("%A"),
        "label": label,
        "measured_at": utc_now().isoformat(timespec="seconds"),
        "machine": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "cpus": os.cpu_count(),
            "container_memory_limit": limit,
        },
        "versions": {
            "duckdb": duckdb.__version__,
            "pyarrow": pyarrow.__version__,
            "pyiceberg": pyiceberg.__version__,
        },
        "command": "gbmi silver build-day --day <day> --feed <feed>",
        "summary": summary,
        "runs": runs,
    }
    out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    print(f"the result is in {out}")
    passed = all(
        part["every_run_ended_with_exit_code_0"]
        and part["every_checksum_equals_the_version_before"]
        for part in summary.values()
    )
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
