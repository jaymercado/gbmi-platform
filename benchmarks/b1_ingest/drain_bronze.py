"""Drain the bronze backlog of a window of days through the DAG gdelt_ingest (WP-09, step 3).

After ``gbmi gdelt enqueue`` the slots of the window are listed. The script triggers
``gdelt_ingest`` through the REST API of Airflow with a large batch limit, again and again,
until no slot of the window is listed any more. Each run is written as one JSON line with the
duration and the bytes: the 30-day part of benchmark B1 (ingestion throughput).

The script stops when a run verifies no file. A file which GDELT lists and does not serve
stays listed; the runbook ``docs/runbooks/gdelt-slot-quarantined-or-overdue.md`` gives the
ways out.

Data: The GDELT Project, https://www.gdeltproject.org/

Run on the Compose network of the working stack:
    make toolbox CMD="python -m benchmarks.b1_ingest.drain_bronze --from 2026-10-26 --to 2026-11-24"
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from typing import Any

from gbmi.core.clock import parse_day, utc_now
from gbmi.core.gdelt import Feed
from gbmi.ledger.calendar import first_slot_of, last_slot_of
from gbmi.ledger.repository import Ledger, open_ledger
from gbmi.ledger.states import State
from tests.e2e.airflow import AirflowClient

DAG = "gdelt_ingest"
RESULTS = Path(__file__).resolve().parent / "results"
COUNTS = (
    "selected",
    "verified",
    "written",
    "already_in_bronze",
    "bytes",
    "rejected",
    "quarantined",
    "unavailable",
    "errors",
    "rate_limited",
    "left_for_the_next_run",
)


def window_states(ledger: Ledger, first_day: date, last_day: date) -> dict[str, int]:
    """The number of slots of the window in each state."""
    counts: dict[str, int] = {}
    for entry in ledger.status(first_day, last_day):
        counts[entry["state"]] = counts.get(entry["state"], 0) + entry["slots"]
    return counts


def task_record(ledger: Ledger, run_id: str) -> dict[str, Any]:
    """Duration and bytes of the ingest task of a run, as the task recorded them."""
    row = ledger.connection.execute(
        "SELECT started_at, duration_ms, byte_count, row_count FROM ops.pipeline_run "
        "WHERE entry_point = %s AND run_id = %s AND status = 'success' "
        "ORDER BY pipeline_run_id DESC LIMIT 1",
        (f"{ledger.source}.ingest", run_id),
    ).fetchone()
    if row is None:
        return {}
    seconds = row[1] / 1000
    megabytes = (row[2] or 0) / 1_000_000
    return {
        "started_at": row[0].isoformat(),
        "task_seconds": round(seconds, 1),
        "megabytes": round(megabytes, 1),
        "megabytes_per_second": round(megabytes / seconds, 2) if seconds else None,
        "files_per_minute": round((row[3] or 0) / seconds * 60, 1) if seconds else None,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument("--from", dest="first", required=True, help="first UTC day, YYYY-MM-DD")
    parser.add_argument("--to", dest="last", required=True, help="last UTC day, YYYY-MM-DD")
    parser.add_argument("--batch-limit", type=int, default=1500, help="files of one run at most")
    parser.add_argument("--max-runs", type=int, default=40, help="stop after this many runs")
    parser.add_argument("--source", default="live", choices=["live", "fixture"])
    parser.add_argument("--as-of", default=None, help="a fixed clock for the runs (tests)")
    parser.add_argument("--out", type=Path, default=None, help="the JSON lines file to append to")
    arguments = parser.parse_args()
    first_day, last_day = parse_day(arguments.first), parse_day(arguments.last)
    out = arguments.out or RESULTS / f"drain-{first_day}-to-{last_day}.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)
    conf: dict[str, Any] = {"source": arguments.source, "batch_limit": arguments.batch_limit}
    if arguments.as_of:
        conf["as_of"] = arguments.as_of

    client = AirflowClient()
    runs = 0
    with open_ledger() as ledger:
        slots = len(Feed) * 96 * ((last_day - first_day).days + 1)
        while True:
            before = window_states(ledger, first_day, last_day)
            listed = before.get(State.LISTED.value, 0)
            print(
                f"window {first_day} to {last_day}: {slots} slots in the calendar, states {before}"
            )
            if listed == 0:
                print("no slot of the window is listed: the drain is complete")
                break
            if runs >= arguments.max_runs:
                print(f"stopped after {runs} runs; {listed} slots are still listed")
                return 1
            run = client.run(DAG, conf, timeout=3 * 3600)
            runs += 1
            ingest = client.xcom(DAG, run.run_id, "ingest")
            record = {
                "run": runs,
                "run_id": run.run_id,
                "recorded_at": utc_now().isoformat(),
                "batch_limit": arguments.batch_limit,
                "listed_before": listed,
                "run_seconds": round(run.seconds, 1),
                **task_record(ledger, run.run_id),
                **{name: ingest.get(name, 0) for name in COUNTS},
            }
            with out.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(record, sort_keys=True) + "\n")
            print(json.dumps(record, sort_keys=True))
            if ingest.get("verified", 0) == 0:
                # Nothing landed: the listed files are not served, or the source refuses.
                first, last = first_slot_of(first_day), last_slot_of(last_day)
                left = ledger.find(first=first, last=last, states=[State.LISTED], limit=10)
                print(f"the run verified no file; {listed} slots stay listed, for example:")
                for slot in left:
                    print(f"  {slot.key.label}  attempts={slot.attempts}  {slot.last_error or ''}")
                return 1
    print(f"{runs} runs; the results are in {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
