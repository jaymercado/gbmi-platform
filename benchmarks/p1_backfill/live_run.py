"""What the scheduled jobs did during the live run, read from the ledger database (WP-09).

Every task of the three GDELT DAGs writes one row to ``ops.pipeline_run`` for each try, with
the scheduled time (``as_of``), the real start, the status and a summary. The script reads
the rows of a time range and prints JSON with:

- the tries of each job by status, and the starts which came more than a minute after the
  scheduled time (a late start is the sign of a laptop which slept);
- each failed try with the error;
- what the ingest runs landed, and the runs which found a gap behind them;
- what the hourly silver runs built;
- the same counts for each period in which the laptop was awake, when the option
  ``--awake`` names the result of ``awake_periods.py``.

The script only reads.

Run on the Compose network of the working stack:
    make toolbox CMD="python -m benchmarks.p1_backfill.live_run \
        --from 2026-11-26T20:47:52+00:00 --to 2026-11-27T17:45:00+00:00 \
        --awake benchmarks/p1_backfill/results/awake-periods.json"
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from gbmi.core.clock import utc_now
from gbmi.ledger.repository import open_ledger

JOBS = {
    "gdelt.ingest": "ingest",
    "gdelt.reconcile": "reconcile",
    "gdelt.report_overdue": "report_overdue",
    "gdelt.silver_units": "silver_hourly",
    "gdelt.silver_day": "silver_daily",
}
LATE = timedelta(seconds=60)


def instant(text: str) -> datetime:
    return datetime.fromisoformat(text).astimezone(UTC)


def rows_of(connection: Any, first: datetime, last: datetime) -> list[dict[str, Any]]:
    names: tuple[str, ...] = (
        "entry_point",
        "run_id",
        "try_number",
        "as_of",
        "started_at",
        "ended_at",
        "status",
        "row_count",
        "error",
        "detail",
    )
    found = connection.execute(
        "SELECT entry_point, run_id, try_number, as_of, started_at, ended_at, status, row_count, "
        "error, detail FROM ops.pipeline_run WHERE entry_point = ANY(%s) "
        "AND started_at >= %s AND started_at < %s ORDER BY started_at, pipeline_run_id",
        (list(JOBS), first, last),
    ).fetchall()
    return [dict(zip(names, row, strict=True)) for row in found]


def is_late(row: dict[str, Any]) -> bool:
    return row["as_of"] is not None and row["started_at"] - row["as_of"] > LATE


def counts(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Tries by job and status, late starts, and the work which the tries did."""
    by_job: dict[str, dict[str, int]] = {}
    for row in rows:
        job = by_job.setdefault(
            JOBS[row["entry_point"]], {"success": 0, "failed": 0, "started_late": 0}
        )
        job[row["status"]] += 1
        if is_late(row):
            job["started_late"] += 1
    ingest = [r for r in rows if r["entry_point"] == "gdelt.ingest" and r["status"] == "success"]
    hourly = [
        r for r in rows if r["entry_point"] == "gdelt.silver_units" and r["status"] == "success"
    ]

    def total(items: list[dict[str, Any]], key: str) -> int:
        return sum(int((item["detail"] or {}).get(key) or 0) for item in items)

    return {
        "tries": by_job,
        "files_verified": total(ingest, "verified"),
        "bytes_landed": total(ingest, "bytes"),
        "answers_http_404": total(ingest, "unavailable"),
        "files_rejected_by_gate_g1": total(ingest, "rejected"),
        "ingest_runs_after_a_gap": sum(1 for r in ingest if (r["detail"] or {}).get("after_a_gap")),
        "units_built": sum(len((r["detail"] or {}).get("built") or []) for r in hourly),
        "units_failed": sum(len((r["detail"] or {}).get("failed") or []) for r in hourly),
        "rows_committed_by_the_hourly_job": sum(int(r["row_count"] or 0) for r in hourly),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument("--from", dest="first", required=True, help="start, ISO 8601")
    parser.add_argument("--to", dest="last", required=True, help="end, ISO 8601")
    parser.add_argument("--awake", type=Path, default=None, help="result of awake_periods.py")
    parser.add_argument("--out", type=Path, default=None)
    arguments = parser.parse_args()
    first, last = instant(arguments.first), instant(arguments.last)
    with open_ledger() as ledger:
        rows = rows_of(ledger.connection, first, last)
    result: dict[str, Any] = {
        "from": first.isoformat(),
        "to": last.isoformat(),
        "read_at": utc_now().isoformat(timespec="seconds"),
        "whole_range": counts(rows),
        "failed_tries": [
            {
                "job": JOBS[row["entry_point"]],
                "run_id": row["run_id"],
                "try_number": row["try_number"],
                "started_at": row["started_at"].isoformat(timespec="seconds"),
                "error": (row["error"] or "")[:160],
            }
            for row in rows
            if row["status"] == "failed"
        ],
        "ingest_runs_after_a_gap": [
            {
                "run_id": row["run_id"],
                "started_at": row["started_at"].isoformat(timespec="seconds"),
                "files_listed": (row["detail"] or {}).get("listed"),
                "files_verified": (row["detail"] or {}).get("verified"),
            }
            for row in rows
            if row["entry_point"] == "gdelt.ingest"
            and row["status"] == "success"
            and (row["detail"] or {}).get("after_a_gap")
        ],
    }
    if arguments.awake is not None:
        periods = json.loads(arguments.awake.read_text(encoding="utf-8"))["periods"]
        by_period = []
        for period in periods:
            start = max(instant(period["awake_from"]), first)
            end = min(instant(period["awake_to"]) if period["awake_to"] else last, last)
            if end <= start:
                continue
            inside = [row for row in rows if start <= row["started_at"] < end]
            by_period.append(
                {
                    "awake_from": start.isoformat(timespec="seconds"),
                    "awake_to": end.isoformat(timespec="seconds"),
                    "minutes": round((end - start).total_seconds() / 60, 1),
                    "sleep_cause": period["sleep_cause"],
                    **counts(inside),
                }
            )
        result["awake_periods"] = by_period
        awake_rows = {
            id(row)
            for period in by_period
            for row in rows
            if instant(period["awake_from"]) <= row["started_at"] < instant(period["awake_to"])
        }
        result["while_the_laptop_slept"] = counts([r for r in rows if id(r) not in awake_rows])
    text = json.dumps(result, indent=2, sort_keys=True, default=str)
    if arguments.out is not None:
        arguments.out.parent.mkdir(parents=True, exist_ok=True)
        arguments.out.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
