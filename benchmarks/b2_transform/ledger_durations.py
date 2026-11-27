"""Benchmark B2, the parts which need no new run: build durations which the ledger recorded.

Every build of a unit writes one row for each table to ``ops.silver_partition``, with the
duration of the build. The script reads two samples:

- the day builds of the 30-day backfill of WP-09 (run identifiers ``backfill__…``): seconds,
  raw lines and raw lines for each second, for each feed and day;
- the hour builds of the live run (difference D16 of the week 2 plan: the hourly job runs every
  15 minutes, so one hour is built up to four times): for each feed and hour the number of
  builds, the seconds of all builds together and the seconds of the last build.

The script only reads.

Run on the Compose network of the working stack:
    make toolbox CMD="python -m benchmarks.b2_transform.ledger_durations \
        --from 2026-10-26 --to 2026-11-24 \
        --live-from 2026-11-26T21:15:00+00:00 --live-to 2026-11-27T08:30:00+00:00"
"""

from __future__ import annotations

import argparse
import json
import statistics
from datetime import date, datetime
from pathlib import Path
from typing import Any

from gbmi.core.clock import parse_day, utc_now
from gbmi.core.gdelt import Feed
from gbmi.lake.tables import FEED_TABLES
from gbmi.ledger.repository import open_ledger

RESULTS = Path(__file__).resolve().parent / "results"
#: A complete hour holds four files of a feed.
FILES_OF_AN_HOUR = 4


def spread(values: list[float]) -> dict[str, float] | None:
    if not values:
        return None
    return {
        "median": round(statistics.median(values), 2),
        "min": round(min(values), 2),
        "max": round(max(values), 2),
    }


def backfill(connection: Any, source: str, first_day: date, last_day: date) -> dict[str, Any]:
    """The day builds of the backfill: one row for each feed and day."""
    feeds: dict[str, Any] = {}
    for feed in Feed:
        rows = connection.execute(
            "SELECT p.slot_date, p.duration_ms, p.raw_lines, p.committed_rows, p.file_count, "
            "p.run_id, (SELECT sum(s.expected_bytes) FROM ops.source_slot s "
            " WHERE s.source = p.source AND s.feed = p.feed "
            " AND (s.slot_ts AT TIME ZONE 'UTC')::date = p.slot_date) "
            "FROM ops.silver_partition p WHERE p.source = %s AND p.table_name = %s "
            "AND p.unit_kind = 'day' AND p.run_id LIKE 'backfill\\_\\_%%' "
            "AND p.slot_date >= %s AND p.slot_date <= %s ORDER BY p.slot_date",
            (source, FEED_TABLES[feed][0], first_day, last_day),
        ).fetchall()
        days = [
            {
                "day": row[0].isoformat(),
                "weekday": row[0].strftime("%a"),
                "seconds": round(row[1] / 1000, 2),
                "raw_lines": int(row[2]),
                "committed_rows": int(row[3]),
                "files": int(row[4]),
                "zip_bytes": int(row[6] or 0),
                "raw_lines_per_second": round(row[2] / (row[1] / 1000)) if row[1] else None,
                "run_id": row[5],
            }
            for row in rows
        ]
        feeds[feed.value] = {
            "days": len(days),
            "seconds_total": round(sum(day["seconds"] for day in days), 1),
            "seconds": spread([day["seconds"] for day in days]),
            "raw_lines_per_second": spread(
                [day["raw_lines_per_second"] for day in days if day["raw_lines_per_second"]]
            ),
            "megabytes_zipped_per_second": spread(
                [day["zip_bytes"] / 1e6 / day["seconds"] for day in days if day["seconds"]]
            ),
            "by_day": days,
        }
    return feeds


def hour_builds(connection: Any, source: str, first: datetime, last: datetime) -> dict[str, Any]:
    """The hour builds whose commit lies in the given time span, by feed and hour."""
    feeds: dict[str, Any] = {}
    for feed in Feed:
        rows = connection.execute(
            "SELECT slot_date, slot_hour, count(*), sum(duration_ms), "
            "(array_agg(duration_ms ORDER BY partition_version_id DESC))[1], "
            "(array_agg(file_count ORDER BY partition_version_id DESC))[1], "
            "(array_agg(committed_rows ORDER BY partition_version_id DESC))[1] "
            "FROM ops.silver_partition WHERE source = %s AND table_name = %s "
            "AND unit_kind = 'hour' AND committed_at >= %s AND committed_at <= %s "
            "GROUP BY slot_date, slot_hour ORDER BY slot_date, slot_hour",
            (source, FEED_TABLES[feed][0], first, last),
        ).fetchall()
        hours = [
            {
                "day": row[0].isoformat(),
                "hour": int(row[1]),
                "builds": int(row[2]),
                "seconds_of_all_builds": round(row[3] / 1000, 2),
                "seconds_of_the_last_build": round(row[4] / 1000, 2),
                "files_in_the_last_build": int(row[5]),
                "rows_in_the_last_build": int(row[6]),
            }
            for row in rows
        ]
        whole = [
            hour
            for hour in hours
            if hour["builds"] == FILES_OF_AN_HOUR
            and hour["files_in_the_last_build"] == FILES_OF_AN_HOUR
        ]
        feeds[feed.value] = {
            "hours": len(hours),
            "builds": sum(hour["builds"] for hour in hours),
            "hours_with_four_builds_and_four_files": len(whole),
            "seconds_of_four_builds": spread([hour["seconds_of_all_builds"] for hour in whole]),
            "seconds_of_the_last_build": spread(
                [hour["seconds_of_the_last_build"] for hour in whole]
            ),
            "four_builds_against_the_last_build": spread(
                [
                    hour["seconds_of_all_builds"] / hour["seconds_of_the_last_build"]
                    for hour in whole
                    if hour["seconds_of_the_last_build"]
                ]
            ),
            "by_hour": hours,
        }
    return feeds


def hourly_runs(connection: Any, first: datetime, last: datetime) -> dict[str, Any]:
    """The runs of the hourly silver job in the time span: units and seconds of each run."""
    rows = connection.execute(
        "SELECT started_at, duration_ms, jsonb_array_length(detail -> 'built'), row_count, status "
        "FROM ops.pipeline_run WHERE entry_point = 'gdelt.silver_units' "
        "AND started_at >= %s AND started_at <= %s ORDER BY started_at",
        (first, last),
    ).fetchall()
    return {
        "runs": len(rows),
        "runs_with_status_success": sum(1 for row in rows if row[4] == "success"),
        "seconds": spread([row[1] / 1000 for row in rows]),
        "units_built": spread([float(row[2] or 0) for row in rows]),
        "seconds_total": round(sum(row[1] for row in rows) / 1000, 1),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument("--from", dest="first", required=True, help="first UTC day of the backfill")
    parser.add_argument("--to", dest="last", required=True, help="last UTC day of the backfill")
    parser.add_argument("--live-from", default=None, help="start of the live sample, ISO 8601")
    parser.add_argument("--live-to", default=None, help="end of the live sample, ISO 8601")
    parser.add_argument("--out", type=Path, default=None)
    arguments = parser.parse_args()
    first_day, last_day = parse_day(arguments.first), parse_day(arguments.last)
    out = arguments.out or RESULTS / f"ledger-durations-{first_day}-to-{last_day}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    result: dict[str, Any] = {
        "benchmark": "B2",
        "measured_at": utc_now().isoformat(timespec="seconds"),
        "backfill": {"from": first_day.isoformat(), "to": last_day.isoformat()},
    }
    with open_ledger() as ledger:
        result["backfill"]["feeds"] = backfill(
            ledger.connection, ledger.source, first_day, last_day
        )
        if arguments.live_from and arguments.live_to:
            first = datetime.fromisoformat(arguments.live_from)
            last = datetime.fromisoformat(arguments.live_to)
            result["live_run"] = {
                "from": first.isoformat(),
                "to": last.isoformat(),
                "hour_builds": hour_builds(ledger.connection, ledger.source, first, last),
                "runs_of_the_hourly_job": hourly_runs(ledger.connection, first, last),
            }
    out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    def short(value: Any) -> Any:
        if isinstance(value, dict):
            return {k: short(v) for k, v in value.items() if k not in ("by_day", "by_hour")}
        return value

    print(json.dumps(short(result), indent=2, sort_keys=True))
    print(f"the result is in {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
