"""The evidence of the 30-day backfill, read from the ledger of the stack (WP-09, step 5).

For a window of UTC days the script prints, as JSON:

- the slots of the window by state (exit-gate row 1: each slot is loaded or confirmed missing);
- the completeness of each day from the ledger code, and the smallest value (row 2);
- the accounting of the loaded files: raw lines = parsed rows + quarantined rows (row 3);
- the day-level partition version of each table for each day (row 1);
- the durations of the day builds, by feed.

The script only reads.

Run on the Compose network of the working stack:
    make toolbox CMD="python -m benchmarks.p1_backfill.evidence --from 2026-10-26 --to 2026-11-24"
"""

from __future__ import annotations

import argparse
import json
import statistics
from datetime import date
from typing import Any

from gbmi.core.clock import parse_day, utc_now
from gbmi.core.gdelt import Feed
from gbmi.lake.tables import FEED_TABLES
from gbmi.ledger.calendar import days_between, first_slot_of, last_slot_of
from gbmi.ledger.repository import Ledger, open_ledger

TABLES = [name for feed in Feed for name in FEED_TABLES[feed]]


def slot_states(ledger: Ledger, first_day: date, last_day: date) -> dict[str, Any]:
    by_state: dict[str, int] = {}
    by_feed: dict[str, dict[str, int]] = {}
    for entry in ledger.status(first_day, last_day):
        by_state[entry["state"]] = by_state.get(entry["state"], 0) + entry["slots"]
        feed = by_feed.setdefault(entry["feed"], {})
        feed[entry["state"]] = feed.get(entry["state"], 0) + entry["slots"]
    days = (last_day - first_day).days + 1
    return {
        "calendar_slots": len(Feed) * 96 * days,
        "ledger_slots": sum(by_state.values()),
        "by_state": by_state,
        "by_feed": by_feed,
        "closed": by_state.get("loaded", 0) + by_state.get("confirmed_missing", 0),
    }


def completeness(ledger: Ledger, first_day: date, last_day: date) -> dict[str, Any]:
    report = ledger.completeness(first_day, last_day)
    days = [entry for entry in report if entry["feed"] is None]
    lowest = min(days, key=lambda entry: entry["completeness"])
    return {
        "days": len(days),
        "complete_days": sum(1 for entry in days if entry["complete"]),
        "lowest": {"slot_date": lowest["slot_date"], "completeness": lowest["completeness"]},
        "below_99_5_percent": [
            entry["slot_date"] for entry in days if entry["completeness"] < 0.995
        ],
        "by_day": {entry["slot_date"]: entry["completeness"] for entry in days},
    }


def accounting(ledger: Ledger, first_day: date, last_day: date) -> dict[str, Any]:
    row = ledger.connection.execute(
        "SELECT count(*), count(*) FILTER (WHERE raw_lines = parsed_rows + quarantined_rows), "
        "coalesce(sum(raw_lines), 0), coalesce(sum(parsed_rows), 0), "
        "coalesce(sum(quarantined_rows), 0), coalesce(sum(expected_bytes), 0) "
        "FROM ops.source_slot WHERE source = %s AND state = 'loaded' "
        "AND slot_ts >= %s AND slot_ts <= %s",
        (ledger.source, first_slot_of(first_day).start, last_slot_of(last_day).start),
    ).fetchone()
    assert row is not None
    names = ("loaded_files", "balanced_files", "raw_lines", "parsed_rows", "quarantined_rows")
    result: dict[str, Any] = dict(zip(names, (int(value) for value in row[:5]), strict=True))
    result["bytes"] = int(row[5])
    result["accounting"] = result["balanced_files"] / result["loaded_files"] if row[0] else None
    return result


def day_versions(ledger: Ledger, first_day: date, last_day: date) -> dict[str, Any]:
    rows = ledger.connection.execute(
        "SELECT DISTINCT ON (table_name, slot_date) table_name, slot_date, unit_kind, "
        "committed_rows, file_count, duration_ms, run_id FROM ops.silver_partition "
        "WHERE source = %s AND slot_date >= %s AND slot_date <= %s "
        "ORDER BY table_name, slot_date, partition_version_id DESC",
        (ledger.source, first_day, last_day),
    ).fetchall()
    wanted = {day.isoformat() for day in days_between(first_day, last_day)}
    tables: dict[str, Any] = {}
    for name in TABLES:
        mine = [row for row in rows if row[0] == name]
        finalised = {row[1].isoformat() for row in mine if row[2] == "day"}
        seconds = sorted(row[5] / 1000 for row in mine if row[2] == "day" and row[5] is not None)
        tables[name] = {
            "days_with_a_day_level_version": len(finalised & wanted),
            "days_without": sorted(wanted - finalised),
            "rows": sum(int(row[3]) for row in mine if row[2] == "day"),
            "files": sum(int(row[4]) for row in mine if row[2] == "day"),
            "build_seconds_median": round(statistics.median(seconds), 1) if seconds else None,
            "build_seconds_max": round(seconds[-1], 1) if seconds else None,
        }
    return tables


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument("--from", dest="first", required=True, help="first UTC day, YYYY-MM-DD")
    parser.add_argument("--to", dest="last", required=True, help="last UTC day, YYYY-MM-DD")
    arguments = parser.parse_args()
    first_day, last_day = parse_day(arguments.first), parse_day(arguments.last)
    with open_ledger() as ledger:
        evidence = {
            "window": {"from": first_day.isoformat(), "to": last_day.isoformat()},
            "evaluated_at": utc_now().isoformat(timespec="seconds"),
            "slots": slot_states(ledger, first_day, last_day),
            "completeness": completeness(ledger, first_day, last_day),
            "accounting": accounting(ledger, first_day, last_day),
            "silver": day_versions(ledger, first_day, last_day),
        }
    print(json.dumps(evidence, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
