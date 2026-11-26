"""Quarantine and reconciliation of a window of days (WP-09, step 7).

The script reads the ledger, the bronze listing, the quarantine area and the partition
versions of the stack, and prints JSON with:

- the quarantined rows by feed and reason, from the quarantine files of the units;
- the quarantined files: the slots in the state quarantined, and the files kept in quarantine;
- for each feed and day: the files and the bytes of the ledger (loaded slots), of the bronze
  listing and of the newest day-level partition version, and each difference.

The counts of quarantined rows are also read from the accounting columns of the ledger, which
count each rejected line once (review point R8); the two numbers have to agree.

The script only reads.

Run on the Compose network of the working stack:
    make toolbox CMD="python -m benchmarks.p1_backfill.quarantine_reconciliation \
        --from 2026-10-26 --to 2026-11-24"
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from gbmi.config import get_settings
from gbmi.core.clock import parse_day, utc_now
from gbmi.core.gdelt import Feed
from gbmi.lake.tables import FEED_TABLES
from gbmi.ledger.calendar import days_between, first_slot_of, last_slot_of
from gbmi.ledger.repository import Ledger, open_ledger
from gbmi.sources.gdelt.land import Bronze

#: A reason names the column and the rule; the number in a reason is replaced, so that
#: "found 26" and "found 25" count as one kind of problem.
MAX_REASONS = 20


def lake_path() -> Path:
    return Path(get_settings().lake_uri.removeprefix("file://"))


def quarantined_rows(ledger: Ledger, first_day: date, last_day: date) -> dict[str, Any]:
    """The rejected rows of the window: by feed and reason, from the files of the units."""
    root = lake_path() / "quarantine" / ledger.source
    by_feed: dict[str, Counter[str]] = {feed.value: Counter() for feed in Feed}
    files = 0
    for feed in Feed:
        for day in days_between(first_day, last_day):
            directory = root / feed.value / f"dt={day.isoformat()}"
            # A day build writes day/rows.parquet and removes the files of the hour units.
            for path in sorted(directory.glob("*/rows.parquet")):
                files += 1
                reasons = pq.read_table(path, columns=["reason"])["reason"].to_pylist()
                by_feed[feed.value].update(reasons)
    from_ledger = ledger.connection.execute(
        "SELECT feed, coalesce(sum(quarantined_rows), 0), coalesce(sum(raw_lines), 0) "
        "FROM ops.source_slot WHERE source = %s AND state = 'loaded' "
        "AND slot_ts >= %s AND slot_ts <= %s GROUP BY feed ORDER BY feed",
        (ledger.source, first_slot_of(first_day).start, last_slot_of(last_day).start),
    ).fetchall()
    ledger_counts = {
        row[0]: {"quarantined_rows": int(row[1]), "raw_lines": int(row[2])} for row in from_ledger
    }
    return {
        "quarantine_files_of_units": files,
        "by_feed": {
            feed: {
                "rows_in_quarantine_files": sum(reasons.values()),
                "rows_in_the_ledger": ledger_counts.get(feed, {}).get("quarantined_rows", 0),
                "raw_lines": ledger_counts.get(feed, {}).get("raw_lines", 0),
                "reasons": dict(reasons.most_common(MAX_REASONS)),
            }
            for feed, reasons in by_feed.items()
        },
    }


def quarantined_files(ledger: Ledger, first_day: date, last_day: date) -> dict[str, Any]:
    """Slots in the state quarantined, and whole files kept in the quarantine area."""
    rows = ledger.connection.execute(
        "SELECT feed, to_char(slot_ts AT TIME ZONE 'UTC', 'YYYYMMDDHH24MISS'), attempts, "
        "last_error "
        "FROM ops.source_slot WHERE source = %s AND state = 'quarantined' "
        "AND slot_ts >= %s AND slot_ts <= %s ORDER BY slot_ts, feed",
        (ledger.source, first_slot_of(first_day).start, last_slot_of(last_day).start),
    ).fetchall()
    root = lake_path() / "quarantine" / ledger.source
    kept = []
    for feed in Feed:
        for day in days_between(first_day, last_day):
            directory = root / feed.value / f"dt={day.isoformat()}"
            kept += [str(path.relative_to(lake_path())) for path in sorted(directory.glob("*.zip"))]
    return {
        "slots_in_state_quarantined": [
            {"feed": row[0], "slot": row[1], "attempts": row[2], "last_error": row[3]}
            for row in rows
        ],
        "files_kept_in_quarantine": kept,
    }


def reconciliation(ledger: Ledger, first_day: date, last_day: date) -> dict[str, Any]:
    """Ledger against bronze against the partition versions, for each feed and day."""
    bronze = Bronze(get_settings().lake_uri)
    ledger_rows = ledger.connection.execute(
        "SELECT feed, (slot_ts AT TIME ZONE 'UTC')::date, count(*), "
        "coalesce(sum(expected_bytes), 0) "
        "FROM ops.source_slot WHERE source = %s AND state = 'loaded' "
        "AND slot_ts >= %s AND slot_ts <= %s GROUP BY 1, 2",
        (ledger.source, first_slot_of(first_day).start, last_slot_of(last_day).start),
    ).fetchall()
    in_ledger = {(row[0], row[1]): (int(row[2]), int(row[3])) for row in ledger_rows}
    version_rows = ledger.connection.execute(
        "SELECT DISTINCT ON (table_name, slot_date) table_name, slot_date, unit_kind, file_count "
        "FROM ops.silver_partition WHERE source = %s AND slot_date >= %s AND slot_date <= %s "
        "ORDER BY table_name, slot_date, partition_version_id DESC",
        (ledger.source, first_day, last_day),
    ).fetchall()
    versions = {(row[0], row[1]): (row[2], int(row[3])) for row in version_rows}
    days: list[dict[str, Any]] = []
    differences: list[dict[str, Any]] = []
    totals: dict[str, Counter[str]] = {feed.value: Counter() for feed in Feed}
    for day in days_between(first_day, last_day):
        for feed in Feed:
            sizes = bronze.sizes(f"raw/gdelt/v2/{feed.value}/dt={day.isoformat()}")
            files, size = in_ledger.get((feed.value, day), (0, 0))
            kind, version_files = versions.get((FEED_TABLES[feed][0], day), (None, 0))
            entry = {
                "slot_date": day.isoformat(),
                "feed": feed.value,
                "ledger_loaded_files": files,
                "ledger_bytes": size,
                "bronze_files": len(sizes),
                "bronze_bytes": sum(sizes.values()),
                "version_kind": kind,
                "version_files": version_files,
            }
            days.append(entry)
            totals[feed.value]["ledger_loaded_files"] += files
            totals[feed.value]["ledger_bytes"] += size
            totals[feed.value]["bronze_files"] += len(sizes)
            totals[feed.value]["bronze_bytes"] += sum(sizes.values())
            totals[feed.value]["version_files"] += version_files
            if not (
                files == len(sizes) == version_files
                and size == sum(sizes.values())
                and kind == "day"
            ):
                differences.append(entry)
    return {
        "feed_days": len(days),
        "feed_days_which_agree": len(days) - len(differences),
        "differences": differences,
        "totals": {feed: dict(counts) for feed, counts in totals.items()},
        "by_day": days,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument("--from", dest="first", required=True, help="first UTC day, YYYY-MM-DD")
    parser.add_argument("--to", dest="last", required=True, help="last UTC day, YYYY-MM-DD")
    parser.add_argument(
        "--days", action="store_true", help="also print one entry for each feed and day"
    )
    arguments = parser.parse_args()
    first_day, last_day = parse_day(arguments.first), parse_day(arguments.last)
    with open_ledger() as ledger:
        report = {
            "window": {"from": first_day.isoformat(), "to": last_day.isoformat()},
            "evaluated_at": utc_now().isoformat(timespec="seconds"),
            "quarantined_rows": quarantined_rows(ledger, first_day, last_day),
            "quarantined_files": quarantined_files(ledger, first_day, last_day),
            "reconciliation": reconciliation(ledger, first_day, last_day),
        }
    if not arguments.days:
        del report["reconciliation"]["by_day"]
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
