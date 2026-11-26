"""Drill DR8, silver part: a closed day is built twice (blueprint 9.8; week 2 plan WP-11).

Fault: none. The drill repeats work which is done: the build of a whole day.

Checked for each of the four silver tables: the second build records the same content
checksum, the same number of rows and the same file-set hash as the first build, and a new
snapshot. On the fixtures the drill also reads the table and compares the content with the
recorded checksum.

Without an argument the drill runs on the recorded fixture hour, in a throwaway database and a
lake of its own. With ``--day YYYY-MM-DD`` the drill builds a real day of the stack again, with
the function behind ``gbmi silver build-day``, and compares the new partition versions with the
stored versions: the proof for the 30 days of the backfill (week 2 plan, exit-gate row 4). In
that form the drill adds one partition version for each table, with an equal checksum.

The mart half of DR8 is work of phase P3.
"""

from __future__ import annotations

import argparse
from typing import Any

from drills.common import DAY, Drill, world
from gbmi.config import get_settings
from gbmi.core.clock import parse_day, utc_now
from gbmi.core.gdelt import Feed
from gbmi.lake.commit import Unit
from gbmi.lake.tables import FEED_TABLES
from gbmi.ledger.repository import Ledger, open_ledger
from gbmi.transforms.build import build_unit

EVIDENCE = ("committed_rows", "file_count", "fileset_hash", "content_digest")


def day_versions(ledger: Ledger, table_name: str, unit: Unit) -> list[dict[str, Any]]:
    """The day-level partition versions of a table for the day, oldest first."""
    rows = ledger.connection.execute(
        "SELECT committed_rows, file_count, fileset_hash, content_digest, snapshot_id "
        "FROM ops.silver_partition WHERE source = %s AND table_name = %s AND slot_date = %s "
        "AND unit_kind = 'day' ORDER BY partition_version_id",
        (ledger.source, table_name, unit.slot_date),
    ).fetchall()
    return [dict(zip((*EVIDENCE, "snapshot_id"), row, strict=True)) for row in rows]


def compare(drill: Drill, ledger: Ledger, unit: Unit, facts: dict[str, Any]) -> None:
    """The two newest day builds of each table have the same evidence and other snapshots."""
    for feed in Feed:
        for name in FEED_TABLES[feed]:
            recorded = day_versions(ledger, name, unit)
            if not drill.check(
                f"{name} has two day builds or more", len(recorded) >= 2, len(recorded)
            ):
                continue
            first, second = recorded[-2], recorded[-1]
            facts[name] = {"first": first, "second": second}
            drill.check(
                f"{name}: the same content checksum, rows, files and file-set hash",
                {key: first[key] for key in EVIDENCE} == {key: second[key] for key in EVIDENCE},
                {key: (first[key], second[key]) for key in EVIDENCE if first[key] != second[key]},
            )
            # A rebuild of a day with rows is a new snapshot; an empty day needs no commit.
            drill.check(
                f"{name}: the second build is a new snapshot",
                second["committed_rows"] == 0 or first["snapshot_id"] != second["snapshot_id"],
                (first["snapshot_id"], second["snapshot_id"]),
            )


def on_the_fixtures() -> int:
    drill = Drill("DR8", "two builds of a closed day give equal checksums (fixture day)")
    facts: dict[str, Any] = {"day": DAY.isoformat()}
    unit = Unit(DAY)
    with world("dr8") as stack:
        stack.land()
        for attempt in ("first", "second"):
            drill.step(f"the {attempt} build of the day, for the three feeds")
            for feed in Feed:
                stack.build(feed, unit)
        compare(drill, stack.ledger, unit, facts)
        drill.step("the tables hold the content of the recorded checksum")
        for feed in Feed:
            for name in FEED_TABLES[feed]:
                held = stack.checksum(name, unit)
                newest = day_versions(stack.ledger, name, unit)[-1]
                drill.check(
                    f"{name}: the content of the table equals the recorded checksum",
                    (held.rows, held.digest)
                    == (newest["committed_rows"], newest["content_digest"]),
                    str(held),
                )
                drill.check(f"{name} has no repeated key", stack.repeated_keys(name, unit) == 0)
    return drill.finish(facts)


def on_the_stack(day: str) -> int:
    unit = Unit(parse_day(day))
    drill = Drill("DR8", f"a second build of the day {unit.label} gives equal checksums (stack)")
    facts: dict[str, Any] = {"day": unit.label}
    settings = get_settings()
    with open_ledger(run_id=f"drill-dr8-{unit.label}") as ledger:
        for feed in Feed:
            # A day without a day build gets its first build here; the usual case is a day
            # which the backfill built.
            builds = 1 if day_versions(ledger, FEED_TABLES[feed][0], unit) else 2
            for _ in range(builds):
                drill.step(f"build the day of the feed {feed.value}")
                summary = build_unit(feed, unit, as_of=utc_now(), ledger=ledger, settings=settings)
                facts.setdefault("seconds", {})[feed.value] = round(
                    summary["duration_ms"] / 1000, 1
                )
        compare(drill, ledger, unit, facts)
    return drill.finish(facts)


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument("--day", help="a UTC day of the stack, YYYY-MM-DD; default: the fixtures")
    arguments = parser.parse_args()
    return on_the_stack(arguments.day) if arguments.day else on_the_fixtures()


if __name__ == "__main__":
    raise SystemExit(main())
