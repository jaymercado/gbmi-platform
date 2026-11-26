"""Task entry points of pipeline PL-2 (blueprint 8.1 and 8.4).

A task of a DAG and a command of the command line call one function of this module. Each
function reads the settings, opens the ledger, does the work inside a run record and returns
built-in types, because Airflow hands the result from one Python environment to another.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from gbmi.config import get_settings
from gbmi.core.clock import parse_day, resolve_as_of, resolve_day
from gbmi.core.gdelt import Feed, parse_hour
from gbmi.lake.commit import Unit
from gbmi.lake.tables import FEED_TABLES
from gbmi.ledger.repository import PendingUnit, open_ledger
from gbmi.ops.partitions import versions_of_run
from gbmi.ops.runs import recorded_run
from gbmi.transforms import build


def _as_dict(item: PendingUnit) -> dict[str, str]:
    return {
        "feed": item.feed.value,
        "unit": Unit(item.slot_date, item.slot_hour).label,
        "unit_kind": "day" if item.slot_hour is None else "hour",
    }


def _as_unit(entry: Mapping[str, str]) -> PendingUnit:
    """A unit as a task hands it on: ``{"feed": "gkg", "unit": "2026-11-25T08"}``."""
    label = entry["unit"]
    if "T" in label:
        slot_date, slot_hour = parse_hour(label)
        return PendingUnit(slot_date, Feed(entry["feed"]), slot_hour)
    return PendingUnit(parse_day(label), Feed(entry["feed"]), None)


def pending_units_task(as_of: str | None = None) -> list[dict[str, str]]:
    """The units which the silver jobs have to build, oldest first."""
    moment = resolve_as_of(as_of)
    with open_ledger() as ledger:
        return [_as_dict(item) for item in ledger.pending_units(moment)]


def find_units_task(as_of: str | None = None, max_units: int | None = None) -> dict[str, Any]:
    """The pending units which one run builds: the oldest, 48 at most (blueprint 8.1)."""
    moment = resolve_as_of(as_of)
    pending = pending_units_task(moment.isoformat())
    return {
        "as_of": moment.isoformat(),
        "pending": len(pending),
        "units": pending[: max_units or get_settings().silver_max_units],
    }


def _build(
    entry_point: str,
    feed: str,
    unit: Unit,
    as_of: str | None,
    run_id: str | None,
    accept_volume: str | None = None,
) -> dict[str, Any]:
    settings = get_settings()
    moment = resolve_as_of(as_of)
    with (
        open_ledger(run_id=run_id) as ledger,
        recorded_run(
            ledger.connection, f"{ledger.source}.{entry_point}", as_of=moment, run_id=run_id
        ) as run,
    ):
        run.detail = {"feed": feed, "unit": unit.label}
        summary = build.build_unit(
            Feed(feed),
            unit,
            as_of=moment,
            ledger=ledger,
            settings=settings,
            run_id=run_id,
            accept_volume=accept_volume,
        )
        run.row_count = summary["counts"]["committed"]
        run.detail = {name: value for name, value in summary.items() if name != "files"}
    return summary


def build_hour_task(
    hour: str, feed: str, as_of: str | None = None, run_id: str | None = None
) -> dict[str, Any]:
    """Build the slot-hour ``YYYY-MM-DDTHH`` of one feed."""
    slot_date, slot_hour = parse_hour(hour)
    return _build("silver_hour", feed, Unit(slot_date, slot_hour), as_of, run_id)


def build_hour_feeds_task(
    hour: str, as_of: str | None = None, run_id: str | None = None
) -> dict[str, Any]:
    """Build the slot-hour ``YYYY-MM-DDTHH`` of the three feeds, one after the other.

    The entry point of the tracer. The result holds the summary of each feed, and the facts
    of the events table once more for the tasks after the build: the pin, dbt and the count
    check.
    """
    feeds = {feed.value: build_hour_task(hour, feed.value, as_of, run_id) for feed in Feed}
    events = feeds[Feed.EXPORT.value]
    return {
        "hour": hour,
        "feeds": feeds,
        "counts": events["counts"],
        "snapshot_id": events["snapshot_id"],
        "snapshot_operations": events["snapshot_operations"],
        "pipeline_version": events["pipeline_version"],
        "mart_partitions": events["mart_partitions"],
    }


def build_day_task(
    day: str | None,
    feed: str,
    as_of: str | None = None,
    accept_volume: str | None = None,
    run_id: str | None = None,
    logical_date: str | None = None,
) -> dict[str, Any]:
    """Build one whole slot-date of one feed.

    The date is ``day`` (``YYYY-MM-DD``), or the UTC date of the logical date of the run.
    ``accept_volume`` is the note of an operator who accepts a day outside the volume band.
    """
    unit = Unit(resolve_day(day, logical_date))
    return _build("silver_day", feed, unit, as_of, run_id, accept_volume)


def day_summary_task(
    day: str | None, logical_date: str | None = None, run_id: str | None = None
) -> dict[str, Any]:
    """What the builds of one run did for a day: one partition version for each silver table.

    Raises BuildError when a table has no day-level version from the run: the build of the
    feed failed, and the run must not count as a success.
    """
    slot_date = resolve_day(day, logical_date)
    with (
        open_ledger(run_id=run_id) as ledger,
        recorded_run(
            ledger.connection, f"{ledger.source}.silver_day_summary", run_id=run_id
        ) as run,
    ):
        tables = versions_of_run(
            ledger.connection, source=ledger.source, slot_date=slot_date, run_id=run_id
        )
        summary = {
            "day": slot_date.isoformat(),
            "run_id": run_id,
            "tables": tables,
            "slots": ledger.status(slot_date, slot_date),
        }
        run.row_count = sum(table["rows"] for table in tables.values())
        run.detail = summary
        missing = [name for feed in Feed for name in FEED_TABLES[feed] if name not in tables]
        if missing:
            raise build.BuildError(
                f"the run built no day-level version of {', '.join(missing)} for {slot_date}"
            )
    return summary


def build_units_task(
    units: Sequence[Mapping[str, str]],
    as_of: str | None = None,
    run_id: str | None = None,
    entry_point: str = "silver_units",
) -> dict[str, Any]:
    """Build the given units, one after the other.

    The task fails when a unit failed; the other units of the run are built all the same.
    """
    settings = get_settings()
    moment = resolve_as_of(as_of)
    with (
        open_ledger(run_id=run_id) as ledger,
        recorded_run(
            ledger.connection, f"{ledger.source}.{entry_point}", as_of=moment, run_id=run_id
        ) as run,
    ):
        summary = build.build_units(
            [_as_unit(entry) for entry in units],
            as_of=moment,
            ledger=ledger,
            settings=settings,
            run_id=run_id,
        )
        run.row_count = summary["rows"]
        run.detail = summary
        if summary["failed"]:
            names = ", ".join(f"{item['feed']} {item['unit']}" for item in summary["failed"])
            raise build.BuildError(
                f"{len(summary['failed'])} of {summary['selected']} units failed: {names}"
            )
    return summary


def build_pending_task(
    as_of: str | None = None, max_units: int | None = None, run_id: str | None = None
) -> dict[str, Any]:
    """Find the pending units of the trailing seven days and build them: one silver run."""
    found = find_units_task(as_of, max_units)
    summary = build_units_task(found["units"], found["as_of"], run_id, "silver_pending")
    return {**summary, "pending": found["pending"]}
