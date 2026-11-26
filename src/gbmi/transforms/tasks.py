"""Task entry points of pipeline PL-2 (blueprint 8.1 and 8.4).

A task of a DAG and a command of the command line call one function of this module. Each
function reads the settings, opens the ledger, does the work inside a run record and returns
built-in types, because Airflow hands the result from one Python environment to another.
"""

from __future__ import annotations

from typing import Any

from gbmi.config import get_settings
from gbmi.core.clock import parse_day, resolve_as_of
from gbmi.core.gdelt import Feed, parse_hour
from gbmi.lake.commit import Unit
from gbmi.ledger.repository import open_ledger
from gbmi.ops.runs import recorded_run
from gbmi.transforms import build


def pending_units_task(as_of: str | None = None) -> list[dict[str, Any]]:
    """The units which the silver jobs have to build, oldest first."""
    moment = resolve_as_of(as_of)
    with open_ledger() as ledger:
        return [
            {
                "feed": item.feed.value,
                "unit": Unit(item.slot_date, item.slot_hour).label,
                "unit_kind": "day" if item.slot_hour is None else "hour",
            }
            for item in ledger.pending_units(moment)
        ]


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


def build_day_task(
    day: str,
    feed: str,
    as_of: str | None = None,
    accept_volume: str | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Build the whole slot-date ``YYYY-MM-DD`` of one feed.

    ``accept_volume`` is the note of an operator who accepts a day outside the volume band.
    """
    return _build("silver_day", feed, Unit(parse_day(day)), as_of, run_id, accept_volume)


def build_pending_task(
    as_of: str | None = None, max_units: int | None = None, run_id: str | None = None
) -> dict[str, Any]:
    """Build the pending units of the trailing seven days, 48 at most in one run.

    The task fails when a unit failed; the other units of the run are built all the same.
    """
    settings = get_settings()
    moment = resolve_as_of(as_of)
    with (
        open_ledger(run_id=run_id) as ledger,
        recorded_run(
            ledger.connection, f"{ledger.source}.silver_pending", as_of=moment, run_id=run_id
        ) as run,
    ):
        summary = build.build_pending(
            as_of=moment, ledger=ledger, settings=settings, run_id=run_id, max_units=max_units
        )
        run.row_count = summary["rows"]
        run.detail = summary
        if summary["failed"]:
            units = ", ".join(f"{item['feed']} {item['unit']}" for item in summary["failed"])
            raise build.BuildError(
                f"{len(summary['failed'])} of {summary['selected']} units failed: {units}"
            )
    return summary
