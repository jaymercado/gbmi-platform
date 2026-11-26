"""Task entry points of pipeline PL-1 (blueprint 8.1).

A task of a DAG and a command of the command line call one function of this module. Each
function reads the settings, opens the ledger, does the work inside a run record and returns
built-in types, because Airflow hands the result from one Python environment to another.
"""

from __future__ import annotations

from typing import Any

from gbmi.config import get_settings
from gbmi.core.clock import parse_day, resolve_as_of
from gbmi.core.gdelt import parse_hour
from gbmi.ledger.repository import open_ledger
from gbmi.ops.runs import recorded_run
from gbmi.sources.gdelt import ingest, reconcile
from gbmi.sources.gdelt.land import Bronze, fetcher_for


def ingest_task(
    as_of: str | None = None,
    source: str = "live",
    batch_limit: int | None = None,
    concurrency: int | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """One ingest run: calendar, index, download, gate G1, changes of state."""
    settings = get_settings()
    moment = resolve_as_of(as_of)
    with (
        open_ledger(run_id=run_id) as ledger,
        recorded_run(
            ledger.connection, f"{ledger.source}.ingest", as_of=moment, run_id=run_id
        ) as run,
    ):
        summary = ingest.run_ingest(
            as_of=moment,
            fetcher=fetcher_for(source, settings.fixtures_dir),
            ledger=ledger,
            bronze=Bronze(settings.lake_uri),
            lake_uri=settings.lake_uri,
            batch_limit=batch_limit or settings.ingest_batch_limit,
            concurrency=concurrency or settings.ingest_concurrency,
        )
        run.row_count = summary["verified"]
        run.byte_count = summary["bytes"]
        run.detail = summary
    return summary


def land_hour_task(
    hour: str, source: str = "fixture", as_of: str | None = None, run_id: str | None = None
) -> dict[str, Any]:
    """Land the files of the hour ``YYYY-MM-DDTHH`` for the three feeds, through the ledger.

    The entry point of the tracer. ``source`` is ``live`` for the GDELT servers or ``fixture``
    for the recorded hour. Raises FileNotFoundError when the hour has no verified file at the
    end: the source lists no file of the hour.
    """
    settings = get_settings()
    moment = resolve_as_of(as_of)
    slot_date, slot_hour = parse_hour(hour)
    with (
        open_ledger(run_id=run_id) as ledger,
        recorded_run(
            ledger.connection, f"{ledger.source}.land_hour", as_of=moment, run_id=run_id
        ) as run,
    ):
        summary = ingest.land_hour(
            slot_date,
            slot_hour,
            as_of=moment,
            fetcher=fetcher_for(source, settings.fixtures_dir),
            ledger=ledger,
            bronze=Bronze(settings.lake_uri),
            lake_uri=settings.lake_uri,
            concurrency=settings.ingest_concurrency,
        )
        summary["source"] = source
        run.row_count = summary["verified"]
        run.byte_count = summary["bytes"]
        run.detail = {name: value for name, value in summary.items() if name != "files"}
        landed = summary["states"].get("verified", 0) + summary["states"].get("loaded", 0)
        if not landed:
            raise FileNotFoundError(f"the {source} source lists no file for {hour}")
    return summary


def enqueue_task(
    first_day: str,
    last_day: str,
    source: str = "live",
    reserve: str | None = None,
    as_of: str | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Create the calendar rows of a range of UTC days and list the files of the master list."""
    settings = get_settings()
    moment = resolve_as_of(as_of)
    with (
        open_ledger(run_id=run_id) as ledger,
        recorded_run(
            ledger.connection, f"{ledger.source}.enqueue", as_of=moment, run_id=run_id
        ) as run,
    ):
        summary = ingest.enqueue(
            parse_day(first_day),
            parse_day(last_day),
            as_of=moment,
            fetcher=fetcher_for(source, settings.fixtures_dir),
            ledger=ledger,
            reserve=reserve,
        )
        run.row_count = summary["listed"]
        run.detail = summary
    return summary


def reconcile_task(
    as_of: str | None = None,
    source: str = "live",
    force: bool = False,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Reconcile the trailing seven days against the master list, when the reconciliation is due.

    The reconciliation is due once in each UTC day from 08:00; ``force`` reconciles in any case.
    """
    settings = get_settings()
    moment = resolve_as_of(as_of)
    with open_ledger(run_id=run_id) as ledger:
        name = reconcile.entry_point(ledger)
        with recorded_run(ledger.connection, name, as_of=moment, run_id=run_id) as run:
            if not force and not reconcile.is_due(ledger.connection, name, moment):
                summary: dict[str, Any] = {"as_of": moment.isoformat(), "reconciled": False}
            else:
                summary = reconcile.reconcile(
                    as_of=moment,
                    fetcher=fetcher_for(source, settings.fixtures_dir),
                    ledger=ledger,
                    bronze=Bronze(settings.lake_uri),
                )
            run.detail = summary
    return summary


def report_overdue_task(as_of: str | None = None, run_id: str | None = None) -> dict[str, Any]:
    """Report the slots which are not verified six hours after their time."""
    moment = resolve_as_of(as_of)
    with (
        open_ledger(run_id=run_id) as ledger,
        recorded_run(
            ledger.connection, f"{ledger.source}.report_overdue", as_of=moment, run_id=run_id
        ) as run,
    ):
        summary = reconcile.report_overdue(as_of=moment, ledger=ledger)
        run.row_count = summary["overdue"]
        run.detail = summary
    return summary
