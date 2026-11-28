"""Task entry points of the three SEC pipelines (blueprint 8.1; week 3 plan, WP-06 step 5).

A task of a DAG and a command of the command line call one function of this module. Each
function reads the settings, opens the object ledger and one client of the SEC, does the work
inside a run record and returns built-in types: the objects by state, the rows, the requests,
the seconds which the limiter waited and the blocks (the monitoring list of blueprint 8.5).
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from gbmi.config import get_settings
from gbmi.core.clock import parse_day, resolve_as_of
from gbmi.ledger.objects import open_object_ledger
from gbmi.ops.runs import Run, recorded_run
from gbmi.sources.sec.client import SecClient
from gbmi.sources.sec.facts import run_facts
from gbmi.sources.sec.filings import run_filings
from gbmi.sources.sec.reference import run_reference
from gbmi.sources.sec.silver import SecRun, new_run
from gbmi.sources.sec.watchlist import watchlist


@contextmanager
def _pipeline(
    name: str, as_of: str | None, run_id: str | None, accept_volume: str | None
) -> Iterator[tuple[SecRun, Run, SecClient]]:
    """One run of a pipeline: the ledger, the run record and the client, opened together.

    The client holds the lock of the source as long as the run lasts, so no other run and no
    command of an operator sends a request beside the run (week 3 plan 3.3).
    """
    settings = get_settings()
    moment = resolve_as_of(as_of)
    with (
        open_object_ledger(run_id=run_id) as ledger,
        recorded_run(
            ledger.connection, f"{ledger.source}.{name}", as_of=moment, run_id=run_id
        ) as record,
        SecClient(settings, run_id=run_id, source=ledger.source) as client,
    ):
        run = new_run(
            as_of=moment,
            settings=settings,
            ledger=ledger,
            client=client,
            accept_volume=accept_volume,
        )
        yield run, record, client


def _finish(
    summary: dict[str, Any], run: SecRun, record: Run, client: SecClient, rows: int
) -> dict[str, Any]:
    stats = client.stats
    summary["requests"] = {
        "sent": stats.requests,
        "seconds_waited": round(stats.waited_seconds, 3),
        "blocks": stats.blocks,
        "missing": stats.missing,
        "retries": stats.retries,
    }
    summary["objects"] = run.ledger.status(first=run.day, last=run.day)
    summary["pipeline_version"] = run.facts.pipeline_version
    record.row_count = rows
    record.byte_count = run.lander.counts.bytes
    record.detail = summary
    return summary


def filings_task(
    as_of: str | None = None,
    first_day: str | None = None,
    last_day: str | None = None,
    accept_volume: str | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """One run of the filings pipeline (PL-4). A range of days is a backfill."""
    settings = get_settings()
    with _pipeline("filings", as_of, run_id, accept_volume) as (run, record, client):
        summary = run_filings(
            run,
            watchlist=watchlist(settings),
            first=parse_day(first_day) if first_day else None,
            last=parse_day(last_day) if last_day else None,
        )
        rows = summary["index"]["rows"] + summary["detail"].get("rows", 0)
        return _finish(summary, run, record, client, rows)


def reference_task(
    as_of: str | None = None,
    bulk: bool = False,
    accept_volume: str | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """One run of the reference pipeline (PL-3). ``bulk`` asks for the archive on any day."""
    settings = get_settings()
    with _pipeline("reference", as_of, run_id, accept_volume) as (run, record, client):
        summary = run_reference(run, watchlist=watchlist(settings), bulk=bulk)
        return _finish(summary, run, record, client, summary["snapshot"]["rows"])


def facts_task(
    as_of: str | None = None,
    full: bool = False,
    accept_volume: str | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """One run of the facts pipeline (PL-5). ``full`` takes every watchlist company."""
    settings = get_settings()
    with _pipeline("facts", as_of, run_id, accept_volume) as (run, record, client):
        summary = run_facts(run, watchlist=watchlist(settings), full=full)
        return _finish(summary, run, record, client, summary.get("rows", 0))
