"""Command-line entry points.

Airflow tasks and Make targets are thin wrappers around these commands; all logic stays
importable and testable without Airflow (blueprint 8.1).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Annotated

import typer

from gbmi import __version__
from gbmi.config import get_settings
from gbmi.logging import configure_logging

app = typer.Typer(
    name="gbmi",
    help="GBMI platform command line.",
    no_args_is_help=True,
    add_completion=False,
)
config_app = typer.Typer(help="Inspect the active configuration.", no_args_is_help=True)
db_app = typer.Typer(help="The operational database: schema migrations.", no_args_is_help=True)
gdelt_app = typer.Typer(help="GDELT: expect, list and land the files.", no_args_is_help=True)
ledger_app = typer.Typer(help="The ledger of expected source files.", no_args_is_help=True)
lake_app = typer.Typer(help="The lake as the warehouse sees it.", no_args_is_help=True)
quality_app = typer.Typer(help="Quality gates and reconciliation.", no_args_is_help=True)
silver_app = typer.Typer(help="Silver: build units from verified files.", no_args_is_help=True)
sec_app = typer.Typer(help="SEC EDGAR: the three pipelines and the client.", no_args_is_help=True)
sec_circuit_app = typer.Typer(
    help="The circuit which remembers a block by the SEC.", no_args_is_help=True
)
sec_objects_app = typer.Typer(help="The object ledger: the files which the SEC is asked for.")
sec_requests_app = typer.Typer(help="The request log of the SEC.", no_args_is_help=True)
sec_app.add_typer(sec_circuit_app, name="circuit")
sec_app.add_typer(sec_objects_app, name="objects")
sec_app.add_typer(sec_requests_app, name="requests")
app.add_typer(config_app, name="config")
app.add_typer(db_app, name="db")
app.add_typer(gdelt_app, name="gdelt")
app.add_typer(lake_app, name="lake")
app.add_typer(ledger_app, name="ledger")
app.add_typer(quality_app, name="quality")
app.add_typer(sec_app, name="sec")
app.add_typer(silver_app, name="silver")

HourArgument = Annotated[str, typer.Option("--hour", help="UTC slot-hour, YYYY-MM-DDTHH.")]
FromDay = Annotated[str, typer.Option("--from", help="First UTC day of the range, YYYY-MM-DD.")]
ToDay = Annotated[str, typer.Option("--to", help="Last UTC day of the range, YYYY-MM-DD.")]
AsOf = Annotated[
    str | None,
    typer.Option(
        "--as-of", help="The clock to work with, ISO 8601 with a time zone (default: now)."
    ),
]
Feeds = Annotated[
    list[str] | None,
    typer.Option("--feed", help="export, mentions or gkg; repeat the option (default: all three)."),
]
Note = Annotated[
    str, typer.Option("--note", help="Why the slots are changed; stored in the ledger.")
]
DecidedBy = Annotated[
    str,
    typer.Option("--decided-by", envvar="GBMI_OPERATOR", help="The name of the operator."),
]
CircuitNote = Annotated[
    str,
    typer.Option("--note", help="What was found about the block; stored with the close."),
]
MigrationsDirectory = Annotated[
    Path | None,
    typer.Option("--dir", help="Directory of the migration files (default: GBMI_MIGRATIONS_DIR)."),
]


def _print(result: object) -> None:
    """Results go to standard output as JSON; logs go to standard error."""
    typer.echo(json.dumps(result, indent=2, sort_keys=True, default=str))


@app.callback()
def main(
    run_id: Annotated[
        str | None,
        typer.Option(
            "--run-id",
            envvar="GBMI_RUN_ID",
            help="Orchestrator run identifier, attached to every log line.",
        ),
    ] = None,
    log_level: Annotated[
        str,
        typer.Option("--log-level", envvar="GBMI_LOG_LEVEL", help="Minimum log level."),
    ] = "INFO",
) -> None:
    """Configure logging before any command runs."""
    configure_logging(run_id=run_id, level=log_level.upper())


@app.command()
def version() -> None:
    """Print the package version."""
    typer.echo(__version__)


@config_app.command("show")
def config_show() -> None:
    """Print the active settings as JSON, with secrets masked."""
    settings = get_settings()
    typer.echo(json.dumps(settings.model_dump(mode="json"), indent=2, sort_keys=True))


@db_app.command("migrate")
def db_migrate(directory: MigrationsDirectory = None) -> None:
    """Apply every migration file which the operational database does not hold yet."""
    from gbmi.ledger.db import LedgerConfigurationError
    from gbmi.ledger.migrate import MigrationError, migrate

    try:
        applied = migrate(directory=directory)
    except (MigrationError, LedgerConfigurationError) as error:
        typer.echo(f"gbmi db migrate: {error}", err=True)
        raise typer.Exit(code=1) from error
    _print({"applied": applied})


@db_app.command("status")
def db_status(directory: MigrationsDirectory = None) -> None:
    """List each migration file with its state: applied, pending, changed or file_missing."""
    from gbmi.ledger.db import LedgerConfigurationError
    from gbmi.ledger.migrate import MigrationError, status

    try:
        report = status(directory=directory)
    except (MigrationError, LedgerConfigurationError) as error:
        typer.echo(f"gbmi db status: {error}", err=True)
        raise typer.Exit(code=1) from error
    _print(report)


def _fail(command: str, error: Exception) -> typer.Exit:
    """One line on standard error and exit code 1, for a wrong argument or a refused request."""
    typer.echo(f"gbmi {command}: {error}", err=True)
    return typer.Exit(code=1)


@ledger_app.command("status")
def ledger_status(first: FromDay, last: ToDay) -> None:
    """Count the slots in each state, for each feed and UTC day of the range."""
    from gbmi.core.clock import parse_day
    from gbmi.ledger.repository import open_ledger

    try:
        first_day, last_day = parse_day(first), parse_day(last)
    except ValueError as error:
        raise _fail("ledger status", error) from error
    with open_ledger() as ledger:
        _print(ledger.status(first_day, last_day))


@ledger_app.command("completeness")
def ledger_completeness(first: FromDay, last: ToDay) -> None:
    """Completeness of each UTC day: loaded slots ÷ (expected slots - confirmed-missing slots)."""
    from gbmi.core.clock import parse_day
    from gbmi.ledger.repository import open_ledger

    try:
        first_day, last_day = parse_day(first), parse_day(last)
    except ValueError as error:
        raise _fail("ledger completeness", error) from error
    with open_ledger() as ledger:
        _print(ledger.completeness(first_day, last_day))


@ledger_app.command("overdue")
def ledger_overdue(as_of: AsOf = None) -> None:
    """List the slots which are still not verified six hours after their time."""
    from gbmi.core.clock import resolve_as_of
    from gbmi.ledger.repository import open_ledger

    try:
        moment = resolve_as_of(as_of)
    except ValueError as error:
        raise _fail("ledger overdue", error) from error
    with open_ledger() as ledger:
        _print(
            [
                {"slot": record.key.label, "state": record.state.value, "attempts": record.attempts}
                for record in ledger.overdue(moment)
            ]
        )


@ledger_app.command("confirm-missing")
def ledger_confirm_missing(
    first: FromDay,
    last: ToDay,
    note: Note,
    state: Annotated[
        list[str],
        typer.Option("--state", help="expected, listed or quarantined; repeat the option."),
    ],
    feed: Feeds = None,
    decided_by: DecidedBy = "operator",
    as_of: AsOf = None,
) -> None:
    """Decide: no file will come for the slots of the range which are in the named states."""
    from gbmi.core.clock import parse_day, resolve_as_of
    from gbmi.core.gdelt import Feed
    from gbmi.ledger import operator
    from gbmi.ledger.repository import open_ledger
    from gbmi.ledger.states import State

    try:
        with open_ledger() as ledger:
            confirmed = operator.confirm_missing(
                ledger,
                first_day=parse_day(first),
                last_day=parse_day(last),
                states=[State(name) for name in state],
                feeds=[Feed(name) for name in feed] if feed else None,
                decided_by=decided_by,
                note=note,
                as_of=resolve_as_of(as_of),
            )
    except ValueError as error:
        raise _fail("ledger confirm-missing", error) from error
    _print({"confirmed_missing": confirmed})


@ledger_app.command("requeue")
def ledger_requeue(
    first: FromDay,
    last: ToDay,
    note: Note,
    feed: Feeds = None,
    decided_by: DecidedBy = "operator",
    as_of: AsOf = None,
) -> None:
    """Send the quarantined slots of the range back to the download."""
    from gbmi.core.clock import parse_day, resolve_as_of
    from gbmi.core.gdelt import Feed
    from gbmi.ledger import operator
    from gbmi.ledger.repository import open_ledger

    try:
        with open_ledger() as ledger:
            requeued = operator.requeue(
                ledger,
                first_day=parse_day(first),
                last_day=parse_day(last),
                feeds=[Feed(name) for name in feed] if feed else None,
                decided_by=decided_by,
                note=note,
                as_of=resolve_as_of(as_of),
            )
    except ValueError as error:
        raise _fail("ledger requeue", error) from error
    _print({"requeued": requeued})


Source = Annotated[str, typer.Option(help="live (the GDELT servers) or fixture (recorded files).")]
RunId = Annotated[str | None, typer.Option(envvar="GBMI_RUN_ID")]


@gdelt_app.command("ingest")
def gdelt_ingest(
    source: Source = "live",
    as_of: AsOf = None,
    batch_limit: Annotated[
        int | None, typer.Option(help="Files of one run at most (default: 200).")
    ] = None,
    concurrency: Annotated[
        int | None, typer.Option(help="Parallel downloads (default: 4).")
    ] = None,
    run_id: RunId = None,
) -> None:
    """One ingest run: expect, list, download, verify and record the newest GDELT files."""
    from gbmi.sources.gdelt.tasks import ingest_task

    try:
        _print(ingest_task(as_of, source, batch_limit, concurrency, run_id))
    except ValueError as error:
        raise _fail("gdelt ingest", error) from error


@gdelt_app.command("enqueue")
def gdelt_enqueue(
    first: FromDay,
    last: ToDay,
    source: Source = "live",
    reserve: Annotated[
        str | None,
        typer.Option(
            help="Claim for the new rows, for example spark_backfill; silver jobs skip them."
        ),
    ] = None,
    as_of: AsOf = None,
    run_id: RunId = None,
) -> None:
    """Create the ledger rows of a range of UTC days from the master list, for a backfill."""
    from gbmi.sources.gdelt.tasks import enqueue_task

    try:
        _print(enqueue_task(first, last, source, reserve, as_of, run_id))
    except ValueError as error:
        raise _fail("gdelt enqueue", error) from error


@gdelt_app.command("reconcile")
def gdelt_reconcile(
    source: Source = "live",
    as_of: AsOf = None,
    force: Annotated[
        bool, typer.Option("--force", help="Reconcile also when no reconciliation is due.")
    ] = False,
    run_id: RunId = None,
) -> None:
    """Reconcile the trailing seven days of the ledger against the master list."""
    from gbmi.sources.gdelt.tasks import reconcile_task

    try:
        _print(reconcile_task(as_of, source, force, run_id))
    except ValueError as error:
        raise _fail("gdelt reconcile", error) from error


def _build_feeds(command: str, feeds: list[str] | None, build: Callable[[str], object]) -> None:
    """Build one unit for each feed. A feed which fails does not stop the feeds after it."""
    from gbmi.core.gdelt import Feed
    from gbmi.ledger.repository import StaleStateError
    from gbmi.quality.audit import AuditError
    from gbmi.transforms.build import BuildError

    results: dict[str, object] = {}
    failed = False
    for name in feeds or [feed.value for feed in Feed]:
        try:
            Feed(name)
            results[name] = build(name)
        except (AuditError, BuildError, StaleStateError, ValueError) as error:
            typer.echo(f"gbmi {command}: {name}: {error}", err=True)
            failed = True
    _print(results)
    if failed:
        raise typer.Exit(code=1)


@silver_app.command("units")
def silver_units(as_of: AsOf = None) -> None:
    """List the units which the silver jobs have to build: hours, or whole days."""
    from gbmi.transforms.tasks import pending_units_task

    try:
        _print(pending_units_task(as_of))
    except ValueError as error:
        raise _fail("silver units", error) from error


@silver_app.command("build-hour")
def silver_build_hour(
    hour: HourArgument, feed: Feeds = None, as_of: AsOf = None, run_id: RunId = None
) -> None:
    """Build one slot-hour: parse the verified files, audit, commit, record."""
    from gbmi.core.gdelt import parse_hour
    from gbmi.transforms.tasks import build_hour_task

    try:
        parse_hour(hour)
    except ValueError as error:
        raise _fail("silver build-hour", error) from error
    _build_feeds("silver build-hour", feed, lambda name: build_hour_task(hour, name, as_of, run_id))


@silver_app.command("build-day")
def silver_build_day(
    day: Annotated[str, typer.Option("--day", "--date", help="UTC day, YYYY-MM-DD.")],
    feed: Feeds = None,
    accept_volume: Annotated[
        str | None,
        typer.Option(
            "--accept-volume",
            help="Accept a day whose row count is outside the volume band; the text is the "
            "note which is stored with the check.",
        ),
    ] = None,
    as_of: AsOf = None,
    run_id: RunId = None,
) -> None:
    """Build one whole slot-date, which also replaces the hourly files of the day with one file."""
    from gbmi.core.clock import parse_day
    from gbmi.transforms.tasks import build_day_task

    try:
        parse_day(day)
    except ValueError as error:
        raise _fail("silver build-day", error) from error
    _build_feeds(
        "silver build-day",
        feed,
        lambda name: build_day_task(day, name, as_of, accept_volume, run_id),
    )


@lake_app.command("pin")
def lake_pin(
    table: Annotated[
        str, typer.Option(help="Silver table to pin as lake.<table>.")
    ] = "gdelt_event",
) -> None:
    """Re-create a ClickHouse table bound to the silver table's current metadata file."""
    from gbmi.warehouse.pin import pin_silver_table_task

    _print(pin_silver_table_task(table))


@lake_app.command("check")
def lake_check(
    scratch: Annotated[
        str | None,
        typer.Option(
            help="Where one object is written, read and deleted (default: below the lake).",
        ),
    ] = None,
    release: Annotated[
        str | None,
        typer.Option(help="The commit which the pipeline version has to equal."),
    ] = None,
    table: Annotated[
        str, typer.Option(help="The silver table which ClickHouse reads.")
    ] = "gdelt_event",
) -> None:
    """Check the lake backend of the stack: catalog, object store, pinned read, version."""
    from gbmi.lake.check import run_checks

    checks = run_checks(get_settings(), scratch_uri=scratch, release=release, table=table)
    _print(
        {
            "passed": all(check.passed for check in checks),
            "checks": {check.name: {"passed": check.passed, **check.detail} for check in checks},
        }
    )
    if not all(check.passed for check in checks):
        raise typer.Exit(1)


@quality_app.command("assert-counts")
def quality_assert_counts(
    hour: HourArgument,
    committed: Annotated[int, typer.Option(help="Rows the silver build committed for the hour.")],
    snapshot_id: Annotated[int, typer.Option(help="The Iceberg snapshot that was pinned.")],
) -> None:
    """Check that silver, the pinned lake table and the mart agree."""
    from gbmi.quality.reconcile import assert_tracer_counts

    _print(assert_tracer_counts(hour, committed, snapshot_id))


@sec_circuit_app.command("status")
def sec_circuit_status(as_of: AsOf = None) -> None:
    """Show the circuit of the SEC: closed, open, waiting for the probe, or locked."""
    from gbmi.core.clock import resolve_as_of
    from gbmi.ledger.db import LedgerConfigurationError, connect
    from gbmi.ops.circuit import circuit_events, read_circuit
    from gbmi.sources.sec.client import SOURCE

    try:
        now = resolve_as_of(as_of)
        with connect(autocommit=True) as connection:
            row = read_circuit(connection, SOURCE)
            events = circuit_events(connection, SOURCE)
    except (LedgerConfigurationError, ValueError) as error:
        raise _fail("sec circuit status", error) from error
    if row.locked:
        state = "locked"
    elif row.blocks(now):
        state = "open"
    else:
        state = "probe" if row.recorded else "closed"
    _print(
        {
            "source": SOURCE,
            "state": state,
            "blocked_until": row.blocked_until,
            "opened_at": row.opened_at,
            "reason": row.reason,
            "http_status": row.http_status,
            "open_count": row.open_count,
            "locked": row.locked,
            "events": events,
        }
    )


@sec_circuit_app.command("close")
def sec_circuit_close(
    note: CircuitNote, decided_by: DecidedBy = "operator", as_of: AsOf = None
) -> None:
    """Remove a recorded block and a lock, after a person has found the cause.

    The command is no way around a block: the SEC ends a block when the requests of the
    address have stayed below the limit (blueprint 4.3). Close the circuit only when the
    cause is known and the block has ended.
    """
    from gbmi.core.clock import resolve_as_of
    from gbmi.ledger.db import LedgerConfigurationError, connect
    from gbmi.ops.circuit import close_circuit, read_circuit
    from gbmi.sources.sec.client import SOURCE

    try:
        if not note.strip():
            raise ValueError("the note is empty: say what was found about the block")
        now = resolve_as_of(as_of)
        with connect(autocommit=True) as connection:
            before = read_circuit(connection, SOURCE)
            closed = close_circuit(
                connection, SOURCE, closed_by=decided_by, note=note.strip(), now=now
            )
    except (LedgerConfigurationError, ValueError) as error:
        raise _fail("sec circuit close", error) from error
    _print(
        {
            "source": SOURCE,
            "closed": closed,
            "was_locked": before.locked,
            "was_blocked_until": before.blocked_until,
            "closed_by": decided_by if closed else None,
        }
    )


# --- the three SEC pipelines (week 3 plan, WP-06 step 6) ---------------------------------------

FromDayOrNone = Annotated[
    str | None, typer.Option("--from", help="First UTC day of the range, YYYY-MM-DD.")
]
ToDayOrNone = Annotated[
    str | None, typer.Option("--to", help="Last UTC day of the range, YYYY-MM-DD.")
]
AcceptVolume = Annotated[
    str | None,
    typer.Option(
        "--accept-volume",
        help="Accept a file outside its volume band; the text says why and is stored.",
    ),
]


def _sec_pipeline(command: str, run: Callable[[], object]) -> None:
    """Run one pipeline; a refused run is one line on standard error and exit code 1."""
    from gbmi.ledger.db import LedgerConfigurationError
    from gbmi.quality.audit import AuditError
    from gbmi.sources.sec.errors import SecError
    from gbmi.sources.sec.land import BronzeError
    from gbmi.sources.sec.silver import PipelineError

    try:
        _print(run())
    except (
        ValueError,
        SecError,
        PipelineError,
        BronzeError,
        AuditError,
        LedgerConfigurationError,
    ) as error:
        raise _fail(command, error) from error


@sec_app.command("filings")
def sec_filings(
    first: FromDayOrNone = None,
    last: ToDayOrNone = None,
    accept_volume: AcceptVolume = None,
    as_of: AsOf = None,
    run_id: RunId = None,
) -> None:
    """Pipeline PL-4: the daily index files and the filing detail of the watchlist.

    Without a range the run looks at the seven days before the day of the run. A range of
    days is a backfill of the index files.
    """
    from gbmi.sources.sec.tasks import filings_task

    _sec_pipeline("sec filings", lambda: filings_task(as_of, first, last, accept_volume, run_id))


@sec_app.command("reference")
def sec_reference(
    bulk: Annotated[
        bool,
        typer.Option("--bulk", help="Load the bulk archive of the submissions documents."),
    ] = False,
    accept_volume: AcceptVolume = None,
    as_of: AsOf = None,
    run_id: RunId = None,
) -> None:
    """Pipeline PL-3: the company universe of the day and the former names.

    The first run and a Sunday run load the bulk archive without the option.
    """
    from gbmi.sources.sec.tasks import reference_task

    _sec_pipeline("sec reference", lambda: reference_task(as_of, bulk, accept_volume, run_id))


@sec_app.command("facts")
def sec_facts(
    full: Annotated[
        bool, typer.Option("--full", help="Load the facts of every watchlist company.")
    ] = False,
    accept_volume: AcceptVolume = None,
    as_of: AsOf = None,
    run_id: RunId = None,
) -> None:
    """Pipeline PL-5: the financial facts of the watchlist companies."""
    from gbmi.sources.sec.tasks import facts_task

    _sec_pipeline("sec facts", lambda: facts_task(as_of, full, accept_volume, run_id))


@sec_objects_app.callback(invoke_without_command=True)
def sec_objects(
    context: typer.Context, first: FromDayOrNone = None, last: ToDayOrNone = None
) -> None:
    """Count the objects in each state, for each dataset, for a range of days."""
    from gbmi.core.clock import parse_day
    from gbmi.ledger.db import LedgerConfigurationError
    from gbmi.ledger.objects import open_object_ledger

    if context.invoked_subcommand is not None:
        return
    try:
        first_day = parse_day(first) if first else None
        last_day = parse_day(last) if last else None
        with open_object_ledger() as ledger:
            report = {
                "source": ledger.source,
                "states": ledger.status(first=first_day, last=last_day),
                "open": [
                    {
                        "object": item.key.label,
                        "state": item.state.value,
                        "attempts": item.attempts,
                        "reason": item.reason or item.last_error,
                    }
                    for item in ledger.pending(first=first_day, last=last_day)
                ],
            }
    except (LedgerConfigurationError, ValueError) as error:
        raise _fail("sec objects", error) from error
    _print(report)


@sec_objects_app.command("requeue")
def sec_objects_requeue(
    first: FromDay,
    last: ToDay,
    note: Note,
    dataset: Annotated[
        str,
        typer.Option(help="One of reference, submissions, index, companyfacts and bulk."),
    ],
    decided_by: DecidedBy = "operator",
    as_of: AsOf = None,
) -> None:
    """Send the quarantined objects of one dataset and a range of days back to the pipelines.

    An object which gate G2 or G3 refused has its bytes in bronze: the next run reads the
    bytes again and sends no request. An object which gate G1 refused has no bytes in bronze:
    the next run asks the SEC again, up to three times. The result lists such objects under
    asked_again. For the dataset bulk, one object is one request for the archive of 1.6 GB.
    """
    from gbmi.core.clock import parse_day, resolve_as_of
    from gbmi.ledger.db import LedgerConfigurationError
    from gbmi.ledger.object_states import ObjectState
    from gbmi.ledger.objects import Dataset, open_object_ledger

    try:
        if not note.strip():
            raise ValueError("the note is empty: say why the objects go back")
        with open_object_ledger() as ledger:
            quarantined = ledger.select(
                states={ObjectState.QUARANTINED},
                dataset=Dataset(dataset),
                first=parse_day(first),
                last=parse_day(last),
            )
            moved = ledger.requeue(
                [item.key for item in quarantined],
                decided_by=decided_by,
                note=note.strip(),
                as_of=resolve_as_of(as_of),
            )
    except (LedgerConfigurationError, ValueError) as error:
        raise _fail("sec objects requeue", error) from error
    without_bytes = [item.key for item in quarantined if item.bronze_uri is None]
    _print(
        {
            "requeued": sorted(key.label for key in moved),
            "asked_again": sorted(key.label for key in without_bytes if key in moved),
        }
    )


@sec_requests_app.command("max-burst")
def sec_requests_max_burst(first: FromDayOrNone = None, last: ToDayOrNone = None) -> None:
    """The largest number of requests to the SEC in any second (exit-gate row 1).

    The range counts whole UTC days; without a range the command reads the whole log.
    """
    from datetime import UTC, datetime, time, timedelta

    from gbmi.core.clock import parse_day
    from gbmi.ledger.db import LedgerConfigurationError, connect
    from gbmi.ops.requests import (
        max_requests_in_session_window,
        max_requests_in_window,
        request_counts,
    )
    from gbmi.sources.sec.client import SOURCE

    try:
        start = datetime.combine(parse_day(first), time.min, tzinfo=UTC) if first else None
        end = (
            datetime.combine(parse_day(last) + timedelta(days=1), time.min, tzinfo=UTC)
            if last
            else None
        )
        with connect(autocommit=True) as connection:
            report = {
                "source": SOURCE,
                "from": start,
                "to": end,
                "ceiling": get_settings().sec_max_rps,
                "max_requests_in_one_second": max_requests_in_window(
                    connection, source=SOURCE, start=start, end=end
                ),
                "max_requests_in_one_second_of_one_client": max_requests_in_session_window(
                    connection, source=SOURCE, start=start, end=end
                ),
                "requests": request_counts(connection, source=SOURCE, start=start, end=end),
            }
    except (LedgerConfigurationError, ValueError) as error:
        raise _fail("sec requests max-burst", error) from error
    _print(report)
