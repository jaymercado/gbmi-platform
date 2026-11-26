"""Command-line entry points.

Airflow tasks and Make targets are thin wrappers around these commands; all logic stays
importable and testable without Airflow (blueprint 8.1).
"""

from __future__ import annotations

import json
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
gdelt_app = typer.Typer(help="GDELT: land files and build silver tables.", no_args_is_help=True)
lake_app = typer.Typer(help="The lake as the warehouse sees it.", no_args_is_help=True)
quality_app = typer.Typer(help="Quality gates and reconciliation.", no_args_is_help=True)
app.add_typer(config_app, name="config")
app.add_typer(db_app, name="db")
app.add_typer(gdelt_app, name="gdelt")
app.add_typer(lake_app, name="lake")
app.add_typer(quality_app, name="quality")

HourArgument = Annotated[str, typer.Option("--hour", help="UTC slot-hour, YYYY-MM-DDTHH.")]
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


@gdelt_app.command("land")
def gdelt_land(
    hour: HourArgument,
    source: Annotated[
        str, typer.Option(help="fixture (recorded files) or live (GDELT).")
    ] = "fixture",
    fixtures_dir: Annotated[
        str, typer.Option(envvar="GBMI_FIXTURES_DIR", help="Recorded files for --source fixture.")
    ] = "/fixtures/gdelt/v2",
) -> None:
    """Land the export files of one hour in bronze, verified and unchanged."""
    from gbmi.sources.gdelt.land import land_export_hour

    _print(land_export_hour(hour, source, fixtures_dir=fixtures_dir))


@gdelt_app.command("build-silver")
def gdelt_build_silver(
    hour: HourArgument,
    run_id: Annotated[str | None, typer.Option(envvar="GBMI_RUN_ID")] = None,
) -> None:
    """Parse, audit and commit one slot-hour of the silver table gdelt_event."""
    from gbmi.transforms.duckdb.gdelt_event import build_event_hour_task

    _print(build_event_hour_task(hour, run_id))


@lake_app.command("pin")
def lake_pin(
    table: Annotated[
        str, typer.Option(help="Silver table to pin as lake.<table>.")
    ] = "gdelt_event",
) -> None:
    """Re-create a ClickHouse table bound to the silver table's current metadata file."""
    from gbmi.warehouse.pin import pin_silver_table_task

    _print(pin_silver_table_task(table))


@quality_app.command("assert-counts")
def quality_assert_counts(
    hour: HourArgument,
    committed: Annotated[int, typer.Option(help="Rows the silver build committed for the hour.")],
    snapshot_id: Annotated[int, typer.Option(help="The Iceberg snapshot that was pinned.")],
) -> None:
    """Check that silver, the pinned lake table and the mart agree."""
    from gbmi.quality.reconcile import assert_tracer_counts

    _print(assert_tracer_counts(hour, committed, snapshot_id))
