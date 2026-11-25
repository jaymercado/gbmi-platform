"""Command-line entry points.

Airflow tasks and Make targets are thin wrappers around these commands; all logic stays
importable and testable without Airflow (blueprint 8.1).
"""

from __future__ import annotations

import json
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
app.add_typer(config_app, name="config")


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
