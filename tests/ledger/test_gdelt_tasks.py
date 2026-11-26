"""The task entry points and the commands of pipeline PL-1, on the recorded fixture hour.

The entry points work on the source ``gdelt`` and read the settings, so the test takes a
database of its own, a lake in a temporary directory and the fixture files of the repository.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path

import pytest
from typer.testing import CliRunner

from gbmi.cli import app
from gbmi.config import get_settings
from gbmi.ledger.db import connect
from gbmi.ledger.migrate import migrate
from gbmi.ledger.repository import open_ledger
from gbmi.ledger.states import State
from gbmi.ops.runs import recorded_run
from gbmi.quality.results import record_results
from gbmi.sources.gdelt import tasks

from .conftest import MIGRATIONS

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "gdelt" / "v2"
#: The clock of the test: the files of the fixture hour (08:00 to 08:45) are published.
AS_OF = "2026-11-25T09:00:00+00:00"
FIXTURE_BYTES = 263_180 + 288_245 + 938_979  # export, mentions, gkg (ATTRIBUTION.md)


@pytest.fixture
def stack(empty_db: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """Settings which point at a new database, a new lake and the fixture files."""
    migrate(empty_db, MIGRATIONS)
    monkeypatch.setenv("GBMI_PG_DSN", empty_db)
    monkeypatch.setenv("GBMI_LAKE_URI", f"file://{tmp_path}")
    monkeypatch.setenv("GBMI_CATALOG_URI", f"sqlite:///{tmp_path}/catalog/catalog.db")
    monkeypatch.setenv("GBMI_FIXTURES_DIR", str(FIXTURES))
    monkeypatch.setenv("GBMI_PIPELINE_VERSION", "test-version")
    get_settings.cache_clear()
    yield empty_db
    get_settings.cache_clear()


def runs(dsn: str) -> list[tuple[object, ...]]:
    with connect(dsn, autocommit=True) as connection:
        return connection.execute(
            "SELECT entry_point, status, row_count, byte_count, pipeline_version, run_id, "
            "detail ->> 'reconciled' FROM ops.pipeline_run ORDER BY pipeline_run_id"
        ).fetchall()


def test_the_fixture_hour_is_listed_landed_and_recorded(stack: str, tmp_path: Path) -> None:
    enqueued = tasks.enqueue_task("2026-11-25", "2026-11-25", source="fixture", as_of=AS_OF)

    # The calendar reaches 09:15 at 09:00: 38 slots of three feeds. The fixture list has twelve.
    assert (enqueued["slots"], enqueued["created"]) == (114, 114)
    assert (enqueued["in_the_master_list"], enqueued["listed"]) == (12, 12)

    first = tasks.ingest_task(AS_OF, "fixture", run_id="run-1")

    assert (first["selected"], first["verified"], first["written"]) == (12, 12, 12)
    assert first["bytes"] == FIXTURE_BYTES
    assert len(list((tmp_path / "raw").rglob("*.zip"))) == 12

    second = tasks.ingest_task(AS_OF, "fixture", run_id="run-2")

    # A second run downloads nothing and writes nothing.
    assert (second["selected"], second["verified"], second["written"]) == (0, 0, 0)
    with open_ledger() as ledger:
        verified = ledger.find(states=[State.VERIFIED])
        assert len(verified) == 12
        assert {record.key.slot.stamp[8:12] for record in verified} == {
            "0800",
            "0815",
            "0830",
            "0845",
        }
    assert runs(stack) == [
        ("gdelt.enqueue", "success", 12, None, "test-version", None, None),
        ("gdelt.ingest", "success", 12, FIXTURE_BYTES, "test-version", "run-1", None),
        ("gdelt.ingest", "success", 0, 0, "test-version", "run-2", None),
    ]


def test_the_reconciliation_runs_when_it_is_due_or_forced(stack: str) -> None:
    tasks.enqueue_task("2026-11-25", "2026-11-25", source="fixture", as_of=AS_OF)
    tasks.ingest_task(AS_OF, "fixture")

    early = tasks.reconcile_task("2026-11-25T07:53:00+00:00", "fixture")
    due = tasks.reconcile_task(AS_OF, "fixture")
    done = tasks.reconcile_task("2026-11-25T09:15:00+00:00", "fixture")
    forced = tasks.reconcile_task("2026-11-25T09:30:00+00:00", "fixture", force=True)

    assert [early["reconciled"], due["reconciled"], done["reconciled"], forced["reconciled"]] == [
        False,
        True,
        False,
        True,
    ]
    assert (due["files_in_the_master_list"], due["bronze_differences"]) == (12, 0)
    assert [row[6] for row in runs(stack) if row[0] == "gdelt.reconcile"] == [
        "false",
        "true",
        "false",
        "true",
    ]

    overdue = tasks.report_overdue_task("2026-11-25T16:00:00+00:00")
    # Every slot of the calendar except the twelve of the fixture hour: 114 - 12.
    assert overdue["overdue"] == 102 and len(overdue["slots"]) == 50


def test_a_failed_run_is_recorded_and_raised(stack: str) -> None:
    with pytest.raises(ValueError, match="'fixture' or 'live'"):
        tasks.ingest_task(AS_OF, "tape")

    with connect(stack, autocommit=True) as connection:
        row = connection.execute(
            "SELECT entry_point, status, error FROM ops.pipeline_run"
        ).fetchone()
    assert row is not None
    assert row[:2] == ("gdelt.ingest", "failed")
    assert row[2].startswith("ValueError: source must be")


def test_a_run_record_takes_the_context_of_airflow(
    stack: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "gdelt_ingest")
    monkeypatch.setenv("AIRFLOW_CTX_TASK_ID", "ingest")
    monkeypatch.setenv("AIRFLOW_CTX_DAG_RUN_ID", "scheduled__2026-11-25T08:08:00+00:00")
    monkeypatch.setenv("AIRFLOW_CTX_TRY_NUMBER", "2")

    with connect(stack, autocommit=True) as connection:
        with recorded_run(connection, "gdelt.ingest") as run:
            run.row_count = 3
        row = connection.execute(
            "SELECT dag_id, task_id, run_id, try_number, row_count, duration_ms >= 0, as_of "
            "FROM ops.pipeline_run"
        ).fetchone()
        assert record_results(connection, [], source="gdelt") == 0

    assert row == (
        "gdelt_ingest",
        "ingest",
        "scheduled__2026-11-25T08:08:00+00:00",
        2,
        3,
        True,
        None,
    )


def test_the_commands_of_gbmi_gdelt(stack: str) -> None:
    runner = CliRunner()

    def run(*arguments: str) -> tuple[int, dict[str, object], str]:
        result = runner.invoke(app, ["gdelt", *arguments])
        value = json.loads(result.stdout) if result.exit_code == 0 else {}
        return result.exit_code, value, result.output

    code, enqueued, _ = run(
        "enqueue", "--from", "2026-11-25", "--to", "2026-11-25", "--source", "fixture",
        "--as-of", AS_OF, "--reserve", "spark_backfill",
    )  # fmt: skip
    assert (code, enqueued["listed"], enqueued["reserved_for"]) == (0, 12, "spark_backfill")

    code, ingested, _ = run("ingest", "--source", "fixture", "--as-of", AS_OF, "--batch-limit", "5")
    assert (code, ingested["selected"], ingested["verified"], ingested["still_listed"]) == (
        0,
        5,
        5,
        5,
    )
    code, ingested, _ = run("ingest", "--source", "fixture", "--as-of", AS_OF, "--concurrency", "1")
    assert (code, ingested["verified"], ingested["still_listed"]) == (0, 7, 0)

    code, reconciled, _ = run("reconcile", "--source", "fixture", "--as-of", AS_OF)
    assert (code, reconciled["reconciled"]) == (0, True)
    code, reconciled, _ = run("reconcile", "--source", "fixture", "--as-of", AS_OF, "--force")
    assert (code, reconciled["reconciled"]) == (0, True)

    for arguments in (
        ("ingest", "--source", "tape", "--as-of", AS_OF),
        ("ingest", "--source", "fixture", "--as-of", "nine o'clock"),
        (
            "enqueue",
            "--from",
            "2026-11-26",
            "--to",
            "2026-11-25",
            "--source",
            "fixture",
            "--as-of",
            AS_OF,
        ),
        ("enqueue", "--from", "yesterday", "--to", "2026-11-25", "--source", "fixture"),
        ("reconcile", "--source", "tape", "--as-of", AS_OF, "--force"),
    ):
        code, _, output = run(*arguments)
        assert code == 1, arguments
        assert output.startswith(f"gbmi gdelt {arguments[0]}: "), output
        assert "Traceback" not in output
