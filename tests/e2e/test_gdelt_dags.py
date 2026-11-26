"""The three GDELT DAGs run once on the demo stack, by hand, on the recorded files (WP-07).

The DAG tests (``make test-dags``) check the wiring. The tests here check each DAG as Airflow
runs the DAG: the templates, the hand-over between the environments and the entry points. The
module runs after ``test_demo.py``: the tracer has landed and built the fixture hour.

The demo stack gives the DAGs no schedule, so an unpaused DAG never starts a run by the clock
against the live source.
"""

from __future__ import annotations

from datetime import date

import pytest

from gbmi.lake.commit import Unit
from tests.e2e.airflow import AirflowClient
from tests.e2e.test_demo import silver_state

pytestmark = pytest.mark.e2e

GDELT_DAGS = ("gdelt_ingest", "gdelt_silver_hourly", "gdelt_silver_daily")
#: The clock of the recorded hour: the files of 08:00 to 08:45 are published.
AS_OF = "2026-11-25T09:00:00+00:00"
#: The run which may start at 08:25 on 26 November has the logical date of the day before.
LOGICAL_DATE = "2026-11-25T08:25:00Z"
FIXTURE_DAY = Unit(date(2026, 11, 25))


@pytest.fixture(scope="module")
def airflow() -> AirflowClient:
    return AirflowClient()


def test_the_demo_stack_gives_the_gdelt_dags_no_schedule(airflow: AirflowClient) -> None:
    for dag_id in GDELT_DAGS:
        airflow.wait_until_listed(dag_id)
        details = airflow.dag(dag_id)

        assert details["timetable_summary"] is None, dag_id
        assert details["next_dagrun_run_after"] is None, dag_id


def test_gdelt_ingest_runs_by_hand_and_downloads_no_file_twice(airflow: AirflowClient) -> None:
    run = airflow.run("gdelt_ingest", {"source": "fixture", "as_of": AS_OF})

    assert run.task_states == {
        "reconcile": "success",
        "ingest": "success",
        "report_overdue": "success",
    }
    ingest = airflow.xcom("gdelt_ingest", run.run_id, "ingest")
    # The tracer has landed the twelve files of the hour: nothing is left to download.
    assert (ingest["as_of"], ingest["newest_slot"]) == (AS_OF, "20261125091500")
    assert (ingest["written"], ingest["rejected"], ingest["errors"]) == (0, 0, 0)
    # The reconciliation does its work once in each day of the clock.
    reconcile = airflow.xcom("gdelt_ingest", run.run_id, "reconcile")
    if reconcile["reconciled"]:
        assert (reconcile["files_in_the_master_list"], reconcile["bronze_differences"]) == (12, 0)
    assert airflow.xcom("gdelt_ingest", run.run_id, "report_overdue")["overdue"] == 0


def test_gdelt_silver_hourly_runs_with_the_clock_of_the_run(airflow: AirflowClient) -> None:
    # No parameter as_of: the clock is the time of the run, and the run has no logical date.
    run = airflow.run("gdelt_silver_hourly", {})

    assert run.task_states == {"find_units": "success", "build_units": "success"}
    found = airflow.xcom("gdelt_silver_hourly", run.run_id, "find_units")
    built = airflow.xcom("gdelt_silver_hourly", run.run_id, "build_units")
    assert built["selected"] == len(found["units"])
    assert (len(built["built"]), built["failed"]) == (len(found["units"]), [])


def test_gdelt_silver_daily_builds_the_day_of_its_logical_date(
    airflow: AirflowClient, expected_rows: dict[str, int]
) -> None:
    before = {name: silver_state(name)[1] for name in expected_rows}
    airflow.wait_until_listed("gdelt_silver_daily")
    airflow.delete_runs("gdelt_silver_daily", LOGICAL_DATE)

    run = airflow.run("gdelt_silver_daily", {"as_of": AS_OF}, logical_date=LOGICAL_DATE)

    assert run.task_states == {
        "build_export": "success",
        "build_mentions": "success",
        "build_gkg": "success",
        "summary": "success",
    }
    summary = airflow.xcom("gdelt_silver_daily", run.run_id, "summary")
    assert (summary["day"], summary["run_id"]) == ("2026-11-25", run.run_id)
    assert {name: table["rows"] for name, table in summary["tables"].items()} == expected_rows
    assert {table["files"] for table in summary["tables"].values()} == {4}
    loaded = {
        entry["feed"]: entry["slots"] for entry in summary["slots"] if entry["state"] == "loaded"
    }
    assert loaded == {"export": 4, "gkg": 4, "mentions": 4}
    # The day build replaced the hourly files with one file, and the content is the same.
    for name, checksum in before.items():
        assert silver_state(name)[1] == checksum, name
    built = airflow.xcom("gdelt_silver_daily", run.run_id, "build_gkg")
    assert built["unit_kind"] == "day"
    assert built["tables"]["gdelt_gkg_org"]["snapshot_operations"] == ["delete", "append"]
