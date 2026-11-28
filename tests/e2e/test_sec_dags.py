"""The three SEC DAGs run once on the demo stack, by hand, on the recorded files (WP-07).

The DAG tests (``make test-dags``) check the wiring, and ``tests/ledger/test_sec_pipelines.py``
checks the rules of the pipelines. The tests here check each DAG as Airflow runs the DAG: the
templates, the hand-over between the environments, the pool and the entry points. The clock of
each run is fixed: 2026-11-26 12:00 UTC, the day after the recorded index file.

The module runs on a stack on which ``make demo`` may have run before: every assertion holds for
the first demo of a stack and for a later one.
"""

from __future__ import annotations

from typing import Any

import pytest

from gbmi.config import get_settings
from gbmi.core.checksum import ContentChecksum, content_checksum
from gbmi.lake.catalog import load_lake_catalog
from gbmi.ledger.db import connect as connect_ledger
from gbmi.ops.requests import max_requests_in_window
from tests.e2e.airflow import AirflowClient
from tests.e2e.sec_fixture_counts import sec_rows, watchlist

pytestmark = pytest.mark.e2e

#: The DAGs in the order of their schedules, each with its task which asks the SEC.
SEC_DAGS = {"sec_filings": "filings", "sec_reference_daily": "reference", "sec_xbrl_facts": "facts"}
#: The fixed clock of the demo.
AS_OF = "2026-11-26T12:00:00+00:00"
DAY = "2026-11-26"


@pytest.fixture(scope="module")
def airflow() -> AirflowClient:
    return AirflowClient()


def table_state(name: str) -> ContentChecksum:
    """The content checksum of every row of a silver table."""
    settings = get_settings()
    table = load_lake_catalog(settings).load_table((settings.silver_namespace, name))
    return content_checksum(table.scan().to_arrow())


def ledger_rows(statement: str, *arguments: Any) -> list[tuple[Any, ...]]:
    with connect_ledger(autocommit=True) as connection:
        rows: list[tuple[Any, ...]] = connection.execute(statement, arguments).fetchall()
    return rows


def requests_sent() -> int:
    return int(ledger_rows("SELECT count(*) FROM ops.source_request WHERE source = 'sec'")[0][0])


def run_the_three(airflow: AirflowClient, **conf: Any) -> dict[str, dict[str, Any]]:
    """Run the three DAGs one after the other; give the summary of each."""
    summaries = {}
    for dag_id, task_id in SEC_DAGS.items():
        run = airflow.run(dag_id, {"as_of": AS_OF, **conf.get(dag_id, {})})
        assert run.task_states == {task_id: "success", "summary": "success"}, dag_id
        summaries[dag_id] = airflow.xcom(dag_id, run.run_id, "summary")
        assert summaries[dag_id]["run_id"] == run.run_id
    return summaries


def test_the_demo_stack_gives_the_sec_dags_no_schedule(airflow: AirflowClient) -> None:
    for dag_id in SEC_DAGS:
        airflow.wait_until_listed(dag_id)
        details = airflow.dag(dag_id)

        assert details["timetable_summary"] is None, dag_id
        assert details["next_dagrun_run_after"] is None, dag_id


def test_the_three_sec_dags_load_the_recorded_files(airflow: AirflowClient) -> None:
    summaries = run_the_three(airflow)

    # The rows of the five tables equal the counts which the recorded files give to code
    # which shares nothing with the pipeline.
    expected = sec_rows()
    assert expected == {
        "sec_filing_index": 298,
        "sec_filing_detail": 293,
        "sec_company_snapshot": 10,
        "sec_company_former_name": 4,
        "sec_xbrl_fact": 12264,
    }
    assert {name: table_state(name).rows for name in expected} == expected
    assert {dag_id: summary["status"] for dag_id, summary in summaries.items()} == dict.fromkeys(
        SEC_DAGS, "success"
    )
    assert summaries["sec_filings"]["day"] == DAY
    # Every object of the SEC is in a final state.
    open_objects = ledger_rows(
        "SELECT dataset, object_date::text, object_name, state FROM ops.source_object "
        "WHERE source = 'sec' AND state IN ('expected', 'verified', 'quarantined')"
    )
    assert open_objects == []
    loaded = dict(
        ledger_rows(
            "SELECT dataset, count(*) FROM ops.source_object WHERE source = 'sec' "
            "AND object_date = %s AND state = 'loaded' GROUP BY dataset",
            DAY,
        )
    )
    assert loaded == {
        "bulk": 1,
        "companyfacts": len(watchlist()),
        "index": 1,  # the listing of the quarter; the index file has its own date
        "reference": 1,
        "submissions": 8,
    }
    # The four weekdays before the recorded index file were closed without a request.
    closed = ledger_rows(
        "SELECT count(*), count(http_status) FROM ops.source_object WHERE source = 'sec' "
        "AND dataset = 'index' AND state = 'not_published'"
    )
    assert closed == [(4, 0)]


def test_a_second_run_sends_no_request_and_leaves_the_content(airflow: AirflowClient) -> None:
    names = list(sec_rows())
    before = {name: table_state(name) for name in names}
    sent = requests_sent()

    # The parameters ask for the most work: the archive again, and every facts document.
    summaries = run_the_three(
        airflow, sec_reference_daily={"bulk": True}, sec_xbrl_facts={"full": True}
    )

    assert requests_sent() == sent
    assert {dag_id: summary["requests"]["sent"] for dag_id, summary in summaries.items()} == (
        dict.fromkeys(SEC_DAGS, 0)
    )
    after = {name: table_state(name) for name in names}
    assert {name: (state.rows, state.digest) for name, state in after.items()} == {
        name: (state.rows, state.digest) for name, state in before.items()
    }


def test_no_second_of_the_request_log_holds_more_than_eight_requests() -> None:
    # The DAG tasks used the real clock of the limiter.
    with connect_ledger(autocommit=True) as connection:
        most = max_requests_in_window(connection, source="sec")

    assert 1 <= most <= 8
    assert requests_sent() >= 14
    refused = ledger_rows(
        "SELECT count(*) FROM ops.source_request WHERE source = 'sec' AND outcome = 'blocked'"
    )
    assert refused == [(0,)]
    assert ledger_rows(
        "SELECT count(*) FROM ops.source_circuit WHERE blocked_until IS NOT NULL"
    ) == [(0,)]
