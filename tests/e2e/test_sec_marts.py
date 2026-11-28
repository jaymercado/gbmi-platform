"""The DAG marts_build on the demo stack, after the three SEC DAGs (week 3 plan, WP-08).

The module runs after ``test_sec_dags.py``: the five silver tables hold the recorded files of
the SEC. The run pins the five tables and builds the SEC models through Cosmos. The assertions
are exit-gate rows 2 and 3 of week 3 on the recorded files: the name change of Meta in the
company dimension, and the dbt unit tests for validity intervals and for as-of logic. Expected
numbers come from ``sec_fixture_counts``, code which shares nothing with the pipeline.

The module runs on a stack on which ``make demo`` may have run before: every assertion holds
for the first demo of a stack and for a later one.
"""

from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from clickhouse_connect.driver.client import Client

from gbmi.config import get_settings
from gbmi.lake.catalog import load_lake_catalog
from gbmi.ledger.db import connect as connect_ledger
from gbmi.warehouse.client import connect
from tests.e2e import sec_fixture_counts as recorded
from tests.e2e.airflow import AirflowClient, DagRun

pytestmark = pytest.mark.e2e

DAG = "marts_build"
DBT = Path(__file__).resolve().parents[2] / "dbt"
META, MATTEL = 1326801, 63276
#: Each mart with its main silver table (the table whose snapshot the mart records).
MARTS = {
    "dim_company": "sec_company_snapshot",
    "fct_filing": "sec_filing_index",
    "fct_filing_item": "sec_filing_detail",
    "fct_financial_fact": "sec_xbrl_fact",
    "fct_financial_metric": "sec_xbrl_fact",
}
#: The annual report of Mattel for 2025: filed on 23 February 2026, accepted at 22:03:18 UTC.
ANNUAL_REPORT = "0001628280-26-010716"
ACCEPTED = datetime(2026, 2, 23, 22, 3, 18, tzinfo=UTC)
NET_INCOME = ("us-gaap", "NetIncomeLoss", "USD")


@pytest.fixture(scope="module")
def airflow() -> AirflowClient:
    return AirflowClient()


@pytest.fixture(scope="module")
def clickhouse() -> Client:
    return connect()


@pytest.fixture(scope="module")
def build(airflow: AirflowClient) -> DagRun:
    """One run of marts_build on the tables which the three SEC DAGs have loaded."""
    return airflow.run(DAG, {})


def rows(clickhouse: Client, statement: str) -> list[tuple[Any, ...]]:
    return list(clickhouse.query(statement).result_rows)


def test_the_demo_stack_gives_marts_build_no_schedule(airflow: AirflowClient) -> None:
    airflow.wait_until_listed(DAG)
    details = airflow.dag(DAG)

    assert details["timetable_summary"] is None
    assert details["next_dagrun_run_after"] is None


def test_the_run_builds_the_five_marts_from_the_pinned_tables(
    airflow: AirflowClient, build: DagRun
) -> None:
    assert set(build.task_states.values()) == {"success"}
    assert {"pin_lake", "record", "dbt.dim_company.run"} <= set(build.task_states)
    # No task of the run is a dbt unit test: a build checks data, not code.
    assert [name for name in build.task_states if "unit" in name] == []

    pinned = airflow.xcom(DAG, build.run_id, "pin_lake")
    record = airflow.xcom(DAG, build.run_id, "record")

    settings = get_settings()
    catalog = load_lake_catalog(settings)
    current = {}
    for name in set(MARTS.values()) | {"sec_company_former_name"}:
        snapshot = catalog.load_table((settings.silver_namespace, name)).current_snapshot()
        assert snapshot is not None, name
        current[name] = snapshot.snapshot_id
    assert pinned["snapshot_ids"] == current
    assert {name: entry["rows"] for name, entry in pinned["tables"].items()} == recorded.sec_rows()
    assert record["created"] == []
    assert record["status"] == "success"
    assert record["pipeline_version"] == settings.pipeline_version
    for mart, silver in MARTS.items():
        assert record["marts"][mart]["silver_snapshot_ids"] == [current[silver]], mart
        assert record["marts"][mart]["pipeline_versions"] == [settings.pipeline_version], mart
    # The run is recorded once, with the rows of the five marts.
    with connect_ledger(autocommit=True) as connection:
        stored = connection.execute(
            "SELECT status, row_count, detail -> 'marts' FROM ops.pipeline_run "
            "WHERE entry_point = 'marts.build' AND run_id = %s",
            (build.run_id,),
        ).fetchall()
    counts = {mart: state["rows"] for mart, state in record["marts"].items()}
    assert stored == [("success", sum(counts.values()), counts)]


def test_the_company_dimension_shows_the_name_change_of_meta(
    clickhouse: Client, build: DagRun
) -> None:
    # Exit-gate row 2 of week 3: "Facebook Inc" from former-name data, valid from 2005-05-06
    # to 2021-10-27, and "Meta Platforms, Inc." from 2021-10-27 on. The first snapshot of the
    # demo is the day of its fixed clock.
    history = rows(
        clickhouse,
        "SELECT name, history_source, toString(valid_from), toString(valid_to), is_current, "
        f"is_watchlist, primary_ticker FROM marts.dim_company WHERE cik = {META} "
        "ORDER BY valid_from",
    )

    assert history == [
        ("Facebook Inc", "former_name", "2005-05-06", "2021-10-27", False, True, None),
        ("Meta Platforms, Inc.", "former_name", "2021-10-27", "2026-11-26", False, True, None),
        ("Meta Platforms, Inc.", "snapshot", "2026-11-26", "9999-12-31", True, True, "META"),
    ]


def test_the_company_dimension_covers_the_universe(clickhouse: Client, build: DagRun) -> None:
    universe = recorded.universe()
    # The former names with a first day, as silver holds them.
    former = {
        cik: len({(name["name"], name["from"]) for name in document["formerNames"] if name["from"]})
        for cik, document in recorded.submissions_documents().items()
        if document["formerNames"]
    }

    counted = rows(
        clickhouse,
        "SELECT count(), countIf(is_current), uniqExact(cik), uniqExactIf(cik, is_watchlist), "
        "countIf(history_source = 'former_name') FROM marts.dim_company",
    )
    dropped = rows(
        clickhouse,
        "SELECT count() FROM intermediate.int_company__former_name_versions "
        "WHERE range_status != 'kept'",
    )

    # One current row for each company; each company with former names has one more row
    # for each former name and one for its current name before the first snapshot.
    before = sum(count + 1 for count in former.values())
    assert counted == [
        (len(universe) + before, len(universe), len(universe), len(recorded.watchlist()), before)
    ]
    assert dropped == [(0,)]


def test_fct_filing_has_one_row_for_each_filing_and_filer(
    clickhouse: Client, build: DagRun
) -> None:
    pairs = recorded.filing_pairs()

    counted = rows(
        clickhouse,
        "SELECT count(), uniqExact(accession_number, cik), countIf(detail_source = 'index_only'), "
        "countIf(detail_source = 'submissions_only'), "
        "countIf(detail_source = 'index_and_submissions') FROM marts.fct_filing",
    )
    items = rows(clickhouse, "SELECT count() FROM marts.fct_filing_item")
    link = rows(
        clickhouse,
        "SELECT form, filing_url, primary_document_url, accepted_at FROM marts.fct_filing "
        f"WHERE cik = {MATTEL} AND accession_number = '{ANNUAL_REPORT}'",
    )

    in_index = recorded.index_pairs()
    both = len(in_index) + recorded.detail_rows() - len(pairs)
    assert counted == [
        (len(pairs), len(pairs), len(in_index) - both, len(pairs) - len(in_index), both)
    ]
    assert both > 0
    assert items == [(recorded.filing_item_rows(),)]
    assert items[0][0] > 0
    # The two addresses of a filing at sec.gov (use case UC-1).
    folder = "https://www.sec.gov/Archives/edgar/data/63276"
    (form, filing_url, document_url, accepted_at) = link[0]
    assert (form, filing_url) == ("10-K", f"{folder}/{ANNUAL_REPORT}-index.html")
    assert document_url.startswith(f"{folder}/{ANNUAL_REPORT.replace('-', '')}/")
    assert accepted_at.replace(tzinfo=UTC) == ACCEPTED


def known_in_the_mart(clickhouse: Client, moment: datetime) -> dict[tuple[str, str], Decimal]:
    """The as-of question of blueprint 7.10 on the mart, for the net income of Mattel."""
    taxonomy, concept, unit = NET_INCOME
    answer = rows(
        clickhouse,
        "SELECT toString(period_start), toString(period_end), "
        "toString(argMax(value, (known_at, accession_number))) FROM marts.fct_financial_fact "
        f"WHERE cik = {MATTEL} AND taxonomy = '{taxonomy}' AND concept = '{concept}' "
        f"AND unit = '{unit}' AND known_at <= toDateTime('{moment:%Y-%m-%d %H:%M:%S}', 'UTC') "
        "GROUP BY period_start, period_end",
    )
    return {(start, end): Decimal(value) for start, end, value in answer}


def test_an_as_of_question_gives_what_was_known_at_the_time(
    clickhouse: Client, build: DagRun
) -> None:
    year = ("2025-01-01", "2025-12-31")
    just_before = ACCEPTED - timedelta(seconds=1)
    expected = recorded.known_values(MATTEL, *NET_INCOME, moment=ACCEPTED)
    expected_before = recorded.known_values(MATTEL, *NET_INCOME, moment=just_before)
    everything = recorded.known_values(MATTEL, *NET_INCOME)

    # The recorded file makes the question a real one: the annual report adds the year 2025
    # in the second of its acceptance, and later reports add periods and change values.
    assert year in expected and year not in expected_before
    assert len(expected_before) < len(expected) < len(everything)
    restated = [period for period, value in expected.items() if everything[period] != value]
    assert restated

    # The answers of the mart equal the answers of the separate code, for each period. The
    # acceptance time decides: by the filing date alone, the year 2025 would be known seven
    # hours later.
    assert known_in_the_mart(clickhouse, ACCEPTED) == expected
    assert known_in_the_mart(clickhouse, just_before) == expected_before
    assert known_in_the_mart(clickhouse, datetime(2100, 1, 1, tzinfo=UTC)) == everything
    source = rows(
        clickhouse,
        "SELECT known_at_source, toString(known_at) FROM marts.fct_financial_fact "
        f"WHERE cik = {MATTEL} AND concept = 'NetIncomeLoss' AND period_start = '{year[0]}' "
        f"AND period_end = '{year[1]}' AND accession_number = '{ANNUAL_REPORT}'",
    )
    assert source == [("acceptance_time", "2026-02-23 22:03:18")]


def test_the_metric_mart_keeps_the_first_and_the_latest_value(
    clickhouse: Client, build: DagRun
) -> None:
    first = recorded.first_values(MATTEL, *NET_INCOME)
    latest = recorded.known_values(MATTEL, *NET_INCOME)

    metric = rows(
        clickhouse,
        "SELECT toString(period_start), toString(period_end), toString(value_first_reported), "
        "toString(value_latest), is_restated FROM marts.fct_financial_metric "
        f"WHERE cik = {MATTEL} AND metric = 'net_income' AND concept = 'NetIncomeLoss' "
        "AND NOT is_derived",
    )

    # Each reported period of the mart shows the first and the newest value of the recorded
    # file, and the flag says whether the two differ.
    assert len(metric) > 100
    for start, end, value_first, value_latest, is_restated in metric:
        period = (start, end)
        assert (Decimal(value_first), Decimal(value_latest)) == (first[period], latest[period])
        assert is_restated == (first[period] != latest[period]), period
    assert sum(1 for row in metric if row[4]) > 0
    # Every company of the watchlist has revenue, net income, total assets, equity and cash.
    coverage = dict(
        rows(clickhouse, "SELECT metric, uniqExact(cik) FROM marts.fct_financial_metric GROUP BY 1")
    )
    watched = len(recorded.watchlist())
    assert {name: coverage.get(name) for name in ("revenue", "net_income", "total_assets")} == (
        dict.fromkeys(("revenue", "net_income", "total_assets"), watched)
    )


def test_the_dbt_unit_tests_pass(tmp_path: Path, build: DagRun) -> None:
    # Exit-gate row 3 of week 3: the unit tests for validity intervals and for as-of logic.
    # The models of the stack give the unit tests the types of their input columns, so the
    # tests run after the build; they build no model.
    result = subprocess.run(
        [
            "/opt/venvs/dbt/bin/dbt",
            "test",
            "--project-dir",
            str(DBT),
            "--profiles-dir",
            str(DBT / "profiles"),
            "--target-path",
            str(tmp_path / "target"),
            "--select",
            "test_type:unit",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=600,
    )

    assert result.returncode == 0, result.stdout[-4000:]
    results = json.loads((tmp_path / "target" / "run_results.json").read_text(encoding="utf-8"))
    statuses = {entry["unique_id"]: entry["status"] for entry in results["results"]}
    assert set(statuses.values()) == {"pass"}
    assert all(name.startswith("unit_test.gbmi.") for name in statuses)
    # The step fails when no unit test ran: an empty selection is no proof.
    names = {name.rsplit(".", 1)[-1] for name in statuses}
    assert len(names) >= 8
    assert {
        "snapshot_versions_a_b_a_gives_three_intervals",
        "former_name_versions_end_where_the_snapshots_start",
        "metric_facts_as_of_a_time_before_the_restatement_give_the_first_value",
        "quarters_derive_the_missing_quarter_as_year_minus_nine_months",
    } <= names
