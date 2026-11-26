"""The operational models: completeness comes from the ledger, and no model holds a password.

``make demo`` has built ``ops.ops_slot_status_daily`` after the tracer (week 2 plan, WP-08).
ClickHouse reads the ledger through the named collection ``pg_ops``, as the read-only
PostgreSQL role ``gbmi_ro``.
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import psycopg
import pytest
from clickhouse_connect.driver.client import Client

from gbmi.ledger.db import connect as connect_postgres
from gbmi.ledger.repository import open_ledger
from gbmi.warehouse.client import connect

pytestmark = pytest.mark.e2e

DAY = date(2026, 11, 25)
DBT = Path(__file__).resolve().parents[2] / "dbt"
COMPILED = DBT / "target" / "compiled" / "gbmi" / "models" / "staging" / "ops"
DENIED = r"ACCESS_DENIED|Not enough privileges"


@pytest.fixture(scope="module")
def clickhouse() -> Client:
    return connect()


def reader_dsn(database: str = "gbmi") -> str:
    return f"postgresql://gbmi_ro:{os.environ['GBMI_RO_DB_PASSWORD']}@postgres:5432/{database}"


def test_the_slot_status_model_shows_the_fixture_day(clickhouse: Client) -> None:
    rows = clickhouse.query(
        "SELECT feed, calendar_slots, loaded_slots, confirmed_missing_slots, "
        "round(completeness, 6), toBool(is_complete), raw_lines, parsed_rows + quarantined_rows "
        "FROM ops.ops_slot_status_daily "
        "WHERE source = 'gdelt' AND slot_date = '2026-11-25' ORDER BY feed"
    ).result_rows

    # Four loaded slots of 96 for each feed: the fixture holds one hour of the day.
    lines = {"export": 4023, "gkg": 240, "mentions": 7882}
    assert rows == [
        ("all", 288, 12, 0, round(12 / 288, 6), False, sum(lines.values()), sum(lines.values())),
        *(
            (feed, 96, 4, 0, round(4 / 96, 6), False, lines[feed], lines[feed])
            for feed in ("export", "gkg", "mentions")
        ),
    ]


def test_the_model_and_the_ledger_report_the_same_completeness(clickhouse: Client) -> None:
    model = {
        row[0]: row[1:]
        for row in clickhouse.query(
            "SELECT feed, calendar_slots, loaded_slots, confirmed_missing_slots, "
            "round(completeness, 6), toBool(is_complete) FROM ops.ops_slot_status_daily "
            "WHERE source = 'gdelt' AND slot_date = '2026-11-25'"
        ).result_rows
    }
    with open_ledger() as ledger:
        report = ledger.completeness(DAY, DAY)

    assert len(report) == len(model) == 4
    for entry in report:
        assert model[entry["feed"] or "all"] == (
            entry["expected"],
            entry["loaded"],
            entry["confirmed_missing"],
            entry["completeness"],
            entry["complete"],
        ), entry


def test_no_dbt_artefact_holds_the_password_of_the_reader() -> None:
    password = os.environ["GBMI_RO_DB_PASSWORD"]
    compiled = COMPILED / "stg_ops__source_slot.sql"
    # Without the compiled model the search would pass on an empty directory.
    assert compiled.is_file(), "the operational models were not built: no compiled model"
    assert "postgresql(pg_ops," in compiled.read_text(encoding="utf-8")

    holders = [
        str(path.relative_to(DBT))
        for directory in (DBT / "target", DBT / "logs")
        for path in directory.rglob("*")
        if path.is_file() and password in path.read_text(encoding="utf-8", errors="ignore")
    ]

    assert holders == []


def test_the_reader_role_reads_the_schema_ops_and_nothing_else() -> None:
    with connect_postgres(reader_dsn(), autocommit=True) as reader:
        assert reader.execute("SELECT count(*) FROM ops.source_slot").fetchone() >= (12,)
        assert reader.execute("SELECT count(*) FROM ops.silver_partition").fetchone() >= (4,)
        for statement in (
            "UPDATE ops.source_slot SET attempts = 0",
            "DELETE FROM ops.slot_transition",
            "INSERT INTO ops.dq_result (gate, check_name, status) VALUES ('G1', 'x', 'passed')",
            "CREATE TABLE ops.reader_table (x integer)",
            "CREATE TABLE public.reader_table (x integer)",
        ):
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                reader.execute(statement)
    for database in ("airflow", "metabase"):
        with (
            pytest.raises(psycopg.OperationalError, match="permission denied for database"),
            connect_postgres(reader_dsn(database), autocommit=True),
        ):
            pass


def test_the_bi_user_reads_the_operational_mart_and_not_the_ledger(clickhouse: Client) -> None:
    password = os.environ["CH_BI_PASSWORD"]
    host, port = os.environ.get("DBT_CH_HOST", "clickhouse"), os.environ.get("DBT_CH_PORT", "8123")
    bi = connect(f"http://bi:{password}@{host}:{port}")

    assert bi.query("SELECT count() FROM ops.ops_slot_status_daily").result_rows[0][0] >= 4
    for forbidden in (
        "staging.stg_ops__source_slot",
        "postgresql(pg_ops, schema = 'ops', table = 'source_slot')",
    ):
        with pytest.raises(Exception, match=DENIED):
            bi.query(f"SELECT count() FROM {forbidden}")
