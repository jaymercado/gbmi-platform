"""The SEC pipelines on the recorded files, read back through ClickHouse (week 3 plan, WP-06).

``tests/ledger/test_sec_pipelines.py`` holds the rules of the pipelines. The test here runs the
three task entry points once in the Compose network, with the real clock of the limiter, and
reads the five tables through ClickHouse: the rows which the pipelines write are rows which the
warehouse can read.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from clickhouse_connect.driver.client import Client

from gbmi.config import get_settings
from gbmi.lake.tables import (
    SEC_COMPANY_FORMER_NAME,
    SEC_COMPANY_SNAPSHOT,
    SEC_FILING_DETAIL,
    SEC_FILING_INDEX,
    SEC_SCHEMAS,
    SEC_XBRL_FACT,
)
from gbmi.ledger.db import connect
from gbmi.ops.requests import max_requests_in_window
from gbmi.sources.sec import tasks
from gbmi.warehouse.pin import pin_table
from tests.integration.conftest import SHARED_LAKE, Lake

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[3]
AS_OF = "2026-11-26T12:00:00+00:00"
META, NVIDIA, MATTEL, ALPHABET = 1326801, 1045810, 63276, 1652044


@pytest.fixture
def loaded(local_lake: Lake, scratch_dsn: str, monkeypatch: pytest.MonkeyPatch) -> Iterator[Lake]:
    """The three pipelines ran once on the recorded files, into a lake which ClickHouse sees."""
    if not local_lake.settings.lake_uri.startswith(f"file://{SHARED_LAKE}/"):
        pytest.skip("needs the lake mount of the Compose stack (run with make integration)")
    for name, value in {
        "GBMI_PG_DSN": scratch_dsn,
        "GBMI_LAKE_BACKEND": "local",
        "GBMI_LAKE_URI": local_lake.settings.lake_uri,
        "GBMI_CATALOG_URI": local_lake.settings.catalog_uri,
        "GBMI_SEC_SOURCE": "fixture",
        "GBMI_SEC_FIXTURES_DIR": str(REPO / "tests" / "fixtures" / "sec"),
        "GBMI_WATCHLIST_FILE": str(REPO / "dbt" / "seeds" / "watchlist.csv"),
        "GBMI_WATCHLIST_SIZE": "5",
    }.items():
        monkeypatch.setenv(name, value)
    get_settings.cache_clear()
    tasks.filings_task(as_of=AS_OF)
    tasks.reference_task(as_of=AS_OF)
    tasks.facts_task(as_of=AS_OF)
    yield local_lake
    get_settings.cache_clear()


@pytest.fixture
def pinned(clickhouse: Client) -> Iterator[dict[str, str]]:
    names = {name: f"{name}_{uuid.uuid4().hex[:8]}" for name in SEC_SCHEMAS}
    try:
        yield names
    finally:
        for name in names.values():
            clickhouse.command(f"DROP TABLE IF EXISTS lake.{name}")


def test_clickhouse_reads_the_rows_which_the_pipelines_wrote(
    loaded: Lake, scratch_dsn: str, clickhouse: Client, pinned: dict[str, str]
) -> None:
    counts = {}
    for name in SEC_SCHEMAS:
        pin_table(
            clickhouse, loaded.catalog.load_table((loaded.namespace, name)), name=pinned[name]
        )
        counts[name] = clickhouse.query(f"SELECT count() FROM lake.{pinned[name]}").result_rows[0][
            0
        ]

    assert counts == {
        SEC_COMPANY_SNAPSHOT: 10,
        SEC_COMPANY_FORMER_NAME: 4,
        SEC_FILING_INDEX: 298,
        SEC_FILING_DETAIL: 293,
        SEC_XBRL_FACT: 12264,
    }
    # The name change of exit-gate row 2, as the dimension of WP-08 will read the row.
    former = clickhouse.query(
        f"SELECT former_name, valid_from, valid_to FROM lake.{pinned[SEC_COMPANY_FORMER_NAME]} "
        f"WHERE cik = {META}"
    ).result_rows
    assert former == [("Facebook Inc", date(2005, 5, 6), date(2021, 10, 27))]
    # The lists arrive as arrays, in the order of the ticker file.
    tickers = clickhouse.query(
        f"SELECT tickers, primary_ticker, is_stale FROM lake.{pinned[SEC_COMPANY_SNAPSHOT]} "
        f"WHERE cik = {ALPHABET}"
    ).result_rows
    assert tickers == [(["GOOGL", "GOOG", "GOOGM", "GOOGN"], "GOOGL", False)]
    earnings = clickhouse.query(
        f"SELECT countIf(has(items, '2.02')), countIf(acceptance_ts IS NULL), uniqExact(cik) "
        f"FROM lake.{pinned[SEC_FILING_DETAIL]}"
    ).result_rows[0]
    assert earnings[0] > 0 and earnings[1:] == (0, 5)
    # A decimal value arrives digit for digit: the sum of ClickHouse is the sum of the rows.
    table = loaded.catalog.load_table((loaded.namespace, SEC_XBRL_FACT))
    values = table.scan(selected_fields=("cik", "value")).to_arrow().to_pylist()
    by_rows = sum((row["value"] for row in values if row["cik"] == MATTEL), Decimal(0))
    by_clickhouse = clickhouse.query(
        f"SELECT sum(value), count() FROM lake.{pinned[SEC_XBRL_FACT]} WHERE cik = {MATTEL}"
    ).result_rows[0]
    assert (Decimal(by_clickhouse[0]), by_clickhouse[1]) == (by_rows, 3215)
    # The example of finding F6: the revenue of the year to January 2021 from the report of 2022.
    revenue = clickhouse.query(
        f"SELECT fiscal_year, fiscal_period, value_text, toString(value), is_instant "
        f"FROM lake.{pinned[SEC_XBRL_FACT]} WHERE cik = {NVIDIA} AND concept = 'Revenues' "
        f"AND period_start = '2020-01-27' AND period_end = '2021-01-31' "
        f"AND accession_number = '0001045810-22-000036'"
    ).result_rows
    assert revenue == [(2022, "FY", "16675000000", "16675000000", False)]
    # With the real clock of the limiter: no second of the request log holds more than 8.
    with connect(scratch_dsn, autocommit=True) as connection:
        assert 1 <= max_requests_in_window(connection, source="sec") <= 8
        sent = connection.execute("SELECT count(*) FROM ops.source_request").fetchone()
    assert sent == (14,)
