"""End-to-end assertions on a finished ``make demo`` (week 1 plan, WP-09 step 10).

The demo has run the tracer on the committed fixture hour and provisioned Metabase. These
tests then check the acceptance criterion of phase P0 from the outside:

(a) the chart's total, read through Metabase's API, equals raw lines minus quarantined rows
    minus duplicate rows - each counted here, independently, straight from the fixture zips;
(b) silver, the mart and the chart agree;
(c) a second run of the DAG leaves the content checksums of silver and of the mart unchanged
    (blueprint 1.5, recommendation 7).

They run on the Compose network: ``make demo``, or ``make e2e`` against a stack that is up.
"""

from __future__ import annotations

import os
import zipfile
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from clickhouse_connect.driver.client import Client

from gbmi.config import get_settings
from gbmi.core.checksum import ContentChecksum, content_checksum
from gbmi.lake.catalog import load_lake_catalog
from gbmi.lake.commit import Unit
from gbmi.warehouse.client import connect
from monitoring.metabase.provision import ATTRIBUTION, CARD_NAME, DASHBOARD_NAME, MetabaseClient
from tests.e2e.airflow import FIXTURE_HOUR, AirflowClient, run_tracer

pytestmark = pytest.mark.e2e

FIXTURE_DIR = Path(os.environ.get("GBMI_FIXTURES_DIR", "/fixtures/gdelt/v2")) / "export"
UNIT = Unit(date(2026, 11, 25), 8)
EXPORT_COLUMNS = 61


# --- An independent count from the fixture zips ----------------------------------------------
# Written without the pipeline's code, so that it cannot inherit the pipeline's mistakes.

_INTEGER_FIELDS = (0, 25, 29, 31, 32, 33, 51)  # ids, flags and counts that must be integers
_NUMBER_FIELDS = (30, 34)  # Goldstein scale, average tone
_DATE_FIELD, _TIMESTAMP_FIELD = 1, 59


def _is_integer(text: str) -> bool:
    return text.removeprefix("-").isdigit()


def _is_number(text: str) -> bool:
    try:
        float(text)
    except ValueError:
        return False
    return True


def _row_is_valid(fields: list[str]) -> bool:
    if len(fields) != EXPORT_COLUMNS or not _is_integer(fields[0]):
        return False
    if any(fields[index] and not _is_integer(fields[index]) for index in _INTEGER_FIELDS[1:]):
        return False
    if any(fields[index] and not _is_number(fields[index]) for index in _NUMBER_FIELDS):
        return False
    day, stamp = fields[_DATE_FIELD], fields[_TIMESTAMP_FIELD]
    return (not day or (len(day) == 8 and day.isdigit())) and (
        not stamp or (len(stamp) == 14 and stamp.isdigit())
    )


@pytest.fixture(scope="module")
def expected() -> dict[str, int]:
    """Raw lines, quarantined rows and duplicate rows of the fixture hour."""
    raw_lines = quarantined = 0
    seen: set[str] = set()
    duplicates = 0
    archives = sorted(FIXTURE_DIR.glob("20261125080000.export.CSV.zip")) + sorted(
        path
        for path in FIXTURE_DIR.glob("202611250[8]*.export.CSV.zip")
        if "080000" not in path.name
    )
    assert len(archives) == 4, f"expected four fixture zips in {FIXTURE_DIR}"
    for archive_path in archives:
        with zipfile.ZipFile(archive_path) as archive:
            (member,) = archive.namelist()
            lines = archive.read(member).decode("utf-8", errors="replace").split("\n")
        if lines[-1] == "":
            lines.pop()
        for line in lines:
            raw_lines += 1
            fields = line.split("\t")
            if not _row_is_valid(fields):
                quarantined += 1
            elif fields[0] in seen:
                duplicates += 1
            else:
                seen.add(fields[0])
    return {
        "raw_lines": raw_lines,
        "quarantined": quarantined,
        "duplicates": duplicates,
        "committed": raw_lines - quarantined - duplicates,
    }


# --- The running platform ---------------------------------------------------------------------


@pytest.fixture(scope="module")
def clickhouse() -> Client:
    return connect()


@pytest.fixture(scope="module")
def metabase() -> MetabaseClient:
    client = MetabaseClient()
    client.wait_until_healthy()
    client.set_up_and_log_in()
    return client


def silver_state() -> tuple[int | None, ContentChecksum]:
    """Current snapshot and content checksum of the fixture hour in silver."""
    settings = get_settings()
    table = load_lake_catalog(settings).load_table((settings.silver_namespace, "gdelt_event"))
    snapshot = table.current_snapshot()
    checksum = content_checksum(table.scan(row_filter=UNIT.row_filter).to_arrow())
    return (snapshot.snapshot_id if snapshot else None), checksum


def mart_state(clickhouse: Client) -> dict[str, Any]:
    """Row count, event total and a checksum over the mart's business columns for November."""
    rows, events, checksum, snapshots, builds = clickhouse.query(
        """
        SELECT count(),
               sum(events),
               sum(cityHash64(date, action_geo_country_code, event_root_code, actor1_type1_code,
                              events, mentions, round(mean_tone, 6))),
               groupUniqArray(_silver_snapshot_id),
               groupUniqArray(_dbt_invocation_id)
        FROM marts.fct_gdelt_event_daily
        WHERE toYYYYMM(date) = 202611
        """
    ).result_rows[0]
    return {
        "rows": int(rows),
        "events": int(events),
        "checksum": int(checksum),
        "snapshots": [int(value) for value in snapshots],
        "builds": list(builds),
    }


def card_total(metabase: MetabaseClient) -> tuple[int, int]:
    card_id = metabase.find_card(CARD_NAME)
    assert card_id is not None, (
        f"Metabase has no question named {CARD_NAME!r}: run the provisioning"
    )
    rows = metabase.card_rows(card_id)
    return sum(int(row[1]) for row in rows), len(rows)


# --- (a) ---------------------------------------------------------------------------------------


def test_fixture_hour_is_what_the_plan_says(expected: dict[str, int]) -> None:
    """The four slots of 2026-11-25 08:00-08:45 UTC: 4,023 rows, none bad, none repeated."""
    assert expected == {"raw_lines": 4023, "quarantined": 0, "duplicates": 0, "committed": 4023}


def test_chart_total_equals_raw_minus_quarantined_minus_duplicates(
    metabase: MetabaseClient, expected: dict[str, int]
) -> None:
    total, bars = card_total(metabase)

    assert total == expected["raw_lines"] - expected["quarantined"] - expected["duplicates"]
    assert bars > 1  # a chart of several CAMEO root codes, not a single number


# --- (b) ---------------------------------------------------------------------------------------


def test_silver_mart_and_chart_agree(
    metabase: MetabaseClient, clickhouse: Client, expected: dict[str, int]
) -> None:
    _, silver = silver_state()
    mart = mart_state(clickhouse)
    total, _ = card_total(metabase)

    assert silver.rows == expected["committed"]
    assert mart["events"] == silver.rows
    assert total == mart["events"]
    # ClickHouse reads the same rows through the pinned table.
    assert clickhouse.query("SELECT count() FROM lake.gdelt_event").result_rows[0][0] == silver.rows


def test_dashboard_has_one_chart_and_the_gdelt_attribution(metabase: MetabaseClient) -> None:
    dashboard_id = metabase.find_dashboard(DASHBOARD_NAME)
    assert dashboard_id is not None, f"Metabase has no dashboard named {DASHBOARD_NAME!r}"

    dashcards = metabase.dashboard(dashboard_id)["dashcards"]

    charts = [card for card in dashcards if card["card_id"] is not None]
    texts = [
        card["visualization_settings"].get("text", "")
        for card in dashcards
        if card["card_id"] is None
    ]
    assert len(charts) == 1
    assert charts[0]["card"]["name"] == CARD_NAME
    assert texts == [ATTRIBUTION]
    assert "https://www.gdeltproject.org/" in texts[0]


def test_bi_user_reads_marts_only(clickhouse: Client) -> None:
    """Metabase connects as ``bi``: it may read marts and nothing below them (blueprint 5.8)."""
    password = os.environ["CH_BI_PASSWORD"]
    host, port = os.environ.get("DBT_CH_HOST", "clickhouse"), os.environ.get("DBT_CH_PORT", "8123")
    bi = connect(f"http://bi:{password}@{host}:{port}")

    assert bi.query("SELECT count() FROM marts.fct_gdelt_event_daily").result_rows[0][0] > 0
    for forbidden in ("lake.gdelt_event", "staging.stg_gdelt__event"):
        with pytest.raises(Exception, match=r"ACCESS_DENIED|Not enough privileges"):
            bi.query(f"SELECT count() FROM {forbidden}")
    with pytest.raises(Exception, match=r"ACCESS_DENIED|Not enough privileges"):
        bi.command("TRUNCATE TABLE marts.fct_gdelt_event_daily")


# --- (c) ---------------------------------------------------------------------------------------


def test_second_run_leaves_the_content_checksums_unchanged(
    metabase: MetabaseClient, clickhouse: Client, expected: dict[str, int]
) -> None:
    snapshot_before, silver_before = silver_state()
    mart_before = mart_state(clickhouse)

    rerun = run_tracer(AirflowClient(), FIXTURE_HOUR, "fixture")

    snapshot_after, silver_after = silver_state()
    mart_after = mart_state(clickhouse)
    # Same rows in silver; a new snapshot, by design (blueprint 5.6).
    assert silver_after == silver_before
    assert snapshot_after != snapshot_before
    assert rerun["build_silver"]["snapshot_operations"] == ["delete", "append"]
    counts = rerun["build_silver"]["counts"]
    assert {name: counts[name] for name in expected} == expected
    assert counts["parsed"] == expected["raw_lines"] - expected["quarantined"]
    # Nothing was downloaded again: the files were already in bronze.
    assert rerun["land"]["written"] == 0
    # Same rows in the mart, rebuilt by a new dbt run from the new snapshot.
    assert mart_after["checksum"] == mart_before["checksum"]
    assert (mart_after["rows"], mart_after["events"]) == (
        mart_before["rows"],
        mart_before["events"],
    )
    assert mart_after["builds"] != mart_before["builds"]
    assert mart_after["snapshots"] == [snapshot_after]
    # And the chart still shows the same total.
    assert card_total(metabase)[0] == expected["committed"]
