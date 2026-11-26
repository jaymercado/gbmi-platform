"""The entry points which the tasks of the DAGs call, on the recorded fixture hour (WP-07).

The tracer lands one named hour through the ledger and builds the hour of the three feeds. The
hourly job finds units and builds the units it was given. The daily job builds the day of its
logical date and sums up what the run built.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from gbmi.ledger.db import connect
from gbmi.sources.gdelt import tasks as gdelt_tasks
from gbmi.transforms import build, tasks

HOUR = "2026-11-25T08"
AS_OF = "2026-11-25T09:00:00+00:00"
LOGICAL_DATE = "2026-11-25T08:25:00+00:00"
FIXTURE_BYTES = 263_180 + 288_245 + 938_979  # export, mentions, gkg
ROWS = {"gdelt_event": 4023, "gdelt_mention": 7762, "gdelt_gkg_doc": 240, "gdelt_gkg_org": 541}


def runs(dsn: str, pattern: str) -> list[tuple[object, ...]]:
    with connect(dsn, autocommit=True) as connection:
        return connection.execute(
            "SELECT entry_point, status, row_count, run_id FROM ops.pipeline_run "
            "WHERE entry_point LIKE %s ORDER BY pipeline_run_id",
            (pattern,),
        ).fetchall()


def test_the_tracer_lands_the_hour_of_the_three_feeds_through_the_ledger(
    stack: str, tmp_path: Path
) -> None:
    first = gdelt_tasks.land_hour_task(HOUR, "fixture", AS_OF, run_id="run-1")

    assert (first["slots"], first["expected_created"], first["in_the_master_list"]) == (12, 12, 12)
    assert (first["listed"], first["selected"], first["verified"], first["written"]) == (
        12,
        12,
        12,
        12,
    )
    assert (first["bytes"], first["source"], first["states"]) == (
        FIXTURE_BYTES,
        "fixture",
        {"verified": 12},
    )
    assert sorted({file["feed"] for file in first["files"]}) == ["export", "gkg", "mentions"]
    assert len(list((tmp_path / "raw").rglob("*.zip"))) == 12
    # The ledger holds the twelve slots of the hour and no other slot of the day.
    with connect(stack, autocommit=True) as connection:
        assert connection.execute("SELECT count(*) FROM ops.source_slot").fetchone() == (12,)

    second = gdelt_tasks.land_hour_task(HOUR, "fixture", AS_OF)

    # A second run downloads nothing.
    assert (second["expected_created"], second["listed"], second["selected"]) == (0, 0, 0)
    assert (second["written"], second["bytes"], second["states"]) == (0, 0, {"verified": 12})
    assert runs(stack, "gdelt.land_hour") == [
        ("gdelt.land_hour", "success", 12, "run-1"),
        ("gdelt.land_hour", "success", 0, None),
    ]


def test_an_hour_which_the_source_does_not_list_fails_the_task(stack: str) -> None:
    with pytest.raises(FileNotFoundError, match="the fixture source lists no file for"):
        gdelt_tasks.land_hour_task("2026-11-25T09", "fixture", AS_OF)

    assert runs(stack, "gdelt.land_hour") == [("gdelt.land_hour", "failed", 0, None)]
    with pytest.raises(ValueError, match="an hour is written"):
        gdelt_tasks.land_hour_task("nine", "fixture", AS_OF)


def test_the_tracer_builds_the_hour_of_the_three_feeds(stack: str) -> None:
    gdelt_tasks.land_hour_task(HOUR, "fixture", AS_OF)

    first = tasks.build_hour_feeds_task(HOUR, AS_OF, run_id="run-1")
    second = tasks.build_hour_feeds_task(HOUR, AS_OF, run_id="run-2")

    assert sorted(first["feeds"]) == ["export", "gkg", "mentions"]
    assert {feed: summary["loaded"] for feed, summary in first["feeds"].items()} == {
        "export": 4,
        "mentions": 4,
        "gkg": 4,
    }
    # The facts of the events table, for the pin, dbt and the count check of the tracer.
    assert first["counts"] == {
        "raw_lines": 4023,
        "parsed": 4023,
        "quarantined": 0,
        "duplicates": 0,
        "committed": 4023,
    }
    assert (first["mart_partitions"], first["pipeline_version"]) == ([202611], "test-version")
    assert first["snapshot_id"] == first["feeds"]["export"]["tables"]["gdelt_event"]["snapshot_id"]
    # A second run builds the hour again: a new snapshot which replaces the unit.
    assert (first["snapshot_operations"], second["snapshot_operations"]) == (
        ["append"],
        ["delete", "append"],
    )
    assert second["snapshot_id"] != first["snapshot_id"]
    tables = {
        name: table["rows"]
        for summary in second["feeds"].values()
        for name, table in summary["tables"].items()
    }
    assert tables == ROWS
    # After the build the slots are loaded, and a further landing still downloads nothing.
    again = gdelt_tasks.land_hour_task(HOUR, "fixture", AS_OF)
    assert (again["written"], again["states"]) == (0, {"loaded": 12})
    assert len(runs(stack, "gdelt.silver_hour")) == 6


def test_the_hourly_job_finds_units_and_builds_the_units_it_was_given(stack: str) -> None:
    gdelt_tasks.land_hour_task(HOUR, "fixture", AS_OF)

    found = tasks.find_units_task(AS_OF, max_units=2)

    assert found == {
        "as_of": AS_OF,
        "pending": 3,
        "units": [
            {"feed": "export", "unit": HOUR, "unit_kind": "hour"},
            {"feed": "gkg", "unit": HOUR, "unit_kind": "hour"},
        ],
    }

    built = tasks.build_units_task(found["units"], AS_OF, run_id="run-1")
    nothing = tasks.build_units_task([], AS_OF)

    assert (built["selected"], built["failed"], built["rows"]) == (2, [], 4023 + 240)
    assert [unit["feed"] for unit in built["built"]] == ["export", "gkg"]
    assert (nothing["selected"], nothing["built"], nothing["rows"]) == (0, [], 0)
    assert tasks.find_units_task(AS_OF)["units"] == [
        {"feed": "mentions", "unit": HOUR, "unit_kind": "hour"}
    ]
    # A whole day is handed on by its date.
    day = tasks.build_units_task([{"feed": "mentions", "unit": "2026-11-25"}], AS_OF)
    assert [(unit["unit"], unit["committed"]) for unit in day["built"]] == [("2026-11-25", 7762)]
    assert tasks.find_units_task(AS_OF) == {"as_of": AS_OF, "pending": 0, "units": []}
    assert [row[:3] for row in runs(stack, "gdelt.silver_units")] == [
        ("gdelt.silver_units", "success", 4023 + 240),
        ("gdelt.silver_units", "success", 0),
        ("gdelt.silver_units", "success", 7762),
    ]


def test_the_daily_job_builds_the_day_of_its_logical_date_and_sums_up_the_run(stack: str) -> None:
    gdelt_tasks.land_hour_task(HOUR, "fixture", AS_OF)
    run_id = "backfill__2026-11-25T08:25:00+00:00"
    for feed in ("export", "mentions"):
        built = tasks.build_day_task(None, feed, AS_OF, None, run_id, LOGICAL_DATE)
        assert (built["unit"], built["unit_kind"]) == ("2026-11-25", "day")

    # The build of one feed is missing: the summary must not report a success.
    with pytest.raises(
        build.BuildError, match="no day-level version of gdelt_gkg_doc, gdelt_gkg_org"
    ):
        tasks.day_summary_task(None, LOGICAL_DATE, run_id)

    # A template hands a missing value on as an empty text.
    tasks.build_day_task("", "gkg", AS_OF, "", run_id, LOGICAL_DATE)
    summary = tasks.day_summary_task("", LOGICAL_DATE, run_id)

    assert (summary["day"], summary["run_id"]) == ("2026-11-25", run_id)
    assert {name: table["rows"] for name, table in summary["tables"].items()} == ROWS
    assert {table["files"] for table in summary["tables"].values()} == {4}
    assert summary["slots"] == [
        {"slot_date": "2026-11-25", "feed": feed, "state": "loaded", "slots": 4}
        for feed in ("export", "gkg", "mentions")
    ]
    # Another run built nothing for the day.
    with pytest.raises(build.BuildError, match="no day-level version"):
        tasks.day_summary_task("2026-11-25", None, "manual__another")
    with pytest.raises(ValueError, match="name the day with the parameter day"):
        tasks.build_day_task(None, "export", AS_OF)
    assert [row[1] for row in runs(stack, "gdelt.silver_day_summary")] == [
        "failed",
        "success",
        "failed",
    ]
