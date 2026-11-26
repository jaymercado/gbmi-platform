"""The task entry points and the commands of pipeline PL-2, on the recorded fixture hour.

The fixture ``stack`` gives a database of its own, a lake in a temporary directory and the
fixture files of the repository (week 2 plan WP-06, "Done when").
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

import pytest
from typer.testing import CliRunner

from gbmi.cli import app
from gbmi.core.gdelt import Feed
from gbmi.ledger.db import connect
from gbmi.ops.partitions import PartitionVersion, record_versions
from gbmi.sources.gdelt import tasks as gdelt_tasks
from gbmi.transforms import build, tasks

AS_OF = "2026-11-25T09:00:00+00:00"
HOUR = "2026-11-25T08"
DAY = "2026-11-25"
COMMITTED = {"export": 4023, "gkg": 240, "mentions": 7762}

runner = CliRunner()


@pytest.fixture
def landed(stack: str) -> str:
    """The twelve files of the fixture hour are verified and in bronze."""
    gdelt_tasks.enqueue_task(DAY, DAY, source="fixture", as_of=AS_OF)
    gdelt_tasks.ingest_task(AS_OF, "fixture")
    return stack


def silver(*arguments: str) -> tuple[int, Any, str]:
    result = runner.invoke(app, ["silver", *arguments])
    value = json.loads(result.stdout) if result.stdout.strip() else None
    return result.exit_code, value, result.output


def slot_states(dsn: str) -> dict[tuple[str, str], int]:
    with connect(dsn, autocommit=True) as connection:
        rows = connection.execute(
            "SELECT feed, state, count(*) FROM ops.source_slot WHERE state <> 'expected' "
            "GROUP BY feed, state"
        ).fetchall()
    return {(row[0], row[1]): row[2] for row in rows}


def test_the_fixture_day_is_built_by_the_commands(landed: str) -> None:
    code, units, _ = silver("units", "--as-of", AS_OF)
    assert (code, units) == (
        0,
        [
            {"feed": "export", "unit": HOUR, "unit_kind": "hour"},
            {"feed": "gkg", "unit": HOUR, "unit_kind": "hour"},
            {"feed": "mentions", "unit": HOUR, "unit_kind": "hour"},
        ],
    )

    code, hour, _ = silver(
        "build-hour", "--hour", HOUR, "--feed", "export", "--as-of", AS_OF, "--run-id", "run-1"
    )
    assert (code, list(hour)) == (0, ["export"])
    assert hour["export"]["counts"] == {
        "raw_lines": 4023,
        "parsed": 4023,
        "quarantined": 0,
        "duplicates": 0,
        "committed": 4023,
    }
    assert (hour["export"]["unit_kind"], hour["export"]["loaded"]) == ("hour", 4)
    assert [unit["feed"] for unit in silver("units", "--as-of", AS_OF)[1]] == ["gkg", "mentions"]

    code, day, _ = silver("build-day", "--day", DAY, "--as-of", AS_OF)
    assert (code, sorted(day)) == (0, ["export", "gkg", "mentions"])
    assert {feed: day[feed]["counts"]["committed"] for feed in day} == COMMITTED
    assert day["mentions"]["counts"]["duplicates"] == 120
    assert day["gkg"]["tables"]["gdelt_gkg_org"]["rows"] == 541
    assert {day[feed]["unit_kind"] for feed in day} == {"day"}
    # The export day replaced the hour: one commit which deletes and appends.
    assert day["export"]["snapshot_operations"] == ["delete", "append"]
    assert (
        day["export"]["tables"]["gdelt_event"]["checksum"]
        == (hour["export"]["tables"]["gdelt_event"]["checksum"])
    )

    # Done when: the twelve slots are loaded, and the accounting holds for twelve of twelve files.
    assert slot_states(landed) == {
        ("export", "loaded"): 4,
        ("gkg", "loaded"): 4,
        ("mentions", "loaded"): 4,
    }
    with connect(landed, autocommit=True) as connection:
        accounting = connection.execute(
            "SELECT count(*), count(*) FILTER (WHERE raw_lines = parsed_rows + quarantined_rows), "
            "sum(raw_lines) FROM ops.source_slot WHERE state = 'loaded'"
        ).fetchone()
        runs = connection.execute(
            "SELECT entry_point, status, row_count, run_id, detail ->> 'feed' "
            "FROM ops.pipeline_run WHERE entry_point LIKE 'gdelt.silver%' ORDER BY pipeline_run_id"
        ).fetchall()
        versions = connection.execute(
            "SELECT table_name, unit_kind, committed_rows FROM ops.silver_partition "
            "ORDER BY partition_version_id"
        ).fetchall()
    assert accounting == (12, 12, 4023 + 7882 + 240)
    assert runs == [
        ("gdelt.silver_hour", "success", 4023, "run-1", "export"),
        ("gdelt.silver_day", "success", 4023, None, "export"),
        ("gdelt.silver_day", "success", 7762, None, "mentions"),
        ("gdelt.silver_day", "success", 240, None, "gkg"),
    ]
    assert versions == [
        ("gdelt_event", "hour", 4023),
        ("gdelt_event", "day", 4023),
        ("gdelt_mention", "day", 7762),
        ("gdelt_gkg_doc", "day", 240),
        ("gdelt_gkg_org", "day", 541),
    ]
    assert silver("units", "--as-of", AS_OF)[:2] == (0, [])


def test_the_pending_task_builds_what_is_pending_in_bounded_runs(landed: str) -> None:
    first = tasks.build_pending_task(AS_OF, max_units=2, run_id="run-1")
    second = tasks.build_pending_task(AS_OF, run_id="run-2")
    third = tasks.build_pending_task(AS_OF)

    assert (first["pending"], first["selected"], first["failed"]) == (3, 2, [])
    assert [(unit["feed"], unit["unit"], unit["committed"]) for unit in first["built"]] == [
        ("export", HOUR, 4023),
        ("gkg", HOUR, 240),
    ]
    assert (second["pending"], second["rows"], second["mart_partitions"]) == (1, 7762, [202611])
    assert (third["pending"], third["selected"], third["built"], third["rows"]) == (0, 0, [], 0)
    assert set(slot_states(landed).values()) == {4}
    with connect(landed, autocommit=True) as connection:
        runs = connection.execute(
            "SELECT status, row_count, run_id FROM ops.pipeline_run "
            "WHERE entry_point = 'gdelt.silver_pending' ORDER BY pipeline_run_id"
        ).fetchall()
    assert runs == [
        ("success", 4023 + 240, "run-1"),
        ("success", 7762, "run-2"),
        ("success", 0, None),
    ]


def test_a_unit_which_fails_does_not_stop_the_other_units_and_fails_the_task(
    landed: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(*arguments: object, **options: object) -> None:
        raise RuntimeError("the parser broke")

    monkeypatch.setitem(build.PARSERS, Feed.GKG, broken)

    with pytest.raises(build.BuildError, match="1 of 3 units failed: gkg 2026-11-25T08"):
        tasks.build_pending_task(AS_OF)

    assert slot_states(landed) == {
        ("export", "loaded"): 4,
        ("gkg", "verified"): 4,
        ("mentions", "loaded"): 4,
    }
    with connect(landed, autocommit=True) as connection:
        run = connection.execute(
            "SELECT status, error, row_count, detail FROM ops.pipeline_run "
            "WHERE entry_point = 'gdelt.silver_pending'"
        ).fetchone()
    assert run is not None
    assert run[:3] == ("failed", "BuildError: 1 of 3 units failed: gkg 2026-11-25T08", 4023 + 7762)
    assert [unit["feed"] for unit in run[3]["built"]] == ["export", "mentions"]
    assert run[3]["failed"] == [
        {"feed": "gkg", "unit": HOUR, "error": "RuntimeError: the parser broke"}
    ]


def test_a_day_outside_the_volume_band_fails_the_command_until_an_operator_accepts_the_day(
    landed: str,
) -> None:
    day = date.fromisoformat(DAY)
    with connect(landed, autocommit=True) as connection:
        record_versions(
            connection,
            [
                PartitionVersion(
                    feed="export",
                    table_name="gdelt_event",
                    slot_date=day - timedelta(weeks=weeks),
                    slot_hour=None,
                    committed_rows=100_000,
                    file_count=96,
                    fileset_hash="prepared",
                    content_digest="prepared",
                    pipeline_version="test",
                )
                for weeks in (1, 2, 3, 4)
            ],
            source="gdelt",
        )

    code, value, output = silver("build-day", "--day", DAY, "--feed", "export", "--as-of", AS_OF)

    assert (code, value) == (1, {})
    assert "gbmi silver build-day: export: unit 2026-11-25 failed the pre-commit audit" in output
    assert "4023 rows are outside 0.5 to 2.0 times the median 100000" in output
    # Nothing was committed and no slot changed.
    assert slot_states(landed)[("export", "verified")] == 4

    code, value, _ = silver(
        "build-day", "--day", DAY, "--feed", "export", "--as-of", AS_OF,
        "--accept-volume", "the fixture day holds one hour",
    )  # fmt: skip

    assert (code, value["export"]["checks"]) == (0, {"passed": 8, "accepted": 1})
    assert slot_states(landed)[("export", "loaded")] == 4
    with connect(landed, autocommit=True) as connection:
        checks = connection.execute(
            "SELECT status, detail ->> 'note', run_id FROM ops.dq_result "
            "WHERE check_name = 'volume_band' ORDER BY dq_result_id"
        ).fetchall()
        runs = connection.execute(
            "SELECT status, left(error, 10) FROM ops.pipeline_run "
            "WHERE entry_point = 'gdelt.silver_day' ORDER BY pipeline_run_id"
        ).fetchall()
    assert checks == [
        ("failed", None, None),
        ("accepted", "the fixture day holds one hour", None),
    ]
    assert runs == [
        ("failed", "AuditError"),
        ("success", None),
    ]


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        (("build-hour", "--hour", "eight"), "gbmi silver build-hour: an hour is written"),
        (("build-day", "--day", "yesterday"), "gbmi silver build-day: a day is written"),
        (("units", "--as-of", "now"), "gbmi silver units: not a time in ISO 8601"),
        (
            ("build-hour", "--hour", HOUR, "--feed", "news"),
            "gbmi silver build-hour: news: 'news' is not a valid Feed",
        ),
    ],
)
def test_a_wrong_argument_is_one_line_and_exit_code_1(
    stack: str, arguments: tuple[str, ...], message: str
) -> None:
    code, _, output = silver(*arguments)

    assert code == 1
    assert message in output
    with connect(stack, autocommit=True) as connection:
        assert connection.execute("SELECT count(*) FROM ops.pipeline_run").fetchone() == (0,)
