"""A day unit: one staged file for each table, parsed hour by hour (week 2 plan WP-06, step 7)."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from gbmi.core.gdelt import Feed
from gbmi.lake.commit import Unit
from gbmi.lake.tables import FEED_TABLES
from gbmi.ledger.repository import Ledger, PendingUnit
from gbmi.ledger.states import State
from gbmi.ops.partitions import PartitionVersion, record_versions
from gbmi.quality.audit import AuditError
from gbmi.transforms.build import build_pending, build_unit

from ..conftest import Lake
from .helpers import (
    AS_OF,
    DAY,
    HOUR,
    ROWS,
    checks,
    copy_fixtures,
    data_files,
    day_files_on_storage,
    drop_last_column,
    lake_path,
    land,
    read_lines,
    silver,
    states,
    stored,
    versions,
    write_file,
)

pytestmark = pytest.mark.integration

WHOLE_DAY = Unit(DAY)


def test_a_day_build_after_hour_builds_gives_one_file_for_each_table_and_the_same_content(
    ledger: Ledger, local_lake: Lake, tmp_path: Path
) -> None:
    # One line of an export file loses a column: one quarantined row, far below 1% of the file.
    fixtures = copy_fixtures(tmp_path / "fixtures")
    stamp = "20261125080000"
    write_file(
        fixtures, Feed.EXPORT, stamp, drop_last_column(read_lines(fixtures, Feed.EXPORT, stamp), 1)
    )
    land(ledger, local_lake, fixtures=fixtures)
    settings = local_lake.settings
    hours = {
        feed: build_unit(feed, HOUR, as_of=AS_OF, ledger=ledger, settings=settings) for feed in Feed
    }
    hour_quarantine = lake_path(local_lake, hours[Feed.EXPORT]["quarantine_file"])
    assert hours[Feed.EXPORT]["counts"]["quarantined"] == 1
    assert hour_quarantine.is_file()

    days = {
        feed: build_unit(feed, WHOLE_DAY, as_of=AS_OF, ledger=ledger, settings=settings)
        for feed in Feed
    }

    for feed in Feed:
        assert days[feed]["counts"] == hours[feed]["counts"]
        assert days[feed]["fileset_hash"] == hours[feed]["fileset_hash"]
        assert (days[feed]["unit_kind"], days[feed]["loaded"]) == ("day", 0)
        for name in FEED_TABLES[feed]:
            hour, day = hours[feed]["tables"][name], days[feed]["tables"][name]
            # The same content, in one commit which deletes the hourly file and adds the day.
            assert day["checksum"] == hour["checksum"]
            assert stored(local_lake, name, WHOLE_DAY).digest == day["checksum"]["digest"]
            assert day["snapshot_operations"] == ["delete", "append"]
            assert day["previous_snapshot_id"] == hour["snapshot_id"]
            (file,) = data_files(local_lake, name)
            assert file.endswith("-day.parquet")
            assert [version["unit_kind"] for version in versions(ledger, name)] == ["hour", "day"]
    # The day has one quarantine file; the file of the hour is gone (review point R8).
    day_quarantine = days[Feed.EXPORT]["quarantine_file"]
    assert day_quarantine == "quarantine/gdelt/export/dt=2026-11-25/day/rows.parquet"
    rejected = pq.read_table(lake_path(local_lake, day_quarantine))
    assert rejected["reason"].to_pylist() == ["column_count: expected 61, found 60"]
    assert not hour_quarantine.exists()
    assert ledger.pending_units(AS_OF) == []
    assert checks(ledger, "failed") == []


@pytest.mark.parametrize("feed", list(Feed))
def test_a_day_without_files_records_an_empty_version_and_leaves_no_file(
    feed: Feed, ledger: Ledger, local_lake: Lake
) -> None:
    land(ledger, local_lake)
    day_before = Unit(DAY - timedelta(days=1))  # the ledger holds no slot of the day

    built = build_unit(feed, day_before, as_of=AS_OF, ledger=ledger, settings=local_lake.settings)

    assert (built["files"], built["loaded"], built["quarantine_file"]) == ([], 0, None)
    assert built["counts"] == dict.fromkeys(
        ("raw_lines", "parsed", "quarantined", "duplicates", "committed"), 0
    )
    for name in FEED_TABLES[feed]:
        assert built["tables"][name]["committed"] is False
        assert silver(local_lake, name).current_snapshot() is None
        assert day_files_on_storage(local_lake, name) == []
        (version,) = versions(ledger, name)
        assert (version["unit_kind"], version["committed_rows"], version["file_count"]) == (
            "day",
            0,
            0,
        )
        assert version["content_digest"] == "0" * 16
    assert states(ledger, feed) == {State.VERIFIED: 4}
    assert checks(ledger, "failed") == []


def test_a_slot_which_arrives_after_the_day_build_causes_a_rebuild_of_the_day(
    ledger: Ledger, local_lake: Lake
) -> None:
    settings = local_lake.settings
    # Nine files land first: the slots 08:00, 08:15 and 08:30 of each feed.
    assert land(ledger, local_lake, batch_limit=9)["verified"] == 9
    early = {
        feed: build_unit(feed, WHOLE_DAY, as_of=AS_OF, ledger=ledger, settings=settings)
        for feed in Feed
    }
    assert {early[feed]["loaded"] for feed in Feed} == {3}
    assert ledger.pending_units(AS_OF) == []

    assert land(ledger, local_lake)["verified"] == 3  # the slot 08:45 arrives

    # The date has a day-level version, so the late slot makes the whole day pending.
    assert ledger.pending_units(AS_OF) == [
        PendingUnit(DAY, Feed.EXPORT, None),
        PendingUnit(DAY, Feed.GKG, None),
        PendingUnit(DAY, Feed.MENTIONS, None),
    ]
    summary = build_pending(as_of=AS_OF, ledger=ledger, settings=settings)

    assert (summary["selected"], len(summary["built"]), summary["failed"]) == (3, 3, [])
    assert states(ledger) == {State.LOADED: 12}
    assert ledger.pending_units(AS_OF) == []
    for name, rows in ROWS.items():
        recorded = versions(ledger, name)
        assert [(version["unit_kind"], version["file_count"]) for version in recorded] == [
            ("day", 3),
            ("day", 4),
        ]
        assert recorded[0]["fileset_hash"] != recorded[1]["fileset_hash"]
        assert recorded[1]["committed_rows"] == rows
        assert stored(local_lake, name, WHOLE_DAY).rows == rows
        (file,) = data_files(local_lake, name)
        assert file.endswith("-day.parquet")


def test_the_day_build_removes_the_duplicates_across_hours(
    ledger: Ledger, local_lake: Lake, tmp_path: Path
) -> None:
    # The files of 09:00 repeat the lines of the files of 08:00: the same keys, one hour later.
    fixtures = copy_fixtures(tmp_path / "fixtures")
    repeated = {}
    for feed in Feed:
        lines = read_lines(fixtures, feed, "20261125080000")
        write_file(fixtures, feed, "20261125090000", lines)
        repeated[feed] = len(lines)
    land(ledger, local_lake, fixtures=fixtures)
    settings = local_lake.settings
    hours = {
        (feed, hour): build_unit(
            feed, Unit(DAY, hour), as_of=AS_OF, ledger=ledger, settings=settings
        )
        for feed in Feed
        for hour in (8, 9)
    }
    # An hour build sees one hour: the repeated keys of 09:00 are in silver beside 08:00.
    assert stored(local_lake, "gdelt_event", WHOLE_DAY).rows == (
        ROWS["gdelt_event"] + hours[Feed.EXPORT, 9]["counts"]["committed"]
    )

    days = {
        feed: build_unit(feed, WHOLE_DAY, as_of=AS_OF, ledger=ledger, settings=settings)
        for feed in Feed
    }

    # Events and documents: the row of the earlier hour stays, the later row is a duplicate.
    for feed, name in ((Feed.EXPORT, "gdelt_event"), (Feed.GKG, "gdelt_gkg_doc")):
        counts = days[feed]["counts"]
        assert counts["committed"] == ROWS[name]
        assert counts["duplicates"] == hours[feed, 9]["counts"]["committed"]
        assert counts["raw_lines"] == hours[feed, 8]["counts"]["raw_lines"] + repeated[feed]
        assert days[feed]["tables"][name]["checksum"] == hours[feed, 8]["tables"][name]["checksum"]
    # The organisation rows of a dropped document are dropped with the document.
    assert days[Feed.GKG]["tables"]["gdelt_gkg_org"]["rows"] == ROWS["gdelt_gkg_org"]
    for name in ("gdelt_event", "gdelt_gkg_doc", "gdelt_gkg_org"):
        kept = silver(local_lake, name).scan(selected_fields=("slot_hour",)).to_arrow()
        assert set(kept["slot_hour"].to_pylist()) == {8}
        assert stored(local_lake, name, WHOLE_DAY).rows == ROWS[name]
    # Mentions: the slot is a part of the key, so the same pair in another slot is another row.
    mentions = days[Feed.MENTIONS]["counts"]
    assert mentions["committed"] == sum(
        hours[Feed.MENTIONS, hour]["counts"]["committed"] for hour in (8, 9)
    )
    assert mentions["duplicates"] == sum(
        hours[Feed.MENTIONS, hour]["counts"]["duplicates"] for hour in (8, 9)
    )
    assert states(ledger) == {State.LOADED: 15}
    assert checks(ledger, "failed") == []


def test_a_day_outside_the_volume_band_commits_nothing_and_leaves_no_staged_file(
    ledger: Ledger, local_lake: Lake
) -> None:
    land(ledger, local_lake)
    settings = local_lake.settings
    record_versions(
        ledger.connection,
        [
            PartitionVersion(
                feed="gkg",
                table_name="gdelt_gkg_doc",
                slot_date=DAY - timedelta(weeks=weeks),
                slot_hour=None,
                committed_rows=rows,
                file_count=96,
                fileset_hash="prepared",
                content_digest="prepared",
                pipeline_version="test",
            )
            for weeks, rows in ((1, 9_000), (2, 11_000), (3, 10_000), (4, 10_000))
        ],
        source=ledger.source,
    )

    with pytest.raises(
        AuditError, match=r"240 rows are outside 0\.5 to 2\.0 times the median 10000"
    ):
        build_unit(Feed.GKG, WHOLE_DAY, as_of=AS_OF, ledger=ledger, settings=settings)

    for name in FEED_TABLES[Feed.GKG]:
        assert silver(local_lake, name).current_snapshot() is None
        assert day_files_on_storage(local_lake, name) == []
        assert [version for version in versions(ledger, name) if version["file_count"] == 4] == []
    assert states(ledger, Feed.GKG) == {State.VERIFIED: 4}
    assert checks(ledger, "failed") == [("G3", "volume_band", "gdelt_gkg_doc", "2026-11-25")]
    # The organisation table has no history of four weekdays: the check was skipped.
    assert checks(ledger, "skipped") == [("G3", "volume_band", "gdelt_gkg_org", "2026-11-25")]

    accepted = build_unit(
        Feed.GKG,
        WHOLE_DAY,
        as_of=AS_OF,
        ledger=ledger,
        settings=settings,
        accept_volume="the fixture day holds one hour",
    )

    assert accepted["checks"] == {"passed": 13, "skipped": 1, "accepted": 1}
    assert accepted["loaded"] == 4
    assert checks(ledger, "accepted") == [("G3", "volume_band", "gdelt_gkg_doc", "2026-11-25")]
    assert stored(local_lake, "gdelt_gkg_doc", WHOLE_DAY).rows == ROWS["gdelt_gkg_doc"]
