"""What the silver build asks of the ledger: the files of a unit, and the days to finalise again.

Also the partition versions in ``ops.silver_partition`` (week 2 plan WP-06).
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from gbmi.core.gdelt import Feed
from gbmi.ledger.db import Connection
from gbmi.ledger.repository import Ledger, PendingUnit, SlotKey, fileset_hash
from gbmi.ledger.states import State
from gbmi.ops.partitions import PartitionVersion, day_rows, record_versions

from .helpers import T0, key, listed, listing, loaded, verified

DAY = T0.date()
AS_OF = datetime(2026, 11, 25, 12, 0, tzinfo=UTC)


def version(
    feed: Feed,
    table: str,
    *,
    files: list[SlotKey],
    hour: int | None = None,
    rows: int = 0,
    day: date = DAY,
) -> PartitionVersion:
    """A partition version built from the given files; the helpers list every file with one MD5."""
    return PartitionVersion(
        feed=feed.value,
        table_name=table,
        slot_date=day,
        slot_hour=hour,
        committed_rows=rows,
        file_count=len(files),
        fileset_hash=fileset_hash((slot_key.slot.stamp, "0" * 32) for slot_key in files),
        content_digest="0" * 16,
        pipeline_version="test",
    )


def test_the_files_of_a_unit_are_verified_or_loaded_and_without_a_claim(ledger: Ledger) -> None:
    verified(ledger, key(0), key(45))
    loaded(ledger, key(15))
    listed(ledger, key(30))  # not downloaded yet
    verified(ledger, key(60))  # the next hour
    verified(ledger, key(0, Feed.GKG))  # another feed
    ledger.ensure_expected([key(75)], reserve="spark_backfill")
    ledger.mark_listed([listing(key(75))])
    ledger.record_download(key(75), bronze_uri="file:///lake/raw/claimed.zip")  # claimed
    verified(ledger, key(-15))
    ledger.quarantine(key(-15), from_state=State.VERIFIED, reason="more than 1% bad rows")

    hour = ledger.unit_slots(Feed.EXPORT, DAY, 8)
    day = ledger.unit_slots(Feed.EXPORT, DAY)

    assert [(record.key, record.state) for record in hour] == [
        (key(0), State.VERIFIED),
        (key(15), State.LOADED),
        (key(45), State.VERIFIED),
    ]
    assert [record.key for record in day] == [key(0), key(15), key(45), key(60)]
    assert ledger.unit_slots(Feed.EXPORT, DAY, 7) == []
    assert ledger.unit_slots(Feed.MENTIONS, DAY) == []


def test_a_finalised_day_with_the_same_loaded_files_is_not_pending(
    ledger: Ledger, ops: Connection
) -> None:
    loaded(ledger, key(0), key(15))
    record_versions(
        ops, [version(Feed.EXPORT, "gdelt_event", files=[key(0), key(15)])], source=ledger.source
    )

    assert ledger.pending_units(AS_OF) == []


def test_a_finalised_day_with_another_loaded_file_is_pending_as_a_whole_day(
    ledger: Ledger, ops: Connection
) -> None:
    loaded(ledger, key(0), key(15), key(0, Feed.MENTIONS))
    record_versions(
        ops,
        [
            # The day build of the export feed saw one of the two files which are loaded now.
            version(Feed.EXPORT, "gdelt_event", files=[key(0)]),
            version(Feed.MENTIONS, "gdelt_mention", files=[key(0, Feed.MENTIONS)]),
        ],
        source=ledger.source,
    )

    assert ledger.pending_units(AS_OF) == [PendingUnit(DAY, Feed.EXPORT, None)]


def test_an_hour_build_after_the_day_build_makes_the_day_pending(
    ledger: Ledger, ops: Connection
) -> None:
    loaded(ledger, key(0))
    record_versions(
        ops, [version(Feed.EXPORT, "gdelt_event", files=[key(0)])], source=ledger.source
    )
    assert ledger.pending_units(AS_OF) == []

    record_versions(
        ops, [version(Feed.EXPORT, "gdelt_event", files=[key(0)], hour=8)], source=ledger.source
    )
    assert ledger.pending_units(AS_OF) == [PendingUnit(DAY, Feed.EXPORT, None)]

    # The next day build finalises the day again.
    record_versions(
        ops, [version(Feed.EXPORT, "gdelt_event", files=[key(0)])], source=ledger.source
    )
    assert ledger.pending_units(AS_OF) == []


def test_the_two_tables_of_one_feed_are_one_day_build(ledger: Ledger, ops: Connection) -> None:
    files = [key(0, Feed.GKG)]
    loaded(ledger, *files)
    record_versions(
        ops,
        [
            version(Feed.GKG, "gdelt_gkg_doc", files=files),
            version(Feed.GKG, "gdelt_gkg_org", files=files),
        ],
        source=ledger.source,
    )

    assert ledger.pending_units(AS_OF) == []


def test_days_without_a_day_build_and_days_before_the_trailing_week_are_left_alone(
    ledger: Ledger, ops: Connection
) -> None:
    old = key(-8 * 24 * 60)  # eight days before
    loaded(ledger, key(0), old)
    record_versions(
        ops,
        [
            # Hour builds only: the rule is about finalised days.
            version(Feed.EXPORT, "gdelt_event", files=[], hour=8),
            # A finalised day with another file set, outside the trailing seven days.
            version(Feed.EXPORT, "gdelt_event", files=[], day=old.slot.slot_date),
        ],
        source=ledger.source,
    )

    assert ledger.pending_units(AS_OF) == []
    assert ledger.pending_units(AS_OF, days=9) == [
        PendingUnit(old.slot.slot_date, Feed.EXPORT, None)
    ]


def test_a_partition_version_is_stored_with_its_evidence(ops: Connection, source: str) -> None:
    built = PartitionVersion(
        feed="export",
        table_name="gdelt_event",
        slot_date=DAY,
        slot_hour=8,
        committed_rows=7,
        file_count=4,
        fileset_hash="f" * 64,
        content_digest="d" * 16,
        pipeline_version="test",
        commit_id="c" * 32,
        snapshot_id=2,
        previous_snapshot_id=1,
        raw_lines=10,
        parsed_rows=9,
        quarantined_rows=1,
        duplicate_rows=2,
        run_id="run-1",
        duration_ms=120,
        detail={"operations": ["delete", "append"]},
    )
    empty = PartitionVersion(
        feed="gkg",
        table_name="gdelt_gkg_org",
        slot_date=DAY,
        slot_hour=None,
        committed_rows=0,
        file_count=0,
        fileset_hash=fileset_hash([]),
        content_digest="0" * 16,
        pipeline_version="test",
    )

    record_versions(ops, [built, empty], source=source)

    stored = ops.execute(
        "SELECT table_name, unit_kind, slot_hour, commit_id, snapshot_id, previous_snapshot_id, "
        "raw_lines, parsed_rows, quarantined_rows, duplicate_rows, committed_rows, file_count, "
        "run_id, duration_ms, detail, committed_at IS NOT NULL FROM ops.silver_partition "
        "WHERE source = %s ORDER BY partition_version_id",
        (source,),
    ).fetchall()
    assert stored == [
        ("gdelt_event", "hour", 8, "c" * 32, 2, 1, 10, 9, 1, 2, 7, 4, "run-1", 120,
         {"operations": ["delete", "append"]}, True),
        ("gdelt_gkg_org", "day", None, None, None, None, None, None, None, None, 0, 0, None, None,
         {}, True),
    ]  # fmt: skip
    assert (built.unit_kind, empty.unit_kind) == ("hour", "day")


def test_the_volume_history_is_the_newest_day_version_of_each_date(
    ops: Connection, source: str
) -> None:
    weekdays = [DAY - timedelta(weeks=weeks) for weeks in (1, 2, 3, 4)]
    record_versions(
        ops,
        [
            version(Feed.EXPORT, "gdelt_event", files=[], day=weekdays[0], rows=100),
            version(Feed.EXPORT, "gdelt_event", files=[], day=weekdays[0], rows=120),  # a rebuild
            version(Feed.EXPORT, "gdelt_event", files=[], day=weekdays[1], rows=5, hour=8),
            version(Feed.MENTIONS, "gdelt_mention", files=[], day=weekdays[2], rows=900),
            version(Feed.EXPORT, "gdelt_event", files=[], day=weekdays[3], rows=0),
        ],
        source=source,
    )

    history = day_rows(ops, source=source, table_name="gdelt_event", dates=weekdays)

    # An hour build and the version of another table are no day-level version of the table.
    assert history == [120, None, None, 0]
    assert day_rows(ops, source=source, table_name="gdelt_event", dates=[]) == []
    assert day_rows(ops, source="another source", table_name="gdelt_event", dates=weekdays) == [
        None,
        None,
        None,
        None,
    ]
