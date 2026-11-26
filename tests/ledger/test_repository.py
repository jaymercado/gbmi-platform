"""The ledger in PostgreSQL: each function, its guard and its log (week 2 plan, WP-03)."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import psycopg
import pytest

from gbmi.config import get_settings
from gbmi.core.gdelt import Feed
from gbmi.ledger.db import Connection, connect
from gbmi.ledger.repository import (
    Accounting,
    Ledger,
    PendingUnit,
    StaleStateError,
    open_ledger,
)
from gbmi.ledger.states import ALLOWED, IllegalTransitionError, State, replay

from .helpers import T0, accounting, key, listed, listing, loaded, verified

AS_OF = datetime(2026, 11, 25, 9, 0, tzinfo=UTC)


def state_of(ledger: Ledger, slot_key) -> State:  # type: ignore[no-untyped-def]
    record = ledger.get(slot_key)
    assert record is not None
    return record.state


def test_the_ledger_needs_an_autocommit_connection(ops_db: str) -> None:
    with connect(ops_db) as connection, pytest.raises(ValueError, match="autocommit"):
        Ledger(connection)


def test_expected_rows_are_created_once(ledger: Ledger) -> None:
    keys = [key(0), key(15), key(0, Feed.GKG)]

    assert ledger.ensure_expected(keys) == 3
    assert ledger.ensure_expected([*keys, key(30)]) == 1
    assert ledger.ensure_expected([]) == 0

    record = ledger.get(key(0))
    assert record is not None
    assert (record.state, record.attempts, record.absent_count) == (State.EXPECTED, 0, 0)
    assert record.url is None and record.reserved_for is None
    assert ledger.get(key(45)) is None
    assert ledger.transitions(key(0)) == []  # the creation of a slot is no change of state


def test_a_claim_is_put_on_new_rows_only(ledger: Ledger) -> None:
    ledger.ensure_expected([key(0)])
    ledger.ensure_expected([key(0), key(15)], reserve="spark_backfill")

    claims = {record.key: record.reserved_for for record in ledger.find()}
    assert claims == {key(0): None, key(15): "spark_backfill"}


def test_listing_moves_expected_slots_and_writes_the_log(ledger: Ledger, ops: Connection) -> None:
    ledger.ensure_expected([key(0), key(15)])

    moved = ledger.mark_listed([listing(key(0), 81069, "a" * 32), listing(key(30))], as_of=AS_OF)

    assert moved == {key(0)}  # key(30) has no row, key(15) was not in the list
    record = ledger.get(key(0))
    assert record is not None
    assert record.state is State.LISTED
    assert record.listing == listing(key(0), 81069, "a" * 32)
    assert record.listed_at is not None and record.listed_at.tzinfo is UTC
    assert ledger.mark_listed([listing(key(0))]) == set()  # already listed: nothing moves
    assert state_of(ledger, key(15)) is State.EXPECTED
    assert ledger.mark_listed([]) == set()

    row = ops.execute(
        "SELECT from_state, to_state, reason, run_id, as_of FROM ops.slot_transition "
        "WHERE source = %s",
        (ledger.source,),
    ).fetchall()
    assert row == [("expected", "listed", "found in feed index", "test-run", AS_OF)]
    with pytest.raises(ValueError, match="never listed"):
        _ = ledger.get(key(15)).listing  # type: ignore[union-attr]


def test_a_changed_size_or_md5_is_taken_for_a_listed_slot_only(ledger: Ledger) -> None:
    listed(ledger, key(0), key(15))
    verified(ledger, key(30))
    ledger.record_failed_attempt(key(0), error="wrong MD5")

    changed = ledger.update_listing(
        [listing(key(0), 11, "b" * 32), listing(key(15)), listing(key(30), 99, "c" * 32)]
    )

    assert changed == {key(0)}  # key(15) lists the same facts, key(30) is verified already
    record = ledger.get(key(0))
    assert record is not None
    assert (record.expected_bytes, record.expected_md5, record.attempts) == (11, "b" * 32, 0)
    assert ledger.get(key(30)).expected_bytes == 10  # type: ignore[union-attr]
    assert ledger.update_listing([]) == set()


def test_a_download_is_two_transitions_in_one_transaction(ledger: Ledger) -> None:
    listed(ledger, key(0))
    uploaded = T0 - timedelta(minutes=10)

    ledger.record_download(key(0), bronze_uri="file:///lake/raw/a.zip", published_at=uploaded)

    record = ledger.get(key(0))
    assert record is not None
    assert record.state is State.VERIFIED
    assert (record.bronze_uri, record.published_at) == ("file:///lake/raw/a.zip", uploaded)
    assert record.verified_at is not None
    assert ledger.transitions(key(0))[1:] == [
        (State.LISTED, State.DOWNLOADED),
        (State.DOWNLOADED, State.VERIFIED),
    ]


def test_a_download_of_a_slot_which_is_not_listed_changes_nothing(ledger: Ledger) -> None:
    ledger.ensure_expected([key(0)])

    with pytest.raises(StaleStateError, match="not in state 'listed'") as raised:
        ledger.record_download(key(0), bronze_uri="file:///lake/raw/a.zip")

    assert (raised.value.key, raised.value.from_state) == (key(0), State.LISTED)
    assert state_of(ledger, key(0)) is State.EXPECTED
    assert ledger.transitions(key(0)) == []
    with pytest.raises(StaleStateError):
        ledger.record_download(key(45), bronze_uri="file:///lake/raw/none.zip")  # no such slot


def test_a_failed_download_goes_back_to_listed_and_counts(ledger: Ledger) -> None:
    listed(ledger, key(0))

    assert ledger.record_failed_attempt(key(0), error="size 9 differs from the index (10)") == 1
    assert ledger.record_failed_attempt(key(0), error="MD5 differs", as_of=AS_OF) == 2

    record = ledger.get(key(0))
    assert record is not None
    assert (record.state, record.attempts, record.last_error) == (State.LISTED, 2, "MD5 differs")
    assert (
        ledger.transitions(key(0))[1:]
        == [
            (State.LISTED, State.DOWNLOADED),
            (State.DOWNLOADED, State.LISTED),
        ]
        * 2
    )
    # A good download afterwards clears the error and keeps the count as history.
    ledger.record_download(key(0), bronze_uri="file:///lake/raw/a.zip")
    record = ledger.get(key(0))
    assert record is not None and (record.last_error, record.attempts) == (None, 2)
    with pytest.raises(StaleStateError):
        ledger.record_failed_attempt(key(0), error="too late")


def test_the_first_404_sets_the_time_and_a_download_clears_the_time(ledger: Ledger) -> None:
    listed(ledger, key(0))
    first, second = AS_OF, AS_OF + timedelta(hours=3)

    assert ledger.record_unavailable(key(0), as_of=first, error="HTTP 404") == first
    assert ledger.record_unavailable(key(0), as_of=second, error="HTTP 404") == first

    record = ledger.get(key(0))
    assert record is not None
    assert (record.state, record.unavailable_since, record.published_at) == (
        State.LISTED,
        first,
        None,
    )
    ledger.record_download(key(0), bronze_uri="file:///lake/raw/a.zip")
    assert ledger.get(key(0)).unavailable_since is None  # type: ignore[union-attr]
    with pytest.raises(StaleStateError):
        ledger.record_unavailable(key(0), as_of=second, error="HTTP 404")


def test_quarantine_from_listed_and_from_verified_and_from_nowhere_else(ledger: Ledger) -> None:
    listed(ledger, key(0))
    verified(ledger, key(15))
    ledger.ensure_expected([key(30)])

    ledger.quarantine(key(0), from_state=State.LISTED, reason="gate G1 failed three times")
    ledger.quarantine(key(15), from_state=State.VERIFIED, reason="gate G2: 3.2% bad rows")

    assert state_of(ledger, key(0)) is State.QUARANTINED
    assert ledger.get(key(15)).last_error == "gate G2: 3.2% bad rows"  # type: ignore[union-attr]
    with pytest.raises(IllegalTransitionError):  # the table has no such transition
        ledger.quarantine(key(30), from_state=State.EXPECTED, reason="no")
    with pytest.raises(StaleStateError):  # the slot is not where the caller thinks
        ledger.quarantine(key(30), from_state=State.LISTED, reason="no")
    assert state_of(ledger, key(30)) is State.EXPECTED


def test_a_commit_loads_verified_slots_with_their_accounting(ledger: Ledger) -> None:
    verified(ledger, key(0), key(15))

    moved = ledger.mark_loaded(
        [accounting(key(0), 1212, 1212, 0), accounting(key(15), 742, 740, 2)],
        snapshot_id=77,
        as_of=AS_OF,
    )

    assert moved == {key(0), key(15)}
    record = ledger.get(key(15))
    assert record is not None
    assert record.state is State.LOADED
    assert (record.raw_lines, record.parsed_rows, record.quarantined_rows) == (742, 740, 2)
    assert record.silver_snapshot_id == 77 and record.loaded_at is not None
    assert ledger.mark_loaded([], snapshot_id=1) == set()


def test_a_rebuild_keeps_loaded_slots_loaded_and_takes_the_new_accounting(ledger: Ledger) -> None:
    loaded(ledger, key(0))
    verified(ledger, key(15))
    before = ledger.transitions(key(0))

    moved = ledger.mark_loaded(
        [accounting(key(0), 20, 20, 0), accounting(key(15), 5, 5, 0)], snapshot_id=None
    )

    assert moved == {key(15)}  # only the verified slot changed its state
    record = ledger.get(key(0))
    assert record is not None
    assert (record.raw_lines, record.silver_snapshot_id) == (20, None)
    assert ledger.transitions(key(0)) == before  # a rebuild is no change of state


def test_a_commit_with_a_slot_in_another_state_records_nothing(ledger: Ledger) -> None:
    verified(ledger, key(0))
    listed(ledger, key(15))

    with pytest.raises(StaleStateError, match=r"20261125081500\.export"):
        ledger.mark_loaded([accounting(key(0)), accounting(key(15))], snapshot_id=1)

    assert state_of(ledger, key(0)) is State.VERIFIED
    assert ledger.get(key(0)).raw_lines is None  # type: ignore[union-attr]


def test_the_database_refuses_a_wrong_accounting(ledger: Ledger) -> None:
    verified(ledger, key(0))

    with pytest.raises(psycopg.errors.CheckViolation, match="source_slot_loaded"):
        ledger.mark_loaded([Accounting(key(0), 10, 8, 1)], snapshot_id=1)

    assert state_of(ledger, key(0)) is State.VERIFIED
    assert (State.VERIFIED, State.LOADED) not in ledger.transitions(key(0))


def test_confirm_requeue_and_reopen(ledger: Ledger) -> None:
    ledger.ensure_expected([key(0)])
    listed(ledger, key(15))
    listed(ledger, key(30))
    ledger.quarantine(key(30), from_state=State.LISTED, reason="gate G1 failed three times")

    assert ledger.confirm_missing(
        [key(0), key(15)], from_state=State.EXPECTED, decided_by="auto", note="absent twice"
    ) == {key(0)}
    assert ledger.confirm_missing(
        [key(15)], from_state=State.LISTED, decided_by="auto", note="HTTP 404 for 48 hours"
    ) == {key(15)}
    record = ledger.get(key(0))
    assert record is not None
    assert (record.state, record.decided_by, record.decision_note) == (
        State.CONFIRMED_MISSING,
        "auto",
        "absent twice",
    )

    assert ledger.requeue([key(30), key(0)], decided_by="jay", note="the file is good now") == {
        key(30)
    }
    record = ledger.get(key(30))
    assert record is not None
    assert (record.state, record.attempts, record.last_error) == (State.LISTED, 0, None)

    assert ledger.reopen([listing(key(0), 12, "d" * 32)], reason="slot appears later") == {key(0)}
    record = ledger.get(key(0))
    assert record is not None
    assert (record.state, record.decided_by, record.decision_note) == (State.LISTED, None, None)
    assert record.listing == listing(key(0), 12, "d" * 32)
    assert ledger.reopen([listing(key(30))], reason="not confirmed") == set()
    for empty in (
        ledger.confirm_missing([], from_state=State.EXPECTED, decided_by="auto", note="n"),
        ledger.requeue([], decided_by="jay", note="n"),
        ledger.reopen([], reason="n"),
    ):
        assert empty == set()
    with pytest.raises(IllegalTransitionError):
        ledger.confirm_missing([key(0)], from_state=State.VERIFIED, decided_by="jay", note="n")


def test_absent_counts_grow_for_expected_slots_and_listing_resets_the_count(ledger: Ledger) -> None:
    ledger.ensure_expected([key(0), key(15)])
    listed(ledger, key(30))

    assert ledger.count_absent([key(0), key(15), key(30)]) == 2
    assert ledger.count_absent([key(0)]) == 1
    assert ledger.count_absent([]) == 0

    counts = {record.key: record.absent_count for record in ledger.find()}
    assert counts == {key(0): 2, key(15): 1, key(30): 0}
    ledger.mark_listed([listing(key(0))])
    assert ledger.get(key(0)).absent_count == 0  # type: ignore[union-attr]


def test_find_filters_by_time_feed_and_state(ledger: Ledger) -> None:
    listed(ledger, key(15), key(0, Feed.GKG))
    ledger.ensure_expected([key(0), key(30), key(45, Feed.MENTIONS)])

    assert [record.key for record in ledger.find()] == [
        key(0),
        key(0, Feed.GKG),
        key(15),
        key(30),
        key(45, Feed.MENTIONS),
    ]
    assert [record.key for record in ledger.find(first=key(15).slot, last=key(30).slot)] == [
        key(15),
        key(30),
    ]
    assert [record.key for record in ledger.find(feeds=[Feed.GKG, Feed.MENTIONS])] == [
        key(0, Feed.GKG),
        key(45, Feed.MENTIONS),
    ]
    assert [record.key for record in ledger.find(states=[State.EXPECTED], limit=2)] == [
        key(0),
        key(30),
    ]
    assert [record.key for record in ledger.pending_downloads()] == [key(0, Feed.GKG), key(15)]
    assert [record.key for record in ledger.pending_downloads(limit=1)] == [key(0, Feed.GKG)]
    assert ledger.newest_slot() == key(45).slot
    assert ledger.newest_slot(listed=True) == key(15).slot


def test_an_empty_ledger_has_no_newest_slot(ledger: Ledger) -> None:
    assert ledger.newest_slot() is None
    assert ledger.newest_slot(listed=True) is None
    assert ledger.find() == []
    assert ledger.pending_units(AS_OF) == []
    assert ledger.overdue(AS_OF) == []


def test_pending_units_are_hours_until_a_day_version_exists(
    ledger: Ledger, ops: Connection
) -> None:
    verified(ledger, key(0), key(15), key(60), key(0, Feed.GKG))
    loaded(ledger, key(30))  # loaded: not pending
    listed(ledger, key(120))  # not verified: not pending
    ledger.ensure_expected([key(180)], reserve="spark_backfill")
    ledger.mark_listed([listing(key(180))])
    ledger.record_download(key(180), bronze_uri="file:///lake/raw/claimed.zip")  # claimed
    old = key(-8 * 24 * 60)  # eight days before: outside the trailing seven days
    verified(ledger, old)
    day = T0.date()

    assert ledger.pending_units(AS_OF) == [
        PendingUnit(day, Feed.EXPORT, 8),
        PendingUnit(day, Feed.EXPORT, 9),
        PendingUnit(day, Feed.GKG, 8),
    ]
    assert PendingUnit(old.slot.slot_date, Feed.EXPORT, 8) in ledger.pending_units(AS_OF, days=9)

    ops.execute(
        "INSERT INTO ops.silver_partition (source, feed, table_name, slot_date, unit_kind, "
        "committed_rows, file_count, fileset_hash, content_digest, pipeline_version) "
        "VALUES (%s, 'export', 'gdelt_event', %s, 'day', 0, 0, 'f', '0', 'test')",
        (ledger.source, day),
    )
    # The date of the feed has a day-level version now: a late slot rebuilds the whole day.
    assert ledger.pending_units(AS_OF) == [
        PendingUnit(day, Feed.EXPORT, None),
        PendingUnit(day, Feed.GKG, 8),
    ]


def test_overdue_slots_are_not_verified_six_hours_after_their_time(ledger: Ledger) -> None:
    ledger.ensure_expected([key(0), key(60)])
    listed(ledger, key(15))
    verified(ledger, key(30))
    ledger.ensure_expected([key(45)])
    ledger.confirm_missing([key(45)], from_state=State.EXPECTED, decided_by="auto", note="n")

    six_hours_after_the_first_slot = T0 + timedelta(hours=6, minutes=20)

    assert [record.key for record in ledger.overdue(six_hours_after_the_first_slot)] == [
        key(0),
        key(15),
    ]
    assert ledger.overdue(T0 + timedelta(hours=5)) == []
    assert len(ledger.overdue(T0 + timedelta(hours=1), after=timedelta(0))) == 3


def test_status_and_completeness_of_a_day(ledger: Ledger) -> None:
    day = date(2026, 11, 25)
    export = [key(minutes) for minutes in range(-8 * 60, 16 * 60, 15)]  # the 96 slots of the day
    assert len(export) == 96 and {slot_key.slot.slot_date for slot_key in export} == {day}
    loaded(ledger, *export[:94])
    ledger.ensure_expected(export[94:])
    ledger.confirm_missing(export[94:95], from_state=State.EXPECTED, decided_by="auto", note="n")
    loaded(ledger, key(0, Feed.MENTIONS))

    assert ledger.status(day, day) == [
        {"slot_date": "2026-11-25", "feed": "export", "state": "confirmed_missing", "slots": 1},
        {"slot_date": "2026-11-25", "feed": "export", "state": "expected", "slots": 1},
        {"slot_date": "2026-11-25", "feed": "export", "state": "loaded", "slots": 94},
        {"slot_date": "2026-11-25", "feed": "mentions", "state": "loaded", "slots": 1},
    ]
    report = {entry["feed"]: entry for entry in ledger.completeness(day, day)}
    assert report["export"] == {
        "slot_date": "2026-11-25",
        "feed": "export",
        "expected": 96,
        "loaded": 94,
        "confirmed_missing": 1,
        "completeness": round(94 / 95, 6),
        "complete": False,
    }
    assert report["gkg"]["completeness"] == 0.0 and not report["gkg"]["complete"]
    assert report[None]["expected"] == 288
    assert report[None]["completeness"] == round(95 / 287, 6)

    # The last open slot is confirmed too: the feed is complete, at 100%.
    ledger.confirm_missing(export[95:], from_state=State.EXPECTED, decided_by="auto", note="n")
    finished = {entry["feed"]: entry for entry in ledger.completeness(day, day)}
    assert finished["export"]["completeness"] == 1.0 and finished["export"]["complete"]

    two_days = ledger.completeness(day, date(2026, 11, 26), feeds=[Feed.EXPORT])
    assert [(entry["slot_date"], entry["feed"]) for entry in two_days] == [
        ("2026-11-25", "export"),
        ("2026-11-25", None),
        ("2026-11-26", "export"),
        ("2026-11-26", None),
    ]
    assert two_days[2]["completeness"] == 0.0  # a day without rows is not complete


def test_a_day_on_which_every_slot_is_confirmed_missing_is_complete(ledger: Ledger) -> None:
    day = date(2026, 11, 25)
    export = [key(minutes) for minutes in range(-8 * 60, 16 * 60, 15)]
    ledger.ensure_expected(export)
    ledger.confirm_missing(export, from_state=State.EXPECTED, decided_by="jay", note="outage")

    (feed, _) = ledger.completeness(day, day, feeds=[Feed.EXPORT])

    assert (feed["completeness"], feed["complete"], feed["confirmed_missing"]) == (1.0, True, 96)


def test_every_recorded_history_replays_to_the_state_of_the_slot(ledger: Ledger) -> None:
    loaded(ledger, key(0))
    listed(ledger, key(15))
    ledger.record_failed_attempt(key(15), error="e")
    ledger.quarantine(key(15), from_state=State.LISTED, reason="r")
    ledger.requeue([key(15)], decided_by="jay", note="n")

    for record in ledger.find():
        history = ledger.transitions(record.key)
        assert replay(history) is record.state
        assert set(history) <= ALLOWED


def test_open_ledger_reads_the_connection_string_from_the_settings(
    ops_db: str, source: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("GBMI_PG_DSN", ops_db)
    get_settings.cache_clear()
    try:
        with open_ledger(source=source, run_id="r1") as opened:
            assert opened.ensure_expected([key(0)]) == 1
            assert (opened.source, opened.run_id) == (source, "r1")
    finally:
        get_settings.cache_clear()
    with open_ledger(ops_db, source=source) as again:
        assert again.get(key(0)) is not None
