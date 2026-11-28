"""The object ledger against PostgreSQL: each guarded change, and the questions of the pipelines."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

from gbmi.ledger.db import Connection, connect
from gbmi.ledger.object_states import ObjectState
from gbmi.ledger.objects import (
    Dataset,
    Download,
    Expected,
    ObjectAccounting,
    ObjectKey,
    ObjectLedger,
    StaleObjectStateError,
    open_object_ledger,
)
from gbmi.ledger.repository import StaleStateError
from gbmi.ledger.states import IllegalTransitionError

DAY = date(2026, 11, 25)
AS_OF = datetime(2026, 11, 26, 8, 30, tzinfo=UTC)
MODIFIED = datetime(2026, 11, 26, 3, 1, tzinfo=UTC)


def key(
    name: str = "CIK0001326801.json", days: int = 0, dataset: Dataset = Dataset.SUBMISSIONS
) -> ObjectKey:
    return ObjectKey(dataset, DAY + timedelta(days=days), name)


def expected(object_key: ObjectKey, cik: int | None = 1326801) -> Expected:
    return Expected(object_key, f"https://data.sec.gov/submissions/{object_key.name}", cik)


def download(object_key: ObjectKey) -> Download:
    return Download(
        bronze_uri=f"file:///lake/raw/sec/{object_key.label}",
        byte_count=157_103,
        md5="a" * 32,
        http_status=200,
        last_modified=MODIFIED,
    )


def verified(ledger: ObjectLedger, *keys: ObjectKey) -> None:
    ledger.ensure_expected([expected(object_key) for object_key in keys])
    for object_key in keys:
        ledger.record_download(object_key, download(object_key), as_of=AS_OF)


@pytest.fixture
def objects(ops: Connection, source: str) -> ObjectLedger:
    """An object ledger which sees only the objects of the test."""
    return ObjectLedger(ops, source=source, run_id="test-run")


def test_the_object_ledger_needs_autocommit(ops_db: str) -> None:
    with connect(ops_db) as connection, pytest.raises(ValueError, match="autocommit"):
        ObjectLedger(connection)


def test_an_expected_object_is_created_once(objects: ObjectLedger) -> None:
    first, second = key(), key("CIK0001045810.json")

    assert objects.ensure_expected([]) == 0
    assert objects.ensure_expected([expected(first, 1326801), expected(second, 1045810)]) == 2
    assert objects.ensure_expected([expected(first, 1326801)]) == 0

    record = objects.get(first)
    assert record is not None
    assert record.state is ObjectState.EXPECTED
    assert (record.cik, record.attempts, record.detail) == (1326801, 0, {})
    assert record.url.endswith("/CIK0001326801.json")
    assert objects.get(key("missing.json")) is None
    assert objects.transitions(first) == []


def test_a_download_makes_the_object_verified_with_the_facts_of_the_bytes(
    objects: ObjectLedger,
) -> None:
    object_key = key()
    objects.ensure_expected([expected(object_key)])

    objects.record_download(object_key, download(object_key), as_of=AS_OF)

    record = objects.get(object_key)
    assert record is not None
    assert record.state is ObjectState.VERIFIED
    assert (record.byte_count, record.md5, record.http_status) == (157_103, "a" * 32, 200)
    assert record.last_modified == MODIFIED
    assert record.verified_at is not None and record.bronze_uri is not None
    assert objects.transitions(object_key) == [(ObjectState.EXPECTED, ObjectState.VERIFIED)]
    row = objects.connection.execute(
        "SELECT reason, run_id, as_of FROM ops.object_transition WHERE source = %s",
        (objects.source,),
    ).fetchone()
    assert row == ("stored in bronze, gate G1 passed", "test-run", AS_OF)


def test_a_stale_change_is_told_and_names_the_object(objects: ObjectLedger) -> None:
    object_key = key()
    verified(objects, object_key)

    with pytest.raises(StaleObjectStateError, match="not in state 'expected'") as raised:
        objects.record_download(object_key, download(object_key))
    assert raised.value.key == object_key
    assert "object submissions/2026-11-25/CIK0001326801.json" in str(raised.value)
    # The error of phase P1 is the base class: one handler catches both ledgers.
    assert isinstance(raised.value, StaleStateError)
    with pytest.raises(StaleObjectStateError):
        objects.mark_not_published(object_key, reason="HTTP 404")
    with pytest.raises(StaleObjectStateError):
        objects.record_failed_attempt(object_key, error="short body")
    with pytest.raises(StaleObjectStateError):
        objects.quarantine(key("other.json"), from_state=ObjectState.EXPECTED, reason="x")


def test_a_change_outside_the_table_is_refused_before_the_database_is_asked(
    objects: ObjectLedger,
) -> None:
    object_key = key()
    verified(objects, object_key)

    with pytest.raises(IllegalTransitionError):
        objects.quarantine(object_key, from_state=ObjectState.LOADED, reason="never")
    assert objects.transitions(object_key) == [(ObjectState.EXPECTED, ObjectState.VERIFIED)]


def test_failed_tries_are_counted_and_the_third_try_can_quarantine(objects: ObjectLedger) -> None:
    object_key = key()
    objects.ensure_expected([expected(object_key)])

    assert objects.record_failed_attempt(object_key, error="short body", http_status=200) == 1
    assert objects.record_failed_attempt(object_key, error="not JSON", http_status=200) == 2
    assert objects.record_failed_attempt(object_key, error="not JSON") == 3
    objects.quarantine(
        object_key, from_state=ObjectState.EXPECTED, reason="gate G1 failed three times"
    )

    record = objects.get(object_key)
    assert record is not None
    assert record.state is ObjectState.QUARANTINED
    assert (record.attempts, record.last_error) == (3, "gate G1 failed three times")
    assert record.reason == "gate G1 failed three times"


def test_a_requeue_sends_a_quarantined_object_back_with_the_name_of_the_operator(
    objects: ObjectLedger,
) -> None:
    first, second = key(), key("CIK0001045810.json")
    verified(objects, first, second)
    objects.quarantine(first, from_state=ObjectState.VERIFIED, reason="gate G2: no CIK")

    assert objects.requeue([], decided_by="jay", note="x") == set()
    moved = objects.requeue([first, second], decided_by="jay", note="model fixed", as_of=AS_OF)

    assert moved == {first}  # the second object was not quarantined
    record = objects.get(first)
    assert record is not None
    assert record.state is ObjectState.EXPECTED
    assert (record.attempts, record.last_error, record.reason) == (0, None, None)
    assert record.decided_by == "jay"
    assert record.bronze_uri is not None  # the bytes are still in bronze
    objects.record_download(first, download(first))  # and pass the gate again
    assert objects.transitions(first)[-3:] == [
        (ObjectState.VERIFIED, ObjectState.QUARANTINED),
        (ObjectState.QUARANTINED, ObjectState.EXPECTED),
        (ObjectState.EXPECTED, ObjectState.VERIFIED),
    ]


def test_a_missing_file_is_not_published_and_can_appear_later(objects: ObjectLedger) -> None:
    object_key = key("form.20261126.idx", dataset=Dataset.INDEX)
    objects.ensure_expected([expected(object_key, None)])

    objects.mark_not_published(object_key, reason="the listing names a newer file", as_of=AS_OF)

    record = objects.get(object_key)
    assert record is not None
    assert record.state is ObjectState.NOT_PUBLISHED
    assert record.reason == "the listing names a newer file"
    assert objects.reopen([], reason="x") == set()
    assert objects.reopen([object_key], reason="the listing names the file") == {object_key}
    record = objects.get(object_key)
    assert record is not None
    assert (record.state, record.reason) == (ObjectState.EXPECTED, None)


def test_a_commit_loads_the_verified_objects_with_their_accounting(objects: ObjectLedger) -> None:
    first, second = key(), key("CIK0001045810.json")
    verified(objects, first, second)

    assert objects.mark_loaded([]) == set()
    moved = objects.mark_loaded(
        [ObjectAccounting(first, 1000, 998, 2), ObjectAccounting(second, 40, 40)], as_of=AS_OF
    )

    assert moved == {first, second}
    record = objects.get(first)
    assert record is not None
    assert record.state is ObjectState.LOADED
    assert (record.raw_records, record.parsed_rows, record.quarantined_rows) == (1000, 998, 2)
    assert record.loaded_at is not None
    # A rebuild is no change of state: the object stays loaded and takes the new accounting.
    assert objects.mark_loaded([ObjectAccounting(first, 1000, 1000, 0)]) == set()
    record = objects.get(first)
    assert record is not None
    assert (record.parsed_rows, record.quarantined_rows) == (1000, 0)
    assert objects.transitions(first) == [
        (ObjectState.EXPECTED, ObjectState.VERIFIED),
        (ObjectState.VERIFIED, ObjectState.LOADED),
    ]


def test_a_commit_with_an_object_in_another_state_records_nothing(objects: ObjectLedger) -> None:
    ready, waiting = key(), key("CIK0001045810.json")
    verified(objects, ready)
    objects.ensure_expected([expected(waiting)])

    with pytest.raises(StaleObjectStateError, match="CIK0001045810"):
        objects.mark_loaded([ObjectAccounting(ready, 10, 10), ObjectAccounting(waiting, 5, 5)])

    record = objects.get(ready)
    assert record is not None and record.state is ObjectState.VERIFIED


def test_a_loaded_object_supersedes_every_older_open_object_of_the_same_name(
    objects: ObjectLedger,
) -> None:
    never_fetched, never_loaded, broken, done, newest = (key(days=days) for days in range(5))
    other_company = key("CIK0001045810.json")
    objects.ensure_expected([expected(never_fetched), expected(other_company)])
    verified(objects, never_loaded, broken, done, newest)
    objects.quarantine(broken, from_state=ObjectState.VERIFIED, reason="gate G3")
    objects.mark_loaded([ObjectAccounting(done, 1, 1)])

    # The newest object is not loaded yet: nothing is closed (superseded is final).
    assert objects.supersede_older(newest, as_of=AS_OF) == set()
    assert objects.supersede_older(key(days=9)) == set()  # and for an object which is unknown
    objects.mark_loaded([ObjectAccounting(newest, 1, 1)])

    moved = objects.supersede_older(newest, as_of=AS_OF)

    assert moved == {never_fetched, never_loaded, broken}
    for object_key in moved:
        record = objects.get(object_key)
        assert record is not None
        assert record.state is ObjectState.SUPERSEDED
        assert record.reason == "the object of 2026-11-29 was loaded first"
    for object_key, state in ((done, ObjectState.LOADED), (other_company, ObjectState.EXPECTED)):
        record = objects.get(object_key)
        assert record is not None and record.state is state
    assert objects.supersede_older(newest) == set()


def test_the_objects_of_many_companies_are_superseded_in_one_call(objects: ObjectLedger) -> None:
    names = [f"CIK{cik:010d}.json" for cik in (1, 2, 3)]
    old = [key(name, days=0) for name in names]
    new = [key(name, days=1) for name in names]
    older_index = key("form.20261126.idx", dataset=Dataset.INDEX)
    objects.ensure_expected([expected(older_index)])
    verified(objects, *old, *new)
    # The first company is loaded today, the second is not, and the third was loaded before.
    objects.mark_loaded([ObjectAccounting(old[2], 1, 1), ObjectAccounting(new[0], 1, 1)])
    objects.mark_loaded([ObjectAccounting(new[2], 1, 1)])

    moved = objects.supersede_older_many(
        Dataset.SUBMISSIONS, DAY + timedelta(days=1), names, as_of=AS_OF
    )

    assert moved == {old[0]}
    states = {object_key: objects.get(object_key) for object_key in (*old, older_index)}
    assert [record.state for record in states.values() if record is not None] == [
        ObjectState.SUPERSEDED,
        ObjectState.VERIFIED,
        ObjectState.LOADED,
        ObjectState.EXPECTED,
    ]
    assert objects.supersede_older_many(Dataset.SUBMISSIONS, DAY, []) == set()


def test_the_newest_object_of_each_company_in_one_question(objects: ObjectLedger) -> None:
    meta = [key(days=days) for days in range(3)]
    nvidia = key("CIK0001045810.json")
    listing = key("index.2026QTR4.json", dataset=Dataset.INDEX)
    objects.ensure_expected(
        [*(expected(item) for item in meta), expected(nvidia, cik=1045810), expected(listing, None)]
    )
    for item in (meta[0], meta[1], nvidia):
        objects.record_download(item, download(item))
    objects.mark_loaded([ObjectAccounting(meta[0], 4, 4)])
    objects.mark_not_published(meta[2], reason="HTTP 404", http_status=404)
    readable = {ObjectState.VERIFIED, ObjectState.LOADED}

    newest = objects.newest_by_cik(Dataset.SUBMISSIONS)
    read = objects.newest_by_cik(Dataset.SUBMISSIONS, states=readable)
    early = objects.newest_by_cik(Dataset.SUBMISSIONS, states=readable, not_after=DAY)

    assert {cik: record.key for cik, record in newest.items()} == {
        1326801: meta[2],
        1045810: nvidia,
    }
    assert newest[1326801].state is ObjectState.NOT_PUBLISHED
    assert {cik: record.key for cik, record in read.items()} == {1326801: meta[1], 1045810: nvidia}
    assert early[1326801].key == meta[0] and early[1326801].raw_records == 4
    # An object without a company, for example a listing, belongs to no company.
    assert objects.newest_by_cik(Dataset.INDEX) == {}


def test_a_pipeline_can_say_how_an_object_was_used_up(objects: ObjectLedger) -> None:
    listing = key("index.2026QTR4.json", dataset=Dataset.INDEX)
    verified(objects, listing)

    objects.mark_loaded([ObjectAccounting(listing, 5, 5)], reason="the listing was read")

    row = objects.connection.execute(
        "SELECT reason FROM ops.object_transition WHERE source = %s AND to_state = 'loaded'",
        (objects.source,),
    ).fetchone()
    assert row == ("the listing was read",)


def test_the_questions_of_the_pipelines(objects: ObjectLedger) -> None:
    meta = [key(days=days) for days in range(3)]
    index = key("form.20261125.idx", dataset=Dataset.INDEX)
    objects.ensure_expected([expected(index, None)])
    verified(objects, *meta)
    objects.mark_loaded([ObjectAccounting(meta[0], 1, 1)])
    objects.mark_not_published(index, reason="US holiday")

    assert [record.key for record in objects.pending()] == [meta[1], meta[2]]
    assert [record.key for record in objects.pending(dataset=Dataset.INDEX)] == []
    assert [record.key for record in objects.pending(first=meta[2].object_date)] == [meta[2]]
    assert [record.key for record in objects.pending(last=meta[1].object_date)] == [meta[1]]
    assert len(objects.select()) == 4

    newest = objects.newest(Dataset.SUBMISSIONS, cik=1326801)
    assert newest is not None and newest.key == meta[2]
    loaded = objects.newest(Dataset.SUBMISSIONS, cik=1326801, states={ObjectState.LOADED})
    assert loaded is not None and loaded.key == meta[0]
    by_name = objects.newest(
        Dataset.SUBMISSIONS, name="CIK0001326801.json", not_after=meta[1].object_date
    )
    assert by_name is not None and by_name.key == meta[1]
    assert objects.newest(Dataset.COMPANYFACTS, cik=1326801) is None
    with pytest.raises(ValueError, match="a CIK or a file name"):
        objects.newest(Dataset.SUBMISSIONS)
    with pytest.raises(ValueError, match="a CIK or a file name"):
        objects.newest(Dataset.SUBMISSIONS, cik=1, name="x")

    assert objects.status() == [
        {
            "dataset": "index",
            "state": "not_published",
            "objects": 1,
            "first_date": "2026-11-25",
            "last_date": "2026-11-25",
        },
        {
            "dataset": "submissions",
            "state": "loaded",
            "objects": 1,
            "first_date": "2026-11-25",
            "last_date": "2026-11-25",
        },
        {
            "dataset": "submissions",
            "state": "verified",
            "objects": 2,
            "first_date": "2026-11-26",
            "last_date": "2026-11-27",
        },
    ]
    assert objects.status(first=date(2026, 12, 1)) == []


def test_free_detail_is_merged_into_the_row(objects: ObjectLedger) -> None:
    object_key = key()
    objects.ensure_expected([expected(object_key)])

    objects.set_detail(object_key, {"via": "bulk"})
    objects.set_detail(object_key, {"archive_md5": "b" * 32})

    record = objects.get(object_key)
    assert record is not None
    assert record.detail == {"via": "bulk", "archive_md5": "b" * 32}


def test_open_object_ledger_closes_its_connection(ops_db: str, source: str) -> None:
    with open_object_ledger(ops_db, source=source, run_id="r") as ledger:
        assert ledger.ensure_expected([expected(key())]) == 1
        connection = ledger.connection
    assert connection.closed
