"""Properties of the object ledger against PostgreSQL (week 3 plan, WP-02 step 6).

hypothesis generates sequences of calls. After every sequence:

* the state of each object equals the replay of the object's rows in ``ops.object_transition``;
* no stored transition is outside the table;
* each call succeeded exactly when the pure state table allows the call.

A second test lets two connections move the same object: exactly one connection wins.
"""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta

from hypothesis import given, settings
from hypothesis import strategies as st

from gbmi.ledger.object_states import ALLOWED, CLOSED, INITIAL, OPEN, ObjectState, replay
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

AS_OF = datetime(2026, 11, 26, 9, 0, tzinfo=UTC)
DAY = date(2026, 11, 25)
NAME = "CIK0001326801.json"
#: Three objects of one name on three days in a row: the oldest first.
KEYS = [ObjectKey(Dataset.SUBMISSIONS, DAY + timedelta(days=index), NAME) for index in range(3)]
DOWNLOAD = Download("file:///lake/raw/sec/x.json", 10, "0" * 32, 200)

Call = Callable[[ObjectLedger, ObjectKey], None]


def _moved(
    moved: set[ObjectKey], key: ObjectKey, from_state: ObjectState, to_state: ObjectState
) -> None:
    """The functions for many objects return the objects which moved; one object was asked for."""
    if key not in moved:
        raise StaleObjectStateError(key, from_state, to_state)


def _download(ledger: ObjectLedger, key: ObjectKey) -> None:
    ledger.record_download(key, DOWNLOAD, as_of=AS_OF)


def _fail(ledger: ObjectLedger, key: ObjectKey) -> None:
    ledger.record_failed_attempt(key, error="short body")


def _quarantine(from_state: ObjectState) -> Call:
    def call(ledger: ObjectLedger, key: ObjectKey) -> None:
        ledger.quarantine(key, from_state=from_state, reason="gate failed", as_of=AS_OF)

    return call


def _load(ledger: ObjectLedger, key: ObjectKey) -> None:
    ledger.mark_loaded([ObjectAccounting(key, 10, 9, 1)], as_of=AS_OF)


def _not_published(ledger: ObjectLedger, key: ObjectKey) -> None:
    ledger.mark_not_published(key, reason="HTTP 404", as_of=AS_OF)


def _reopen(ledger: ObjectLedger, key: ObjectKey) -> None:
    moved = ledger.reopen([key], reason="appears later", as_of=AS_OF)
    _moved(moved, key, ObjectState.NOT_PUBLISHED, ObjectState.EXPECTED)


def _requeue(ledger: ObjectLedger, key: ObjectKey) -> None:
    moved = ledger.requeue([key], decided_by="jay", note="again", as_of=AS_OF)
    _moved(moved, key, ObjectState.QUARANTINED, ObjectState.EXPECTED)


#: name -> (the call, the states the object must be in, the state afterwards)
CALLS: dict[str, tuple[Call, frozenset[ObjectState], ObjectState | None]] = {
    "download": (_download, frozenset({ObjectState.EXPECTED}), ObjectState.VERIFIED),
    "fail": (_fail, frozenset({ObjectState.EXPECTED}), None),
    "quarantine_expected": (
        _quarantine(ObjectState.EXPECTED),
        frozenset({ObjectState.EXPECTED}),
        ObjectState.QUARANTINED,
    ),
    "quarantine_verified": (
        _quarantine(ObjectState.VERIFIED),
        frozenset({ObjectState.VERIFIED}),
        ObjectState.QUARANTINED,
    ),
    "load": (_load, frozenset({ObjectState.VERIFIED, ObjectState.LOADED}), ObjectState.LOADED),
    "not_published": (
        _not_published,
        frozenset({ObjectState.EXPECTED}),
        ObjectState.NOT_PUBLISHED,
    ),
    "reopen": (_reopen, frozenset({ObjectState.NOT_PUBLISHED}), ObjectState.EXPECTED),
    "requeue": (_requeue, frozenset({ObjectState.QUARANTINED}), ObjectState.EXPECTED),
}
SUPERSEDE = "supersede_older"

calls = st.lists(
    st.tuples(st.sampled_from([*sorted(CALLS), SUPERSEDE]), st.integers(min_value=0, max_value=2)),
    max_size=30,
)


@settings(max_examples=120, deadline=None)
@given(calls)
def test_any_sequence_of_calls_follows_the_object_table(
    ops_db: str, sequence: list[tuple[str, int]]
) -> None:
    model = dict.fromkeys(KEYS, INITIAL)
    source = f"property-{uuid.uuid4().hex}"
    with open_object_ledger(ops_db, source=source, run_id="property") as ledger:
        ledger.ensure_expected([Expected(key, f"https://data.sec.gov/{NAME}", 1) for key in KEYS])
        for name, index in sequence:
            key = KEYS[index]
            if name == SUPERSEDE:
                # When the object is loaded, every older object which is still open is closed;
                # no other object changes. When the object is not loaded, nothing changes.
                loaded = model[key] is ObjectState.LOADED
                older = {other for other in KEYS[:index] if loaded and model[other] in OPEN}
                assert ledger.supersede_older(key, as_of=AS_OF) == older
                for other in older:
                    model[other] = ObjectState.SUPERSEDED
                continue
            call, needed, afterwards = CALLS[name]
            try:
                call(ledger, key)
                succeeded = True
            except StaleObjectStateError:
                succeeded = False
            assert succeeded == (model[key] in needed), (name, model[key])
            if succeeded and afterwards is not None:
                model[key] = afterwards

        for key in KEYS:
            record = ledger.get(key)
            assert record is not None
            history = ledger.transitions(key)
            assert record.state is model[key]
            assert replay(history) is record.state
            assert set(history) <= ALLOWED
            # No recorded change leaves loaded or superseded.
            assert not {from_state for from_state, _ in history} & CLOSED


def test_two_connections_move_the_same_object_and_exactly_one_wins(ops_db: str) -> None:
    source = f"race-{uuid.uuid4().hex}"
    keys = [ObjectKey(Dataset.COMPANYFACTS, DAY, f"CIK{index:010d}.json") for index in range(20)]
    with open_object_ledger(ops_db, source=source) as setup:
        setup.ensure_expected(
            [
                Expected(key, f"https://data.sec.gov/{key.name}", index)
                for index, key in enumerate(keys)
            ]
        )

    def racer(barrier: threading.Barrier, results: list[list[bool]], lane: int) -> None:
        with open_object_ledger(ops_db, source=source, run_id=f"racer-{lane}") as ledger:
            for index, key in enumerate(keys):
                barrier.wait(timeout=30)
                try:
                    ledger.record_download(key, DOWNLOAD)
                    results[lane].append(True)
                except StaleObjectStateError:
                    results[lane].append(False)
                assert len(results[lane]) == index + 1

    barrier = threading.Barrier(2)
    results: list[list[bool]] = [[], []]
    with ThreadPoolExecutor(max_workers=2) as pool:
        for future in [pool.submit(racer, barrier, results, lane) for lane in range(2)]:
            future.result(timeout=120)

    # For every object one connection won and the other one was told so.
    assert [first != second for first, second in zip(*results, strict=True)] == [True] * len(keys)
    with open_object_ledger(ops_db, source=source) as check:
        for key in keys:
            assert check.transitions(key) == [(ObjectState.EXPECTED, ObjectState.VERIFIED)]
