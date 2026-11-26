"""Properties of the ledger against PostgreSQL (week 2 plan, WP-03 step 4).

hypothesis generates sequences of repository calls. After every sequence:

* the state of each slot equals the replay of the slot's rows in ``ops.slot_transition``;
* no stored transition is outside the table;
* each call succeeded exactly when the pure state table allows the call.

A second test lets two connections move the same slot: exactly one connection wins.
"""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

from hypothesis import given, settings
from hypothesis import strategies as st

from gbmi.ledger.repository import Ledger, SlotKey, StaleStateError, open_ledger
from gbmi.ledger.states import ALLOWED, INITIAL, State, replay

from .helpers import accounting, key, listed, listing

AS_OF = datetime(2026, 11, 25, 9, 0, tzinfo=UTC)


def _moved(moved: set[SlotKey], slot_key: SlotKey, from_state: State, to_state: State) -> None:
    """The functions for many slots return the slots which moved; one slot was asked for."""
    if slot_key not in moved:
        raise StaleStateError(slot_key, from_state, to_state)


def _list(ledger: Ledger, k: SlotKey) -> None:
    _moved(ledger.mark_listed([listing(k)], as_of=AS_OF), k, State.EXPECTED, State.LISTED)


def _download(ledger: Ledger, k: SlotKey) -> None:
    ledger.record_download(k, bronze_uri="file:///lake/raw/x.zip", as_of=AS_OF)


def _fail(ledger: Ledger, k: SlotKey) -> None:
    ledger.record_failed_attempt(k, error="wrong MD5", as_of=AS_OF)


def _unavailable(ledger: Ledger, k: SlotKey) -> None:
    ledger.record_unavailable(k, as_of=AS_OF, error="HTTP 404")


def _quarantine(from_state: State) -> Callable[[Ledger, SlotKey], None]:
    def call(ledger: Ledger, k: SlotKey) -> None:
        ledger.quarantine(k, from_state=from_state, reason="gate failed", as_of=AS_OF)

    return call


def _load(ledger: Ledger, k: SlotKey) -> None:
    moved = ledger.mark_loaded([accounting(k)], snapshot_id=1, as_of=AS_OF)
    record = ledger.get(k)
    # A loaded slot stays loaded on a rebuild: no error and no move.
    if k not in moved and (record is None or record.state is not State.LOADED):
        raise StaleStateError(k, State.VERIFIED, State.LOADED)


def _confirm(from_state: State) -> Callable[[Ledger, SlotKey], None]:
    def call(ledger: Ledger, k: SlotKey) -> None:
        moved = ledger.confirm_missing(
            [k], from_state=from_state, decided_by="auto", note="rule", as_of=AS_OF
        )
        _moved(moved, k, from_state, State.CONFIRMED_MISSING)

    return call


def _requeue(ledger: Ledger, k: SlotKey) -> None:
    moved = ledger.requeue([k], decided_by="jay", note="again", as_of=AS_OF)
    _moved(moved, k, State.QUARANTINED, State.LISTED)


def _reopen(ledger: Ledger, k: SlotKey) -> None:
    moved = ledger.reopen([listing(k, 11, "1" * 32)], reason="appears later", as_of=AS_OF)
    _moved(moved, k, State.CONFIRMED_MISSING, State.LISTED)


#: name -> (the call, the state the slot must be in, the state afterwards)
CALLS: dict[str, tuple[Callable[[Ledger, SlotKey], None], frozenset[State], State | None]] = {
    "list": (_list, frozenset({State.EXPECTED}), State.LISTED),
    "download": (_download, frozenset({State.LISTED}), State.VERIFIED),
    "fail": (_fail, frozenset({State.LISTED}), None),
    "unavailable": (_unavailable, frozenset({State.LISTED}), None),
    "quarantine_listed": (_quarantine(State.LISTED), frozenset({State.LISTED}), State.QUARANTINED),
    "quarantine_verified": (
        _quarantine(State.VERIFIED),
        frozenset({State.VERIFIED}),
        State.QUARANTINED,
    ),
    "load": (_load, frozenset({State.VERIFIED, State.LOADED}), State.LOADED),
    "confirm_expected": (
        _confirm(State.EXPECTED),
        frozenset({State.EXPECTED}),
        State.CONFIRMED_MISSING,
    ),
    "confirm_listed": (_confirm(State.LISTED), frozenset({State.LISTED}), State.CONFIRMED_MISSING),
    "confirm_quarantined": (
        _confirm(State.QUARANTINED),
        frozenset({State.QUARANTINED}),
        State.CONFIRMED_MISSING,
    ),
    "requeue": (_requeue, frozenset({State.QUARANTINED}), State.LISTED),
    "reopen": (_reopen, frozenset({State.CONFIRMED_MISSING}), State.LISTED),
}

calls = st.lists(
    st.tuples(st.sampled_from(sorted(CALLS)), st.integers(min_value=0, max_value=2)), max_size=30
)


@settings(max_examples=120, deadline=None)
@given(calls)
def test_any_sequence_of_calls_follows_the_state_diagram(
    ops_db: str, sequence: list[tuple[str, int]]
) -> None:
    keys = [key(0), key(15), key(30)]
    model = dict.fromkeys(keys, INITIAL)
    with open_ledger(ops_db, source=f"property-{uuid.uuid4().hex}", run_id="property") as ledger:
        ledger.ensure_expected(keys)
        for name, index in sequence:
            call, needed, afterwards = CALLS[name]
            slot_key = keys[index]
            try:
                call(ledger, slot_key)
                succeeded = True
            except StaleStateError:
                succeeded = False
            assert succeeded == (model[slot_key] in needed), (name, model[slot_key])
            if succeeded and afterwards is not None:
                model[slot_key] = afterwards

        for slot_key in keys:
            record = ledger.get(slot_key)
            assert record is not None
            history = ledger.transitions(slot_key)
            assert record.state is model[slot_key]
            assert replay(history) is record.state
            assert set(history) <= ALLOWED
            assert State.DOWNLOADED is not record.state  # never left between two transactions


def test_two_connections_move_the_same_slot_and_exactly_one_wins(ops_db: str) -> None:
    source = f"race-{uuid.uuid4().hex}"
    keys = [key(15 * index) for index in range(20)]
    with open_ledger(ops_db, source=source) as setup:
        listed(setup, *keys)

    def racer(barrier: threading.Barrier, results: list[list[bool]], lane: int) -> None:
        with open_ledger(ops_db, source=source, run_id=f"racer-{lane}") as ledger:
            for index, slot_key in enumerate(keys):
                barrier.wait(timeout=30)
                try:
                    ledger.record_download(slot_key, bronze_uri=f"file:///lake/raw/{lane}.zip")
                    results[lane].append(True)
                except StaleStateError:
                    results[lane].append(False)
                assert len(results[lane]) == index + 1

    barrier = threading.Barrier(2)
    results: list[list[bool]] = [[], []]
    with ThreadPoolExecutor(max_workers=2) as pool:
        for future in [pool.submit(racer, barrier, results, lane) for lane in range(2)]:
            future.result(timeout=120)

    # For every slot one connection won and the other one was told so.
    assert [first != second for first, second in zip(*results, strict=True)] == [True] * len(keys)
    with open_ledger(ops_db, source=source) as check:
        for slot_key in keys:
            assert check.transitions(slot_key) == [
                (State.EXPECTED, State.LISTED),
                (State.LISTED, State.DOWNLOADED),
                (State.DOWNLOADED, State.VERIFIED),
            ]
