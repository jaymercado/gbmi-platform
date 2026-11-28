"""The rules of a block against PostgreSQL, with a controlled clock (week 3 plan 3.3)."""

from __future__ import annotations

from datetime import timedelta

import pytest

from gbmi.ledger.db import Connection
from gbmi.ops.circuit import circuit_events, close_circuit, open_circuit, read_circuit
from gbmi.sources.sec.circuit import BLOCK, MAX_WAIT, Circuit
from gbmi.sources.sec.errors import SourceBlockedError

from .sec_server import FakeTime


@pytest.fixture
def clock() -> FakeTime:
    return FakeTime()


@pytest.fixture
def circuit(ops: Connection, source: str, clock: FakeTime) -> Circuit:
    return Circuit(ops, source=source, run_id="test-run", now=clock.now, sleep=clock.sleep)


def block(ops: Connection, source: str, clock: FakeTime, minutes: float = 15) -> None:
    open_circuit(
        ops,
        source,
        until=clock.now() + timedelta(minutes=minutes),
        reason="HTTP 429",
        http_status=429,
        now=clock.now(),
    )


def test_a_closed_circuit_lets_a_request_go_which_is_no_probe(
    circuit: Circuit, clock: FakeTime
) -> None:
    assert circuit.before_request() is False
    assert clock.sleeps == []
    assert circuit.waited is False


def test_a_run_waits_once_for_an_open_circuit_and_then_sends_the_probe(
    circuit: Circuit, ops: Connection, source: str, clock: FakeTime
) -> None:
    block(ops, source, clock)

    assert circuit.before_request() is True  # the request after the wait is the probe

    assert clock.sleeps == [pytest.approx(900.0)]
    assert circuit.waited is True
    assert read_circuit(ops, source).recorded  # only the object closes the circuit
    circuit.found(probe=True)
    assert not read_circuit(ops, source).recorded
    assert circuit.before_request() is False
    events = circuit_events(ops, source)
    assert [event["event"] for event in events] == ["closed", "opened"]
    assert (events[0]["decided_by"], events[0]["run_id"]) == ("probe", "test-run")


def test_a_second_block_in_the_same_run_fails_the_run(
    circuit: Circuit, ops: Connection, source: str, clock: FakeTime
) -> None:
    block(ops, source, clock)
    circuit.before_request()
    circuit.found(probe=True)
    block(ops, source, clock)

    with pytest.raises(SourceBlockedError, match="has waited for a block before"):
        circuit.before_request()
    assert len(clock.sleeps) == 1


def test_a_wait_of_more_than_16_minutes_fails_the_run_at_once(
    circuit: Circuit, ops: Connection, source: str, clock: FakeTime
) -> None:
    block(ops, source, clock, minutes=MAX_WAIT.total_seconds() / 60 + 1)

    with pytest.raises(SourceBlockedError, match="longer than 16 minutes"):
        circuit.before_request()
    assert clock.sleeps == [] and circuit.waited is False


def test_a_block_whose_time_has_passed_makes_the_next_request_the_probe_without_a_wait(
    circuit: Circuit, ops: Connection, source: str, clock: FakeTime
) -> None:
    block(ops, source, clock)
    clock.advance(BLOCK.total_seconds() + 1)

    assert circuit.before_request() is True
    assert clock.sleeps == [] and circuit.waited is False


def test_a_block_which_another_client_extended_during_the_wait_fails_the_run(
    ops: Connection, source: str, clock: FakeTime
) -> None:
    block(ops, source, clock)

    def sleep_while_the_block_grows(seconds: float) -> None:
        clock.sleep(seconds)
        block(ops, source, clock, minutes=30)

    circuit = Circuit(ops, source=source, now=clock.now, sleep=sleep_while_the_block_grows)

    with pytest.raises(SourceBlockedError, match="still blocked after the wait"):
        circuit.before_request()


def test_a_refusal_opens_the_circuit_for_15_minutes_or_for_the_time_of_the_header(
    circuit: Circuit, ops: Connection, source: str, clock: FakeTime
) -> None:
    circuit.refused(status=429, retry_after=None, probe=False)

    row = read_circuit(ops, source)
    assert row.blocked_until == clock.now() + BLOCK
    assert (row.reason, row.http_status, row.open_count, row.locked) == ("HTTP 429", 429, 1, False)

    circuit.refused(status=403, retry_after=3600, probe=False)

    row = read_circuit(ops, source)
    assert row.blocked_until == clock.now() + timedelta(hours=1)
    assert (row.reason, row.open_count) == ("HTTP 403", 2)


def test_a_refused_probe_locks_the_circuit_until_an_operator_closes(
    circuit: Circuit, ops: Connection, source: str, clock: FakeTime
) -> None:
    block(ops, source, clock)
    assert circuit.before_request() is True

    with pytest.raises(SourceBlockedError, match="the probe was refused with HTTP 429"):
        circuit.refused(status=429, retry_after=None, probe=True)

    assert read_circuit(ops, source).locked
    newest = circuit_events(ops, source)[0]
    assert (newest["event"], newest["http_status"]) == ("locked", 429)
    assert newest["reason"] == "the probe was refused with HTTP 429"
    clock.advance(86_400)
    fresh = Circuit(ops, source=source, now=clock.now, sleep=clock.sleep)
    with pytest.raises(SourceBlockedError, match="locked"):
        fresh.before_request()
    assert close_circuit(ops, source, closed_by="jay", note="checked", now=clock.now())
    assert fresh.before_request() is False


def test_an_object_which_is_no_probe_leaves_the_row_alone(
    circuit: Circuit, ops: Connection, source: str, clock: FakeTime
) -> None:
    block(ops, source, clock)

    circuit.found(probe=False)

    assert read_circuit(ops, source).recorded


def test_a_missing_listed_object_as_the_answer_to_a_probe_locks_the_circuit(
    circuit: Circuit, ops: Connection, source: str, clock: FakeTime
) -> None:
    # A block can look like a missing file: such a probe counts as refused (review 1).
    block(ops, source, clock)
    clock.advance(BLOCK.total_seconds() + 1)
    assert circuit.before_request() is True

    with pytest.raises(SourceBlockedError, match="answered 'no such object' for a listed object"):
        circuit.refused(status=403, retry_after=None, probe=True, missing=True)

    assert read_circuit(ops, source).locked


@pytest.mark.parametrize("retry_after", [float("inf"), float("nan"), -1.0, 10.0**12])
def test_a_wrong_retry_after_cannot_stop_the_block_from_being_recorded(
    circuit: Circuit, ops: Connection, source: str, clock: FakeTime, retry_after: float
) -> None:
    circuit.refused(status=429, retry_after=retry_after, probe=False)

    row = read_circuit(ops, source)
    assert row.blocked_until is not None
    assert BLOCK <= row.blocked_until - clock.now() <= timedelta(days=7)


def test_three_missing_listed_objects_in_a_row_open_the_circuit(
    circuit: Circuit, ops: Connection, source: str, clock: FakeTime
) -> None:
    # An object which no listing names can be missing as often as the SEC says.
    for _ in range(5):
        circuit.missing(status=404, listed=False)
    circuit.missing(status=403, listed=True)
    circuit.missing(status=403, listed=True)
    circuit.found(probe=False)  # an object in between: the count starts again
    circuit.missing(status=403, listed=True)
    circuit.missing(status=403, listed=True)
    assert not read_circuit(ops, source).recorded

    with pytest.raises(SourceBlockedError, match="a block can look like a missing file"):
        circuit.missing(status=403, listed=True)

    row = read_circuit(ops, source)
    assert row.blocked_until == clock.now() + BLOCK
    assert row.http_status == 403 and "listed objects in a row" in (row.reason or "")
