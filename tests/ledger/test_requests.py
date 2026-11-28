"""The request log and the circuit row against PostgreSQL (week 3 plan, WP-02 step 5).

``max_requests_in_window`` is the evidence of exit-gate row 1: the tests place rows by hand at
known times and check the edges of a window.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from gbmi.ledger.db import Connection, connect
from gbmi.ops.circuit import (
    CircuitRow,
    circuit_events,
    close_circuit,
    lock_circuit,
    open_circuit,
    read_circuit,
)
from gbmi.ops.requests import (
    RequestLog,
    max_requests_in_session_window,
    max_requests_in_window,
    request_counts,
)

T0 = datetime(2026, 11, 28, 8, 30, tzinfo=UTC)
SECOND_NS = 1_000_000_000


def ms(value: float) -> timedelta:
    return timedelta(milliseconds=value)


@pytest.fixture
def log(ops: Connection, source: str) -> RequestLog:
    return RequestLog(ops, source=source, run_id="test-run")


def place(log: RequestLog, *offsets_ms: float, mono_ms: list[float] | None = None) -> list[int]:
    """One row for each offset from T0; the monotonic clock follows the wall clock by default."""
    monotonic = mono_ms if mono_ms is not None else list(offsets_ms)
    return [
        log.started(
            host="data.sec.gov",
            path=f"/submissions/CIK{index:010d}.json",
            method="GET",
            granted_at=T0 + ms(offset),
            granted_mono_ns=round(mono * 1_000_000),
            waited_ms=0,
            attempt=1,
        )
        for index, (offset, mono) in enumerate(zip(offsets_ms, monotonic, strict=True))
    ]


# --- the request log --------------------------------------------------------------------------


def test_the_request_log_needs_autocommit(ops_db: str) -> None:
    with connect(ops_db) as connection, pytest.raises(ValueError, match="autocommit"):
        RequestLog(connection, source="sec")


def test_a_row_is_written_before_the_request_and_the_outcome_follows(
    log: RequestLog, ops: Connection
) -> None:
    (request_id,) = place(log, 0)
    row = ops.execute(
        "SELECT host, method, granted_at, client_session, run_id, outcome, completed_at, probe "
        "FROM ops.source_request WHERE request_id = %s",
        (request_id,),
    ).fetchone()
    assert row == ("data.sec.gov", "GET", T0, log.session, "test-run", None, None, False)
    # The database stamps the row with its own clock: a witness beside the clocks of the client.
    witness = ops.execute(
        "SELECT recorded_at > now() - interval '1 minute' AND recorded_at <= clock_timestamp() "
        "FROM ops.source_request WHERE request_id = %s",
        (request_id,),
    ).fetchone()
    assert witness == (True,)
    probe = log.started(
        host="data.sec.gov",
        path="/probe",
        method="GET",
        granted_at=T0,
        granted_mono_ns=1,
        waited_ms=0,
        attempt=1,
        probe=True,
    )
    marked = ops.execute(
        "SELECT probe FROM ops.source_request WHERE request_id = %s", (probe,)
    ).fetchone()
    assert marked == (True,)
    log.finished(probe, completed_at=T0, outcome="ok", http_status=200, duration_ms=1)
    assert request_counts(ops, source=log.source) == {"ok": 1, "open": 1}
    log.finished(
        request_id,
        completed_at=T0 + ms(120),
        outcome="ok",
        http_status=200,
        duration_ms=120,
        byte_count=157_103,
    )

    row = ops.execute(
        "SELECT outcome, http_status, duration_ms, byte_count, error FROM ops.source_request "
        "WHERE request_id = %s",
        (request_id,),
    ).fetchone()
    assert row == ("ok", 200, 120, 157_103, None)
    assert request_counts(ops, source=log.source) == {"ok": 2}


def test_an_empty_log_has_no_request_in_any_window(ops: Connection, source: str) -> None:
    assert max_requests_in_window(ops, source=source) == 0
    assert max_requests_in_session_window(ops, source=source) == 0
    assert request_counts(ops, source=source) == {}


def test_a_window_counts_its_start_and_not_its_end(log: RequestLog, ops: Connection) -> None:
    # Eight requests in the first 700 ms, and a ninth exactly one second after the first.
    place(log, 0, 100, 200, 300, 400, 500, 600, 700, 1000)

    assert max_requests_in_window(ops, source=log.source) == 8


def test_a_ninth_request_one_microsecond_inside_the_window_is_counted(
    log: RequestLog, ops: Connection
) -> None:
    place(log, 0, 100, 200, 300, 400, 500, 600, 700, 999.999)

    assert max_requests_in_window(ops, source=log.source) == 9


def test_the_largest_window_can_start_at_any_request(log: RequestLog, ops: Connection) -> None:
    # One early request, then a burst of five which starts 1.5 seconds later.
    place(log, 0, 1500, 1500, 1600, 1700, 2499.999, 2500)

    assert max_requests_in_window(ops, source=log.source) == 5
    assert max_requests_in_window(ops, source=log.source, window=ms(100)) == 2
    assert max_requests_in_window(ops, source=log.source, window=timedelta(seconds=3)) == 7


def test_the_two_times_limit_the_requests_which_are_counted(
    log: RequestLog, ops: Connection
) -> None:
    place(log, 0, 100, 200, 5000, 5100)

    assert max_requests_in_window(ops, source=log.source) == 3
    # The start is counted and the end is not.
    assert max_requests_in_window(ops, source=log.source, start=T0 + ms(100)) == 2
    assert max_requests_in_window(ops, source=log.source, end=T0 + ms(200)) == 2
    assert max_requests_in_window(ops, source=log.source, start=T0 + ms(5000)) == 2
    assert max_requests_in_window(ops, source=log.source, start=T0 + ms(6000)) == 0
    assert request_counts(ops, source=log.source, start=T0 + ms(100), end=T0 + ms(5100)) == {
        "open": 3
    }


def test_the_requests_of_another_source_are_not_counted(ops: Connection, source: str) -> None:
    place(RequestLog(ops, source=source), 0, 10, 20)
    place(RequestLog(ops, source=f"{source}-other"), 0, 10, 20, 30)

    assert max_requests_in_window(ops, source=source) == 3


def test_the_wall_clock_counts_every_client_and_the_monotonic_clock_one_session(
    ops: Connection, source: str
) -> None:
    first = RequestLog(ops, source=source, session=uuid.uuid4())
    second = RequestLog(ops, source=source, session=uuid.uuid4())
    place(first, 0, 100, 200, 300)
    # The monotonic clock of another process has another zero.
    place(second, 400, 500, 600, mono_ms=[90_000, 90_100, 90_200])

    assert max_requests_in_window(ops, source=source) == 7
    assert max_requests_in_session_window(ops, source=source) == 4


def test_a_step_of_the_wall_clock_shows_in_one_number_only(
    log: RequestLog, ops: Connection
) -> None:
    # Sixteen requests of one session, 150 ms apart by the monotonic clock: at most 7 in a
    # second. The wall clock was set back by one second after the eighth request.
    monotonic = [150.0 * index for index in range(16)]
    wall = [value if index < 8 else value - 1000 for index, value in enumerate(monotonic)]
    place(log, *wall, mono_ms=monotonic)

    assert max_requests_in_session_window(ops, source=log.source) == 7
    assert max_requests_in_window(ops, source=log.source) > 8
    assert (
        max_requests_in_session_window(ops, source=log.source, window=timedelta(microseconds=1))
        == 1
    )


# --- the circuit row --------------------------------------------------------------------------


def test_a_source_without_a_row_has_a_closed_circuit(ops: Connection, source: str) -> None:
    circuit = read_circuit(ops, source)

    assert circuit == CircuitRow(source)
    assert not circuit.recorded and not circuit.blocks(T0)
    assert close_circuit(ops, source, closed_by="jay", note=None, now=T0) is False


def test_an_opened_circuit_blocks_until_the_time_and_stays_recorded_for_the_probe(
    ops: Connection, source: str
) -> None:
    until = T0 + timedelta(minutes=15)

    opened = open_circuit(ops, source, until=until, reason="HTTP 429", http_status=429, now=T0)

    assert opened == read_circuit(ops, source)
    assert (opened.blocked_until, opened.opened_at, opened.open_count) == (until, T0, 1)
    assert (opened.reason, opened.http_status, opened.locked) == ("HTTP 429", 429, False)
    assert opened.blocks(T0) and opened.blocks(until - ms(1))
    # After the time the block is still recorded: the next request is the probe.
    assert not opened.blocks(until) and opened.recorded


def test_an_open_call_never_shortens_a_block(ops: Connection, source: str) -> None:
    long = T0 + timedelta(hours=1)
    open_circuit(ops, source, until=long, reason="Retry-After 3600", http_status=429, now=T0)

    again = open_circuit(
        ops,
        source,
        until=T0 + timedelta(minutes=16),
        reason="HTTP 403",
        http_status=403,
        now=T0 + timedelta(minutes=1),
    )

    assert again.blocked_until == long
    assert (again.open_count, again.reason, again.http_status) == (2, "HTTP 403", 403)
    later = open_circuit(
        ops, source, until=long + timedelta(minutes=5), reason="HTTP 429", http_status=429, now=T0
    )
    assert later.blocked_until == long + timedelta(minutes=5)
    assert later.open_count == 3


def test_a_refused_probe_locks_the_circuit_until_an_operator_closes(
    ops: Connection, source: str
) -> None:
    until = T0 + timedelta(minutes=15)
    open_circuit(ops, source, until=until, reason="HTTP 429", http_status=429, now=T0)

    locked = lock_circuit(
        ops, source, reason="probe refused: HTTP 403", http_status=403, now=until + ms(5)
    )

    assert locked.locked and locked.blocked_until == until and locked.open_count == 1
    assert locked.blocks(until + timedelta(days=30))  # no time ends a lock
    assert close_circuit(ops, source, closed_by="jay", note="the SEC answered", now=T0) is True
    closed = read_circuit(ops, source)
    assert not closed.recorded and not closed.locked and not closed.blocks(T0)
    assert closed.open_count == 1  # the history of openings stays
    row = ops.execute(
        "SELECT closed_at, closed_by, close_note FROM ops.source_circuit WHERE source = %s",
        (source,),
    ).fetchone()
    assert row == (T0, "jay", "the SEC answered")
    assert close_circuit(ops, source, closed_by="jay", note=None, now=T0) is False
    # The history: one row for each opening, lock and close, newest first.
    events = circuit_events(ops, source)
    assert [event["event"] for event in events] == ["closed", "locked", "opened"]
    assert (events[0]["decided_by"], events[0]["note"]) == ("jay", "the SEC answered")
    assert (events[1]["reason"], events[1]["http_status"]) == ("probe refused: HTTP 403", 403)
    assert (events[2]["blocked_until"], events[2]["at"]) == (until, T0)
    assert circuit_events(ops, source, limit=1) == events[:1]


def test_a_lock_without_a_recorded_block_records_the_block(ops: Connection, source: str) -> None:
    locked = lock_circuit(ops, source, reason="HTTP 403", http_status=403, now=T0)

    assert locked.locked and locked.blocked_until == T0 and locked.opened_at == T0
    assert locked.open_count == 1
