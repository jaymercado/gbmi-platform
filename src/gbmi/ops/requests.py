"""The request log: one row in ``ops.source_request`` for each attempt to reach a source.

The log is the evidence for the fair-access rule of the SEC (blueprint 4.3 and 11.2; week 3 plan
3.3, difference D38 and change C14): no window of one second holds more than 8 requests. The
client writes a row before the request leaves, and one later statement adds the outcome, so a
killed process leaves the row of every request which left. The database refuses a delete and a
change of the facts of a request. The user agent is never stored.

Two clocks are stored for the moment at which the limiter let a request go. ``granted_at`` is
the wall clock of the client: the value compares requests of different clients. The limiter
decides on the monotonic clock of the process, and ``granted_mono_ns`` holds the value: the
real distance of two requests of one client session, also when the wall clock was set.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta

from gbmi.ledger.db import Connection

#: The window of the fair-access rule.
WINDOW = timedelta(seconds=1)
_MICROSECOND = timedelta(microseconds=1)


class RequestLog:
    """The log of one client session of one source."""

    def __init__(
        self,
        connection: Connection,
        *,
        source: str,
        run_id: str | None = None,
        session: uuid.UUID | None = None,
    ) -> None:
        if not connection.autocommit:
            raise ValueError("the request log needs a connection in autocommit mode")
        self.connection = connection
        self.source = source
        self.run_id = run_id
        self.session = session or uuid.uuid4()

    def started(
        self,
        *,
        host: str,
        path: str,
        method: str,
        granted_at: datetime,
        granted_mono_ns: int,
        waited_ms: int,
        attempt: int,
        probe: bool = False,
    ) -> int:
        """Write the row of an attempt, before the request leaves; returns the row number.

        ``probe`` marks the one request after a block.
        """
        row = self.connection.execute(
            "INSERT INTO ops.source_request (source, host, path, method, granted_at, "
            "client_session, granted_mono_ns, waited_ms, attempt, run_id, probe) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING request_id",
            (
                self.source,
                host,
                path,
                method,
                granted_at,
                self.session,
                granted_mono_ns,
                waited_ms,
                attempt,
                self.run_id,
                probe,
            ),
        ).fetchone()
        assert row is not None  # noqa: S101 - an insert returns its row
        return int(row[0])

    def finished(
        self,
        request_id: int,
        *,
        completed_at: datetime,
        outcome: str,
        http_status: int | None,
        duration_ms: int,
        byte_count: int | None = None,
        error: str | None = None,
    ) -> None:
        """Add the outcome to the row of an attempt. The database allows one such statement."""
        self.connection.execute(
            "UPDATE ops.source_request SET completed_at = %s, outcome = %s, http_status = %s, "
            "duration_ms = %s, byte_count = %s, error = %s WHERE request_id = %s",
            (completed_at, outcome, http_status, duration_ms, byte_count, error, request_id),
        )


def max_requests_in_window(
    connection: Connection,
    *,
    source: str,
    start: datetime | None = None,
    end: datetime | None = None,
    window: timedelta = WINDOW,
) -> int:
    """The largest number of requests of a source in any window, by the wall clock.

    A window counts its start and not its end. Every window with the largest count can be
    moved to the right until the window starts at a request, so the function looks at the
    windows which start at a request. ``start`` (counted) and ``end`` (not counted) limit
    the requests by ``granted_at``. The function is the evidence of exit-gate row 1.
    """
    row = connection.execute(
        "SELECT coalesce(max(in_window), 0) FROM ("
        "SELECT count(*) OVER (ORDER BY granted_at "
        "RANGE BETWEEN CURRENT ROW AND %(span)s FOLLOWING) AS in_window "
        "FROM ops.source_request WHERE source = %(source)s "
        "AND (%(start)s::timestamptz IS NULL OR granted_at >= %(start)s) "
        "AND (%(end)s::timestamptz IS NULL OR granted_at < %(end)s)) AS counted",
        {"source": source, "start": start, "end": end, "span": window - _MICROSECOND},
    ).fetchone()
    assert row is not None  # noqa: S101 - an aggregate returns one row
    return int(row[0])


def max_requests_in_session_window(
    connection: Connection,
    *,
    source: str,
    start: datetime | None = None,
    end: datetime | None = None,
    window: timedelta = WINDOW,
) -> int:
    """The largest number of requests of one client session in any window, by the monotonic clock.

    The number does not depend on the wall clock of a machine. The number cannot compare the
    requests of two sessions: a monotonic clock has no common zero.
    """
    span_ns = window // _MICROSECOND * 1000 - 1
    row = connection.execute(
        "SELECT coalesce(max(in_window), 0) FROM ("
        "SELECT count(*) OVER (PARTITION BY client_session ORDER BY granted_mono_ns "
        "RANGE BETWEEN CURRENT ROW AND %(span_ns)s FOLLOWING) AS in_window "
        "FROM ops.source_request WHERE source = %(source)s "
        "AND (%(start)s::timestamptz IS NULL OR granted_at >= %(start)s) "
        "AND (%(end)s::timestamptz IS NULL OR granted_at < %(end)s)) AS counted",
        {"source": source, "start": start, "end": end, "span_ns": span_ns},
    ).fetchone()
    assert row is not None  # noqa: S101 - an aggregate returns one row
    return int(row[0])


def request_counts(
    connection: Connection,
    *,
    source: str,
    start: datetime | None = None,
    end: datetime | None = None,
) -> dict[str, int]:
    """The number of attempts by outcome; an attempt without an outcome counts as ``open``."""
    rows = connection.execute(
        "SELECT coalesce(outcome, 'open'), count(*) FROM ops.source_request "
        "WHERE source = %(source)s "
        "AND (%(start)s::timestamptz IS NULL OR granted_at >= %(start)s) "
        "AND (%(end)s::timestamptz IS NULL OR granted_at < %(end)s) GROUP BY 1 ORDER BY 1",
        {"source": source, "start": start, "end": end},
    ).fetchall()
    return {str(row[0]): int(row[1]) for row in rows}
