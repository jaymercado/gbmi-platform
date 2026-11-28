"""The circuit row of a source: ``ops.source_circuit`` (blueprint 4.3; week 3 plan 3.3).

The row remembers a block by a source in PostgreSQL, so every later process respects the block.
The functions here only read and write the row. The rules (when a block starts, how long a
task waits, what a probe decides) belong to the client of the source.

* No row, or ``blocked_until`` is NULL: no block is recorded.
* ``blocked_until`` lies in the future: no request leaves.
* ``blocked_until`` has passed: the next request is one probe. A probe which is answered
  closes the circuit; a probe which is refused locks the circuit.
* ``locked``: no request leaves until an operator closes the circuit.

Each opening, lock and close also writes one row to the append-only table
``ops.circuit_event``, in the same statement: the row of the circuit is the state now, and the
events are the history.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from gbmi.ledger.db import Connection


@dataclass(frozen=True)
class CircuitRow:
    """The circuit of one source."""

    source: str
    blocked_until: datetime | None = None
    opened_at: datetime | None = None
    reason: str | None = None
    http_status: int | None = None
    open_count: int = 0
    locked: bool = False

    @property
    def recorded(self) -> bool:
        """True while a block is recorded: before the probe has passed or an operator closed."""
        return self.blocked_until is not None

    def blocks(self, now: datetime) -> bool:
        """True when no request may leave at ``now``."""
        return self.locked or (self.blocked_until is not None and now < self.blocked_until)


def _utc(value: object) -> datetime | None:
    return value.astimezone(UTC) if isinstance(value, datetime) else None


def _row(row: tuple[object, ...]) -> CircuitRow:
    return CircuitRow(
        source=str(row[0]),
        blocked_until=_utc(row[1]),
        opened_at=_utc(row[2]),
        reason=None if row[3] is None else str(row[3]),
        http_status=None if row[4] is None else int(str(row[4])),
        open_count=int(str(row[5])),
        locked=bool(row[6]),
    )


def read_circuit(connection: Connection, source: str) -> CircuitRow:
    """The circuit of a source; a source without a row has a closed circuit."""
    row = connection.execute(
        "SELECT source, blocked_until, opened_at, reason, http_status, open_count, locked "
        "FROM ops.source_circuit WHERE source = %s",
        (source,),
    ).fetchone()
    return _row(row) if row is not None else CircuitRow(source)


def open_circuit(
    connection: Connection,
    source: str,
    *,
    until: datetime,
    reason: str,
    http_status: int | None,
    now: datetime,
    run_id: str | None = None,
) -> CircuitRow:
    """Record a block until ``until``. A call never shortens a block which is recorded."""
    row = connection.execute(
        "WITH changed AS ("
        "INSERT INTO ops.source_circuit AS c "
        "(source, blocked_until, opened_at, reason, http_status, open_count) "
        "VALUES (%(source)s, %(until)s, %(now)s, %(reason)s, %(http_status)s, 1) "
        "ON CONFLICT (source) DO UPDATE SET "
        "blocked_until = GREATEST(c.blocked_until, EXCLUDED.blocked_until), "
        "opened_at = EXCLUDED.opened_at, reason = EXCLUDED.reason, "
        "http_status = EXCLUDED.http_status, open_count = c.open_count + 1 "
        "RETURNING source, blocked_until, opened_at, reason, http_status, open_count, locked), "
        "logged AS ("
        "INSERT INTO ops.circuit_event "
        "(source, event, blocked_until, reason, http_status, run_id, event_at) "
        "SELECT source, 'opened', blocked_until, reason, http_status, %(run_id)s, %(now)s "
        "FROM changed) "
        "SELECT source, blocked_until, opened_at, reason, http_status, open_count, locked "
        "FROM changed",
        {
            "source": source,
            "until": until,
            "now": now,
            "reason": reason,
            "http_status": http_status,
            "run_id": run_id,
        },
    ).fetchone()
    assert row is not None  # noqa: S101 - an upsert returns its row
    return _row(row)


def lock_circuit(
    connection: Connection,
    source: str,
    *,
    reason: str,
    http_status: int | None,
    now: datetime,
    run_id: str | None = None,
) -> CircuitRow:
    """A probe was refused: no request leaves until an operator closes the circuit."""
    row = connection.execute(
        "WITH changed AS ("
        "INSERT INTO ops.source_circuit AS c "
        "(source, blocked_until, opened_at, reason, http_status, open_count, locked) "
        "VALUES (%(source)s, %(now)s, %(now)s, %(reason)s, %(http_status)s, 1, true) "
        "ON CONFLICT (source) DO UPDATE SET "
        "blocked_until = COALESCE(c.blocked_until, EXCLUDED.blocked_until), "
        "opened_at = COALESCE(c.opened_at, EXCLUDED.opened_at), reason = EXCLUDED.reason, "
        "http_status = EXCLUDED.http_status, locked = true "
        "RETURNING source, blocked_until, opened_at, reason, http_status, open_count, locked), "
        "logged AS ("
        "INSERT INTO ops.circuit_event "
        "(source, event, blocked_until, reason, http_status, run_id, event_at) "
        "SELECT source, 'locked', blocked_until, reason, http_status, %(run_id)s, %(now)s "
        "FROM changed) "
        "SELECT source, blocked_until, opened_at, reason, http_status, open_count, locked "
        "FROM changed",
        {
            "source": source,
            "now": now,
            "reason": reason,
            "http_status": http_status,
            "run_id": run_id,
        },
    ).fetchone()
    assert row is not None  # noqa: S101 - an upsert returns its row
    return _row(row)


def close_circuit(
    connection: Connection,
    source: str,
    *,
    closed_by: str,
    note: str | None,
    now: datetime,
    run_id: str | None = None,
) -> bool:
    """Remove the recorded block and the lock; returns False when no block was recorded."""
    row = connection.execute(
        "WITH changed AS ("
        "UPDATE ops.source_circuit SET blocked_until = NULL, locked = false, "
        "closed_at = %(now)s, closed_by = %(closed_by)s, close_note = %(note)s "
        "WHERE source = %(source)s AND blocked_until IS NOT NULL RETURNING source), "
        "logged AS ("
        "INSERT INTO ops.circuit_event (source, event, decided_by, note, run_id, event_at) "
        "SELECT source, 'closed', %(closed_by)s, %(note)s, %(run_id)s, %(now)s FROM changed) "
        "SELECT count(*) FROM changed",
        {"source": source, "now": now, "closed_by": closed_by, "note": note, "run_id": run_id},
    ).fetchone()
    return row is not None and int(row[0]) == 1


def circuit_events(connection: Connection, source: str, *, limit: int = 20) -> list[dict[str, Any]]:
    """The newest events of the circuit of a source, newest first."""
    rows = connection.execute(
        "SELECT event, event_at, blocked_until, reason, http_status, decided_by, note, run_id "
        "FROM ops.circuit_event WHERE source = %s ORDER BY event_id DESC LIMIT %s",
        (source, limit),
    ).fetchall()
    return [
        {
            "event": row[0],
            "at": _utc(row[1]),
            "blocked_until": _utc(row[2]),
            "reason": row[3],
            "http_status": row[4],
            "decided_by": row[5],
            "note": row[6],
            "run_id": row[7],
        }
        for row in rows
    ]
