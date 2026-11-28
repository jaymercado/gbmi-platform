"""The circuit row of a source: ``ops.source_circuit`` (blueprint 4.3; week 3 plan 3.3).

The row remembers a block by a source in PostgreSQL, so every later process respects the block.
The functions here only read and write the row. The rules (when a block starts, how long a
task waits, what a probe decides) belong to the client of the source.

* No row, or ``blocked_until`` is NULL: no block is recorded.
* ``blocked_until`` lies in the future: no request leaves.
* ``blocked_until`` has passed: the next request is one probe. A probe which is answered
  closes the circuit; a probe which is refused locks the circuit.
* ``locked``: no request leaves until an operator closes the circuit.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

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


def _row(row: tuple[object, ...]) -> CircuitRow:
    def utc(value: object) -> datetime | None:
        return value.astimezone(UTC) if isinstance(value, datetime) else None

    return CircuitRow(
        source=str(row[0]),
        blocked_until=utc(row[1]),
        opened_at=utc(row[2]),
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
) -> CircuitRow:
    """Record a block until ``until``. A call never shortens a block which is recorded."""
    row = connection.execute(
        "INSERT INTO ops.source_circuit AS c "
        "(source, blocked_until, opened_at, reason, http_status, open_count) "
        "VALUES (%(source)s, %(until)s, %(now)s, %(reason)s, %(http_status)s, 1) "
        "ON CONFLICT (source) DO UPDATE SET "
        "blocked_until = GREATEST(c.blocked_until, EXCLUDED.blocked_until), "
        "opened_at = EXCLUDED.opened_at, reason = EXCLUDED.reason, "
        "http_status = EXCLUDED.http_status, open_count = c.open_count + 1 "
        "RETURNING source, blocked_until, opened_at, reason, http_status, open_count, locked",
        {
            "source": source,
            "until": until,
            "now": now,
            "reason": reason,
            "http_status": http_status,
        },
    ).fetchone()
    assert row is not None  # noqa: S101 - an upsert returns its row
    return _row(row)


def lock_circuit(
    connection: Connection, source: str, *, reason: str, http_status: int | None, now: datetime
) -> CircuitRow:
    """A probe was refused: no request leaves until an operator closes the circuit."""
    row = connection.execute(
        "INSERT INTO ops.source_circuit AS c "
        "(source, blocked_until, opened_at, reason, http_status, open_count, locked) "
        "VALUES (%(source)s, %(now)s, %(now)s, %(reason)s, %(http_status)s, 1, true) "
        "ON CONFLICT (source) DO UPDATE SET "
        "blocked_until = COALESCE(c.blocked_until, EXCLUDED.blocked_until), "
        "opened_at = COALESCE(c.opened_at, EXCLUDED.opened_at), reason = EXCLUDED.reason, "
        "http_status = EXCLUDED.http_status, locked = true "
        "RETURNING source, blocked_until, opened_at, reason, http_status, open_count, locked",
        {"source": source, "now": now, "reason": reason, "http_status": http_status},
    ).fetchone()
    assert row is not None  # noqa: S101 - an upsert returns its row
    return _row(row)


def close_circuit(
    connection: Connection, source: str, *, closed_by: str, note: str | None, now: datetime
) -> bool:
    """Remove the recorded block and the lock; returns False when no block was recorded."""
    cursor = connection.execute(
        "UPDATE ops.source_circuit SET blocked_until = NULL, locked = false, "
        "closed_at = %(now)s, closed_by = %(closed_by)s, close_note = %(note)s "
        "WHERE source = %(source)s AND blocked_until IS NOT NULL",
        {"source": source, "now": now, "closed_by": closed_by, "note": note},
    )
    return cursor.rowcount == 1
