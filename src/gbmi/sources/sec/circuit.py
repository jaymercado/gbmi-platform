"""The rules of a block by the SEC (blueprint 4.3; week 3 plan 3.3, differences D31 and D47).

``classify`` is pure: it reads an answer and says what the answer means. ``Circuit`` applies the
rules on the row of the source in ``ops.source_circuit``, with a clock and a sleep function as
arguments:

* a 429, and a 403 which is not the answer for a missing file, open the circuit for 15 minutes,
  or for the time of the header Retry-After when the header gives more;
* every client reads the row before every request;
* a run waits for an open circuit once, when the wait is at most 16 minutes; a second block in
  the same run fails the run;
* the first request after a block is one probe: the object (status 200) closes the circuit, a
  refusal locks the circuit until an operator closes the circuit, and every other answer
  decides nothing;
* three "no such object" answers in a row for objects which a listing of the SEC names open
  the circuit: a block can look like a missing file. Such an answer to a probe for a listed
  object counts as a refused probe.
"""

from __future__ import annotations

import math
import time
from collections.abc import Callable
from datetime import datetime, timedelta
from enum import StrEnum

from gbmi.core.clock import utc_now
from gbmi.ledger.db import Connection
from gbmi.logging import get_logger
from gbmi.ops.circuit import close_circuit, lock_circuit, open_circuit, read_circuit
from gbmi.sources.sec.errors import SourceBlockedError
from gbmi.sources.sec.urls import ARCHIVES_PREFIX, DATA_HOST, WWW_HOST

log = get_logger(__name__)

#: How long a block lasts when the SEC names no longer time.
BLOCK = timedelta(minutes=15)
#: The longest wait of a run for an open circuit.
MAX_WAIT = timedelta(minutes=16)
#: This many "no such object" answers in a row for listed objects count as a block.
MISSING_IN_A_ROW = 3
#: The answer of the SEC storage for a missing file below /Archives/ (finding F9).
ACCESS_DENIED = b"<Code>AccessDenied</Code>"
#: The first bytes of a body are enough to recognise the answer.
BODY_START = 512
#: No header of the SEC makes a block longer than a week.
LONGEST_BLOCK = timedelta(days=7)


class Verdict(StrEnum):
    """What an answer of the SEC means for the client."""

    OK = "ok"  # the object
    MISSING = "missing"  # the SEC has no such object: no block, no retry
    BLOCKED = "blocked"  # the SEC refuses the client: the circuit opens
    RETRY = "retry"  # a server error: worth another try
    ERROR = "error"  # an answer which the client cannot use


def classify(status: int, host: str, path: str, body_start: bytes) -> Verdict:
    """Read an answer from its status, the host, the path and the first bytes of the body.

    Only the status 200 is the object: the client asks for whole objects, and no other
    2xx status is an answer to such a request.
    """
    if status == 200:
        return Verdict.OK
    if status == 404 and host == DATA_HOST:
        return Verdict.MISSING
    if (
        status == 403
        and host == WWW_HOST
        and path.startswith(ARCHIVES_PREFIX)
        and ACCESS_DENIED in body_start
    ):
        return Verdict.MISSING
    if status in (403, 429):
        return Verdict.BLOCKED
    if status >= 500:
        return Verdict.RETRY
    return Verdict.ERROR


def block_length(retry_after: float | None) -> timedelta:
    """15 minutes, or the seconds of the header Retry-After when the header gives more.

    A value which is no finite number counts as no value; a week is the longest block.
    """
    if retry_after is None or not math.isfinite(retry_after) or retry_after <= 0:
        return BLOCK
    return max(BLOCK, min(timedelta(seconds=retry_after), LONGEST_BLOCK))


class Circuit:
    """The circuit of one source for one run."""

    def __init__(
        self,
        connection: Connection,
        *,
        source: str,
        run_id: str | None = None,
        now: Callable[[], datetime] = utc_now,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._connection = connection
        self.source = source
        self._run_id = run_id
        self._now = now
        self._sleep = sleep
        #: A run waits for an open circuit once.
        self.waited = False
        self._missing = 0

    def before_request(self) -> bool:
        """Read the row before a request leaves; returns True when the request is a probe.

        Waits for an open circuit when the rules allow the wait, and raises SourceBlockedError
        when no request may leave.
        """
        row = read_circuit(self._connection, self.source)
        now = self._now()
        if row.locked:
            raise SourceBlockedError(
                f"the circuit of {self.source} is locked ({row.reason}): an operator has to "
                "run `gbmi sec circuit close --note`"
            )
        if row.blocked_until is not None and now < row.blocked_until:
            wait = row.blocked_until - now
            if self.waited:
                raise SourceBlockedError(
                    f"the circuit of {self.source} is open until "
                    f"{row.blocked_until.isoformat()} ({row.reason}), and the run has waited "
                    "for a block before"
                )
            if wait > MAX_WAIT:
                raise SourceBlockedError(
                    f"the circuit of {self.source} is open until "
                    f"{row.blocked_until.isoformat()} ({row.reason}): the wait is longer than "
                    f"{MAX_WAIT.total_seconds() / 60:.0f} minutes"
                )
            self.waited = True
            log.warning(
                "sec_circuit_wait",
                blocked_until=row.blocked_until.isoformat(),
                seconds=round(wait.total_seconds(), 1),
                reason=row.reason,
            )
            self._sleep(wait.total_seconds())
            # Another client can have locked or extended the block during the wait.
            row = read_circuit(self._connection, self.source)
            now = self._now()
            if row.blocks(now):
                raise SourceBlockedError(
                    f"the circuit of {self.source} is still blocked after the wait ({row.reason})"
                )
        return row.recorded

    def refused(
        self, *, status: int, retry_after: float | None, probe: bool, missing: bool = False
    ) -> None:
        """The SEC refused a request. A refused probe locks the circuit and raises.

        ``missing`` says: the answer to a probe for a listed object was "no such object".
        """
        now = self._now()
        if probe:
            what = "answered 'no such object' for a listed object" if missing else "refused"
            lock_circuit(
                self._connection,
                self.source,
                reason=f"the probe was {what} with HTTP {status}",
                http_status=status,
                now=now,
                run_id=self._run_id,
            )
            log.error("sec_circuit_locked", http_status=status)
            raise SourceBlockedError(
                f"the probe was {what} with HTTP {status}: the circuit of {self.source} "
                "is locked until an operator runs `gbmi sec circuit close --note`"
            )
        until = now + block_length(retry_after)
        row = open_circuit(
            self._connection,
            self.source,
            until=until,
            reason=f"HTTP {status}",
            http_status=status,
            now=now,
            run_id=self._run_id,
        )
        log.warning(
            "sec_circuit_opened",
            http_status=status,
            blocked_until=row.blocked_until.isoformat() if row.blocked_until else None,
            open_count=row.open_count,
        )

    def found(self, *, probe: bool) -> None:
        """The object arrived (status 200): a probe closes the circuit.

        The count of missing objects in a row starts again.
        """
        self._missing = 0
        if probe:
            close_circuit(
                self._connection,
                self.source,
                closed_by="probe",
                note=None,
                now=self._now(),
                run_id=self._run_id,
            )
            log.info("sec_circuit_closed", closed_by="probe")

    def missing(self, *, status: int, listed: bool) -> None:
        """The SEC has no such object; three in a row for listed objects are a block."""
        if not listed:
            return
        self._missing += 1
        if self._missing < MISSING_IN_A_ROW:
            return
        self._missing = 0
        now = self._now()
        open_circuit(
            self._connection,
            self.source,
            until=now + BLOCK,
            reason=f"{MISSING_IN_A_ROW} listed objects in a row answered 'no such object'",
            http_status=status,
            now=now,
            run_id=self._run_id,
        )
        log.error("sec_circuit_opened_by_missing_objects", http_status=status)
        raise SourceBlockedError(
            f"{MISSING_IN_A_ROW} objects in a row which a listing of the SEC names answered "
            f"'no such object': the circuit of {self.source} is open, because a block can look "
            "like a missing file"
        )
