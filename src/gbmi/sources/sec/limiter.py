"""The sliding-window limiter of the SEC client (week 3 plan 3.3, difference D30). Pure code.

The SEC allows each user 10 requests in each second. The platform sends at most 8: request n
leaves at the earliest 1.050 seconds after request n - 8. Nine requests in a row therefore take
more than one second, so no window of one second holds more than 8 requests. The 50
milliseconds are the margin between the clock of the limiter and the clocks which write the
evidence. A token bucket would allow 16 requests in one second after a pause: 8 saved tokens
and 8 new ones.

A grant is not a departure. After the grant the client still writes a log row and may have to
set up a connection, so a request can leave later than its grant, and two requests can be
closer on the wire than their grants. The client therefore calls ``settle`` when the answer of
a request has arrived: the grant moves to that moment. Request n then leaves at least one
window after request n - 8 was answered, and the rule holds where the requests leave and
where the requests arrive (review 1 of week 3, finding B1).
"""

from __future__ import annotations

import threading
import time
from collections import deque
from collections.abc import Callable

#: The ceiling of the platform (blueprint 4.3); the limiter refuses a higher number.
MAX_REQUESTS = 8
#: One second of the rule plus the margin of 50 milliseconds.
WINDOW_SECONDS = 1.05


class SlidingWindowLimiter:
    """Lets at most ``max_requests`` requests go in any window of ``window`` seconds.

    The clock is the monotonic clock of the process. A test passes its own clock and its own
    sleep function. ``not_before`` holds back the first request: a client starts one window
    after the client received the lock of the source, so the requests of the client before
    and the requests of the new client cannot share a window.
    """

    def __init__(
        self,
        max_requests: int = MAX_REQUESTS,
        window: float = WINDOW_SECONDS,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        not_before: float | None = None,
    ) -> None:
        if not 1 <= max_requests <= MAX_REQUESTS:
            raise ValueError(
                f"the limiter allows 1 to {MAX_REQUESTS} requests in a window, got {max_requests}"
            )
        if window < WINDOW_SECONDS:
            raise ValueError(f"the window is at least {WINDOW_SECONDS} seconds, got {window}")
        self.max_requests = max_requests
        self.window = window
        self._clock = clock
        self._sleep = sleep
        self._not_before = not_before
        self._grants: deque[float] = deque(maxlen=max_requests)
        self._lock = threading.Lock()

    @property
    def last_grant(self) -> float | None:
        """The clock value of the newest grant; None before the first grant."""
        return self._grants[-1] if self._grants else None

    def earliest(self, now: float) -> float:
        """The earliest clock value at which the next request may leave."""
        moment = now
        if self._not_before is not None:
            moment = max(moment, self._not_before)
        if len(self._grants) == self.max_requests:
            moment = max(moment, self._grants[0] + self.window)
        return moment

    def settle(self) -> None:
        """Move the newest grant to now: the request of the grant has been answered."""
        with self._lock:
            if self._grants:
                self._grants[-1] = max(self._grants[-1], self._clock())

    def acquire(self) -> float:
        """Wait until the rule allows one more request; returns the seconds waited."""
        with self._lock:
            start = now = self._clock()
            earliest = self.earliest(now)
            while now < earliest:
                self._sleep(earliest - now)
                now = self._clock()  # a sleep can end early
            self._grants.append(now)
            return now - start
