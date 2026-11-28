"""An SEC server in memory, behind ``httpx.MockTransport``, and a clock which a test controls.

The client tests talk to the real ``SecClient``; only the network is replaced. The server
answers like the two hosts of the SEC (week 3 plan, findings F8 and F9), gives prepared answers
for a path, and keeps a list of every request with the time of arrival, the host, the path and
the headers: the tests of the fair-access rules read the list.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from urllib.parse import urlsplit

import httpx

from gbmi.sources.sec.urls import ARCHIVES_PREFIX, DATA_HOST

T0 = datetime(2026, 11, 28, 8, 30, tzinfo=UTC)
MODIFIED = datetime(2026, 11, 28, 3, 1, tzinfo=UTC)
#: The bodies of the two "no such object" answers (findings F8 and F9), shortened.
NO_SUCH_KEY = b"<?xml version='1.0'?><Error><Code>NoSuchKey</Code></Error>"
ACCESS_DENIED = b"<?xml version='1.0'?><Error><Code>AccessDenied</Code></Error>"
#: A refusal which is not a missing file.
RATE_PAGE = b"<html><body>Your Request Originates from an Undeclared Automated Tool</body></html>"


class FakeTime:
    """A monotonic clock, a wall clock and a sleep function which move together."""

    def __init__(self, start: datetime = T0, monotonic: float = 5_000.0) -> None:
        self._start = start
        self._zero = monotonic
        self.monotonic = monotonic
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.monotonic

    def now(self) -> datetime:
        return self._start + timedelta(seconds=self.monotonic - self._zero)

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.monotonic += seconds

    def advance(self, seconds: float) -> None:
        self.monotonic += seconds


@dataclass(frozen=True)
class Answer:
    """A prepared answer: a status with headers and a body, or an error of the transport."""

    status: int = 200
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b""
    error: BaseException | None = None


def too_many(retry_after: str | None = None) -> Answer:
    return Answer(429, {"Retry-After": retry_after} if retry_after else {}, RATE_PAGE)


def forbidden() -> Answer:
    return Answer(403, {}, RATE_PAGE)


def access_denied() -> Answer:
    return Answer(403, {"Content-Type": "application/xml"}, ACCESS_DENIED)


def server_error(status: int = 503) -> Answer:
    return Answer(status)


def broken(error: BaseException) -> Answer:
    return Answer(error=error)


class SlowBody(httpx.SyncByteStream):
    """A body which arrives in pieces; ``wait`` runs before each piece, for example a clock step."""

    def __init__(self, pieces: list[bytes], wait: Callable[[], None]) -> None:
        self._pieces = pieces
        self._wait = wait

    def __iter__(self) -> Iterator[bytes]:
        for piece in self._pieces:
            self._wait()
            yield piece


@dataclass(frozen=True)
class Arrival:
    at: float
    method: str
    host: str
    path: str
    headers: dict[str, str]


class SecServer:
    def __init__(
        self,
        clock: Callable[[], float] = time.monotonic,
        *,
        advance: Callable[[float], None] | None = None,
    ) -> None:
        self._clock = clock
        #: Moves the clock: ``FakeTime.advance`` or ``time.sleep``.
        self._advance = advance
        #: Request number (from 0) -> seconds until the request arrives, for example the
        #: set-up of a connection; and seconds until the answer has arrived.
        self.setup: dict[int, float] = {}
        self.answer_time = 0.0
        self.objects: dict[str, bytes] = {}  # "<host><path>" -> body
        self.always: dict[str, Answer] = {}  # "<host><path>" -> the answer for every request
        self.once: dict[str, list[Answer]] = {}  # "<host><path>" -> answers before the normal one
        self.arrivals: list[Arrival] = []
        self._lock = threading.Lock()

    @staticmethod
    def _key(url: str) -> str:
        parts = urlsplit(url)
        return f"{parts.hostname}{parts.path}"

    def put(self, url: str, body: bytes = b'{"ok": true}') -> None:
        """The SEC has the object."""
        self.objects[self._key(url)] = body

    def answer_once(self, url: str, *answers: Answer) -> None:
        self.once.setdefault(self._key(url), []).extend(answers)

    def answer_always(self, url: str, answer: Answer) -> None:
        self.always[self._key(url)] = answer

    def handler(self, request: httpx.Request) -> httpx.Response:
        host, path = request.url.host, request.url.path
        key = f"{host}{path}"
        with self._lock:
            number = len(self.arrivals)
            if self._advance is not None and self.setup.get(number):
                self._advance(self.setup[number])  # the request is on its way
            self.arrivals.append(
                Arrival(self._clock(), request.method, host, path, dict(request.headers))
            )
            if self._advance is not None and self.answer_time:
                self._advance(self.answer_time)
            answer = self.once[key].pop(0) if self.once.get(key) else self.always.get(key)
        if answer is not None:
            if answer.error is not None:
                raise answer.error
            return httpx.Response(answer.status, headers=answer.headers, content=answer.body)
        if key not in self.objects:
            if host == DATA_HOST:
                return httpx.Response(404, content=NO_SUCH_KEY)
            if path.startswith(ARCHIVES_PREFIX):
                return httpx.Response(403, content=ACCESS_DENIED)
            return httpx.Response(404, content=b"<html>Not Found</html>")
        body = self.objects[key]
        headers = {"Last-Modified": format_datetime(MODIFIED, usegmt=True)}
        return httpx.Response(200, content=body, headers=headers)

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handler)

    def asked(self, url: str) -> int:
        """How often the address was requested."""
        key = self._key(url)
        return sum(1 for arrival in self.arrivals if f"{arrival.host}{arrival.path}" == key)

    def max_in_window(self, window: float = 1.0) -> int:
        """The largest number of arrivals in any window; a window counts its start, not its end."""
        times = sorted(arrival.at for arrival in self.arrivals)
        return max(
            (sum(1 for other in times if start <= other < start + window) for start in times),
            default=0,
        )
