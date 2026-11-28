"""The fair-access client: the only way a request reaches the SEC (blueprint 4.3; week 3 plan 3.3).

The client cannot exceed the ceiling, cannot hide its identity and cannot ignore a block:

* **One process at a time.** A client holds a PostgreSQL advisory lock for the source as long
  as the client lives. A second client, for example a command of an operator beside a task,
  waits for the lock. The first request of a client leaves at the earliest one window after
  the client has received the lock.
* **At most 8 requests in any second**, over both hosts of the SEC: every attempt, a retry
  too, passes ``SlidingWindowLimiter``.
* **A declared identity.** Every request carries the user agent of the settings, the header
  ``Accept-Encoding: gzip, deflate`` and the host name of the address (finding F1). The client
  follows no redirect.
* **A block is respected.** ``gbmi.sources.sec.circuit.Circuit`` reads the circuit row before
  every request and applies the rules of a block.
* **Evidence.** One row of ``ops.source_request`` is written before each attempt leaves.

Each attempt runs in the same order: read the circuit, wait in the limiter, write the log row,
send, add the outcome to the log row, classify the answer, act.

The user agent holds a contact address. The value stands in no log line, in no table and in
no error message: a message names the host and the path of an address, never a header.
"""

from __future__ import annotations

import random
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from types import TracebackType
from typing import Self

import httpx

from gbmi.config import SecSource, Settings, get_settings
from gbmi.core.clock import utc_now
from gbmi.ledger.db import Connection, connect
from gbmi.logging import get_logger
from gbmi.ops.requests import RequestLog
from gbmi.sources.sec.circuit import BODY_START, Circuit, Verdict, classify
from gbmi.sources.sec.errors import (
    ObjectMissingError,
    SecConfigurationError,
    SecResponseError,
    SecTransportError,
)
from gbmi.sources.sec.limiter import WINDOW_SECONDS, SlidingWindowLimiter
from gbmi.sources.sec.urls import split

log = get_logger(__name__)

SOURCE = "sec"
#: The identity of the recorded files. The value never leaves the machine: a live client
#: refuses it (week 3 plan 3.3).
FIXTURE_USER_AGENT = "gbmi-fixture fixture@example.invalid"
#: Tries for a transport error and for a status of 500 or above (blueprint 8.1).
MAX_ATTEMPTS = 5
#: The longest pause between two tries, in seconds.
MAX_PAUSE = 60.0
CONNECT_TIMEOUT = 10.0
READ_TIMEOUT = 60.0
#: The time limit of one large download, in seconds.
DOWNLOAD_LIMIT = 1800.0
_CHUNK = 1 << 20
_ADDRESS = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


@dataclass(frozen=True)
class SecResponse:
    """An answer of the SEC with the status 200."""

    url: str
    status: int
    #: The body after the transfer encoding is removed; empty for HEAD and for a download.
    content: bytes
    last_modified: datetime | None
    #: The header Content-Length; None when the SEC sends none.
    content_length: int | None
    #: The bytes received: the length of the body, or the size of the downloaded file.
    byte_count: int


@dataclass
class ClientStats:
    """What a client did, for the summary of a task."""

    requests: int = 0
    waited_seconds: float = 0.0
    blocks: int = 0
    missing: int = 0
    retries: int = 0


@dataclass(frozen=True)
class _Answer:
    status: int
    content: bytes
    body_start: bytes
    headers: httpx.Headers = field(repr=False)
    byte_count: int


def checked_user_agent(settings: Settings) -> str:
    """The user agent for the source of the settings; raises when a live client may not start.

    No message holds the value.
    """
    if settings.sec_source is SecSource.FIXTURE:
        return FIXTURE_USER_AGENT
    secret = settings.sec_user_agent
    value = secret.get_secret_value().strip() if secret is not None else ""
    if not value:
        raise SecConfigurationError(
            "GBMI_SEC_USER_AGENT is not set: a live request to the SEC needs a declared user "
            "agent with a name and a contact address"
        )
    if value == FIXTURE_USER_AGENT or value.endswith(".invalid"):
        raise SecConfigurationError(
            "GBMI_SEC_USER_AGENT holds the placeholder of the recorded files: a live request "
            "needs the real name and contact address"
        )
    words = value.split()
    addresses = [word for word in words if _ADDRESS.match(word.strip("<>()"))]
    names = [word for word in words if "@" not in word]
    if not addresses or not names:
        raise SecConfigurationError(
            "GBMI_SEC_USER_AGENT needs a name and a contact address with @, in the form "
            "'Sample Company Name contact@example.org'"
        )
    return value


def _last_modified(headers: httpx.Headers) -> datetime | None:
    header = headers.get("Last-Modified")
    if not header:
        return None
    try:
        return parsedate_to_datetime(header).astimezone(UTC)
    except (TypeError, ValueError):
        return None


def _retry_after(headers: httpx.Headers, now: datetime) -> float | None:
    """The header Retry-After in seconds; the header holds a number or an HTTP date."""
    header = headers.get("Retry-After", "").strip()
    if not header:
        return None
    if header.isdigit():
        return float(header)
    try:
        return max(0.0, (parsedate_to_datetime(header) - now).total_seconds())
    except (TypeError, ValueError):
        return None


def _content_length(headers: httpx.Headers) -> int | None:
    header = headers.get("Content-Length", "")
    return int(header) if header.isdigit() else None


class SecClient:
    """One client of the SEC, as a context manager. One thread uses a client."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        dsn: str | None = None,
        run_id: str | None = None,
        source: str = SOURCE,
        transport: httpx.BaseTransport | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], datetime] = utc_now,
        rng: random.Random | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        if self._settings.sec_source is SecSource.OFF:
            raise SecConfigurationError(
                "GBMI_SEC_SOURCE is off: the stack sends no request to the SEC"
            )
        self._user_agent = checked_user_agent(self._settings)
        if transport is None and self._settings.sec_source is not SecSource.LIVE:
            raise SecConfigurationError(
                "the source fixture needs the transport of the recorded files; only the source "
                "live opens a network connection"
            )
        self._transport = transport
        self._dsn = dsn
        self._run_id = run_id
        self.source = source
        self._clock = clock
        self._sleep = sleep
        self._now = now
        self._rng = rng or random.Random()  # noqa: S311 - jitter of a pause, no secret
        self.stats = ClientStats()
        self._connection: Connection | None = None
        self._http: httpx.Client | None = None
        self._limiter: SlidingWindowLimiter | None = None
        self._circuit: Circuit | None = None
        self._requests: RequestLog | None = None

    def __repr__(self) -> str:
        return f"SecClient(source={self.source!r}, mode={self._settings.sec_source.value!r})"

    # --- life of a client -----------------------------------------------------------------

    def __enter__(self) -> Self:
        connection = connect(self._dsn, autocommit=True)
        try:
            # One client at a time for a source: the lock lasts as long as the connection.
            connection.execute(
                "SELECT pg_advisory_lock(hashtextextended(%s, 0))", (f"gbmi:source:{self.source}",)
            )
            self._connection = connection
            self._limiter = SlidingWindowLimiter(
                self._settings.sec_max_rps,
                clock=self._clock,
                sleep=self._sleep,
                # The client before can have sent 8 requests just before it released the lock.
                not_before=self._clock() + WINDOW_SECONDS,
            )
            self._circuit = Circuit(
                connection, source=self.source, now=self._now, sleep=self._sleep
            )
            self._requests = RequestLog(connection, source=self.source, run_id=self._run_id)
            self._http = httpx.Client(
                transport=self._transport or httpx.HTTPTransport(),
                headers={"User-Agent": self._user_agent, "Accept-Encoding": "gzip, deflate"},
                follow_redirects=False,
                timeout=httpx.Timeout(READ_TIMEOUT, connect=CONNECT_TIMEOUT),
            )
        except BaseException:
            connection.close()
            raise
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._http is not None:
            self._http.close()
            self._http = None
        if self._connection is not None:
            self._connection.close()  # the lock of the source ends with the connection
            self._connection = None

    @property
    def session(self) -> str:
        """The identifier of the client session in the request log."""
        return str(self._parts()[3].session)

    def _parts(self) -> tuple[httpx.Client, SlidingWindowLimiter, Circuit, RequestLog]:
        if (
            self._http is None
            or self._limiter is None
            or self._circuit is None
            or self._requests is None
        ):
            raise SecConfigurationError(
                "the client is not open: use `with SecClient(...) as client`"
            )
        return self._http, self._limiter, self._circuit, self._requests

    # --- requests -------------------------------------------------------------------------

    def get(self, url: str, *, listed: bool = False) -> SecResponse:
        """Fetch one object into memory.

        ``listed`` says: a listing of the SEC names the object, so a "no such object" answer
        is not expected. Raises ObjectMissingError when the SEC has no such object.
        """
        return self._request("GET", url, listed=listed)

    def head(self, url: str) -> SecResponse:
        """Ask for the headers of one object: the size and the time of the last change."""
        return self._request("HEAD", url, listed=False)

    def download(
        self, url: str, path: Path, *, limit: float = DOWNLOAD_LIMIT, attempts: int = 1
    ) -> SecResponse:
        """Stream one large object to ``path`` with bounded memory; one request, one try.

        The bytes arrive in ``<path>.part``, and the file gets its name when the body is
        complete. ``limit`` is the time limit of the whole download in seconds.
        """
        return self._request("GET", url, listed=False, sink=path, limit=limit, attempts=attempts)

    def _request(
        self,
        method: str,
        url: str,
        *,
        listed: bool,
        sink: Path | None = None,
        limit: float = DOWNLOAD_LIMIT,
        attempts: int = MAX_ATTEMPTS,
    ) -> SecResponse:
        _, limiter, circuit, requests = self._parts()
        host, path = split(url)
        where = f"{host}{path}"
        attempt = 0
        failures = 0
        while True:
            probe = circuit.before_request()
            waited = limiter.acquire()
            granted_at = self._now()
            granted = limiter.last_grant or 0.0
            attempt += 1
            self.stats.requests += 1
            self.stats.waited_seconds += waited
            request_id = requests.started(
                host=host,
                path=path,
                method=method,
                granted_at=granted_at,
                granted_mono_ns=round(granted * 1_000_000_000),
                waited_ms=round(waited * 1000),
                attempt=attempt,
            )
            started = self._clock()
            try:
                answer = self._send(method, url, sink, limit)
            except httpx.HTTPError as error:
                failures += 1
                last = failures >= attempts
                kind = type(error).__name__
                requests.finished(
                    request_id,
                    completed_at=self._now(),
                    outcome=(Verdict.ERROR if last else Verdict.RETRY).value,
                    http_status=None,
                    duration_ms=self._elapsed_ms(started),
                    error=kind,
                )
                log.warning("sec_request_failed", where=where, error=kind, attempt=attempt)
                if last:
                    # The error of the transport is not chained: its request holds the headers.
                    raise SecTransportError(
                        f"no answer from {where} after {failures} tries: {kind}"
                    ) from None
                self._pause(failures)
                continue
            verdict = classify(answer.status, host, path, answer.body_start)
            requests.finished(
                request_id,
                completed_at=self._now(),
                outcome=verdict.value,
                http_status=answer.status,
                duration_ms=self._elapsed_ms(started),
                byte_count=answer.byte_count,
            )
            if verdict is Verdict.OK:
                circuit.answered(probe=probe)
                circuit.found()
                return SecResponse(
                    url=url,
                    status=answer.status,
                    content=answer.content,
                    last_modified=_last_modified(answer.headers),
                    content_length=_content_length(answer.headers),
                    byte_count=answer.byte_count,
                )
            if verdict is Verdict.MISSING:
                circuit.answered(probe=probe)
                self.stats.missing += 1
                circuit.missing(status=answer.status, listed=listed)
                raise ObjectMissingError(
                    f"the SEC has no such object: HTTP {answer.status} for {where}"
                )
            if verdict is Verdict.BLOCKED:
                self.stats.blocks += 1
                log.warning("sec_request_refused", where=where, http_status=answer.status)
                circuit.refused(
                    status=answer.status,
                    retry_after=_retry_after(answer.headers, self._now()),
                    probe=probe,
                )
                continue  # the circuit decides: one wait and one probe, or the end of the run
            if verdict is Verdict.RETRY:
                failures += 1
                log.warning(
                    "sec_server_error", where=where, http_status=answer.status, attempt=attempt
                )
                if failures >= attempts:
                    raise SecResponseError(
                        f"HTTP {answer.status} for {where} after {failures} tries",
                        status=answer.status,
                    )
                self._pause(failures)
                continue
            raise SecResponseError(f"HTTP {answer.status} for {where}", status=answer.status)

    def _send(self, method: str, url: str, sink: Path | None, limit: float) -> _Answer:
        http = self._parts()[0]
        if sink is None:
            response = http.request(method, url)
            body = response.content if method == "GET" else b""
            return _Answer(
                response.status_code, body, body[:BODY_START], response.headers, len(body)
            )
        deadline = self._clock() + limit
        part = sink.with_name(sink.name + ".part")
        try:
            with http.stream(method, url) as response:
                if response.status_code != 200:
                    start = response.read()[:BODY_START]
                    return _Answer(response.status_code, b"", start, response.headers, 0)
                size = 0
                with part.open("wb") as out:
                    for chunk in response.iter_bytes(_CHUNK):
                        if self._clock() > deadline:
                            raise httpx.ReadTimeout(
                                f"the download did not end within {limit:.0f} seconds"
                            )
                        out.write(chunk)
                        size += len(chunk)
                part.replace(sink)
                return _Answer(response.status_code, b"", b"", response.headers, size)
        finally:
            part.unlink(missing_ok=True)  # a broken download leaves no partial file

    def _elapsed_ms(self, started: float) -> int:
        return max(0, round((self._clock() - started) * 1000))

    def _pause(self, failures: int) -> None:
        """A growing pause with jitter before the next try, at most 60 seconds."""
        self.stats.retries += 1
        self._sleep(min(MAX_PAUSE, 2.0 ** (failures - 1) + self._rng.uniform(0.0, 1.0)))
