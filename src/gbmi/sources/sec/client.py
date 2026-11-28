"""The fair-access client: the only way a request reaches the SEC (blueprint 4.3; week 3 plan 3.3).

The client cannot exceed the ceiling, cannot hide its identity and cannot ignore a block:

* **One process at a time.** A client holds a PostgreSQL advisory lock for the source as long
  as the client lives. A second client, for example a command of an operator beside a task,
  waits for the lock. The first request of a client leaves at the earliest one window after
  the client has received the lock. Inside a process, one request runs at a time.
* **At most 8 requests in any second**, over both hosts of the SEC: every attempt, a retry
  too, passes ``SlidingWindowLimiter``, and the limiter counts a request from the moment its
  answer has arrived.
* **A declared identity.** Every request carries the user agent of the settings, the header
  ``Accept-Encoding: gzip, deflate`` and the host name of the address (finding F1). The client
  follows no redirect.
* **A block is respected.** ``gbmi.sources.sec.circuit.Circuit`` reads the circuit row before
  every request and applies the rules of a block. The block lives in the database of the
  stack: a client which opens a network connection refuses a throwaway database.
* **Evidence.** One row of ``ops.source_request`` is written before each attempt leaves.

Each attempt runs in the same order: read the circuit, wait in the limiter, write the log row,
send, tell the limiter that the answer has arrived, classify the answer, act.

The user agent holds a contact address. The value stands in no log line, in no table and in
no error message: a message names the host and the path of an address, never a header, and an
error of the transport, whose request object holds the headers, is never part of a raised
error.
"""

from __future__ import annotations

import random
import re
import threading
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
from gbmi.ledger.scratch import PREFIX as SCRATCH_PREFIX
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
#: The time limit of one object which is read into memory, in seconds.
GET_LIMIT = 300.0
#: The time limit of one large download, in seconds.
DOWNLOAD_LIMIT = 1800.0
#: The largest object which is read into memory. A facts document has up to about 10 MB.
MAX_BODY = 64 * 1024 * 1024
#: The longest time of the header Retry-After which is read as seconds: seven digits.
_SECONDS = re.compile(r"[0-9]{1,7}")
_ADDRESS = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
#: Domains which are reserved for examples and tests: no person answers such an address.
_RESERVED = (
    ".invalid",
    ".example",
    ".test",
    ".localhost",
    "example.com",
    "example.org",
    "example.net",
)


@dataclass(frozen=True)
class SecResponse:
    """An answer of the SEC with the status 200."""

    url: str
    status: int
    #: The body after the content encoding is removed; empty for a download.
    content: bytes
    last_modified: datetime | None
    #: The header Content-Length of a body which arrived without a content encoding; None
    #: otherwise, because the header then counts the encoded bytes and not ``byte_count``.
    content_length: int | None
    #: The bytes received after the content encoding is removed: the length of the body, or
    #: the size of the downloaded file.
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


def checked_user_agent(settings: Settings, *, network: bool = True) -> str:
    """The user agent for the source of the settings; raises when a live client may not start.

    ``network`` says: the client opens a real connection. An address of a domain which is
    reserved for examples is then refused: the SEC asks for a contact which answers. No
    message holds the value.
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
    words = value.split()
    addresses = [word.strip("<>()") for word in words if _ADDRESS.match(word.strip("<>()"))]
    names = [word for word in words if "@" not in word]
    placeholder = value == FIXTURE_USER_AGENT or any(
        address.lower().endswith(".invalid") for address in addresses
    )
    if placeholder or (
        network and any(address.lower().endswith(_RESERVED) for address in addresses)
    ):
        raise SecConfigurationError(
            "GBMI_SEC_USER_AGENT holds a placeholder: a live request needs the real name and "
            "a contact address which a person reads"
        )
    if not addresses or not names:
        raise SecConfigurationError(
            "GBMI_SEC_USER_AGENT needs a name and a contact address with @, in the form "
            "'Sample Company Name contact@sample-company.com'"
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
    """The header Retry-After in seconds; the header holds a number or an HTTP date.

    A value which is neither counts as no value: a wrong header must not stop the client
    from recording the block.
    """
    header = headers.get("Retry-After", "").strip()
    if not header:
        return None
    if _SECONDS.fullmatch(header):
        return float(header)
    try:
        return max(0.0, (parsedate_to_datetime(header) - now).total_seconds())
    except (TypeError, ValueError, OverflowError):
        return None


def _content_length(headers: httpx.Headers) -> int | None:
    if headers.get("Content-Encoding", "identity").strip().lower() not in ("", "identity"):
        return None
    header = headers.get("Content-Length", "")
    return int(header) if header.isascii() and header.isdigit() else None


class SecClient:
    """One client of the SEC, as a context manager."""

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
        #: True when the client opens a real connection to the SEC.
        self._network = transport is None
        if self._network and self._settings.sec_source is not SecSource.LIVE:
            raise SecConfigurationError(
                "the source fixture needs the transport of the recorded files; only the source "
                "live opens a network connection"
            )
        if self._network and source != SOURCE:
            raise SecConfigurationError(
                f"a client which opens a network connection works on the source {SOURCE!r} "
                "only: the lock, the circuit and the request log belong to the source"
            )
        self._user_agent = checked_user_agent(self._settings, network=self._network)
        self._transport = transport
        self._dsn = dsn
        self._run_id = run_id
        self.source = source
        self._clock = clock
        self._sleep = sleep
        self._now = now
        self._rng = rng or random.Random()  # noqa: S311 - jitter of a pause, no secret
        self.stats = ClientStats()
        self._busy = threading.Lock()
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
            if self._network:
                row = connection.execute("SELECT current_database()").fetchone()
                if row is not None and str(row[0]).startswith(SCRATCH_PREFIX):
                    raise SecConfigurationError(
                        "a client which opens a network connection needs the database of the "
                        "stack: a throwaway database would forget a block by the SEC"
                    )
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
                connection,
                source=self.source,
                run_id=self._run_id,
                now=self._now,
                sleep=self._sleep,
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

    def get(self, url: str, *, listed: bool = False, limit: float = GET_LIMIT) -> SecResponse:
        """Fetch one object into memory, within ``limit`` seconds.

        ``listed`` says: a listing of the SEC names the object, so a "no such object" answer
        is not expected. Raises ObjectMissingError when the SEC has no such object.
        """
        with self._busy:
            return self._request(url, listed=listed, sink=None, limit=limit, attempts=MAX_ATTEMPTS)

    def download(
        self, url: str, path: Path, *, limit: float = DOWNLOAD_LIMIT, attempts: int = 1
    ) -> SecResponse:
        """Stream one large object to ``path`` with bounded memory; one request, one try.

        The bytes arrive in ``<path>.part``, and the file gets its name when the body is
        complete. ``limit`` is the time limit of the whole download in seconds. The answer
        carries the size and the time of the last change of the object.
        """
        with self._busy:
            return self._request(url, listed=False, sink=path, limit=limit, attempts=attempts)

    def _request(
        self, url: str, *, listed: bool, sink: Path | None, limit: float, attempts: int
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
                method="GET",
                granted_at=granted_at,
                granted_mono_ns=round(granted * 1_000_000_000),
                waited_ms=round(waited * 1000),
                attempt=attempt,
                probe=probe,
            )
            started = self._clock()
            answer: _Answer | None = None
            failure = ""
            try:
                answer = self._send(url, sink, limit)
            except httpx.HTTPError as error:
                # Only the class of the error is kept: the request of the error holds the headers.
                failure = type(error).__name__
            finally:
                # The request has left and is over: the limiter counts from this moment.
                limiter.settle()
            duration_ms = max(0, round((self._clock() - started) * 1000))

            if answer is None:
                failures += 1
                last = failures >= attempts
                requests.finished(
                    request_id,
                    completed_at=self._now(),
                    outcome=(Verdict.ERROR if last else Verdict.RETRY).value,
                    http_status=None,
                    duration_ms=duration_ms,
                    error=failure,
                )
                log.warning("sec_request_failed", where=where, error=failure, attempt=attempt)
                if last:
                    raise SecTransportError(
                        f"no answer from {where} after {failures} tries: {failure}"
                    )
                self._pause(failures)
                continue

            verdict = classify(answer.status, host, path, answer.body_start)

            def finish(found: _Answer = answer, outcome: Verdict = verdict) -> None:
                requests.finished(
                    request_id,  # noqa: B023 - called inside the same turn of the loop
                    completed_at=self._now(),
                    outcome=outcome.value,
                    http_status=found.status,
                    duration_ms=duration_ms,  # noqa: B023
                    byte_count=found.byte_count,
                )

            if verdict is Verdict.BLOCKED:
                self.stats.blocks += 1
                log.warning("sec_request_refused", where=where, http_status=answer.status)
                try:
                    # First the circuit: the refusal is remembered before anything else.
                    circuit.refused(
                        status=answer.status,
                        retry_after=_retry_after(answer.headers, self._now()),
                        probe=probe,
                    )
                finally:
                    finish()
                continue  # the circuit decides: one wait and one probe, or the end of the run
            if verdict is Verdict.MISSING and probe and listed:
                # A probe for an object which a listing names, answered "no such object": a
                # block can look like a missing file, so the probe counts as refused.
                self.stats.blocks += 1
                try:
                    circuit.refused(
                        status=answer.status, retry_after=None, probe=True, missing=True
                    )
                finally:
                    finish()
            finish()
            if verdict is Verdict.OK:
                circuit.found(probe=probe)
                return SecResponse(
                    url=url,
                    status=answer.status,
                    content=answer.content,
                    last_modified=_last_modified(answer.headers),
                    content_length=_content_length(answer.headers),
                    byte_count=answer.byte_count,
                )
            if verdict is Verdict.MISSING:
                self.stats.missing += 1
                circuit.missing(status=answer.status, listed=listed)
                raise ObjectMissingError(
                    f"the SEC has no such object: HTTP {answer.status} for {where}",
                    status=answer.status,
                    body=answer.body_start,
                )
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

    def _send(self, url: str, sink: Path | None, limit: float) -> _Answer:
        """One GET request: the body into memory, or into the file ``sink``.

        The time limit is checked whenever bytes arrive; a read which brings no byte ends
        after 60 seconds, so the whole request ends at most 60 seconds after the limit.
        """
        http = self._parts()[0]
        deadline = self._clock() + limit
        part = sink.with_name(sink.name + ".part") if sink is not None else None
        try:
            with http.stream("GET", url) as response:
                chunks: list[bytes] = []
                size = 0
                complete = response.status_code == 200
                out = part.open("wb") if part is not None and complete else None
                try:
                    for chunk in response.iter_bytes():
                        if self._clock() > deadline:
                            raise httpx.ReadTimeout(
                                f"the body did not end within {limit:.0f} seconds"
                            )
                        size += len(chunk)
                        if out is not None:
                            out.write(chunk)
                            continue
                        chunks.append(chunk)
                        if not complete and size >= BODY_START:
                            break  # the start of a refusal is enough to read the refusal
                        if size > MAX_BODY:
                            raise httpx.DecodingError(f"the body is larger than {MAX_BODY} bytes")
                finally:
                    if out is not None:
                        out.close()
                body = b"".join(chunks)
                if not complete:
                    return _Answer(
                        response.status_code, b"", body[:BODY_START], response.headers, 0
                    )
                if part is not None and sink is not None:
                    part.replace(sink)
                return _Answer(
                    response.status_code, body, body[:BODY_START], response.headers, size
                )
        finally:
            if part is not None:
                part.unlink(missing_ok=True)  # a broken download leaves no partial file

    def _pause(self, failures: int) -> None:
        """A growing pause with jitter before the next try, at most 60 seconds."""
        self.stats.retries += 1
        self._sleep(min(MAX_PAUSE, 2.0 ** (failures - 1) + self._rng.uniform(0.0, 1.0)))
