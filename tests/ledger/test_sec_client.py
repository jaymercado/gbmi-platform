"""The fair-access client against the fake SEC server and PostgreSQL (week 3 plan, WP-03 step 6).

No test opens a network connection: the transport is ``httpx.MockTransport``. Most tests
control the clock; two tests use the real clock, because the rule "no second holds more than 8
requests" is a statement about real time.
"""

from __future__ import annotations

import io
import itertools
import json
import random
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from email.utils import format_datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import structlog

from gbmi.config import Settings
from gbmi.ledger.db import Connection
from gbmi.logging import configure_logging, get_logger
from gbmi.ops.circuit import close_circuit, read_circuit
from gbmi.ops.requests import (
    max_requests_in_session_window,
    max_requests_in_window,
    request_counts,
)
from gbmi.ops.runs import recorded_run
from gbmi.sources.sec.client import FIXTURE_USER_AGENT, SecClient, checked_user_agent
from gbmi.sources.sec.errors import (
    ObjectMissingError,
    SecConfigurationError,
    SecResponseError,
    SecTransportError,
    SourceBlockedError,
)
from gbmi.sources.sec.limiter import WINDOW_SECONDS
from gbmi.sources.sec.urls import (
    DATA_HOST,
    SUBMISSIONS_ARCHIVE_URL,
    TICKER_FILE_URL,
    WWW_HOST,
    companyfacts_url,
    index_file_url,
    index_listing_url,
    submissions_url,
)

from .sec_server import (
    MODIFIED,
    Answer,
    FakeTime,
    SecServer,
    access_denied,
    broken,
    forbidden,
    server_error,
    too_many,
)

#: A made-up identity. No test may find the text anywhere but in the headers of a request.
SECRET = "Example Research Desk secret-contact@example.org"
MARKS = ("secret-contact", "Example Research Desk")
META = submissions_url(1326801)
NVIDIA = submissions_url(1045810)
FACTS = companyfacts_url(1740332)
INDEX = index_file_url(date(2026, 11, 26))


def settings(**values: Any) -> Settings:
    options = {"sec_source": "live", "sec_user_agent": SECRET, **values}
    return Settings(_env_file=None, **options)


@pytest.fixture
def clock() -> FakeTime:
    return FakeTime()


@pytest.fixture
def server(clock: FakeTime) -> SecServer:
    fake = SecServer(clock.clock)
    for url in (TICKER_FILE_URL, META, NVIDIA):
        fake.put(url)
    return fake


@pytest.fixture
def open_client(ops_db: str, source: str, server: SecServer, clock: FakeTime) -> Any:
    def build(**options: Any) -> SecClient:
        arguments = {
            "dsn": ops_db,
            "source": source,
            "run_id": "test-run",
            "transport": server.transport(),
            "clock": clock.clock,
            "sleep": clock.sleep,
            "now": clock.now,
            "rng": random.Random(7),  # noqa: S311 - the jitter of a pause
            **options,
        }
        return SecClient(arguments.pop("settings", settings()), **arguments)

    return build


@pytest.fixture
def captured_log() -> Iterator[io.StringIO]:
    buffer = io.StringIO()
    configure_logging(stream=buffer, level="DEBUG")
    yield buffer
    structlog.reset_defaults()


def log_rows(ops: Connection, source: str) -> list[tuple[Any, ...]]:
    return ops.execute(
        "SELECT attempt, outcome, http_status, path FROM ops.source_request WHERE source = %s "
        "ORDER BY request_id",
        (source,),
    ).fetchall()


# --- identity ---------------------------------------------------------------------------------


def test_every_request_declares_the_identity_and_no_redirect_is_followed(
    open_client: Any, server: SecServer
) -> None:
    listing = index_listing_url(2026, 4)
    server.answer_always(listing, Answer(302, {"Location": "https://example.org/elsewhere"}))

    with open_client() as client:
        client.get(TICKER_FILE_URL)
        client.get(META)
        client.head(TICKER_FILE_URL)
        with pytest.raises(SecResponseError, match=r"HTTP 302 for www\.sec\.gov") as raised:
            client.get(listing)

    assert raised.value.status == 302
    assert len(server.arrivals) == 4  # the redirect was not followed
    assert {arrival.host for arrival in server.arrivals} == {WWW_HOST, DATA_HOST}
    for arrival in server.arrivals:
        assert arrival.headers["user-agent"] == SECRET
        assert arrival.headers["accept-encoding"] == "gzip, deflate"
        assert arrival.headers["host"] == arrival.host
    assert [arrival.method for arrival in server.arrivals] == ["GET", "GET", "HEAD", "GET"]


def test_the_user_agent_stands_in_no_log_line_and_in_no_row(
    open_client: Any,
    server: SecServer,
    ops: Connection,
    source: str,
    captured_log: io.StringIO,
) -> None:
    server.answer_once(META, too_many(), server_error())
    with open_client() as client:
        client.get(TICKER_FILE_URL)
        client.get(META)  # a refusal, a wait, a probe with a server error, the object
        with pytest.raises(ObjectMissingError):
            client.get(FACTS)
        assert SECRET not in repr(client) and SECRET not in str(vars(client.stats))

    logged = captured_log.getvalue()
    assert "sec_circuit_opened" in logged and "sec_server_error" in logged
    stored = ops.execute(
        "SELECT coalesce(string_agg(row_to_json(r)::text, ' '), '') FROM ops.source_request r "
        "WHERE source = %s",
        (source,),
    ).fetchone()
    circuit = ops.execute(
        "SELECT row_to_json(c)::text FROM ops.source_circuit c WHERE source = %s", (source,)
    ).fetchone()
    assert stored is not None and circuit is not None and len(stored[0]) > 100
    for text in (logged, stored[0], circuit[0]):
        for mark in MARKS:
            assert mark not in text


def test_an_error_inside_the_sending_code_shows_no_header(
    open_client: Any,
    server: SecServer,
    ops: Connection,
    source: str,
    clock: FakeTime,
    captured_log: io.StringIO,
) -> None:
    server.answer_always(META, broken(httpx.ConnectError("connection refused")))
    entry_point = f"{source}.filings"

    with (
        pytest.raises(SecTransportError) as raised,
        recorded_run(ops, entry_point, pipeline_version="test"),
    ):
        try:
            with open_client() as client:
                client.get(META)
        except SecTransportError:
            get_logger("test").exception("sec_task_failed")
            raise

    message = str(raised.value)
    assert (
        message
        == "no answer from data.sec.gov/submissions/CIK0001326801.json after 5 tries: ConnectError"
    )
    # The error of the transport is not chained: its request object holds the headers.
    assert raised.value.__cause__ is None and raised.value.__suppress_context__
    row = ops.execute(
        "SELECT status, error FROM ops.pipeline_run WHERE entry_point = %s", (entry_point,)
    ).fetchone()
    assert row == ("failed", f"SecTransportError: {message}")
    line = next(
        json.loads(text)
        for text in captured_log.getvalue().splitlines()
        if "sec_task_failed" in text
    )
    frames = [frame for error in line["exception"] for frame in error["frames"]]
    assert frames and not [frame for frame in frames if frame.get("locals")]
    for mark in MARKS:
        assert mark not in captured_log.getvalue() and mark not in message
    assert server.asked(META) == 5
    assert len(clock.sleeps) == 5  # the start of the client, then four growing pauses
    assert clock.sleeps[1:] == sorted(clock.sleeps[1:]) and max(clock.sleeps) <= 60


@pytest.mark.parametrize(
    ("value", "words"),
    [
        (None, "is not set"),
        ("", "is not set"),
        ("   ", "is not set"),
        ("Example Research Desk no-address", "a name and a contact address with @"),
        ("lonely-contact@example.org", "a name and a contact address with @"),
        ("Example Research Desk broken@address", "a name and a contact address with @"),
        (FIXTURE_USER_AGENT, "placeholder of the recorded files"),
        ("Example Research Desk someone@example.invalid", "placeholder of the recorded files"),
    ],
)
def test_a_live_client_refuses_to_start_without_a_declared_identity(
    value: str | None, words: str
) -> None:
    with pytest.raises(SecConfigurationError, match=words) as raised:
        SecClient(settings(sec_user_agent=value))

    if value and value.strip():
        for word in value.split():
            assert word not in str(raised.value) or word in ("a", "name")


def test_the_form_of_a_declared_identity() -> None:
    assert checked_user_agent(settings()) == SECRET
    assert (
        checked_user_agent(settings(sec_user_agent="  Sample Company Name <admin@sample.com> "))
        == "Sample Company Name <admin@sample.com>"
    )
    # The recorded files are read with the placeholder, whatever the setting holds.
    assert checked_user_agent(settings(sec_source="fixture")) == FIXTURE_USER_AGENT


def test_only_the_source_live_can_open_a_network_connection(
    monkeypatch: pytest.MonkeyPatch, open_client: Any, server: SecServer
) -> None:
    def no_network(*_args: Any, **_options: Any) -> None:
        raise AssertionError("a network transport was built")

    monkeypatch.setattr(httpx, "HTTPTransport", no_network)

    with pytest.raises(SecConfigurationError, match="GBMI_SEC_SOURCE is off"):
        SecClient(settings(sec_source="off"))
    with pytest.raises(SecConfigurationError, match="GBMI_SEC_SOURCE is off"):
        SecClient(Settings(_env_file=None))  # off is the default
    with pytest.raises(SecConfigurationError, match="only the source live opens a network"):
        SecClient(settings(sec_source="fixture"))
    # With the transport of the recorded files the client works and sends the placeholder.
    with open_client(settings=settings(sec_source="fixture")) as client:
        client.get(META)
    assert server.arrivals[0].headers["user-agent"] == FIXTURE_USER_AGENT


def test_a_client_which_is_not_open_sends_nothing(open_client: Any, server: SecServer) -> None:
    client = open_client()

    with pytest.raises(SecConfigurationError, match="not open"):
        client.get(META)
    with pytest.raises(ValueError, match="not an HTTPS address of the SEC"), client:
        client.get("https://example.org/submissions/CIK0001326801.json")
    assert server.arrivals == []


# --- the limiter and the request log ------------------------------------------------------------


def test_the_first_request_waits_one_window_and_the_log_holds_every_attempt(
    open_client: Any, server: SecServer, ops: Connection, source: str, clock: FakeTime
) -> None:
    with open_client() as client:
        for _ in range(9):
            client.get(META)
        assert client.stats.requests == 9
        assert client.stats.waited_seconds == pytest.approx(2 * WINDOW_SECONDS)
        session = uuid.UUID(client.session)

    assert clock.sleeps == [pytest.approx(WINDOW_SECONDS)] * 2
    times = [arrival.at for arrival in server.arrivals]
    assert times[8] - times[0] == pytest.approx(WINDOW_SECONDS)
    rows = ops.execute(
        "SELECT host, method, attempt, waited_ms, run_id, client_session, outcome, http_status, "
        "byte_count, granted_mono_ns FROM ops.source_request WHERE source = %s "
        "ORDER BY request_id",
        (source,),
    ).fetchall()
    assert len(rows) == 9
    assert {row[:3] for row in rows} == {(DATA_HOST, "GET", 1)}
    assert [row[3] for row in rows] == [1050, 0, 0, 0, 0, 0, 0, 0, 1050]
    assert {row[4:9] for row in rows} == {("test-run", session, "ok", 200, 12)}
    assert rows[8][9] - rows[0][9] == pytest.approx(WINDOW_SECONDS * 1e9)
    assert max_requests_in_window(ops, source=source) == 8
    assert max_requests_in_session_window(ops, source=source) == 8


def test_the_setting_lowers_the_ceiling(open_client: Any, clock: FakeTime) -> None:
    with open_client(settings=settings(sec_max_rps=2)) as client:
        for _ in range(5):
            client.get(META)

    assert clock.sleeps == [pytest.approx(WINDOW_SECONDS)] * 3


# --- blocks -----------------------------------------------------------------------------------


def test_a_429_opens_the_circuit_and_one_probe_closes_the_circuit(
    open_client: Any, server: SecServer, ops: Connection, source: str, clock: FakeTime
) -> None:
    server.answer_once(META, too_many())

    with open_client() as client:
        response = client.get(META)
        assert response.status == 200 and client.stats.blocks == 1
        circuit = read_circuit(ops, source)
        assert not circuit.recorded and circuit.open_count == 1
        # A second block in the same run fails the run.
        server.answer_once(NVIDIA, too_many())
        with pytest.raises(SourceBlockedError, match="has waited for a block before"):
            client.get(NVIDIA)

    assert pytest.approx(900.0) in clock.sleeps
    first, probe = (arrival.at for arrival in server.arrivals[:2])
    assert probe - first >= 900.0  # no request arrived while the circuit was open
    assert server.asked(META) == 2 and server.asked(NVIDIA) == 1
    assert [row[:3] for row in log_rows(ops, source)] == [
        (1, "blocked", 429),
        (2, "ok", 200),
        (1, "blocked", 429),
    ]
    assert read_circuit(ops, source).open_count == 2


def test_a_refused_probe_locks_the_circuit_until_the_operator_closes(
    open_client: Any, server: SecServer, ops: Connection, source: str, clock: FakeTime
) -> None:
    server.answer_once(META, too_many(), forbidden())

    with open_client() as client, pytest.raises(SourceBlockedError, match="refused the probe"):
        client.get(META)

    assert read_circuit(ops, source).locked
    clock.advance(7 * 86_400)
    with open_client() as client, pytest.raises(SourceBlockedError, match="locked"):
        client.get(META)
    assert server.asked(META) == 2  # the locked circuit let no request go
    assert close_circuit(
        ops, source, closed_by="jay", note="the SEC lifted the block", now=clock.now()
    )
    with open_client() as client:
        assert client.get(META).status == 200


def test_a_probe_which_ends_with_a_server_error_decides_nothing(
    open_client: Any, server: SecServer, ops: Connection, source: str
) -> None:
    server.answer_once(META, forbidden(), server_error(503))

    with open_client() as client:
        assert client.get(META).status == 200

    assert [row[:3] for row in log_rows(ops, source)] == [
        (1, "blocked", 403),
        (2, "retry", 503),
        (3, "ok", 200),
    ]
    circuit = read_circuit(ops, source)
    assert not circuit.recorded and not circuit.locked


@pytest.mark.parametrize("header", ["1200", "date"])
def test_a_long_retry_after_fails_the_run_without_a_wait(
    open_client: Any, server: SecServer, ops: Connection, source: str, clock: FakeTime, header: str
) -> None:
    value = (
        format_datetime(clock.now() + timedelta(seconds=1200 + WINDOW_SECONDS), usegmt=True)
        if header == "date"
        else header
    )
    server.answer_once(META, too_many(value))

    with open_client() as client, pytest.raises(SourceBlockedError, match="longer than 16 minutes"):
        client.get(META)

    assert max(clock.sleeps) == pytest.approx(WINDOW_SECONDS)  # only the start of the client
    circuit = read_circuit(ops, source)
    assert circuit.blocked_until is not None
    assert (circuit.blocked_until - clock.now()).total_seconds() == pytest.approx(1200, abs=2)
    assert server.asked(META) == 1


# --- missing objects --------------------------------------------------------------------------


def test_a_missing_object_is_no_block_and_is_not_tried_again(
    open_client: Any, server: SecServer, ops: Connection, source: str
) -> None:
    with open_client() as client:
        with pytest.raises(ObjectMissingError, match=r"HTTP 404 for data\.sec\.gov/api/xbrl"):
            client.get(FACTS)
        with pytest.raises(ObjectMissingError, match=r"HTTP 403 for www\.sec\.gov/Archives"):
            client.get(INDEX)
        assert client.stats.missing == 2 and client.stats.blocks == 0
        assert client.get(META).status == 200

    assert server.asked(FACTS) == 1 and server.asked(INDEX) == 1
    assert not read_circuit(ops, source).recorded
    assert request_counts(ops, source=source) == {"missing": 2, "ok": 1}


def test_a_403_with_another_body_below_archives_is_a_block(
    open_client: Any, server: SecServer, ops: Connection, source: str
) -> None:
    server.answer_always(INDEX, forbidden())

    with open_client() as client, pytest.raises(SourceBlockedError, match="refused the probe"):
        client.get(INDEX)

    assert read_circuit(ops, source).locked


def test_three_missing_listed_objects_in_a_row_open_the_circuit(
    open_client: Any, server: SecServer, ops: Connection, source: str
) -> None:
    days = [date(2026, 11, day) for day in (23, 24, 25)]

    with open_client() as client:
        for day in days[:2]:
            with pytest.raises(ObjectMissingError):
                client.get(index_file_url(day), listed=True)
        with pytest.raises(SourceBlockedError, match="a block can look like a missing file"):
            client.get(index_file_url(days[2]), listed=True)

    assert read_circuit(ops, source).recorded
    server.answer_always(index_file_url(days[0]), access_denied())
    assert server.asked(index_file_url(days[0])) == 1


# --- server errors and transport errors ---------------------------------------------------------


def test_a_server_error_is_tried_at_most_five_times_and_each_try_passes_the_limiter(
    open_client: Any, server: SecServer, ops: Connection, source: str, clock: FakeTime
) -> None:
    server.answer_always(META, server_error(500))

    with open_client(settings=settings(sec_max_rps=1)) as client:
        with pytest.raises(SecResponseError, match=r"HTTP 500 .* after 5 tries") as raised:
            client.get(META)
        assert client.stats.retries == 4

    assert raised.value.status == 500
    assert [row[:3] for row in log_rows(ops, source)] == [
        (attempt, "retry", 500) for attempt in range(1, 6)
    ]
    times = [arrival.at for arrival in server.arrivals]
    assert len(times) == 5
    # With a ceiling of one request, two requests in a row are at least one window apart.
    assert min(later - earlier for earlier, later in itertools.pairwise(times)) >= (
        WINDOW_SECONDS - 1e-9
    )


def test_a_passing_error_is_followed_by_the_object(
    open_client: Any, server: SecServer, ops: Connection, source: str
) -> None:
    server.answer_once(META, server_error(503), broken(httpx.ReadTimeout("slow")))

    with open_client() as client:
        response = client.get(META)

    assert response.content == b'{"ok": true}' and response.last_modified == MODIFIED
    rows = ops.execute(
        "SELECT attempt, outcome, http_status, error FROM ops.source_request WHERE source = %s "
        "ORDER BY request_id",
        (source,),
    ).fetchall()
    assert rows == [
        (1, "retry", 503, None),
        (2, "retry", None, "ReadTimeout"),
        (3, "ok", 200, None),
    ]


def test_a_killed_run_leaves_the_row_of_every_request_which_left(
    open_client: Any, server: SecServer, ops: Connection, source: str
) -> None:
    server.answer_once(NVIDIA, broken(KeyboardInterrupt()))

    with pytest.raises(KeyboardInterrupt), open_client() as client:
        client.get(TICKER_FILE_URL)
        client.get(META)
        client.get(NVIDIA)

    assert request_counts(ops, source=source) == {"ok": 2, "open": 1}
    assert len(server.arrivals) == 3


# --- large files ------------------------------------------------------------------------------


def test_a_download_streams_to_a_file_and_counts_as_one_request(
    open_client: Any, server: SecServer, ops: Connection, source: str, tmp_path: Path
) -> None:
    archive = bytes(range(256)) * 12_000  # about 3 MB
    server.put(SUBMISSIONS_ARCHIVE_URL, archive)
    target = tmp_path / "submissions.zip"

    with open_client() as client:
        size = client.head(SUBMISSIONS_ARCHIVE_URL)
        response = client.download(SUBMISSIONS_ARCHIVE_URL, target)

    assert (size.content_length, size.content, size.last_modified) == (len(archive), b"", MODIFIED)
    assert target.read_bytes() == archive
    assert (response.byte_count, response.content) == (len(archive), b"")
    assert list(tmp_path.iterdir()) == [target]
    assert [row[:3] for row in log_rows(ops, source)] == [(1, "ok", 200), (1, "ok", 200)]
    assert [arrival.method for arrival in server.arrivals] == ["HEAD", "GET"]


def test_a_download_which_fails_leaves_no_file_and_is_tried_once(
    open_client: Any, server: SecServer, clock: FakeTime, tmp_path: Path
) -> None:
    server.put(SUBMISSIONS_ARCHIVE_URL, b"x" * 2_000_000)
    target = tmp_path / "submissions.zip"

    def slow(request: httpx.Request) -> httpx.Response:
        response = server.handler(request)
        clock.advance(3600)  # the body takes an hour
        return response

    with open_client(transport=httpx.MockTransport(slow)) as client:
        with pytest.raises(SecTransportError, match="after 1 tries: ReadTimeout"):
            client.download(SUBMISSIONS_ARCHIVE_URL, target, limit=1800)
        server.answer_always(SUBMISSIONS_ARCHIVE_URL, access_denied())
        with pytest.raises(ObjectMissingError):
            client.download(SUBMISSIONS_ARCHIVE_URL, target)

    assert list(tmp_path.iterdir()) == []
    assert server.asked(SUBMISSIONS_ARCHIVE_URL) == 2


# --- real time --------------------------------------------------------------------------------


def test_with_the_real_clock_no_second_holds_more_than_8_of_24_requests(
    ops_db: str, ops: Connection, source: str
) -> None:
    server = SecServer()
    server.put(META)

    with SecClient(settings(), dsn=ops_db, source=source, transport=server.transport()) as client:
        for _ in range(24):
            client.get(META)

    assert len(server.arrivals) == 24
    assert server.max_in_window() <= 8
    assert max_requests_in_window(ops, source=source) <= 8
    assert max_requests_in_session_window(ops, source=source) <= 8
    times = [arrival.at for arrival in server.arrivals]
    assert times[-1] - times[0] >= 2 * WINDOW_SECONDS


def test_a_second_client_waits_for_the_lock_of_the_first_client(
    ops_db: str, ops: Connection, source: str
) -> None:
    server = SecServer()
    server.put(META)

    def eight_requests(_lane: int) -> str:
        with SecClient(
            settings(), dsn=ops_db, source=source, transport=server.transport()
        ) as client:
            for _ in range(8):
                client.get(META)
            return client.session

    with ThreadPoolExecutor(max_workers=2) as pool:
        sessions = list(pool.map(eight_requests, range(2)))

    assert len(set(sessions)) == 2 and len(server.arrivals) == 16
    # Sixteen requests of two clients, and still no second with more than 8.
    assert server.max_in_window() <= 8
    assert max_requests_in_window(ops, source=source) <= 8
    order = ops.execute(
        "SELECT client_session FROM ops.source_request WHERE source = %s ORDER BY request_id",
        (source,),
    ).fetchall()
    # One client at a time: the requests of the two sessions do not mix.
    assert len({row[0] for row in order[:8]}) == 1 and len({row[0] for row in order[8:]}) == 1
