"""The fair-access client against the fake SEC server and PostgreSQL (week 3 plan, WP-03 step 6).

No test opens a network connection: the transport is ``httpx.MockTransport``, and
``tests/conftest.py`` lets the construction of a network transport fail. Most tests control
the clock; three tests use the real clock, because the rule "no second holds more than 8
requests" is a statement about real time.
"""

from __future__ import annotations

import io
import itertools
import json
import random
import time
import uuid
import zipfile
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
from gbmi.ops.circuit import circuit_events, close_circuit, read_circuit
from gbmi.ops.requests import (
    max_requests_in_session_window,
    max_requests_in_window,
    request_counts,
)
from gbmi.ops.runs import recorded_run
from gbmi.sources.sec import client as client_module
from gbmi.sources.sec.client import FIXTURE_USER_AGENT, SecClient, checked_user_agent
from gbmi.sources.sec.errors import (
    ObjectMissingError,
    SecConfigurationError,
    SecResponseError,
    SecTransportError,
    SourceBlockedError,
)
from gbmi.sources.sec.limiter import WINDOW_SECONDS
from gbmi.sources.sec.transport import LAST_MODIFIED
from gbmi.sources.sec.urls import (
    DATA_HOST,
    SUBMISSIONS_ARCHIVE_URL,
    TICKER_FILE_NAME,
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
    SlowBody,
    broken,
    forbidden,
    server_error,
    too_many,
)

#: The recorded files of the source fixture (week 3 plan, WP-04).
SEC_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "sec"
#: A made-up identity. No test may find the text anywhere but in the headers of a request.
SECRET = "Example Research Desk secret-contact@example.org"
MARKS = ("secret-contact", "Example Research Desk")
#: An identity which passes the check of a client with a network connection. No test sends it.
REAL_LOOKING = "Sample Research Desk person@sample-research-desk.com"
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
    fake = SecServer(clock.clock, advance=clock.advance)
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
        "SELECT attempt, outcome, http_status, probe FROM ops.source_request WHERE source = %s "
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
        with pytest.raises(SecResponseError, match=r"HTTP 302 for www\.sec\.gov") as raised:
            client.get(listing)

    assert raised.value.status == 302
    assert len(server.arrivals) == 3  # the redirect was not followed
    assert {arrival.host for arrival in server.arrivals} == {WWW_HOST, DATA_HOST}
    for arrival in server.arrivals:
        assert arrival.headers["user-agent"] == SECRET
        assert arrival.headers["accept-encoding"] == "gzip, deflate"
        assert arrival.headers["host"] == arrival.host
        assert arrival.method == "GET"


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
    tables = ("source_request", "source_circuit", "circuit_event")
    stored = [
        ops.execute(
            f"SELECT coalesce(string_agg(row_to_json(t)::text, ' '), '') FROM ops.{table} t "
            "WHERE source = %s",
            (source,),
        ).fetchone()
        for table in tables
    ]
    assert all(row is not None and len(row[0]) > 50 for row in stored)
    for text in (logged, *(row[0] for row in stored if row is not None)):
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
    def refuse_connection(request: httpx.Request) -> httpx.Response:
        server.handler(request)  # the arrival is counted
        # The error carries the request, and the request carries the headers.
        raise httpx.ConnectError(
            f"refused, sent as {request.headers['user-agent']}", request=request
        )

    entry_point = f"{source}.filings"
    with (
        pytest.raises(SecTransportError) as raised,
        recorded_run(ops, entry_point, pipeline_version="test"),
    ):
        try:
            with open_client(transport=httpx.MockTransport(refuse_connection)) as client:
                client.get(META)
        except SecTransportError:
            get_logger("test").exception("sec_task_failed")
            raise

    message = str(raised.value)
    assert message == (
        "no answer from data.sec.gov/submissions/CIK0001326801.json after 5 tries: ConnectError"
    )
    # The error of the transport is no part of the raised error: neither cause nor context.
    assert raised.value.__cause__ is None and raised.value.__context__ is None
    row = ops.execute(
        "SELECT status, error FROM ops.pipeline_run WHERE entry_point = %s", (entry_point,)
    ).fetchone()
    assert row == ("failed", f"SecTransportError: {message}")
    line = next(
        json.loads(text)
        for text in captured_log.getvalue().splitlines()
        if "sec_task_failed" in text
    )
    assert [error["exc_type"] for error in line["exception"]] == ["SecTransportError"]
    frames = [frame for error in line["exception"] for frame in error["frames"]]
    assert frames and not [frame for frame in frames if frame.get("locals")]
    errors = ops.execute(
        "SELECT string_agg(error, ' ') FROM ops.source_request WHERE source = %s", (source,)
    ).fetchone()
    assert errors == ("ConnectError ConnectError ConnectError ConnectError ConnectError",)
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
        ("Sample Research Desk no-address", "a name and a contact address with @"),
        ("lonely-contact@sample-research-desk.com", "a name and a contact address with @"),
        ("Sample Research Desk broken@address", "a name and a contact address with @"),
        (FIXTURE_USER_AGENT, "holds a placeholder"),
        ("Sample Research Desk someone@example.invalid", "holds a placeholder"),
        # A client with a network connection refuses every domain which is reserved for
        # examples: the SEC asks for a contact which a person reads.
        (SECRET, "holds a placeholder"),
        ("Sample Company Name contact@example.com", "holds a placeholder"),
        ("Sample Company Name <contact@sample.test>", "holds a placeholder"),
    ],
)
def test_a_live_client_refuses_to_start_without_a_declared_identity(
    value: str | None, words: str
) -> None:
    with pytest.raises(SecConfigurationError, match=words) as raised:
        SecClient(settings(sec_user_agent=value))

    if value and value.strip():
        for word in value.split():
            assert word not in str(raised.value) or word in (
                "a",
                "name",
                "Sample",
                "Company",
                "Name",
            )


def test_the_form_of_a_declared_identity() -> None:
    assert checked_user_agent(settings(sec_user_agent=REAL_LOOKING)) == REAL_LOOKING
    assert (
        checked_user_agent(settings(sec_user_agent="  Sample Name <admin@sample-name.com> "))
        == "Sample Name <admin@sample-name.com>"
    )
    # A test client has no network connection and may use a reserved example domain.
    assert checked_user_agent(settings(), network=False) == SECRET
    # The recorded files are read with the placeholder, whatever the setting holds.
    assert checked_user_agent(settings(sec_source="fixture")) == FIXTURE_USER_AGENT


def test_only_the_source_live_can_open_a_network_connection(
    open_client: Any, server: SecServer, tmp_path: Path
) -> None:
    with pytest.raises(SecConfigurationError, match="GBMI_SEC_SOURCE is off"):
        SecClient(settings(sec_source="off"))
    with pytest.raises(SecConfigurationError, match="GBMI_SEC_SOURCE is off"):
        SecClient(Settings(_env_file=None))  # off is the default
    # The source fixture reads the recorded files and never the network: a folder without
    # the files is refused when the client is built.
    with pytest.raises(SecConfigurationError, match="holds no recorded SEC files"):
        SecClient(settings(sec_source="fixture", sec_fixtures_dir=str(tmp_path)))
    # Whatever transport answers, a client of the source fixture sends the placeholder.
    with open_client(settings=settings(sec_source="fixture")) as client:
        client.get(META)
    assert server.arrivals[0].headers["user-agent"] == FIXTURE_USER_AGENT


def test_the_source_fixture_reads_the_recorded_files_through_the_same_client(
    ops_db: str, source: str, ops: Connection, clock: FakeTime, tmp_path: Path
) -> None:
    # make demo and CI: no transport is passed, and no user agent is set (week 3 plan 3.3).
    recorded = Settings(_env_file=None, sec_source="fixture", sec_fixtures_dir=str(SEC_FIXTURES))
    holiday = index_file_url(date(2026, 11, 26))
    target = tmp_path / "submissions.zip"

    with SecClient(
        recorded,
        dsn=ops_db,
        source=source,
        run_id="fixture-run",
        clock=clock.clock,
        sleep=clock.sleep,
        now=clock.now,
    ) as client:
        ticker = client.get(TICKER_FILE_URL, listed=True)
        with pytest.raises(ObjectMissingError) as no_facts:
            client.get(companyfacts_url(1470419))
        with pytest.raises(ObjectMissingError) as no_index:
            client.get(holiday)
        archive = client.download(SUBMISSIONS_ARCHIVE_URL, target)

    assert ticker.content == (SEC_FIXTURES / WWW_HOST / "files" / TICKER_FILE_NAME).read_bytes()
    assert ticker.last_modified == LAST_MODIFIED
    assert (no_facts.value.status, no_index.value.status) == (404, 403)
    assert b"<Code>NoSuchKey</Code>" in no_facts.value.body
    assert b"<Code>AccessDenied</Code>" in no_index.value.body
    assert (archive.status, archive.byte_count) == (200, target.stat().st_size)
    with zipfile.ZipFile(target) as bundle:
        assert len(bundle.namelist()) == 8
    # The limiter, the request log and the circuit work as for the live source.
    assert log_rows(ops, source) == [
        (1, "ok", 200, False),
        (1, "missing", 404, False),
        (1, "missing", 403, False),
        (1, "ok", 200, False),
    ]
    assert max_requests_in_window(ops, source=source) <= 8
    assert not read_circuit(ops, source).recorded


def test_a_client_with_a_network_connection_needs_the_source_sec_and_the_database_of_the_stack(
    ops_db: str,
) -> None:
    real = settings(sec_user_agent=REAL_LOOKING)

    # The lock, the circuit and the request log belong to the source "sec".
    with pytest.raises(SecConfigurationError, match="works on the source 'sec' only"):
        SecClient(real, dsn=ops_db, source="another-tool")
    # A throwaway database would forget a block by the SEC (review 1, finding B2). The check
    # comes before any transport is built: tests/conftest.py lets the construction fail.
    with pytest.raises(SecConfigurationError, match="a throwaway database would forget a block"):
        SecClient(real, dsn=ops_db).__enter__()


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


def test_a_slow_connection_cannot_pull_nine_requests_into_one_second(
    open_client: Any, server: SecServer, ops: Connection, source: str
) -> None:
    # Review 1, finding B1. The first request needs 120 ms until it leaves (the set-up of the
    # connection), every answer needs 50 ms. A limiter which counted the grants only would
    # let the ninth request arrive 0.93 s after the first one.
    server.setup = {0: 0.12}
    server.answer_time = 0.05

    with open_client() as client:
        for _ in range(24):
            client.get(META)

    times = [arrival.at for arrival in server.arrivals]
    assert server.max_in_window() <= 8
    for index in range(8, len(times)):
        assert times[index] - times[index - 8] >= WINDOW_SECONDS
    # The evidence in the request log says the same.
    assert max_requests_in_window(ops, source=source) <= 8
    assert max_requests_in_session_window(ops, source=source) <= 8


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
    assert log_rows(ops, source) == [
        (1, "blocked", 429, False),
        (2, "ok", 200, True),  # the one probe
        (1, "blocked", 429, False),
    ]
    assert read_circuit(ops, source).open_count == 2
    events = [event["event"] for event in circuit_events(ops, source)]
    assert events == ["opened", "closed", "opened"]


def test_a_refused_probe_locks_the_circuit_until_the_operator_closes(
    open_client: Any, server: SecServer, ops: Connection, source: str, clock: FakeTime
) -> None:
    server.answer_once(META, too_many(), forbidden())

    with open_client() as client, pytest.raises(SourceBlockedError, match="probe was refused"):
        client.get(META)

    assert read_circuit(ops, source).locked
    assert log_rows(ops, source) == [(1, "blocked", 429, False), (2, "blocked", 403, True)]
    clock.advance(7 * 86_400)
    with open_client() as client, pytest.raises(SourceBlockedError, match="locked"):
        client.get(META)
    assert server.asked(META) == 2  # the locked circuit let no request go
    assert close_circuit(ops, source, closed_by="jay", note="the block ended", now=clock.now())
    with open_client() as client:
        assert client.get(META).status == 200


def test_a_probe_which_ends_with_a_server_error_decides_nothing(
    open_client: Any, server: SecServer, ops: Connection, source: str
) -> None:
    server.answer_once(META, forbidden(), server_error(503))

    with open_client() as client:
        assert client.get(META).status == 200

    assert log_rows(ops, source) == [
        (1, "blocked", 403, False),
        (2, "retry", 503, True),
        (3, "ok", 200, True),
    ]
    circuit = read_circuit(ops, source)
    assert not circuit.recorded and not circuit.locked


def test_a_missing_object_as_the_answer_to_a_probe_decides_nothing(
    open_client: Any, server: SecServer, ops: Connection, source: str, clock: FakeTime
) -> None:
    # Review 1: "no such object" must not close the circuit. Only the object does.
    server.answer_once(TICKER_FILE_URL, too_many("3600"))

    with open_client() as client, pytest.raises(SourceBlockedError):
        client.get(TICKER_FILE_URL)  # a block of one hour: too long to wait
    assert read_circuit(ops, source).recorded
    clock.advance(3601)  # the block has ended: the next request is the probe

    with open_client() as client:
        with pytest.raises(ObjectMissingError):
            client.get(FACTS)  # no listing names the object: the answer is believed
        assert read_circuit(ops, source).recorded  # and the block is still recorded
        assert client.get(META).status == 200
        assert not read_circuit(ops, source).recorded
    assert [row[3] for row in log_rows(ops, source)][-2:] == [True, True]


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


@pytest.mark.parametrize("header", ["1e999", "99999999999999999999", "-5", "soon"])
def test_a_wrong_retry_after_cannot_stop_the_block_from_being_recorded(
    open_client: Any, server: SecServer, ops: Connection, source: str, clock: FakeTime, header: str
) -> None:
    server.answer_once(META, Answer(429, {"Retry-After": header}))

    with open_client() as client:
        assert client.get(META).status == 200  # a block of 15 minutes, one wait, one probe

    assert pytest.approx(900.0) in clock.sleeps
    assert read_circuit(ops, source).open_count == 1


def test_the_retry_after_header_is_read_as_seconds_or_as_a_date_and_nothing_else() -> None:
    now = FakeTime().now()

    def read(value: bytes) -> float | None:
        return client_module._retry_after(httpx.Headers([(b"Retry-After", value)]), now)

    assert read(b"120") == 120.0
    assert read(format_datetime(now + timedelta(minutes=5), usegmt=True).encode()) == 300.0
    assert read(format_datetime(now - timedelta(minutes=5), usegmt=True).encode()) == 0.0
    # A digit which is no ASCII digit, a number with eight digits, and words: no value.
    for wrong in ("²".encode("latin-1"), b"12345678", b"1e999", b"-5", b"soon", b""):
        assert read(wrong) is None
    assert client_module._retry_after(httpx.Headers(), now) is None


# --- missing objects --------------------------------------------------------------------------


def test_a_missing_object_is_no_block_and_is_not_tried_again(
    open_client: Any, server: SecServer, ops: Connection, source: str
) -> None:
    with open_client() as client:
        with pytest.raises(
            ObjectMissingError, match=r"HTTP 404 for data\.sec\.gov/api/xbrl"
        ) as facts:
            client.get(FACTS)
        with pytest.raises(
            ObjectMissingError, match=r"HTTP 403 for www\.sec\.gov/Archives"
        ) as index:
            client.get(INDEX)
        assert client.stats.missing == 2 and client.stats.blocks == 0
        assert client.get(META).status == 200

    # The error carries the status and the start of the body: the recording keeps both.
    assert (facts.value.status, index.value.status) == (404, 403)
    assert b"NoSuchKey" in facts.value.body and b"AccessDenied" in index.value.body
    assert server.asked(FACTS) == 1 and server.asked(INDEX) == 1
    assert not read_circuit(ops, source).recorded
    assert request_counts(ops, source=source) == {"missing": 2, "ok": 1}


def test_a_403_with_another_body_below_archives_is_a_block(
    open_client: Any, server: SecServer, ops: Connection, source: str
) -> None:
    server.answer_always(INDEX, forbidden())

    with open_client() as client, pytest.raises(SourceBlockedError, match="probe was refused"):
        client.get(INDEX)

    assert read_circuit(ops, source).locked


def test_three_missing_listed_objects_in_a_row_open_the_circuit_and_the_next_one_locks(
    open_client: Any, server: SecServer, ops: Connection, source: str, clock: FakeTime
) -> None:
    days = [date(2026, 11, day) for day in (20, 23, 24, 25)]

    with open_client() as client:
        for day in days[:2]:
            with pytest.raises(ObjectMissingError):
                client.get(index_file_url(day), listed=True)
        with pytest.raises(SourceBlockedError, match="a block can look like a missing file"):
            client.get(index_file_url(days[2]), listed=True)

    circuit = read_circuit(ops, source)
    assert circuit.recorded and not circuit.locked
    # The next run waits for the block and sends one probe. The probe asks for a listed object
    # and is answered "no such object" again: the circuit locks, and a person has to look.
    refused = "answered 'no such object' for a listed"
    with open_client() as client, pytest.raises(SourceBlockedError, match=refused):
        client.get(index_file_url(days[3]), listed=True)
    assert read_circuit(ops, source).locked
    assert pytest.approx(900.0, abs=1.1) in clock.sleeps
    assert len(server.arrivals) == 4
    assert [event["event"] for event in circuit_events(ops, source)] == ["locked", "opened"]


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
    assert log_rows(ops, source) == [(attempt, "retry", 500, False) for attempt in range(1, 6)]
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
    assert (response.content_length, response.byte_count) == (12, 12)
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


def test_an_object_which_is_read_into_memory_has_a_time_limit_and_a_size_limit(
    open_client: Any, server: SecServer, clock: FakeTime, monkeypatch: pytest.MonkeyPatch
) -> None:
    def slow(request: httpx.Request) -> httpx.Response:
        server.handler(request)
        # Each piece of the body takes two minutes: no single read is slow, the whole is.
        pieces = [b"x" * 100] * 10
        return httpx.Response(200, stream=SlowBody(pieces, lambda: clock.advance(120)))

    too_slow = pytest.raises(SecTransportError, match="after 5 tries: ReadTimeout")
    with open_client(transport=httpx.MockTransport(slow)) as client, too_slow:
        client.get(META, limit=300)
    assert server.asked(META) == 5

    monkeypatch.setattr(client_module, "MAX_BODY", 5)
    with open_client() as client, pytest.raises(SecTransportError, match="DecodingError"):
        client.get(META)  # a body of 12 bytes against a limit of 5


# --- large files ------------------------------------------------------------------------------


def test_a_download_streams_to_a_file_and_counts_as_one_request(
    open_client: Any, server: SecServer, ops: Connection, source: str, tmp_path: Path
) -> None:
    archive = bytes(range(256)) * 12_000  # about 3 MB
    server.put(SUBMISSIONS_ARCHIVE_URL, archive)
    target = tmp_path / "submissions.zip"

    with open_client() as client:
        response = client.download(SUBMISSIONS_ARCHIVE_URL, target)

    assert target.read_bytes() == archive
    assert (response.byte_count, response.content) == (len(archive), b"")
    # The answer of the download carries the size and the time of the archive.
    assert (response.content_length, response.last_modified) == (len(archive), MODIFIED)
    assert list(tmp_path.iterdir()) == [target]
    assert log_rows(ops, source) == [(1, "ok", 200, False)]
    assert [arrival.method for arrival in server.arrivals] == ["GET"]


def test_a_download_which_fails_leaves_no_file_and_is_tried_once(
    open_client: Any, server: SecServer, clock: FakeTime, tmp_path: Path
) -> None:
    target = tmp_path / "submissions.zip"

    def slow(request: httpx.Request) -> httpx.Response:
        server.handler(request)
        # Twenty pieces, ten minutes each: the limit of thirty minutes ends inside the body.
        pieces = [b"x" * 1000] * 20
        return httpx.Response(200, stream=SlowBody(pieces, lambda: clock.advance(600)))

    too_slow = pytest.raises(SecTransportError, match="after 1 tries: ReadTimeout")
    with open_client(transport=httpx.MockTransport(slow)) as client, too_slow:
        client.download(SUBMISSIONS_ARCHIVE_URL, target, limit=1800)
    assert list(tmp_path.iterdir()) == []  # the partial file is gone

    with open_client() as client:
        with pytest.raises(ObjectMissingError):
            client.download(SUBMISSIONS_ARCHIVE_URL, target)  # the fake server has no archive
        # Only the status 200 is the object: a part of a file is no download.
        server.answer_always(SUBMISSIONS_ARCHIVE_URL, Answer(206, {}, b"part"))
        with pytest.raises(SecResponseError, match="HTTP 206"):
            client.download(SUBMISSIONS_ARCHIVE_URL, target)

    assert list(tmp_path.iterdir()) == []
    assert server.asked(SUBMISSIONS_ARCHIVE_URL) == 3


# --- real time --------------------------------------------------------------------------------


def test_with_the_real_clock_no_second_holds_more_than_8_of_24_requests(
    ops_db: str, ops: Connection, source: str
) -> None:
    # The first request takes 150 ms until it leaves, as the set-up of a real connection does.
    server = SecServer(advance=time.sleep)
    server.put(META)
    server.setup = {0: 0.15}
    server.answer_time = 0.002

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


def test_two_threads_which_share_one_client_send_one_request_at_a_time(
    ops_db: str, ops: Connection, source: str
) -> None:
    server = SecServer(advance=time.sleep)
    server.put(META)
    server.answer_time = 0.01

    shared = SecClient(settings(), dsn=ops_db, source=source, transport=server.transport())
    with shared as client, ThreadPoolExecutor(max_workers=2) as pool:
        statuses = list(pool.map(lambda _: client.get(META).status, range(16)))

    assert statuses == [200] * 16
    assert server.max_in_window() <= 8
    assert max_requests_in_window(ops, source=source) <= 8
