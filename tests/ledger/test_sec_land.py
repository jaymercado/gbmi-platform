"""Landing one SEC object: the edges which the pipelines on the recorded files do not reach.

The client is the real client on the fake SEC server; the ledger and bronze are real.
"""

from __future__ import annotations

import json
import random
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from gbmi.config import Settings
from gbmi.ledger.db import Connection
from gbmi.ledger.object_states import ObjectState
from gbmi.ledger.objects import Dataset, Expected, ObjectKey, ObjectLedger
from gbmi.sources.gdelt.land import Bronze
from gbmi.sources.sec.client import SecClient
from gbmi.sources.sec.errors import SecResponseError
from gbmi.sources.sec.land import (
    BronzeError,
    GateError,
    Lander,
    bronze_key,
    index_opens,
    json_opens,
    md5_hex,
)
from gbmi.sources.sec.urls import cik_file_name, submissions_url

from .sec_server import Answer, FakeTime, SecServer, server_error

AS_OF = datetime(2026, 11, 26, 12, 0, tzinfo=UTC)
DAY = date(2026, 11, 26)
CIK = 1326801
URL = submissions_url(CIK)
KEY = ObjectKey(Dataset.SUBMISSIONS, DAY, cik_file_name(CIK))
EXPECTED = Expected(KEY, URL, cik=CIK)
BODY = json.dumps({"cik": "0001326801", "name": "Meta Platforms, Inc."}).encode()


@pytest.fixture
def clock() -> FakeTime:
    return FakeTime()


@pytest.fixture
def server(clock: FakeTime) -> SecServer:
    fake = SecServer(clock.clock, advance=clock.advance)
    fake.put(URL, BODY)
    return fake


@pytest.fixture
def lander(
    ops_db: str, ops: Connection, source: str, server: SecServer, clock: FakeTime, tmp_path: Path
) -> Iterator[Lander]:
    settings = Settings(
        _env_file=None, sec_source="live", sec_user_agent="Example Desk contact@example.org"
    )
    with SecClient(
        settings,
        dsn=ops_db,
        source=source,
        run_id="land-run",
        transport=server.transport(),
        clock=clock.clock,
        sleep=clock.sleep,
        now=clock.now,
        rng=random.Random(7),  # noqa: S311 - the jitter of a pause
    ) as client:
        yield Lander(
            client=client,
            ledger=ObjectLedger(ops, source=source, run_id="land-run"),
            bronze=Bronze(f"file://{tmp_path}"),
            lake_uri=f"file://{tmp_path}",
            as_of=AS_OF,
        )


def test_the_bronze_key_of_an_object() -> None:
    assert bronze_key(KEY) == "raw/sec/submissions/dt=2026-11-26/CIK0001326801.json"


def test_an_object_is_fetched_once_and_then_read_from_bronze(
    lander: Lander, server: SecServer, tmp_path: Path
) -> None:
    first = lander.land(EXPECTED, check=json_opens, listed=True)
    second = lander.land(EXPECTED, check=json_opens, listed=True)

    assert first.state is ObjectState.VERIFIED and second == first
    assert server.asked(URL) == 1
    assert (first.byte_count, first.md5, first.http_status) == (len(BODY), md5_hex(BODY), 200)
    assert first.bronze_uri == f"file://{tmp_path}/{bronze_key(KEY)}"
    assert lander.read(first) == BODY
    assert lander.counts.as_dict() == {
        "requests": 1,
        "bytes": len(BODY),
        "verified": 1,
        "from_bronze": 0,
        "not_published": 0,
        "failed_tries": 0,
        "quarantined": 0,
    }


def test_bytes_which_a_run_before_left_in_bronze_need_no_request(
    lander: Lander, server: SecServer
) -> None:
    # A run stored the bytes and stopped before the ledger knew.
    lander.bronze.write(bronze_key(KEY), BODY)

    record = lander.land(EXPECTED, check=json_opens, listed=True)

    assert (record.state, record.http_status, record.md5) == (
        ObjectState.VERIFIED,
        None,
        md5_hex(BODY),
    )
    assert server.asked(URL) == 0
    assert lander.counts.from_bronze == 1


def test_recover_never_sends_a_request(lander: Lander, server: SecServer) -> None:
    # An operator sent a quarantined object back: the bytes are in bronze (review 2, R2-2).
    lander.ledger.ensure_expected([EXPECTED])
    without_bytes = lander.recover(KEY, check=json_opens)
    lander.bronze.write(bronze_key(KEY), b"half a docu")
    broken_bytes = lander.recover(KEY, check=json_opens)
    lander.bronze.write(bronze_key(KEY), BODY)
    with_bytes = lander.recover(KEY, check=json_opens)

    assert without_bytes.state is ObjectState.EXPECTED
    assert broken_bytes.state is ObjectState.EXPECTED
    assert (with_bytes.state, with_bytes.md5) == (ObjectState.VERIFIED, md5_hex(BODY))
    assert lander.recover(KEY, check=json_opens) == with_bytes
    assert server.asked(URL) == 0


def test_broken_bytes_in_bronze_are_fetched_again(lander: Lander, server: SecServer) -> None:
    lander.bronze.write(bronze_key(KEY), b"half a docu")

    record = lander.land(EXPECTED, check=json_opens, listed=True)

    assert record.state is ObjectState.VERIFIED and server.asked(URL) == 1
    assert lander.read(record) == BODY


def test_a_failed_try_which_is_followed_by_good_bytes_ends_verified(
    lander: Lander, server: SecServer
) -> None:
    server.answer_once(URL, Answer(200, body=b"[1, 2, 3]"), Answer(200, body=b"{ broken"))

    record = lander.land(EXPECTED, check=json_opens, listed=True)

    assert (record.state, record.attempts) == (ObjectState.VERIFIED, 2)
    assert server.asked(URL) == 3
    assert lander.counts.failed_tries == 2 and lander.counts.quarantined == 0


def test_an_unusable_answer_ends_the_run_and_the_object_stays_expected(
    lander: Lander, server: SecServer
) -> None:
    server.answer_always(URL, Answer(418))

    with pytest.raises(SecResponseError, match="HTTP 418"):
        lander.land(EXPECTED, check=json_opens, listed=True)

    record = lander.ledger.get(KEY)
    assert record is not None and record.state is ObjectState.EXPECTED
    # The next run asks again and lands the object.
    server.answer_always(URL, Answer(200, body=BODY))
    server.answer_once(URL, server_error())
    assert lander.land(EXPECTED, check=json_opens, listed=True).state is ObjectState.VERIFIED


def test_bronze_has_to_hold_the_bytes_which_the_ledger_records(
    lander: Lander, tmp_path: Path
) -> None:
    record = lander.land(EXPECTED, check=json_opens, listed=True)
    stored = tmp_path / bronze_key(KEY)

    stored.write_bytes(BODY + b" ")
    with pytest.raises(BronzeError, match="differ from the ledger"):
        lander.read(record)
    stored.unlink()
    with pytest.raises(BronzeError, match="bronze does not hold submissions/2026-11-26"):
        lander.read(record)


def test_a_member_of_an_archive_is_stored_without_a_request(
    lander: Lander, server: SecServer
) -> None:
    stored = lander.store(EXPECTED, BODY, check=json_opens, detail={"archive": "submissions.zip"})
    again = lander.store(EXPECTED, b"other bytes", check=json_opens, detail={})

    assert stored is not None and again == stored
    assert (stored.state, stored.http_status, stored.detail) == (
        ObjectState.VERIFIED,
        None,
        {"archive": "submissions.zip"},
    )
    assert server.asked(URL) == 0 and lander.read(stored) == BODY
    # A member which does not open is no object of the day.
    other = Expected(
        ObjectKey(Dataset.SUBMISSIONS, DAY, cik_file_name(7)), submissions_url(7), cik=7
    )
    assert lander.store(other, b"<html/>", check=json_opens, detail={}) is None
    left = lander.ledger.get(other.key)
    assert left is not None and (left.state, left.attempts) == (ObjectState.EXPECTED, 1)


def test_the_two_checks_of_gate_g1() -> None:
    json_opens(b'{"a": 1}')
    for content in (b"", b"{", b"[1]", b'"text"', b"\xff\xfe"):
        with pytest.raises(GateError):
            json_opens(content)
    header = b"Form Type   Company Name\n" + b"-" * 40 + b"\n"
    row = b"4" + b" " * 16 + b"A Company".ljust(62) + b"7".ljust(12) + b"20261125".ljust(12)
    index_opens(header + row + b"edgar/data/7/0000000007-26-000001.txt\n")
    with pytest.raises(GateError, match="no dashed line"):
        index_opens(b"<html>Access Denied</html>")
    with pytest.raises(GateError, match="no row after its header"):
        index_opens(header)
