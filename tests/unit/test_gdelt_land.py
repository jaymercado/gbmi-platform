"""Landing: gate G1, deterministic keys and the skip that makes a rerun a no-op."""

from __future__ import annotations

import io
import os
import signal
import subprocess
import sys
import zipfile
import zlib
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import tenacity

from gbmi.core.failpoint import failpoint
from gbmi.core.gdelt import Feed, Slot
from gbmi.sources.gdelt import land
from gbmi.sources.gdelt.index import IndexLine, IndexTail
from gbmi.sources.gdelt.land import (
    Bronze,
    Fetched,
    FileUnavailableError,
    FixtureFetcher,
    HttpFetcher,
    RateLimitedError,
    VerificationError,
    fetcher_for,
    land_file,
    md5_hex,
    verify,
)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "gdelt" / "v2"
SLOT = Slot.parse("20261125084500")
#: GDELT uploads a file about ten minutes before the time in the file name.
UPLOADED = datetime(2026, 11, 25, 8, 35, 12, tzinfo=UTC)


def zipped(text: str, member: str = "20261125084500.export.CSV") -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member, text)
    return buffer.getvalue()


def line_for(content: bytes, slot: Slot = SLOT) -> IndexLine:
    name = slot.file_name(Feed.EXPORT)
    return IndexLine(
        len(content), md5_hex(content), f"https://example.invalid/{name}", slot, Feed.EXPORT
    )


class CountingFetcher:
    """Serves prepared bytes and counts how often it is asked."""

    def __init__(self, files: dict[str, bytes], index: str = "") -> None:
        self.files = files
        self.index = index
        self.fetched: list[str] = []

    def last_update(self) -> str:
        return self.index

    def master_tail(self, tail_bytes: int) -> IndexTail:
        return IndexTail(self.index, complete=True)

    def fetch(self, line: IndexLine) -> Fetched:
        self.fetched.append(line.file_name)
        return Fetched(self.files[line.file_name], UPLOADED)


def test_verify_accepts_a_matching_zip() -> None:
    content = zipped("1\t2\n")

    verify(content, line_for(content))


def test_verify_rejects_wrong_size_md5_and_broken_zip() -> None:
    content = zipped("1\t2\n")
    good = line_for(content)

    with pytest.raises(VerificationError, match="size"):
        verify(content + b"x", good)
    flipped = bytes([content[0] ^ 0xFF]) + content[1:]
    with pytest.raises(VerificationError, match="MD5"):
        verify(flipped, good)
    not_a_zip = b"this is not a zip archive"
    with pytest.raises(VerificationError, match="not a readable zip"):
        verify(not_a_zip, line_for(not_a_zip))


def test_verify_rejects_a_zip_whose_compressed_stream_is_broken() -> None:
    # The source itself publishes a damaged file: the size and the MD5 equal the list, the
    # directory of the zip is intact, and the compressed bytes cannot be read.
    rows = "".join(f"{number}\t{'x' * (number % 53)}\n" for number in range(20_000))
    content = bytearray(zipped(rows))
    start = 30 + len("20261125084500.export.CSV") + 200  # inside the compressed stream
    for offset in range(start, start + 8):
        content[offset] ^= 0xFF
    broken = bytes(content)
    with zipfile.ZipFile(io.BytesIO(broken)) as archive, pytest.raises(zlib.error):
        archive.testzip()

    with pytest.raises(VerificationError, match=r"not a readable zip \(error: ") as raised:
        verify(broken, line_for(broken))

    # The bytes go with the error, for the copy in the quarantine area.
    assert raised.value.content == broken


def test_landing_writes_the_bytes_unchanged_under_the_deterministic_key(tmp_path: Path) -> None:
    content = zipped("row\n")
    line = line_for(content)
    fetcher = CountingFetcher({line.file_name: content})

    landed = land_file(Bronze(f"file://{tmp_path}"), fetcher, line)

    assert landed.written is True
    assert landed.key == "raw/gdelt/v2/export/dt=2026-11-25/20261125084500.export.CSV.zip"
    assert (tmp_path / landed.key).read_bytes() == content
    assert not list(tmp_path.rglob("*.part"))


def test_rerun_skips_a_file_that_is_already_there(tmp_path: Path) -> None:
    content = zipped("row\n")
    line = line_for(content)
    fetcher = CountingFetcher({line.file_name: content})
    bronze = Bronze(f"file://{tmp_path}")

    first = land_file(bronze, fetcher, line)
    second = land_file(bronze, fetcher, line)

    assert (first.written, second.written) == (True, False)
    assert fetcher.fetched == [line.file_name]


def test_a_damaged_object_in_bronze_is_fetched_again(tmp_path: Path) -> None:
    content = zipped("row\n")
    line = line_for(content)
    bronze = Bronze(f"file://{tmp_path}")
    bronze.write(SLOT.bronze_key(Feed.EXPORT), b"truncated")
    fetcher = CountingFetcher({line.file_name: content})

    landed = land_file(bronze, fetcher, line)

    assert landed.written is True
    assert bronze.read(landed.key) == content


def test_a_file_that_fails_the_gate_is_not_promoted(tmp_path: Path) -> None:
    content = zipped("row\n")
    line = line_for(content)
    fetcher = CountingFetcher({line.file_name: content[:-3]})

    with pytest.raises(VerificationError):
        land_file(Bronze(f"file://{tmp_path}"), fetcher, line)

    assert not list(tmp_path.rglob("*.zip"))


def no_waiting(monkeypatch: pytest.MonkeyPatch) -> None:
    """No waiting between attempts in a unit test."""
    for method in (HttpFetcher._get, HttpFetcher.last_update, HttpFetcher.master_tail):
        monkeypatch.setattr(method.retry, "wait", tenacity.wait_none())  # type: ignore[attr-defined]


def test_http_fetcher_retries_a_server_error_and_returns_the_upload_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    content = zipped("row\n")
    line = line_for(content)
    answers = [503, 502, 200]

    def handler(request: httpx.Request) -> httpx.Response:
        status = answers.pop(0)
        if status != 200:
            return httpx.Response(status)
        headers = {"Last-Modified": "Wed, 25 Nov 2026 08:35:12 GMT"}
        return httpx.Response(200, content=content, headers=headers)

    no_waiting(monkeypatch)
    fetcher = HttpFetcher(httpx.Client(transport=httpx.MockTransport(handler)))

    assert fetcher.fetch(line) == Fetched(content, UPLOADED)
    assert answers == []


def test_http_fetcher_reads_both_lists_and_knows_a_cut_tail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("lastupdate.txt"):
            return httpx.Response(429) if len(requests) == 1 else httpx.Response(200, text="new")
        asked = request.headers["Range"]
        if asked == "bytes=-1000":
            return httpx.Response(
                206, text="tail", headers={"Content-Range": "bytes 500-1499/1500"}
            )
        if asked == "bytes=-2000":
            return httpx.Response(206, text="all", headers={"Content-Range": "bytes 0-1499/1500"})
        return httpx.Response(200, text="all")  # a server which ignores the range

    no_waiting(monkeypatch)
    fetcher = HttpFetcher(httpx.Client(transport=httpx.MockTransport(handler)))

    assert fetcher.last_update() == "new"  # a list request waits out HTTP 429
    assert fetcher.master_tail(1000) == IndexTail("tail", complete=False)
    assert fetcher.master_tail(2000) == IndexTail("all", complete=True)
    assert fetcher.master_tail(3000) == IndexTail("all", complete=True)


@pytest.mark.parametrize(
    ("status", "headers", "error", "retry_after"),
    [
        (404, {}, FileUnavailableError, None),
        (429, {}, RateLimitedError, None),
        (429, {"Retry-After": "7"}, RateLimitedError, 7.0),
        (429, {"Retry-After": "soon"}, RateLimitedError, None),
        (403, {}, httpx.HTTPStatusError, None),
    ],
)
def test_http_fetcher_does_not_retry_a_refused_file(
    monkeypatch: pytest.MonkeyPatch,
    status: int,
    headers: dict[str, str],
    error: type[Exception],
    retry_after: float | None,
) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(status, headers=headers)

    no_waiting(monkeypatch)
    fetcher = HttpFetcher(httpx.Client(transport=httpx.MockTransport(handler)))

    with pytest.raises(error) as raised:
        fetcher.fetch(line_for(zipped("x")))

    # The run decides what follows a 404 or a 429; the fetcher asks once.
    assert calls == 1
    if error is RateLimitedError:
        assert raised.value.retry_after == retry_after  # type: ignore[attr-defined]


def test_retry_after_can_be_a_date(monkeypatch: pytest.MonkeyPatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"Retry-After": "Wed, 25 Nov 2026 08:00:30 GMT"})

    monkeypatch.setattr(land, "utc_now", lambda: datetime(2026, 11, 25, 8, 0, 0, tzinfo=UTC))
    fetcher = HttpFetcher(httpx.Client(transport=httpx.MockTransport(handler)))

    with pytest.raises(RateLimitedError) as raised:
        fetcher.fetch(line_for(zipped("x")))

    assert raised.value.retry_after == 30.0


def test_a_missing_or_odd_upload_time_is_none() -> None:
    content = zipped("row\n")
    line = line_for(content)

    for headers in ({}, {"Last-Modified": "not a date"}):
        transport = httpx.MockTransport(
            lambda request, headers=headers: httpx.Response(200, content=content, headers=headers)
        )
        assert HttpFetcher(httpx.Client(transport=transport)).fetch(line).published_at is None


def test_the_landed_file_carries_the_upload_time_only_when_fetched(tmp_path: Path) -> None:
    content = zipped("row\n")
    line = line_for(content)
    fetcher = CountingFetcher({line.file_name: content})
    bronze = Bronze(f"file://{tmp_path}")

    assert land_file(bronze, fetcher, line).published_at == UPLOADED
    assert land_file(bronze, fetcher, line).published_at is None  # nothing was fetched


def test_a_rejected_file_hands_its_bytes_back(tmp_path: Path) -> None:
    content = zipped("row\n")
    line = line_for(content)
    damaged = content[:-3]

    with pytest.raises(VerificationError) as raised:
        land_file(Bronze(f"file://{tmp_path}"), CountingFetcher({line.file_name: damaged}), line)

    assert raised.value.content == damaged


def test_bronze_lists_finished_objects_and_gives_their_uri(tmp_path: Path) -> None:
    bronze = Bronze(f"file://{tmp_path}/")
    key = SLOT.bronze_key(Feed.EXPORT)
    bronze.write(key, b"12345")
    (tmp_path / "raw/gdelt/v2/export/dt=2026-11-25/half.zip.part").write_bytes(b"1")

    assert bronze.sizes("raw/gdelt/v2/export/dt=2026-11-25") == {key: 5}
    assert bronze.sizes("raw/gdelt/v2/") == {key: 5}
    assert bronze.sizes("raw/gdelt/v2/gkg") == {}
    assert bronze.uri(key) == f"file://{tmp_path}/{key}"


def test_the_fixture_fetcher_serves_both_lists_and_knows_a_missing_file() -> None:
    fetcher = FixtureFetcher(FIXTURES)

    newest = fetcher.last_update().splitlines()
    assert [line.rsplit("/", 1)[-1].split(".")[1] for line in newest] == [
        "export",
        "mentions",
        "gkg",
    ]
    assert all("/20261125084500." in line for line in newest)
    tail = fetcher.master_tail(10)
    assert tail.complete and len(tail.text.splitlines()) == 12
    missing = line_for(zipped("x"), Slot.parse("20261125090000"))
    with pytest.raises(FileUnavailableError, match="not among the recorded files"):
        fetcher.fetch(missing)


def test_a_source_is_live_or_fixture() -> None:
    assert isinstance(fetcher_for("live"), HttpFetcher)
    assert isinstance(fetcher_for("fixture", str(FIXTURES)), FixtureFetcher)
    with pytest.raises(ValueError, match="'fixture' or 'live'"):
        fetcher_for("tape")


def test_a_crash_before_the_move_leaves_no_object_and_a_rerun_lands_the_file(
    tmp_path: Path,
) -> None:
    # The process kills itself between the write of the temporary file and the move.
    program = (
        "import sys; from gbmi.sources.gdelt.land import Bronze; "
        "Bronze(sys.argv[1]).write(sys.argv[2], b'12345')"
    )
    key = SLOT.bronze_key(Feed.EXPORT)
    crashed = subprocess.run(
        [sys.executable, "-c", program, f"file://{tmp_path}", key],
        env={**os.environ, "GBMI_FAILPOINT": "bronze.before_move"},
        capture_output=True,
        check=False,
    )

    assert crashed.returncode == -signal.SIGKILL
    bronze = Bronze(f"file://{tmp_path}")
    assert bronze.read(key) is None  # a reader never sees half a file
    assert [path.name for path in tmp_path.rglob("*.part")] == [
        f"{SLOT.file_name(Feed.EXPORT)}.part"
    ]

    content = zipped("row\n")
    line = line_for(content)
    landed = land_file(bronze, CountingFetcher({line.file_name: content}), line)

    assert landed.written and bronze.read(key) == content
    assert not list(tmp_path.rglob("*.part"))


def test_an_unknown_failpoint_is_a_programming_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GBMI_FAILPOINT", "nowhere")
    failpoint("bronze.before_move")  # another point is named: nothing happens
    with pytest.raises(ValueError, match="unknown failpoint"):
        failpoint("nowhere")
