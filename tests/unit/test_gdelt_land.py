"""Landing: gate G1, deterministic keys and the skip that makes a rerun a no-op."""

from __future__ import annotations

import io
import zipfile
from datetime import date
from pathlib import Path

import httpx
import pytest
import tenacity

from gbmi.core.gdelt import Feed, Slot
from gbmi.sources.gdelt.index import IndexLine
from gbmi.sources.gdelt.land import (
    Bronze,
    FixtureFetcher,
    HttpFetcher,
    VerificationError,
    land_file,
    land_hour,
    md5_hex,
    verify,
)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "gdelt" / "v2"
SLOT = Slot.parse("20261125084500")


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

    def index_text(self) -> str:
        return self.index

    def fetch(self, line: IndexLine) -> bytes:
        self.fetched.append(line.file_name)
        return self.files[line.file_name]


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


def test_fixture_hour_lands_four_verified_files(tmp_path: Path) -> None:
    bronze = Bronze(f"file://{tmp_path}")

    lines, landed = land_hour(bronze, FixtureFetcher(FIXTURES), date(2026, 11, 25), 8)

    assert [line.slot.stamp for line in lines] == [
        "20261125080000", "20261125081500", "20261125083000", "20261125084500",
    ]  # fmt: skip
    assert all(line.url.startswith("https://") for line in lines)
    assert [file.size for file in landed] == [81069, 47061, 58856, 76194]
    assert all(file.written for file in landed)
    # A second run changes nothing.
    _, again = land_hour(bronze, FixtureFetcher(FIXTURES), date(2026, 11, 25), 8)
    assert not any(file.written for file in again)
    # An hour that the source does not list lands nothing.
    assert land_hour(bronze, FixtureFetcher(FIXTURES), date(2026, 11, 25), 9) == ([], [])


def test_http_fetcher_retries_transient_errors_and_reads_the_list_tail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    content = zipped("row\n")
    line = line_for(content)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("masterfilelist.txt"):
            return httpx.Response(206, text=line.render())
        # The first two answers for the file are transient failures.
        failures = {1: 503, 2: 429}
        attempt = sum(1 for seen in requests if seen.url == request.url)
        if attempt in failures:
            return httpx.Response(failures[attempt])
        return httpx.Response(200, content=content)

    # No waiting between attempts in a unit test.
    monkeypatch.setattr(HttpFetcher.fetch.retry, "wait", tenacity.wait_none())  # type: ignore[attr-defined]
    fetcher = HttpFetcher(httpx.Client(transport=httpx.MockTransport(handler)), tail_bytes=1000)

    assert fetcher.index_text() == line.render()
    assert requests[0].headers["Range"] == "bytes=-1000"
    assert fetcher.fetch(line) == content
    assert [request.url.path for request in requests].count(f"/{line.file_name}") == 3


def test_http_fetcher_does_not_retry_a_missing_file(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(404)

    monkeypatch.setattr(HttpFetcher.fetch.retry, "wait", tenacity.wait_none())  # type: ignore[attr-defined]
    fetcher = HttpFetcher(httpx.Client(transport=httpx.MockTransport(handler)))

    with pytest.raises(httpx.HTTPStatusError):
        fetcher.fetch(line_for(zipped("x")))
    assert calls == 1
