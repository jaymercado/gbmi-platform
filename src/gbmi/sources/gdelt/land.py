"""Land GDELT files in bronze: download, verify, store unchanged (blueprint 8.3).

The contract of the hop (blueprint 5.6): one file per slot and feed, a deterministic object
key, and a skip when the object already exists with the expected MD5 - so a rerun changes
nothing. A file is promoted to bronze only after gate G1: its size and MD5 equal the index
values and the zip opens.

Two fetchers implement the same interface: ``HttpFetcher`` reads the live source, and
``FixtureFetcher`` reads recorded files from a directory, which is what ``make demo`` and CI use.
"""

from __future__ import annotations

import hashlib
import io
import zipfile
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any, Protocol

import httpx
import pyarrow.fs as pafs
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential_jitter

from gbmi.config import Settings, get_settings
from gbmi.core.gdelt import Feed, parse_hour
from gbmi.logging import get_logger
from gbmi.sources.gdelt.index import MASTER_LIST_URL, IndexLine, lines_for_hour, parse_index

log = get_logger(__name__)

#: How much of the end of the master list covers recent slots (about 330 bytes per slot).
MASTER_LIST_TAIL_BYTES = 2_000_000
FIXTURE_INDEX = "masterfilelist.txt"


class VerificationError(Exception):
    """A file failed gate G1: wrong size, wrong MD5, or not a readable zip."""


class Fetcher(Protocol):
    """Where index lines and file bytes come from."""

    def index_text(self) -> str:
        """Text of a file list that covers the wanted slots."""
        ...

    def fetch(self, line: IndexLine) -> bytes:
        """The bytes of one listed file."""
        ...


def _is_transient(error: BaseException) -> bool:
    """Network errors, HTTP 429 and 5xx are worth another try (blueprint 8.1)."""
    if isinstance(error, httpx.HTTPStatusError):
        return error.response.status_code == 429 or error.response.status_code >= 500
    return isinstance(error, httpx.TransportError)


_retry = retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=1, max=60),
    reraise=True,
)


class HttpFetcher:
    """The live GDELT source over HTTPS, with a bounded retry."""

    def __init__(
        self, client: httpx.Client | None = None, *, tail_bytes: int = MASTER_LIST_TAIL_BYTES
    ) -> None:
        self._client = client or httpx.Client(timeout=60, follow_redirects=True)
        self._tail_bytes = tail_bytes

    @_retry
    def index_text(self) -> str:
        # The master list is about 130 MB; the server honours range requests, so only its
        # tail is read.
        response = self._client.get(
            MASTER_LIST_URL, headers={"Range": f"bytes=-{self._tail_bytes}"}
        )
        response.raise_for_status()
        return response.text

    @_retry
    def fetch(self, line: IndexLine) -> bytes:
        response = self._client.get(line.url)
        response.raise_for_status()
        return response.content


class FixtureFetcher:
    """Recorded files: ``<root>/masterfilelist.txt`` and ``<root>/<feed>/<file name>``."""

    def __init__(self, root: Path) -> None:
        self._root = root

    def index_text(self) -> str:
        return (self._root / FIXTURE_INDEX).read_text(encoding="utf-8")

    def fetch(self, line: IndexLine) -> bytes:
        return (self._root / line.feed.value / line.file_name).read_bytes()


def md5_hex(content: bytes) -> str:
    # MD5 is the checksum GDELT publishes; it is used for integrity, not for security.
    return hashlib.md5(content, usedforsecurity=False).hexdigest()


def verify(content: bytes, line: IndexLine) -> None:
    """Gate G1: size and MD5 equal the index values, and the zip opens with one member."""
    if len(content) != line.size:
        raise VerificationError(
            f"{line.file_name}: size {len(content)} differs from the index ({line.size})"
        )
    digest = md5_hex(content)
    if digest != line.md5:
        raise VerificationError(
            f"{line.file_name}: MD5 {digest} differs from the index ({line.md5})"
        )
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            members = archive.namelist()
            damaged = archive.testzip()
    except zipfile.BadZipFile as error:
        raise VerificationError(f"{line.file_name}: not a readable zip ({error})") from error
    if damaged is not None or len(members) != 1:
        raise VerificationError(f"{line.file_name}: expected one intact member, found {members}")


@dataclass(frozen=True)
class LandedFile:
    """One file in bronze, with the facts the index gave about it."""

    key: str
    size: int
    md5: str
    slot: str
    feed: str
    #: False when the object was already there with the expected MD5 and nothing was written.
    written: bool


class Bronze:
    """The ``raw/`` area of the lake, on whichever filesystem the lake URI names."""

    def __init__(self, lake_uri: str) -> None:
        self._filesystem, self._root = pafs.FileSystem.from_uri(lake_uri)

    def _path(self, key: str) -> str:
        return f"{self._root.rstrip('/')}/{key}"

    def read(self, key: str) -> bytes | None:
        """The object's bytes, or None when it does not exist."""
        path = self._path(key)
        if self._filesystem.get_file_info(path).type == pafs.FileType.NotFound:
            return None
        with self._filesystem.open_input_stream(path) as stream:
            content: bytes = stream.read()
        return content

    def write(self, key: str, content: bytes) -> None:
        """Write under a temporary name, then move: a reader never sees half a file."""
        path = self._path(key)
        self._filesystem.create_dir(path.rsplit("/", 1)[0], recursive=True)
        temporary = f"{path}.part"
        with self._filesystem.open_output_stream(temporary) as stream:
            stream.write(content)
        self._filesystem.move(temporary, path)


def land_file(bronze: Bronze, fetcher: Fetcher, line: IndexLine) -> LandedFile:
    """Land one listed file, unless bronze already holds it with the expected MD5."""
    key = line.slot.bronze_key(line.feed)
    existing = bronze.read(key)
    if existing is not None and len(existing) == line.size and md5_hex(existing) == line.md5:
        log.info("slot_already_landed", key=key)
        written = False
    else:
        content = fetcher.fetch(line)
        verify(content, line)
        bronze.write(key, content)
        log.info("slot_landed", key=key, bytes=line.size)
        written = True
    return LandedFile(
        key=key,
        size=line.size,
        md5=line.md5,
        slot=line.slot.stamp,
        feed=line.feed.value,
        written=written,
    )


def land_hour(
    bronze: Bronze, fetcher: Fetcher, slot_date: date, slot_hour: int, feed: Feed = Feed.EXPORT
) -> tuple[list[IndexLine], list[LandedFile]]:
    """Land every listed file of one feed and one UTC hour.

    Returns the index lines found and the files now in bronze. A slot that the source does not
    list is absent from both; deciding what a missing slot means is the ledger's job (phase P1).
    """
    lines = lines_for_hour(parse_index(fetcher.index_text()), slot_date, slot_hour, feed)
    return lines, [land_file(bronze, fetcher, line) for line in lines]


def land_export_hour(
    hour: str,
    source: str = "fixture",
    *,
    fixtures_dir: str = "/fixtures/gdelt/v2",
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Task entry point: land the export files of one hour, ``YYYY-MM-DDTHH``.

    ``source`` is ``live`` for the GDELT servers or ``fixture`` for the recorded hour. Returns a
    summary of built-in types.
    """
    settings = settings or get_settings()
    slot_date, slot_hour = parse_hour(hour)
    fetcher: Fetcher
    if source == "live":
        fetcher = HttpFetcher()
    elif source == "fixture":
        fetcher = FixtureFetcher(Path(fixtures_dir))
    else:
        raise ValueError(f"source must be 'fixture' or 'live', got {source!r}")
    lines, landed = land_hour(Bronze(settings.lake_uri), fetcher, slot_date, slot_hour, Feed.EXPORT)
    if not landed:
        raise FileNotFoundError(f"the {source} source lists no export file for {hour}")
    return {
        "hour": hour,
        "source": source,
        "listed": len(lines),
        "written": sum(file.written for file in landed),
        "bytes": sum(file.size for file in landed),
        "files": [asdict(file) for file in landed],
    }
