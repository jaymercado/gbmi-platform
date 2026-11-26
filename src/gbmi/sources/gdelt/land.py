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
from datetime import UTC, date, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Protocol

import httpx
import pyarrow.fs as pafs
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential_jitter

from gbmi.config import Settings, get_settings
from gbmi.core.clock import utc_now
from gbmi.core.failpoint import BRONZE_BEFORE_MOVE, failpoint
from gbmi.core.gdelt import Feed, Slot, parse_hour
from gbmi.logging import get_logger
from gbmi.sources.gdelt.index import (
    LAST_UPDATE_URL,
    MASTER_LIST_URL,
    IndexLine,
    IndexSource,
    IndexTail,
    lines_for_hour,
    read_master_list,
)

log = get_logger(__name__)

FIXTURE_INDEX = "masterfilelist.txt"
FIXTURE_LAST_UPDATE = "lastupdate.txt"


class VerificationError(Exception):
    """A file failed gate G1: wrong size, wrong MD5, or not a readable zip.

    ``content`` holds the bytes which failed, so the caller can put them in quarantine.
    """

    def __init__(self, message: str, content: bytes = b"") -> None:
        super().__init__(message)
        self.content = content


class FileUnavailableError(Exception):
    """The source lists the file and does not serve it (HTTP 404)."""


class RateLimitedError(Exception):
    """The source asks for fewer requests (HTTP 429)."""

    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        #: Seconds to wait, from the header Retry-After; None when the header is missing.
        self.retry_after = retry_after


@dataclass(frozen=True)
class Fetched:
    """The bytes of one file, and when the source uploaded the file."""

    content: bytes
    #: From the HTTP header Last-Modified; None when the source gives no time.
    published_at: datetime | None = None


class Fetcher(IndexSource, Protocol):
    """Where index lines and file bytes come from."""

    def fetch(self, line: IndexLine) -> Fetched:
        """The bytes of one listed file.

        Raises FileUnavailableError for a file which the source does not serve, and
        RateLimitedError when the source asks for fewer requests.
        """
        ...


def _is_transient(error: BaseException) -> bool:
    """Network errors and HTTP 5xx are worth another try at once (blueprint 8.1)."""
    if isinstance(error, httpx.HTTPStatusError):
        return error.response.status_code >= 500
    return isinstance(error, httpx.TransportError)


def _is_transient_or_limited(error: BaseException) -> bool:
    if isinstance(error, httpx.HTTPStatusError) and error.response.status_code == 429:
        return True
    return _is_transient(error)


_retry_file = retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=1, max=60),
    reraise=True,
)
# The two lists are a few requests in a run, so HTTP 429 is waited out here. For a file the run
# decides: after the first 429 the run goes on with one download at a time (week 2 plan 3.3).
_retry_list = retry(
    retry=retry_if_exception(_is_transient_or_limited),
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=1, max=60),
    reraise=True,
)


def _last_modified(response: httpx.Response) -> datetime | None:
    header = response.headers.get("Last-Modified")
    if not header:
        return None
    try:
        return parsedate_to_datetime(header).astimezone(UTC)
    except (TypeError, ValueError):
        return None


def _retry_after(response: httpx.Response) -> float | None:
    """The header Retry-After in seconds; the header holds a number or an HTTP date."""
    header = response.headers.get("Retry-After")
    if not header:
        return None
    if header.strip().isdigit():
        return float(header.strip())
    try:
        return max(0.0, (parsedate_to_datetime(header) - utc_now()).total_seconds())
    except (TypeError, ValueError):
        return None


class HttpFetcher:
    """The live GDELT source over HTTPS, with a bounded retry. One instance serves many threads."""

    def __init__(self, client: httpx.Client | None = None) -> None:
        self._client = client or httpx.Client(timeout=60, follow_redirects=True)

    @_retry_list
    def last_update(self) -> str:
        response = self._client.get(LAST_UPDATE_URL)
        response.raise_for_status()
        return response.text

    @_retry_list
    def master_tail(self, tail_bytes: int) -> IndexTail:
        # The master list is about 130 MB; the server honours range requests.
        response = self._client.get(MASTER_LIST_URL, headers={"Range": f"bytes=-{tail_bytes}"})
        response.raise_for_status()
        whole = response.status_code == 200 or response.headers.get("Content-Range", "").startswith(
            "bytes 0-"
        )
        return IndexTail(text=response.text, complete=whole)

    @_retry_file
    def _get(self, url: str) -> httpx.Response:
        response = self._client.get(url)
        response.raise_for_status()
        return response

    def fetch(self, line: IndexLine) -> Fetched:
        try:
            response = self._get(line.url)
        except httpx.HTTPStatusError as error:
            status = error.response.status_code
            if status == 404:
                raise FileUnavailableError(f"{line.file_name}: HTTP 404") from error
            if status == 429:
                raise RateLimitedError(
                    f"{line.file_name}: HTTP 429", _retry_after(error.response)
                ) from error
            raise
        return Fetched(content=response.content, published_at=_last_modified(response))


class FixtureFetcher:
    """Recorded files: the two lists at ``<root>``, the files at ``<root>/<feed>/<file name>``."""

    def __init__(self, root: Path) -> None:
        self._root = root

    def last_update(self) -> str:
        return (self._root / FIXTURE_LAST_UPDATE).read_text(encoding="utf-8")

    def master_tail(self, tail_bytes: int) -> IndexTail:
        return IndexTail(
            text=(self._root / FIXTURE_INDEX).read_text(encoding="utf-8"), complete=True
        )

    def fetch(self, line: IndexLine) -> Fetched:
        path = self._root / line.feed.value / line.file_name
        if not path.is_file():
            raise FileUnavailableError(f"{line.file_name}: not among the recorded files")
        return Fetched(content=path.read_bytes())


def md5_hex(content: bytes) -> str:
    # MD5 is the checksum GDELT publishes; it is used for integrity, not for security.
    return hashlib.md5(content, usedforsecurity=False).hexdigest()


def verify(content: bytes, line: IndexLine) -> None:
    """Gate G1: size and MD5 equal the index values, and the zip opens with one member."""
    if len(content) != line.size:
        raise VerificationError(
            f"{line.file_name}: size {len(content)} differs from the index ({line.size})", content
        )
    digest = md5_hex(content)
    if digest != line.md5:
        raise VerificationError(
            f"{line.file_name}: MD5 {digest} differs from the index ({line.md5})", content
        )
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            members = archive.namelist()
            damaged = archive.testzip()
    except zipfile.BadZipFile as error:
        raise VerificationError(
            f"{line.file_name}: not a readable zip ({error})", content
        ) from error
    if damaged is not None or len(members) != 1:
        raise VerificationError(
            f"{line.file_name}: expected one intact member, found {members}", content
        )


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
    #: The upload time of the file; None when nothing was fetched or the source gave no time.
    published_at: datetime | None = None


class Bronze:
    """The ``raw/`` area of the lake, on whichever filesystem the lake URI names."""

    def __init__(self, lake_uri: str) -> None:
        self._lake_uri = lake_uri.rstrip("/")
        self._filesystem, self._root = pafs.FileSystem.from_uri(lake_uri)

    def _path(self, key: str) -> str:
        return f"{self._root.rstrip('/')}/{key}"

    def uri(self, key: str) -> str:
        """The full URI of an object, as the ledger stores it."""
        return f"{self._lake_uri}/{key}"

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
        failpoint(BRONZE_BEFORE_MOVE)
        self._filesystem.move(temporary, path)

    def sizes(self, prefix: str) -> dict[str, int]:
        """Key and size of every finished object below a prefix; empty when nothing is there."""
        base = self._root.rstrip("/")
        directory = f"{base}/{prefix.strip('/')}"
        if self._filesystem.get_file_info(directory).type != pafs.FileType.Directory:
            return {}
        found: dict[str, int] = {}
        for info in self._filesystem.get_file_info(pafs.FileSelector(directory, recursive=True)):
            if info.type == pafs.FileType.File and not info.path.endswith(".part"):
                found[info.path.removeprefix(base).lstrip("/")] = info.size
        return found


def land_file(bronze: Bronze, fetcher: Fetcher, line: IndexLine) -> LandedFile:
    """Land one listed file, unless bronze already holds it with the expected MD5.

    Raises VerificationError when the fetched bytes fail gate G1; nothing is written then.
    """
    key = line.slot.bronze_key(line.feed)
    existing = bronze.read(key)
    published_at: datetime | None = None
    if existing is not None and len(existing) == line.size and md5_hex(existing) == line.md5:
        log.info("slot_already_landed", key=key)
        written = False
    else:
        fetched = fetcher.fetch(line)
        verify(fetched.content, line)
        bronze.write(key, fetched.content)
        log.info("slot_landed", key=key, bytes=line.size)
        written = True
        published_at = fetched.published_at
    return LandedFile(
        key=key,
        size=line.size,
        md5=line.md5,
        slot=line.slot.stamp,
        feed=line.feed.value,
        written=written,
        published_at=published_at,
    )


def land_hour(
    bronze: Bronze,
    fetcher: Fetcher,
    slot_date: date,
    slot_hour: int,
    feed: Feed = Feed.EXPORT,
    *,
    as_of: datetime | None = None,
) -> tuple[list[IndexLine], list[LandedFile]]:
    """Land every listed file of one feed and one UTC hour, without the ledger.

    Returns the index lines found and the files now in bronze. A slot that the source does not
    list is absent from both. The ledger-driven ingest (``gbmi.sources.gdelt.ingest``) is the
    routine path; this function serves the tracer of phase P0 and the fixture recorder.
    """
    first = Slot(datetime(slot_date.year, slot_date.month, slot_date.day, slot_hour, tzinfo=UTC))
    listed = read_master_list(fetcher, oldest=first, as_of=as_of or utc_now())
    lines = lines_for_hour(listed, slot_date, slot_hour, feed)
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
    lines, landed = land_hour(
        Bronze(settings.lake_uri),
        fetcher_for(source, fixtures_dir),
        slot_date,
        slot_hour,
        Feed.EXPORT,
    )
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


def fetcher_for(source: str, fixtures_dir: str = "/fixtures/gdelt/v2") -> Fetcher:
    """``live``: the GDELT servers. ``fixture``: the recorded files of a directory."""
    if source == "live":
        return HttpFetcher()
    if source == "fixture":
        return FixtureFetcher(Path(fixtures_dir))
    raise ValueError(f"source must be 'fixture' or 'live', got {source!r}")
