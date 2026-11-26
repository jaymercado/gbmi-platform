"""A GDELT server in memory, behind ``httpx.MockTransport``.

The ingest tests talk to the real ``HttpFetcher``; only the network is replaced. The server
serves the two file lists and the files, honours a range request for the master list, and
answers with prepared failures: damaged bytes, HTTP 404, HTTP 429.
"""

from __future__ import annotations

import io
import threading
import zipfile
from datetime import timedelta
from email.utils import format_datetime

import httpx

from gbmi.core.gdelt import Feed, Slot
from gbmi.ledger.repository import SlotKey
from gbmi.sources.gdelt.land import HttpFetcher, md5_hex

BASE = "http://data.gdeltproject.org/gdeltv2/"
#: GDELT uploads a file about ten minutes before the time in the file name (week 2 plan, F1).
UPLOAD_LEAD = timedelta(minutes=10)

DAMAGED = "damaged"


def zipped(text: str, member: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member, text)
    return buffer.getvalue()


class GdeltServer:
    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.master: list[str] = []  # the lines of the master list, in the order of arrival
        self.newest: list[str] = []  # the lines of lastupdate.txt
        # An answer is DAMAGED, a status code, or a status code with headers.
        self.always: dict[str, object] = {}  # file name -> the answer for every request
        self.once: dict[str, list[object]] = {}  # file name -> answers before the normal one
        self.requests: list[str] = []
        self._lock = threading.Lock()

    # --- what the source has -------------------------------------------------------------

    def line(self, key: SlotKey, content: bytes | None = None) -> str:
        name = key.slot.file_name(key.feed)
        content = self.files[name] if content is None else content
        return f"{len(content)} {md5_hex(content)} {BASE}{name}"

    def publish(self, *keys: SlotKey, text: str = "row\n", listed: bool = True) -> None:
        """The source has the files; with ``listed``, the master list names them."""
        for key in keys:
            name = key.slot.file_name(key.feed)
            self.files[name] = zipped(f"{name}\t{text}", name.removesuffix(".zip"))
            if listed:
                self.master.append(self.line(key))

    def publish_slot(self, slot: Slot, *, newest: bool = True) -> list[SlotKey]:
        """The three files of one slot; with ``newest``, lastupdate.txt shows the slot."""
        keys = [SlotKey(feed, slot) for feed in Feed]
        self.publish(*keys)
        if newest:
            self.newest = [self.line(key) for key in keys]
        return keys

    def relist(self, key: SlotKey, text: str) -> None:
        """The source replaces a file: new bytes, and a new line at the end of the master list."""
        name = key.slot.file_name(key.feed)
        self.files[name] = zipped(f"{name}\t{text}", name.removesuffix(".zip"))
        self.master.append(self.line(key))

    # --- how the source answers ----------------------------------------------------------

    def handler(self, request: httpx.Request) -> httpx.Response:
        name = request.url.path.rsplit("/", 1)[-1]
        with self._lock:
            self.requests.append(name)
            answer = self.once[name].pop(0) if self.once.get(name) else self.always.get(name)
        if name == "lastupdate.txt":
            return httpx.Response(200, text="\n".join(self.newest) + "\n")
        if name == "masterfilelist.txt":
            return self._master(request)
        if isinstance(answer, int):
            return httpx.Response(answer)
        if isinstance(answer, tuple):
            return httpx.Response(answer[0], headers=answer[1])
        if name not in self.files:
            return httpx.Response(404)
        content = self.files[name]
        if answer == DAMAGED:
            content = content[:-4]
        slot = Slot.parse(name[:14])
        headers = {"Last-Modified": format_datetime(slot.start - UPLOAD_LEAD, usegmt=True)}
        return httpx.Response(200, content=content, headers=headers)

    def _master(self, request: httpx.Request) -> httpx.Response:
        text = "\n".join(self.master) + "\n"
        asked = int(request.headers["Range"].removeprefix("bytes=-"))
        if asked >= len(text):
            return httpx.Response(200, text=text)
        start = len(text) - asked
        headers = {"Content-Range": f"bytes {start}-{len(text) - 1}/{len(text)}"}
        return httpx.Response(206, text=text[start:], headers=headers)

    def fetcher(self) -> HttpFetcher:
        return HttpFetcher(httpx.Client(transport=httpx.MockTransport(self.handler)))

    def asked(self, name: str) -> int:
        """How often the file or the list was requested."""
        return self.requests.count(name)

    def file_requests(self) -> list[str]:
        return [name for name in self.requests if name.endswith(".zip")]
