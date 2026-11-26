"""The GDELT file lists: their lines, and how much of a list has to be read.

``masterfilelist.txt`` and ``lastupdate.txt`` give one line per file: size in bytes, MD5 and
URL, separated by single spaces (blueprint 4.2). The lists still give ``http://`` URLs, which
the server redirects; they are rewritten to ``https://`` here.

``lastupdate.txt`` holds the three lines of the newest slot (320 bytes). The master list holds
every file since 2015 (about 130 MB) and serves byte ranges, so only its tail is read. New lines
are appended at the end, a late file included, and neighbouring slots can stand out of time
order (week 2 plan, F3): nothing here assumes sorted lines.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from typing import Protocol

from gbmi.core.gdelt import SLOT_MINUTES, Feed, Slot, parse_file_name, slots_of_hour

MASTER_LIST_URL = "https://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
LAST_UPDATE_URL = "https://data.gdeltproject.org/gdeltv2/lastupdate.txt"

#: One slot has three lines of about 106.5 bytes each (week 2 plan, F3).
BYTES_PER_SLOT = 320
#: A tail is read half as large again as the arithmetic asks for, and never smaller than this.
TAIL_MARGIN = 1.5
MIN_TAIL_BYTES = 2_000_000


@dataclass(frozen=True)
class IndexLine:
    """One listed file: what the source says it is."""

    size: int
    md5: str
    url: str
    slot: Slot
    feed: Feed

    @property
    def file_name(self) -> str:
        return self.url.rsplit("/", 1)[-1]

    def render(self) -> str:
        """The line as GDELT writes it."""
        return f"{self.size} {self.md5} {self.url}"


@dataclass(frozen=True)
class IndexTail:
    """The end of a file list. ``complete`` says that the text starts at the first byte."""

    text: str
    complete: bool


class IndexSource(Protocol):
    """Where the two file lists come from."""

    def last_update(self) -> str:
        """The text of ``lastupdate.txt``: the lines of the newest slot."""
        ...

    def master_tail(self, tail_bytes: int) -> IndexTail:
        """The last ``tail_bytes`` bytes of the master list; the whole list when it is shorter."""
        ...


def parse_index_line(line: str) -> IndexLine | None:
    """Parse one line; None for a blank line or one that is not a GDELT 2.0 file."""
    parts = line.split()
    if len(parts) != 3 or not parts[0].isdigit() or len(parts[1]) != 32:
        return None
    url = parts[2]
    try:
        slot, feed = parse_file_name(url.rsplit("/", 1)[-1])
    except ValueError:
        return None
    if url.startswith("http://"):
        url = "https://" + url.removeprefix("http://")
    return IndexLine(size=int(parts[0]), md5=parts[1].lower(), url=url, slot=slot, feed=feed)


def parse_index(text: str) -> list[IndexLine]:
    return [parsed for line in text.splitlines() if (parsed := parse_index_line(line)) is not None]


def parse_tail(tail: IndexTail) -> list[IndexLine]:
    """The lines of a tail. The first line of a cut tail is dropped.

    A range request can cut the first line anywhere. A line which lost the first digits of its
    size would still look like a line, with a wrong size, so the first line is never trusted.
    """
    text = tail.text
    if not tail.complete:
        _, _, text = text.partition("\n")
    return parse_index(text)


def tail_bytes_for(oldest: Slot, as_of: datetime) -> int:
    """How many bytes of the master list reach back to the slot ``oldest``, with a margin."""
    slots = max(0, int((as_of - oldest.start).total_seconds() // (SLOT_MINUTES * 60))) + 2
    return max(MIN_TAIL_BYTES, int(slots * BYTES_PER_SLOT * TAIL_MARGIN))


def read_master_list(source: IndexSource, *, oldest: Slot, as_of: datetime) -> list[IndexLine]:
    """The lines of the master list for the slot ``oldest`` and every later slot.

    The tail is sized from the oldest wanted slot. When the tail does not reach behind that
    slot, the tail is doubled and read again, until it does or the whole list is read.
    """
    size = tail_bytes_for(oldest, as_of)
    while True:
        tail = source.master_tail(size)
        lines = parse_tail(tail)
        if tail.complete or (lines and min(line.slot for line in lines) < oldest):
            return [line for line in lines if line.slot >= oldest]
        size *= 2


def read_last_update(source: IndexSource) -> list[IndexLine]:
    """The lines of ``lastupdate.txt``: the newest files, one line for each feed."""
    return parse_index(source.last_update())


def latest(lines: Iterable[IndexLine]) -> dict[tuple[Feed, Slot], IndexLine]:
    """One line for each file; when a list names a file twice, the later line counts."""
    return {(line.feed, line.slot): line for line in lines}


def lines_for_hour(
    lines: Iterable[IndexLine], slot_date: date, slot_hour: int, feed: Feed
) -> list[IndexLine]:
    """The lines of one feed and one UTC hour, in slot order. Missing slots are simply absent."""
    wanted = set(slots_of_hour(slot_date, slot_hour))
    return sorted(
        (line for line in lines if line.feed is feed and line.slot in wanted),
        key=lambda line: line.slot,
    )
