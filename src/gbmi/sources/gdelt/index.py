"""Lines of the GDELT file lists.

``masterfilelist.txt`` and ``lastupdate.txt`` give one line per file: size in bytes, MD5 and
URL, separated by single spaces (blueprint 4.2). The lists still give ``http://`` URLs, which
the server redirects; they are rewritten to ``https://`` here.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

from gbmi.core.gdelt import Feed, Slot, parse_file_name, slots_of_hour

MASTER_LIST_URL = "https://data.gdeltproject.org/gdeltv2/masterfilelist.txt"


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


def lines_for_hour(
    lines: Iterable[IndexLine], slot_date: date, slot_hour: int, feed: Feed
) -> list[IndexLine]:
    """The lines of one feed and one UTC hour, in slot order. Missing slots are simply absent."""
    wanted = set(slots_of_hour(slot_date, slot_hour))
    return sorted(
        (line for line in lines if line.feed is feed and line.slot in wanted),
        key=lambda line: line.slot,
    )
