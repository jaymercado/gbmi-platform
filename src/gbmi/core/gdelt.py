"""GDELT 2.0 conventions shared by every engine: slots, feeds, file names and bronze keys.

A *slot* is one 15-minute interval; GDELT publishes one zip per feed per slot, named
``YYYYMMDDHHMMSS.<feed>.<ext>.zip`` (blueprint 4.2). All times are UTC (blueprint 7.11).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum

SLOT_MINUTES = 15
SLOTS_PER_HOUR = 60 // SLOT_MINUTES
_SLOT_FORMAT = "%Y%m%d%H%M%S"
_FILE_NAME = re.compile(r"^(?P<slot>\d{14})\.(?P<feed>export|mentions|gkg)\.(?:CSV|csv)\.zip$")
_HOUR = re.compile(r"^(?P<date>\d{4}-\d{2}-\d{2})T(?P<hour>\d{2})$")


class Feed(StrEnum):
    """The three tables of the GDELT 2.0 English feed."""

    EXPORT = "export"
    MENTIONS = "mentions"
    GKG = "gkg"

    @property
    def extension(self) -> str:
        """GDELT writes ``CSV`` in capitals for events and mentions and ``csv`` for the GKG."""
        return "csv" if self is Feed.GKG else "CSV"


@dataclass(frozen=True, order=True)
class Slot:
    """One 15-minute slot, identified by its start time in UTC."""

    start: datetime

    def __post_init__(self) -> None:
        start = self.start
        if start.tzinfo is None or start.utcoffset() != timedelta(0):
            raise ValueError(f"a slot starts at a UTC time, got {start!r}")
        if start.minute % SLOT_MINUTES or start.second or start.microsecond:
            raise ValueError(f"a slot starts on a quarter hour, got {start.isoformat()}")

    @classmethod
    def parse(cls, text: str) -> Slot:
        """From the ``YYYYMMDDHHMMSS`` prefix of a GDELT file name."""
        return cls(datetime.strptime(text, _SLOT_FORMAT).replace(tzinfo=UTC))

    @property
    def stamp(self) -> str:
        return self.start.strftime(_SLOT_FORMAT)

    @property
    def slot_date(self) -> date:
        return self.start.date()

    @property
    def slot_hour(self) -> int:
        return self.start.hour

    def file_name(self, feed: Feed) -> str:
        return f"{self.stamp}.{feed.value}.{feed.extension}.zip"

    def bronze_key(self, feed: Feed) -> str:
        """Deterministic key below the lake root (blueprint 7.1)."""
        return f"raw/gdelt/v2/{feed.value}/dt={self.slot_date.isoformat()}/{self.file_name(feed)}"


def parse_file_name(name: str) -> tuple[Slot, Feed]:
    """Slot and feed of a GDELT file name; raises ValueError for anything else."""
    match = _FILE_NAME.match(name)
    if match is None:
        raise ValueError(f"not a GDELT 2.0 file name: {name!r}")
    return Slot.parse(match["slot"]), Feed(match["feed"])


def parse_hour(text: str) -> tuple[date, int]:
    """A slot-hour written as ``YYYY-MM-DDTHH``, for example ``2026-11-25T08``."""
    match = _HOUR.match(text)
    if match is None:
        raise ValueError(f"an hour is written YYYY-MM-DDTHH, got {text!r}")
    hour = int(match["hour"])
    if hour > 23:
        raise ValueError(f"hour must be 00-23, got {text!r}")
    return date.fromisoformat(match["date"]), hour


def slots_of_hour(slot_date: date, slot_hour: int) -> list[Slot]:
    """The four slots of one UTC hour, in order."""
    first = datetime(slot_date.year, slot_date.month, slot_date.day, slot_hour, tzinfo=UTC)
    return [
        Slot(first + timedelta(minutes=SLOT_MINUTES * index)) for index in range(SLOTS_PER_HOUR)
    ]
