"""Positional schemas: how a GDELT feed without a header becomes typed columns.

A GDELT file is tab-delimited text with no header and a fixed number of columns in a fixed
order (blueprint 7.8). A feed definition names the source columns in file order and says which
of them become which silver column and how each is converted. Every engine which parses the
feed is generated from the one definition, so the routine engine and the bulk engine cannot
drift apart.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Kind(StrEnum):
    """How a source field becomes a typed value. An empty field is NULL, except for a key."""

    KEY = "key"  # integer, required
    INTEGER = "integer"
    DOUBLE = "double"
    TEXT = "text"
    REQUIRED_TEXT = "required_text"  # text which must not be empty
    FLAG = "flag"  # 0 or 1, stored as a boolean
    DATE8 = "date8"  # YYYYMMDD
    TIMESTAMP14 = "timestamp14"  # YYYYMMDDHHMMSS, UTC
    REQUIRED_TIMESTAMP14 = "required_timestamp14"  # the same, and must not be empty


@dataclass(frozen=True)
class SilverColumn:
    """One silver column which comes straight from one source field."""

    name: str
    source: str
    kind: Kind
    #: One-based position of the source field in a row of the feed.
    position: int


@dataclass(frozen=True)
class FeedSchema:
    """The positional schema of one feed: its source columns and its typed columns."""

    feed: str
    source_columns: tuple[str, ...]
    columns: tuple[SilverColumn, ...]

    @property
    def column_count(self) -> int:
        return len(self.source_columns)

    def position(self, source: str) -> int:
        """One-based position of a source column."""
        return self.source_columns.index(source) + 1


def feed_schema(
    feed: str,
    source_columns: tuple[str, ...],
    expected_count: int,
    columns: tuple[tuple[str, str, Kind], ...],
) -> FeedSchema:
    """Build a feed definition; a typo which shifts the source columns stops the import."""
    if len(source_columns) != expected_count or len(set(source_columns)) != expected_count:
        raise AssertionError(
            f"the {feed} schema has {expected_count} distinct columns, not {len(source_columns)}"
        )
    return FeedSchema(
        feed=feed,
        source_columns=source_columns,
        columns=tuple(
            SilverColumn(name, source, kind, source_columns.index(source) + 1)
            for name, source, kind in columns
        ),
    )


#: A file with a larger share of rows which fail gate G2 is quarantined as a whole (blueprint 5.7).
FILE_THRESHOLD = 0.01


def file_is_rejected(raw_lines: int, failed_rows: int, threshold: float = FILE_THRESHOLD) -> bool:
    """The file-level rule of gate G2: more than 1% of the rows failed.

    A file without lines has no failed rows and is not rejected.
    """
    if failed_rows < 0 or failed_rows > raw_lines:
        raise ValueError(f"{failed_rows} failed rows cannot come from {raw_lines} lines")
    return failed_rows > raw_lines * threshold
