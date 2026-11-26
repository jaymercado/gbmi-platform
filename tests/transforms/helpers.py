"""Builders for the parser tests: a bronze file from lines, and the recorded files of a feed."""

from __future__ import annotations

import io
import zipfile
from datetime import UTC, datetime
from pathlib import Path

from gbmi.core.gdelt import Feed, Slot
from gbmi.sources.gdelt.land import md5_hex
from gbmi.transforms.duckdb.positional import BronzeFile

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "gdelt" / "v2"
SLOT = Slot.parse("20261125084500")
INGESTED_AT = datetime(2026, 11, 25, 9, 0, tzinfo=UTC)


def bronze_file(feed: Feed, lines: list[str], slot: Slot = SLOT) -> BronzeFile:
    """A zip as GDELT serves it: one member, one line for each row."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            slot.file_name(feed).removesuffix(".zip"), "".join(f"{line}\n" for line in lines)
        )
    content = buffer.getvalue()
    return BronzeFile(key=slot.bronze_key(feed), md5=md5_hex(content), content=content)


def recorded(feed: Feed) -> list[BronzeFile]:
    """The four recorded files of the fixture hour, as bronze files."""
    files = []
    for path in sorted((FIXTURES / feed.value).glob("*.zip")):
        content = path.read_bytes()
        key = Slot.parse(path.name[:14]).bronze_key(feed)
        files.append(BronzeFile(key=key, md5=md5_hex(content), content=content))
    return files


def recorded_rows(feed: Feed) -> list[tuple[str, list[str]]]:
    """Every line of the recorded files, split at the tabs, with the stamp of its file.

    Plain Python, which shares nothing with the DuckDB parsers: the tests count with it.
    """
    rows = []
    for path in sorted((FIXTURES / feed.value).glob("*.zip")):
        with zipfile.ZipFile(path) as archive:
            (member,) = archive.namelist()
            text = archive.read(member).decode("utf-8")
        rows += [(path.name[:14], line.split("\t")) for line in text.split("\n") if line]
    return rows
