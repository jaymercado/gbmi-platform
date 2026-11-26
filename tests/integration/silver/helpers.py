"""Shared pieces of the build tests: the fixture hour in a test lake, and changed fixture files."""

from __future__ import annotations

import hashlib
import io
import os
import shutil
import subprocess
import sys
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pyarrow.fs as pafs
from pyiceberg.table import Table

from gbmi.core.checksum import ContentChecksum, content_checksum
from gbmi.core.gdelt import Feed, Slot
from gbmi.lake.commit import Unit
from gbmi.ledger.repository import Ledger
from gbmi.ledger.states import State
from gbmi.sources.gdelt import ingest
from gbmi.sources.gdelt.land import Bronze, FixtureFetcher

from ..conftest import Lake

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "gdelt" / "v2"
DAY = date(2026, 11, 25)
HOUR = Unit(DAY, 8)
#: The clock of the tests: the files of the fixture hour (08:00 to 08:45) are published.
AS_OF = datetime(2026, 11, 25, 9, 0, tzinfo=UTC)
#: The rows which the recorded hour gives to each silver table.
ROWS = {"gdelt_event": 4023, "gdelt_mention": 7762, "gdelt_gkg_doc": 240, "gdelt_gkg_org": 541}
VERSION_COLUMNS = (
    "unit_kind",
    "slot_hour",
    "committed_rows",
    "file_count",
    "fileset_hash",
    "content_digest",
    "commit_id",
    "snapshot_id",
    "previous_snapshot_id",
    "raw_lines",
    "parsed_rows",
    "quarantined_rows",
    "duplicate_rows",
)


def land(
    ledger: Ledger,
    lake: Lake,
    *,
    fixtures: Path = FIXTURES,
    as_of: datetime = AS_OF,
    batch_limit: int = 200,
) -> dict[str, Any]:
    """Enqueue the fixture day and land listed files: each landed slot is verified."""
    fetcher = FixtureFetcher(fixtures)
    lake_uri = lake.settings.lake_uri
    ingest.enqueue(DAY, DAY, as_of=as_of, fetcher=fetcher, ledger=ledger)
    return ingest.run_ingest(
        as_of=as_of,
        fetcher=fetcher,
        ledger=ledger,
        bronze=Bronze(lake_uri),
        lake_uri=lake_uri,
        batch_limit=batch_limit,
    )


def silver(lake: Lake, name: str) -> Table:
    return lake.catalog.load_table((lake.namespace, name))


def stored(lake: Lake, name: str, unit: Unit) -> ContentChecksum:
    """The checksum of the rows which the table holds for the unit now."""
    return content_checksum(silver(lake, name).scan(row_filter=unit.row_filter).to_arrow())


def data_files(lake: Lake, name: str) -> list[str]:
    """The data files of the current snapshot of the table."""
    table = silver(lake, name)
    if table.current_snapshot() is None:
        return []
    return sorted(table.inspect.data_files()["file_path"].to_pylist())


def day_files_on_storage(lake: Lake, name: str) -> list[str]:
    """Every staged day file under the location of the table, in the table or not."""
    filesystem, path = pafs.FileSystem.from_uri(silver(lake, name).location())
    if filesystem.get_file_info(path).type == pafs.FileType.NotFound:
        return []
    found = filesystem.get_file_info(pafs.FileSelector(path, recursive=True))
    return sorted(info.path for info in found if info.path.endswith("-day.parquet"))


def lake_path(lake: Lake, key: str) -> Path:
    """The path of an object of the test lake on the local filesystem."""
    return Path(lake.settings.lake_uri.removeprefix("file://")) / key


def states(ledger: Ledger, feed: Feed | None = None) -> dict[State, int]:
    """The number of slots in each state, without the slots which are only expected."""
    rows = ledger.connection.execute(
        "SELECT state, count(*) FROM ops.source_slot WHERE source = %s AND state <> 'expected' "
        "AND (%s::text IS NULL OR feed = %s) GROUP BY state",
        (ledger.source, feed.value if feed else None, feed.value if feed else None),
    ).fetchall()
    return {State(row[0]): row[1] for row in rows}


def versions(ledger: Ledger, table_name: str) -> list[dict[str, Any]]:
    """The partition versions of a table, oldest first."""
    rows = ledger.connection.execute(
        f"SELECT {', '.join(VERSION_COLUMNS)} FROM ops.silver_partition "
        "WHERE source = %s AND table_name = %s ORDER BY partition_version_id",
        (ledger.source, table_name),
    ).fetchall()
    return [dict(zip(VERSION_COLUMNS, row, strict=True)) for row in rows]


def checks(ledger: Ledger, status: str) -> list[tuple[str, str, str | None, str | None]]:
    """Gate, check, table and unit of the stored results with the status, oldest first."""
    return [
        (row[0], row[1], row[2], row[3])
        for row in ledger.connection.execute(
            "SELECT gate, check_name, table_name, unit FROM ops.dq_result "
            "WHERE source = %s AND status = %s ORDER BY dq_result_id",
            (ledger.source, status),
        ).fetchall()
    ]


# --- changed copies of the recorded files ---------------------------------------------------


def copy_fixtures(target: Path) -> Path:
    """A copy of the recorded files which a test may change."""
    shutil.copytree(FIXTURES, target)
    return target


def read_lines(root: Path, feed: Feed, stamp: str) -> list[str]:
    """The lines of one recorded file."""
    with zipfile.ZipFile(root / feed.value / Slot.parse(stamp).file_name(feed)) as archive:
        (member,) = archive.namelist()
        lines = archive.read(member).decode("utf-8").split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    return lines


def write_file(root: Path, feed: Feed, stamp: str, lines: list[str]) -> bytes:
    """Write a file of the feed and list the file with its size and MD5, as the source would.

    The master list gets a new line, or the line of the file gets the new facts. The list of
    the newest files changes only when it names the file.
    """
    name = Slot.parse(stamp).file_name(feed)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(name.removesuffix(".zip"), "".join(f"{line}\n" for line in lines))
    content = buffer.getvalue()
    (root / feed.value / name).write_bytes(content)
    digest = hashlib.md5(content, usedforsecurity=False).hexdigest()
    entry = f"{len(content)} {digest} http://data.gdeltproject.org/gdeltv2/{name}"
    for list_name, add in (("masterfilelist.txt", True), ("lastupdate.txt", False)):
        path = root / list_name
        listed = [line for line in path.read_text(encoding="utf-8").splitlines() if line]
        named = [index for index, line in enumerate(listed) if line.endswith(f"/{name}")]
        if named:
            listed[named[0]] = entry
        elif add:
            listed.append(entry)
        path.write_text("".join(f"{line}\n" for line in listed), encoding="utf-8")
    return content


def drop_last_column(lines: list[str], count: int) -> list[str]:
    """The lines with the last column cut from the first ``count`` lines: rows which fail G2."""
    return [line.rsplit("\t", 1)[0] for line in lines[:count]] + lines[count:]


# --- a command in a process of its own ------------------------------------------------------


def run_command(
    arguments: list[str], *, lake: Lake, dsn: str, failpoint: str | None = None
) -> subprocess.CompletedProcess[str]:
    """Run a gbmi command in a new process, on the lake and the database of the test.

    With ``failpoint`` the process kills itself at the named point (gbmi.core.failpoint).
    """
    environment = {
        **os.environ,
        "GBMI_PG_DSN": dsn,
        "GBMI_LAKE_BACKEND": "local",
        "GBMI_LAKE_URI": lake.settings.lake_uri,
        "GBMI_CATALOG_URI": lake.settings.catalog_uri,
        "GBMI_PIPELINE_VERSION": lake.settings.pipeline_version,
    }
    environment.pop("GBMI_FAILPOINT", None)
    if failpoint:
        environment["GBMI_FAILPOINT"] = failpoint
    return subprocess.run(
        [sys.executable, "-c", "from gbmi.cli import app; app()", *arguments],
        env=environment,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
