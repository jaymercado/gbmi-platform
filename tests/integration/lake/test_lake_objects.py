"""Raw files and quarantine files are whole objects on both backends (week 3 plan, WP-10 step 1).

The local half runs with ``make integration``. The half on S3 runs with ``make integration-aws``
and proves what the unit tests can only state with a stub: one write leaves exactly one object
below its prefix, with no temporary object and no marker object for a directory, and the
reads of PyArrow find the object.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.parse import urlsplit

import pyarrow as pa
import pytest

from gbmi.lake.commit import Unit
from gbmi.lake.objects import is_s3, s3_client, s3_location
from gbmi.quality.quarantine import (
    QUARANTINE_SCHEMA,
    clear_hour_quarantine,
    write_quarantine,
    write_quarantined_file,
)
from gbmi.sources.gdelt.land import Bronze
from tests.integration.conftest import Lake

pytestmark = pytest.mark.integration

DAY = date(2026, 11, 25)


def stored(lake: Lake, prefix: str) -> dict[str, int]:
    """Every object below a prefix of the lake with its size, as the store lists the objects."""
    lake_uri = lake.settings.lake_uri
    if is_s3(lake_uri):
        bucket, full_prefix = s3_location(lake_uri, prefix)
        pages = (
            s3_client().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=full_prefix)
        )
        root = full_prefix[: len(full_prefix) - len(prefix)]
        return {
            entry["Key"].removeprefix(root): entry["Size"]
            for page in pages
            for entry in page.get("Contents", [])
        }
    root_path = Path(urlsplit(lake_uri).path)
    return {
        str(path.relative_to(root_path)): path.stat().st_size
        for path in (root_path / prefix).rglob("*")
        if path.is_file()
    }


@pytest.fixture
def source(lake: Lake) -> Iterator[str]:
    """A source name of one test; the objects of the test are removed afterwards."""
    name = f"test{uuid.uuid4().hex[:10]}"
    try:
        yield name
    finally:
        lake_uri = lake.settings.lake_uri
        if is_s3(lake_uri):
            for prefix in (f"raw/{name}", f"quarantine/{name}"):
                bucket, full_prefix = s3_location(lake_uri, prefix)
                for key in stored(lake, prefix):
                    s3_client().delete_object(
                        Bucket=bucket, Key=f"{full_prefix}{key.removeprefix(prefix)}"
                    )


def test_a_raw_file_is_one_whole_object(lake: Lake, source: str) -> None:
    bronze = Bronze(lake.settings.lake_uri)
    key = f"raw/{source}/export/dt=2026-11-25/20261125080000.export.CSV.zip"

    bronze.write(key, b"zip bytes")
    bronze.write(key, b"zip bytes, again")  # a second download replaces the object

    # Exactly one object: no ".part" object and no marker object for a directory.
    assert stored(lake, f"raw/{source}") == {key: len(b"zip bytes, again")}
    assert bronze.read(key) == b"zip bytes, again"
    assert bronze.read(key + ".missing") is None
    assert bronze.sizes(f"raw/{source}") == {key: len(b"zip bytes, again")}
    assert bronze.sizes(f"raw/{source}-nothing") == {}


def test_the_quarantine_files_are_whole_objects(lake: Lake, source: str) -> None:
    lake_uri = lake.settings.lake_uri
    unit = Unit(DAY, 8)
    rows = pa.Table.from_pylist(
        [
            {
                "source_file": f"raw/{source}/export/dt=2026-11-25/20261125084500.export.CSV.zip",
                "source_md5": "0" * 32,
                "slot_ts": datetime(2026, 11, 25, 8, 45, tzinfo=UTC),
                "line_number": 1,
                "reason": "bad id",
                "raw_line": "line 1",
                "_run_id": "run-1",
                "_ingested_at": datetime(2026, 11, 25, 9, tzinfo=UTC),
            }
        ],
        schema=QUARANTINE_SCHEMA,
    )

    rows_key = write_quarantine(lake_uri, source, "export", unit, rows)
    file_key = write_quarantined_file(lake_uri, source, "export", DAY, "bad.zip", b"bad")

    assert rows_key is not None
    assert set(stored(lake, f"quarantine/{source}")) == {rows_key, file_key}

    # A rerun which rejects no row removes the file of the rows; the rejected file stays, and
    # no marker object for the folder of the removed file appears (S3: one DELETE request).
    assert write_quarantine(lake_uri, source, "export", unit, rows.slice(0, 0)) is None
    assert set(stored(lake, f"quarantine/{source}")) == {file_key}

    # A day build removes the files of the hour builds of the day and leaves nothing else.
    for hour in (8, 9):
        assert write_quarantine(lake_uri, source, "export", Unit(DAY, hour), rows) is not None
    assert clear_hour_quarantine(lake_uri, source, "export", DAY) == 2
    assert set(stored(lake, f"quarantine/{source}")) == {file_key}
    assert clear_hour_quarantine(lake_uri, source, "export", DAY) == 0
