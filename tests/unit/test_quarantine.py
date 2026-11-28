"""Quarantine files are replaced on a rerun and removed when nothing is rejected."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from gbmi.lake.commit import Unit
from gbmi.quality.quarantine import (
    QUARANTINE_SCHEMA,
    clear_hour_quarantine,
    quarantine_key,
    write_quarantine,
    write_quarantined_file,
)

UNIT = Unit(date(2026, 11, 25), 8)
NOW = datetime(2026, 11, 25, 9, tzinfo=UTC)


def rejected(*reasons: str) -> pa.Table:
    return pa.Table.from_pylist(
        [
            {
                "source_file": "raw/gdelt/v2/export/dt=2026-11-25/20261125084500.export.CSV.zip",
                "source_md5": "0" * 32,
                "slot_ts": datetime(2026, 11, 25, 8, 45, tzinfo=UTC),
                "line_number": number,
                "reason": reason,
                "raw_line": f"line {number}",
                "_run_id": "run-1",
                "_ingested_at": NOW,
            }
            for number, reason in enumerate(reasons, start=1)
        ],
        schema=QUARANTINE_SCHEMA,
    )


def test_key_is_deterministic_per_unit() -> None:
    assert quarantine_key("gdelt", "export", UNIT) == (
        "quarantine/gdelt/export/dt=2026-11-25/hour=08/rows.parquet"
    )
    assert quarantine_key("gdelt", "gkg", Unit(date(2026, 11, 25))) == (
        "quarantine/gdelt/gkg/dt=2026-11-25/day/rows.parquet"
    )


def test_rerun_replaces_the_file_and_a_clean_rerun_removes_it(tmp_path: Path) -> None:
    lake = f"file://{tmp_path}"

    key = write_quarantine(lake, "gdelt", "export", UNIT, rejected("column_count", "bad id"))
    assert key is not None
    assert pq.read_table(tmp_path / key)["reason"].to_pylist() == ["column_count", "bad id"]

    # A rerun that rejects one row replaces the file ...
    write_quarantine(lake, "gdelt", "export", UNIT, rejected("bad id"))
    assert pq.read_table(tmp_path / key).num_rows == 1

    # ... and a rerun that rejects nothing removes it.
    assert write_quarantine(lake, "gdelt", "export", UNIT, rejected()) is None
    assert not (tmp_path / key).exists()
    assert write_quarantine(lake, "gdelt", "export", UNIT, rejected()) is None


def test_a_day_build_removes_the_hour_files_of_the_day_and_nothing_else(tmp_path: Path) -> None:
    lake = f"file://{tmp_path}"
    day = date(2026, 11, 25)
    hours = [
        write_quarantine(lake, "gdelt", "export", Unit(day, hour), rejected("bad id"))
        for hour in (0, 8, 23)
    ]
    kept = [
        write_quarantine(lake, "gdelt", "export", Unit(day), rejected("bad id", "bad id")),
        write_quarantine(lake, "gdelt", "gkg", Unit(day, 8), rejected("another feed")),
        write_quarantine(lake, "gdelt", "export", Unit(date(2026, 11, 24), 8), rejected("a day")),
        write_quarantine(lake, "other", "export", Unit(day, 8), rejected("another source")),
        write_quarantined_file(lake, "gdelt", "export", day, "20261125080000.export.CSV.zip", b"z"),
    ]

    assert clear_hour_quarantine(lake, "gdelt", "export", day) == 3

    assert not any((tmp_path / str(key)).exists() for key in hours)
    assert all((tmp_path / str(key)).exists() for key in kept)
    assert clear_hour_quarantine(lake, "gdelt", "export", day) == 0


def test_on_s3_each_quarantine_file_is_one_put_request(monkeypatch: pytest.MonkeyPatch) -> None:
    # Week 3 plan 3.3: the rows of a unit and a rejected file are each one whole object.
    import io

    import boto3
    from botocore.stub import ANY, Stubber

    from gbmi.lake import objects

    client = boto3.client(
        "s3", region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test"
    )
    monkeypatch.setattr(objects, "s3_client", lambda: client)
    sent: list[bytes] = []
    real = client.put_object

    def recording(**arguments: object) -> object:
        sent.append(arguments["Body"])  # type: ignore[arg-type]
        return real(**arguments)

    monkeypatch.setattr(client, "put_object", recording)
    rows_key = "quarantine/gdelt/export/dt=2026-11-25/hour=08/rows.parquet"
    file_key = "quarantine/gdelt/export/dt=2026-11-25/20261125084500.export.CSV.zip"
    with Stubber(client) as stub:
        stub.add_response("put_object", {}, {"Bucket": "bucket", "Key": rows_key, "Body": ANY})
        stub.add_response("put_object", {}, {"Bucket": "bucket", "Key": file_key, "Body": b"bad"})

        written = write_quarantine("s3://bucket", "gdelt", "export", UNIT, rejected("bad id"))
        kept = write_quarantined_file(
            "s3://bucket",
            "gdelt",
            "export",
            UNIT.slot_date,
            "20261125084500.export.CSV.zip",
            b"bad",
        )

        stub.assert_no_pending_responses()
    assert (written, kept) == (rows_key, file_key)
    # The first object is the Parquet file of the rejected rows.
    assert pq.read_table(io.BytesIO(sent[0]))["reason"].to_pylist() == ["bad id"]
