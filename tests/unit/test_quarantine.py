"""Quarantine files are replaced on a rerun and removed when nothing is rejected."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

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
