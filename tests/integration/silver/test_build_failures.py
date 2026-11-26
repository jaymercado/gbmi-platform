"""What a build does when something is wrong (week 2 plan WP-06, step 7).

A failed audit, a file which gate G2 rejects as a whole, a file which changed in bronze, and a
process which is killed at a failpoint.
"""

from __future__ import annotations

import dataclasses
import signal
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from gbmi.core.failpoint import SILVER_AFTER_COMMIT, SILVER_BEFORE_COMMIT
from gbmi.core.gdelt import Feed, Slot
from gbmi.lake.commit import Unit
from gbmi.lake.tables import FEED_TABLES
from gbmi.ledger import operator
from gbmi.ledger.repository import Ledger, SlotKey
from gbmi.ledger.states import State
from gbmi.quality.audit import AuditError
from gbmi.transforms import build
from gbmi.transforms.build import BuildError, build_unit
from gbmi.transforms.duckdb.gdelt_event import parse_events
from gbmi.transforms.duckdb.positional import Parsed

from ..conftest import Lake
from .helpers import (
    AS_OF,
    DAY,
    HOUR,
    ROWS,
    checks,
    copy_fixtures,
    data_files,
    day_files_on_storage,
    drop_last_column,
    lake_path,
    land,
    read_lines,
    run_command,
    silver,
    states,
    stored,
    versions,
    write_file,
)

pytestmark = pytest.mark.integration

WHOLE_DAY = Unit(DAY)
UNITS = [
    pytest.param(["build-hour", "--hour", "2026-11-25T08"], HOUR, id="hour"),
    pytest.param(["build-day", "--day", "2026-11-25"], WHOLE_DAY, id="day"),
]


def slot(feed: Feed, stamp: str) -> SlotKey:
    return SlotKey(feed, Slot.parse(stamp))


def repeats_a_row(files: Any, **options: Any) -> Parsed:
    """The export parser with a defect: the first event is in the output twice."""
    parsed = parse_events(files, **options)
    events = parsed.events
    return dataclasses.replace(
        parsed, tables={"gdelt_event": pa.concat_tables([events, events.slice(0, 1)])}
    )


def test_a_failed_audit_changes_neither_silver_nor_the_ledger(
    ledger: Ledger, local_lake: Lake, monkeypatch: pytest.MonkeyPatch
) -> None:
    land(ledger, local_lake)
    settings = local_lake.settings
    monkeypatch.setitem(build.PARSERS, Feed.EXPORT, repeats_a_row)

    with pytest.raises(AuditError) as first:
        build_unit(Feed.EXPORT, HOUR, as_of=AS_OF, ledger=ledger, settings=settings)

    assert "global_event_id is not unique: 4024 rows, 4023 distinct keys" in str(first.value)
    assert "committed (4024) != parsed (4023) - duplicates (0)" in str(first.value)
    # Nothing is committed, no slot moved, no version is recorded; the failed checks are stored.
    assert silver(local_lake, "gdelt_event").current_snapshot() is None
    assert states(ledger, Feed.EXPORT) == {State.VERIFIED: 4}
    assert versions(ledger, "gdelt_event") == []
    assert checks(ledger, "failed") == [
        ("G3", "accounting_committed", "gdelt_event", "2026-11-25T08"),
        ("G3", "key_unique", "gdelt_event", "2026-11-25T08"),
    ]

    # With a version of the hour in silver, the failed audit leaves the version live.
    monkeypatch.setitem(build.PARSERS, Feed.EXPORT, parse_events)
    good = build_unit(Feed.EXPORT, HOUR, as_of=AS_OF, ledger=ledger, settings=settings)
    monkeypatch.setitem(build.PARSERS, Feed.EXPORT, repeats_a_row)
    for unit in (HOUR, WHOLE_DAY):
        with pytest.raises(AuditError, match="is not unique"):
            build_unit(Feed.EXPORT, unit, as_of=AS_OF, ledger=ledger, settings=settings)

    current = silver(local_lake, "gdelt_event").current_snapshot()
    assert current is not None and current.snapshot_id == good["snapshot_id"]
    assert (
        stored(local_lake, "gdelt_event", HOUR).digest
        == (good["tables"]["gdelt_event"]["checksum"]["digest"])
    )
    assert len(versions(ledger, "gdelt_event")) == 1
    assert {record.silver_snapshot_id for record in ledger.unit_slots(Feed.EXPORT, DAY, 8)} == {
        good["snapshot_id"]
    }
    # The staged file of the failed day build was removed.
    assert day_files_on_storage(local_lake, "gdelt_event") == []


def test_a_file_above_the_one_percent_threshold_is_quarantined_as_a_whole(
    ledger: Ledger, local_lake: Lake, tmp_path: Path
) -> None:
    """The unit is built from the other files, and an operator can confirm the slot as missing."""
    fixtures = copy_fixtures(tmp_path / "fixtures")
    stamp = "20261125081500"
    lines = read_lines(fixtures, Feed.EXPORT, stamp)
    bad = len(lines) // 50 + 1  # a little more than 2% of the file
    content = write_file(fixtures, Feed.EXPORT, stamp, drop_last_column(lines, bad))
    others = sum(
        len(read_lines(fixtures, Feed.EXPORT, other))
        for other in ("20261125080000", "20261125083000", "20261125084500")
    )
    # The clock of the test is after the end of the day: the calendar holds the 96 slots.
    as_of = datetime(2026, 11, 26, 0, 30, tzinfo=UTC)
    land(ledger, local_lake, fixtures=fixtures, as_of=as_of)
    settings = local_lake.settings
    rejected = slot(Feed.EXPORT, stamp)
    kept = "quarantine/gdelt/export/dt=2026-11-25/20261125081500.export.CSV.zip"

    built = build_unit(Feed.EXPORT, HOUR, as_of=as_of, ledger=ledger, settings=settings)

    assert built["set_aside"] == [
        {
            "slot": rejected.label,
            "gate": "G2",
            "reason": f"{bad} of {len(lines)} rows failed gate G2, more than 1% of the file",
            "kept": kept,
        }
    ]
    # The file gives no row to silver and no row to the quarantine file of the unit.
    assert built["counts"] == {
        "raw_lines": others,
        "parsed": others,
        "quarantined": 0,
        "duplicates": 0,
        "committed": others,
    }
    assert (len(built["files"]), built["loaded"], built["quarantine_file"]) == (3, 3, None)
    assert stored(local_lake, "gdelt_event", HOUR).rows == others
    # The whole file is kept in the quarantine area, and the slot is quarantined.
    assert lake_path(local_lake, kept).read_bytes() == content
    record = ledger.get(rejected)
    assert record is not None and record.state is State.QUARANTINED
    assert ledger.transitions(rejected)[-1] == (State.VERIFIED, State.QUARANTINED)
    assert states(ledger, Feed.EXPORT) == {State.LOADED: 3, State.QUARANTINED: 1}
    assert checks(ledger, "failed") == [("G2", "file_bad_row_share", None, stamp)]

    # A second build reads three files and changes nothing more.
    again = build_unit(Feed.EXPORT, HOUR, as_of=as_of, ledger=ledger, settings=settings)
    assert (again["set_aside"], again["loaded"]) == ([], 0)
    assert again["tables"]["gdelt_event"]["checksum"] == built["tables"]["gdelt_event"]["checksum"]

    # The operator decides: no better file will come. The day of the feed is complete then.
    confirmed = operator.confirm_missing(
        ledger,
        first_day=DAY,
        last_day=DAY,
        states=[State.QUARANTINED, State.EXPECTED],
        feeds=[Feed.EXPORT],
        decided_by="tester",
        note="more than 1% of the rows fail gate G2",
        as_of=as_of,
    )
    assert confirmed == {"expected": 92, "quarantined": 1}
    report = {entry["feed"]: entry for entry in ledger.completeness(DAY, DAY)}
    assert (report["export"]["loaded"], report["export"]["confirmed_missing"]) == (3, 93)
    assert (report["export"]["completeness"], report["export"]["complete"]) == (1.0, True)


def test_a_verified_file_which_changed_in_bronze_is_quarantined(
    ledger: Ledger, local_lake: Lake
) -> None:
    land(ledger, local_lake)
    settings = local_lake.settings
    changed, gone = slot(Feed.MENTIONS, "20261125083000"), slot(Feed.MENTIONS, "20261125084500")
    lake_path(local_lake, changed.slot.bronze_key(Feed.MENTIONS)).write_bytes(b"other bytes")
    lake_path(local_lake, gone.slot.bronze_key(Feed.MENTIONS)).unlink()

    built = build_unit(Feed.MENTIONS, HOUR, as_of=AS_OF, ledger=ledger, settings=settings)

    first, second = built["set_aside"]
    assert (first["slot"], first["gate"]) == (changed.label, "G1")
    assert first["reason"].startswith("the file in bronze has the MD5 ")
    assert (
        first["kept"] == "quarantine/gdelt/mentions/dt=2026-11-25/20261125083000.mentions.CSV.zip"
    )
    assert lake_path(local_lake, first["kept"]).read_bytes() == b"other bytes"
    assert (second["slot"], second["reason"], second["kept"]) == (
        gone.label,
        "the file is not in bronze",
        None,
    )
    assert (len(built["files"]), built["loaded"]) == (2, 2)
    assert states(ledger, Feed.MENTIONS) == {State.LOADED: 2, State.QUARANTINED: 2}
    assert checks(ledger, "failed") == [
        ("G1", "bronze_md5", None, "20261125083000"),
        ("G1", "bronze_md5", None, "20261125084500"),
    ]
    assert stored(local_lake, "gdelt_mention", HOUR).rows == built["counts"]["committed"]


def test_a_loaded_file_which_changed_in_bronze_stops_the_build(
    ledger: Ledger, local_lake: Lake
) -> None:
    land(ledger, local_lake)
    settings = local_lake.settings
    good = build_unit(Feed.EXPORT, HOUR, as_of=AS_OF, ledger=ledger, settings=settings)
    changed = slot(Feed.EXPORT, "20261125080000")
    lake_path(local_lake, changed.slot.bronze_key(Feed.EXPORT)).write_bytes(b"other bytes")

    for unit in (HOUR, WHOLE_DAY):
        with pytest.raises(
            BuildError, match=r"slot 20261125080000\.export is loaded, but the file in bronze has"
        ):
            build_unit(Feed.EXPORT, unit, as_of=AS_OF, ledger=ledger, settings=settings)

    # No state leads out of loaded: nothing changes, and an operator has to look.
    current = silver(local_lake, "gdelt_event").current_snapshot()
    assert current is not None and current.snapshot_id == good["snapshot_id"]
    assert states(ledger, Feed.EXPORT) == {State.LOADED: 4}
    assert len(versions(ledger, "gdelt_event")) == 1
    assert day_files_on_storage(local_lake, "gdelt_event") == []


@pytest.mark.parametrize(("command", "unit"), UNITS)
def test_a_process_killed_before_the_commit_is_repaired_by_the_next_run(
    command: list[str], unit: Unit, ledger: Ledger, scratch_dsn: str, local_lake: Lake
) -> None:
    land(ledger, local_lake)
    arguments = ["silver", *command, "--feed", "gkg", "--as-of", AS_OF.isoformat()]

    killed = run_command(
        arguments, lake=local_lake, dsn=scratch_dsn, failpoint=SILVER_BEFORE_COMMIT
    )

    assert killed.returncode == -signal.SIGKILL, killed.stderr[-2000:]
    # Until the commit nothing is visible: no snapshot, no version, and the slots still wait.
    for name in FEED_TABLES[Feed.GKG]:
        assert silver(local_lake, name).current_snapshot() is None
        assert versions(ledger, name) == []
    assert states(ledger, Feed.GKG) == {State.VERIFIED: 4}

    repaired = build_unit(Feed.GKG, unit, as_of=AS_OF, ledger=ledger, settings=local_lake.settings)

    assert repaired["loaded"] == 4
    assert states(ledger, Feed.GKG) == {State.LOADED: 4}
    for name in FEED_TABLES[Feed.GKG]:
        assert stored(local_lake, name, unit).rows == ROWS[name]
        # A staged file of the killed process is no part of the table.
        assert len(data_files(local_lake, name)) == 1
        assert len(versions(ledger, name)) == 1


@pytest.mark.parametrize(("command", "unit"), UNITS)
def test_a_process_killed_after_the_commit_is_repaired_by_the_next_run(
    command: list[str], unit: Unit, ledger: Ledger, scratch_dsn: str, local_lake: Lake
) -> None:
    land(ledger, local_lake)
    arguments = ["silver", *command, "--feed", "gkg", "--as-of", AS_OF.isoformat()]

    killed = run_command(arguments, lake=local_lake, dsn=scratch_dsn, failpoint=SILVER_AFTER_COMMIT)

    assert killed.returncode == -signal.SIGKILL, killed.stderr[-2000:]
    # The tables hold the new rows, and the ledger does not know yet.
    left_behind = {}
    for name in FEED_TABLES[Feed.GKG]:
        snapshot = silver(local_lake, name).current_snapshot()
        assert snapshot is not None
        left_behind[name] = snapshot.snapshot_id
        assert stored(local_lake, name, unit).rows == ROWS[name]
        assert versions(ledger, name) == []
    assert states(ledger, Feed.GKG) == {State.VERIFIED: 4}
    assert Feed.GKG in {pending.feed for pending in ledger.pending_units(AS_OF)}

    repaired = build_unit(Feed.GKG, unit, as_of=AS_OF, ledger=ledger, settings=local_lake.settings)

    # The next run builds the same unit again and then records the build (blueprint 8.4).
    assert repaired["loaded"] == 4
    assert states(ledger, Feed.GKG) == {State.LOADED: 4}
    for name in FEED_TABLES[Feed.GKG]:
        table = repaired["tables"][name]
        assert table["previous_snapshot_id"] == left_behind[name]
        assert table["snapshot_operations"] == ["delete", "append"]
        assert stored(local_lake, name, unit).digest == table["checksum"]["digest"]
        assert stored(local_lake, name, unit).rows == ROWS[name]
        assert len(data_files(local_lake, name)) == 1
        (version,) = versions(ledger, name)
        assert version["snapshot_id"] == table["snapshot_id"]
    assert Feed.GKG not in {pending.feed for pending in ledger.pending_units(AS_OF)}
