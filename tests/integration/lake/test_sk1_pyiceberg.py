"""Spike SK1 - the PyIceberg contract (week 1 plan, WP-06).

Does PyIceberg 0.12.0 deliver the idempotency contract of blueprint 5.6 on both lake backends
through one catalog factory? Each test answers one question of the spike and stays in the suite
as the evidence behind the decision recorded in docs/reports/p0-spikes.md and ADR-004.

Options that were rejected are kept as strict expected failures: if a later PyIceberg release
makes one of them work, the test starts to pass, the suite fails, and the decision is revisited.

Every test runs on the local backend. With GBMI_TEST_AWS=1 the same tests also run on Glue and
S3 (question Q8). Run with ``-s`` to see the observations that feed the spike report.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest
from pyiceberg.table import Table
from pyiceberg.transforms import IdentityTransform

from gbmi.core.checksum import content_checksum
from gbmi.lake.catalog import silver_namespace_location
from gbmi.lake.commit import (
    COMMIT_ID_PROPERTY,
    Unit,
    UnitMismatchError,
    overwrite_day_streaming,
    overwrite_unit,
)
from gbmi.lake.maintenance import commits_on_main, expire_snapshots
from gbmi.lake.tables import create_table_if_missing
from tests.integration.conftest import Lake
from tests.integration.lake import sk1_table as sk1
from tests.integration.lake.sk1_table import DAY_1, DAY_2

pytestmark = pytest.mark.integration

HOUR_8 = "slot_date = '2026-11-24' and slot_hour = 8"
HOUR_9 = "slot_date = '2026-11-24' and slot_hour = 9"
ONLY_DAY_1 = "slot_date = '2026-11-24'"
ONLY_DAY_2 = "slot_date = '2026-11-25'"


def note(lake: Lake, question: str, text: str) -> None:
    """An observation for the spike report (shown with pytest -s)."""
    print(f"\nSK1 {question} [{lake.backend}] {text}")


def storage_files(table: Table) -> set[str]:
    """Every file under the table's location, data and metadata alike."""
    filesystem, path = pafs.FileSystem.from_uri(table.location())
    selector = pafs.FileSelector(path, recursive=True)
    return {info.path for info in filesystem.get_file_info(selector) if info.is_file}


def whole_table(table: Table) -> pa.Table:
    return table.scan().to_arrow()


# --- Q1 ------------------------------------------------------------------------------------


def test_q1_one_factory_creates_the_table_at_the_namespace_location(lake: Lake) -> None:
    table = sk1.create(lake, "sk1_q1")

    # The table lands at <namespace location>/<table> on both backends (finding F6).
    assert table.location() == f"{silver_namespace_location(lake.settings)}/sk1_q1"
    assert table.metadata.format_version == 2
    assert table.properties["write.parquet.compression-codec"] == "zstd"
    (partition_field,) = table.spec().fields
    assert partition_field.name == "slot_date"
    assert isinstance(partition_field.transform, IdentityTransform)
    assert table.schema().column_names == sk1.SCHEMA.column_names

    # Creating it again loads the same table.
    again = create_table_if_missing(lake.catalog, lake.namespace, "sk1_q1", sk1.SCHEMA)
    assert again.metadata_location == table.metadata_location

    # What is written is ZSTD-compressed Parquet under the table's location.
    overwrite_unit(table, sk1.rows(DAY_1, 8), Unit(DAY_1, 8))
    (path,) = sk1.data_files(table)
    assert path.startswith(table.location())
    with table.io.new_input(path).open() as stream:
        metadata = pq.read_metadata(stream)
    assert metadata.row_group(0).column(0).compression == "ZSTD"
    note(
        lake, "Q1", f"table at {table.location()}; data file {path.removeprefix(table.location())}"
    )


# --- Q2 ------------------------------------------------------------------------------------


def test_q2_hour_overwrite_replaces_only_that_hour(lake: Lake) -> None:
    table = sk1.create(lake, "sk1_q2")
    overwrite_unit(table, sk1.rows(DAY_1, 8), Unit(DAY_1, 8))
    overwrite_unit(table, sk1.rows(DAY_1, 9), Unit(DAY_1, 9))
    before_day_2 = overwrite_unit(table, sk1.rows(DAY_2, 8), Unit(DAY_2, 8))
    files_before = sk1.data_files(table)
    commits_before = sk1.commits_so_far(table)
    hour_9_before = sk1.ids(table, HOUR_9)
    day_2_before = sk1.ids(table, ONLY_DAY_2)

    started = time.perf_counter()
    replacement = sk1.rows(DAY_1, 8, count=3, version=2)
    result = overwrite_unit(table, replacement, Unit(DAY_1, 8))
    elapsed = time.perf_counter() - started

    # One catalog commit, so one new metadata file ...
    assert sk1.commits_so_far(table) == commits_before + 1
    # ... made of two snapshots: the hour's file is dropped whole, then the new rows are added.
    assert result.operations == ("delete", "append")
    assert result.snapshot_id == result.snapshot_ids[-1]
    assert result.previous_snapshot_id == before_day_2.snapshot_id
    # The hour holds exactly the new rows.
    assert sk1.ids(table, HOUR_8) == sorted(replacement["id"].to_pylist())
    # Files of other hours and days keep their paths: a whole-file delete, not a rewrite.
    files_after = sk1.data_files(table)
    removed = set(files_before) - set(files_after)
    added = set(files_after) - set(files_before)
    assert len(removed) == 1
    assert len(added) == 1
    assert {path: files_before[path] for path in files_before if path not in removed} == {
        path: count for path, count in files_after.items() if path not in added
    }
    assert sk1.ids(table, HOUR_9) == hour_9_before
    assert sk1.ids(table, ONLY_DAY_2) == day_2_before
    note(
        lake,
        "Q2",
        f"hour overwrite: snapshots {result.operations}, 1 commit, {elapsed * 1000:.0f} ms",
    )


def test_q2_hour_overwrite_inside_a_compacted_day_rewrites_the_day_file(lake: Lake) -> None:
    """After the daily finalisation a day is one file, so replacing one hour rewrites it."""
    table = sk1.create(lake, "sk1_q2_compacted")
    day = pa.concat_tables([sk1.rows(DAY_1, 8), sk1.rows(DAY_1, 9)])
    overwrite_unit(table, day, Unit(DAY_1))
    assert len(sk1.data_files(table)) == 1
    hour_9_before = sk1.ids(table, HOUR_9)
    commits_before = sk1.commits_so_far(table)

    replacement = sk1.rows(DAY_1, 8, count=2, version=2)
    result = overwrite_unit(table, replacement, Unit(DAY_1, 8))

    assert sk1.commits_so_far(table) == commits_before + 1
    assert "overwrite" in result.operations
    assert result.operations[-1] == "append"
    assert sk1.ids(table, HOUR_8) == sorted(replacement["id"].to_pylist())
    assert sk1.ids(table, HOUR_9) == hour_9_before
    note(lake, "Q2", f"hour overwrite inside a compacted day: snapshots {result.operations}")


# --- Q3 ------------------------------------------------------------------------------------


def test_q3_day_overwrite_replaces_one_day_and_compacts_it(lake: Lake) -> None:
    table = sk1.create(lake, "sk1_q3")
    for hour in (8, 9, 10):
        overwrite_unit(table, sk1.rows(DAY_1, hour), Unit(DAY_1, hour))
    overwrite_unit(table, sk1.rows(DAY_2, 8), Unit(DAY_2, 8))
    files_before = sk1.data_files(table)
    assert len(files_before) == 4
    day_2_before = sk1.ids(table, ONLY_DAY_2)
    commits_before = sk1.commits_so_far(table)

    day = pa.concat_tables([sk1.rows(DAY_1, hour, version=2) for hour in (8, 9, 10)])
    result = overwrite_unit(table, day, Unit(DAY_1))

    assert sk1.commits_so_far(table) == commits_before + 1
    assert result.operations == ("delete", "append")
    assert sk1.ids(table, ONLY_DAY_1) == sorted(day["id"].to_pylist())
    # The three hourly files of the day became one; the other day's file kept its path.
    files_after = sk1.data_files(table)
    assert len(files_after) == 2
    assert len(set(files_before) & set(files_after)) == 1
    assert sk1.ids(table, ONLY_DAY_2) == day_2_before
    note(lake, "Q3", f"day overwrite: snapshots {result.operations}, 3 hourly files -> 1")


# --- Q4 ------------------------------------------------------------------------------------


def test_q4_rerun_gives_the_same_content_checksum(lake: Lake) -> None:
    table = sk1.create(lake, "sk1_q4_rerun")
    overwrite_unit(table, sk1.rows(DAY_1, 9), Unit(DAY_1, 9))

    first = overwrite_unit(table, sk1.rows(DAY_1, 8, run="run-1"), Unit(DAY_1, 8))
    checksum_first = content_checksum(whole_table(table))
    second = overwrite_unit(table, sk1.rows(DAY_1, 8, run="run-2"), Unit(DAY_1, 8))
    checksum_second = content_checksum(whole_table(table))

    # Same rows, new snapshot (blueprint 5.6).
    assert checksum_second == checksum_first
    assert checksum_second.rows == 10
    assert second.snapshot_id != first.snapshot_id
    all_ids = sk1.ids(table)
    assert len(all_ids) == len(set(all_ids))
    note(lake, "Q4", f"rerun: {checksum_second}")


def test_q4_empty_input_empties_the_unit(lake: Lake) -> None:
    table = sk1.create(lake, "sk1_q4_empty")
    overwrite_unit(table, sk1.rows(DAY_1, 8), Unit(DAY_1, 8))
    overwrite_unit(table, sk1.rows(DAY_1, 9), Unit(DAY_1, 9))
    hour_9_before = sk1.ids(table, HOUR_9)

    result = overwrite_unit(table, sk1.empty(), Unit(DAY_1, 8))

    assert result.rows == 0
    assert sk1.ids(table, HOUR_8) == []
    assert sk1.ids(table, HOUR_9) == hour_9_before
    # Emptying a unit that is already empty changes nothing.
    again = overwrite_unit(table, sk1.empty(), Unit(DAY_1, 8))
    assert sk1.ids(table, HOUR_8) == []
    assert sk1.ids(table, HOUR_9) == hour_9_before
    note(lake, "Q4", f"empty input: snapshots {result.operations}; again {again.operations}")


def test_q4_rows_of_another_unit_are_refused(lake: Lake) -> None:
    table = sk1.create(lake, "sk1_q4_unit")

    with pytest.raises(UnitMismatchError):
        overwrite_unit(table, sk1.rows(DAY_1, 9), Unit(DAY_1, 8))
    with pytest.raises(UnitMismatchError):
        overwrite_unit(table, sk1.rows(DAY_2, 8), Unit(DAY_1))

    assert table.current_snapshot() is None


def test_q4_failed_audit_commits_nothing(lake: Lake) -> None:
    table = sk1.create(lake, "sk1_q4_audit")
    live = overwrite_unit(table, sk1.rows(DAY_1, 8), Unit(DAY_1, 8))
    commits_before = sk1.commits_so_far(table)
    ids_before = sk1.ids(table)

    def audit(data: pa.Table) -> None:
        raise AssertionError(f"audit failed on {data.num_rows} rows")

    with pytest.raises(AssertionError, match="audit failed"):
        overwrite_unit(table, sk1.rows(DAY_1, 8, version=2), Unit(DAY_1, 8), audit=audit)

    # The previous version of the unit stays live (gate G3).
    reloaded = lake.catalog.load_table(table.name())
    assert reloaded.current_snapshot().snapshot_id == live.snapshot_id
    assert sk1.commits_so_far(reloaded) == commits_before
    assert sk1.ids(reloaded) == ids_before


# --- Q5 ------------------------------------------------------------------------------------

HOURS = range(24)


def _table_with_two_days(lake: Lake, name: str) -> Table:
    table = sk1.create(lake, name)
    overwrite_unit(table, sk1.rows(DAY_1, 8), Unit(DAY_1, 8))
    overwrite_unit(table, sk1.rows(DAY_1, 9), Unit(DAY_1, 9))
    overwrite_unit(table, sk1.rows(DAY_2, 8), Unit(DAY_2, 8))
    return table


def test_q5a_day_as_hourly_appends_in_one_transaction(lake: Lake) -> None:
    """Pattern (a): one transaction deletes the day and appends it hour by hour."""
    table = _table_with_two_days(lake, "sk1_q5a")
    day_2_before = sk1.ids(table, ONLY_DAY_2)
    commits_before = sk1.commits_so_far(table)
    snapshots_before = len(table.metadata.snapshots)
    expected: list[int] = []

    started = time.perf_counter()
    with table.transaction() as transaction:
        transaction.delete(delete_filter=Unit(DAY_1).row_filter)
        for hour in HOURS:
            hour_rows = sk1.rows(DAY_1, hour, version=2)
            expected += hour_rows["id"].to_pylist()
            transaction.append(hour_rows)
    elapsed = time.perf_counter() - started

    # Atomic: one catalog commit. But one snapshot per append, and no compaction.
    assert sk1.commits_so_far(table) == commits_before + 1
    new_snapshots = len(table.metadata.snapshots) - snapshots_before
    assert new_snapshots == 1 + len(HOURS)
    assert sk1.ids(table, ONLY_DAY_1) == sorted(expected)
    assert sk1.ids(table, ONLY_DAY_2) == day_2_before
    day_1_files = [path for path in sk1.data_files(table) if "slot_date=2026-11-24" in path]
    assert len(day_1_files) == len(HOURS)
    note(
        lake,
        "Q5a",
        f"1 commit, {new_snapshots} snapshots, {len(day_1_files)} files, {elapsed * 1000:.0f} ms",
    )


def test_q5b_day_streamed_into_one_file_then_added(lake: Lake) -> None:
    """Pattern (b): stream the day into one Parquet file, then delete-and-add in one commit."""
    table = _table_with_two_days(lake, "sk1_q5b")
    day_2_before = sk1.ids(table, ONLY_DAY_2)
    day_2_files = {path for path in sk1.data_files(table) if "slot_date=2026-11-25" in path}
    commits_before = sk1.commits_so_far(table)
    expected = sorted(
        identifier
        for hour in HOURS
        for identifier in sk1.rows(DAY_1, hour, version=2)["id"].to_pylist()
    )

    started = time.perf_counter()
    result = overwrite_day_streaming(
        table, DAY_1, (sk1.rows(DAY_1, hour, version=2) for hour in HOURS)
    )
    elapsed = time.perf_counter() - started

    # Atomic, two snapshots, and the day is one file: bounded memory and compaction together.
    assert sk1.commits_so_far(table) == commits_before + 1
    assert result.operations == ("delete", "append")
    assert result.rows == len(expected)
    assert sk1.ids(table, ONLY_DAY_1) == expected
    assert sk1.ids(table, ONLY_DAY_2) == day_2_before
    files = sk1.data_files(table)
    (day_1_file,) = [path for path in files if "slot_date=2026-11-24" in path]
    assert files[day_1_file] == len(expected)
    assert day_2_files <= set(files)
    # The file carries the table's field ids and its codec, like a file PyIceberg wrote itself.
    with table.io.new_input(day_1_file).open() as stream:
        parquet = pq.ParquetFile(stream)
        assert parquet.schema_arrow.field("id").metadata[b"PARQUET:field_id"] == b"3"
        assert parquet.metadata.row_group(0).column(0).compression == "ZSTD"
        assert parquet.metadata.num_row_groups == len(HOURS)
    note(lake, "Q5b", f"1 commit, snapshots {result.operations}, 1 file, {elapsed * 1000:.0f} ms")

    # A rerun gives the same content.
    checksum = content_checksum(whole_table(table))
    overwrite_day_streaming(
        table, DAY_1, (sk1.rows(DAY_1, hour, version=2, run="run-2") for hour in HOURS)
    )
    assert content_checksum(whole_table(table)) == checksum

    # A later hourly overwrite still works on the streamed file.
    replacement = sk1.rows(DAY_1, 8, count=2, version=3)
    overwrite_unit(table, replacement, Unit(DAY_1, 8))
    assert sk1.ids(table, HOUR_8) == sorted(replacement["id"].to_pylist())
    assert len(sk1.ids(table, ONLY_DAY_1)) == len(expected) - 5 + 2

    # An empty stream empties the day and leaves no file behind.
    files_on_storage = storage_files(table)
    emptied = overwrite_day_streaming(table, DAY_1, iter(()))
    assert emptied.rows == 0
    assert sk1.ids(table, ONLY_DAY_1) == []
    assert sk1.ids(table, ONLY_DAY_2) == day_2_before
    assert not [
        path for path in storage_files(table) - files_on_storage if path.endswith("-day.parquet")
    ]


def test_q5b_failed_audit_leaves_the_day_untouched(lake: Lake) -> None:
    table = _table_with_two_days(lake, "sk1_q5b_audit")
    ids_before = sk1.ids(table)
    commits_before = sk1.commits_so_far(table)
    files_before = storage_files(table)

    def audit(rows_written: int) -> None:
        raise AssertionError(f"row total {rows_written} does not match the ledger")

    with pytest.raises(AssertionError, match="does not match"):
        overwrite_day_streaming(
            table, DAY_1, (sk1.rows(DAY_1, hour) for hour in (8, 9)), audit_totals=audit
        )

    reloaded = lake.catalog.load_table(table.name())
    assert sk1.commits_so_far(reloaded) == commits_before
    assert sk1.ids(reloaded) == ids_before
    assert storage_files(reloaded) == files_before


@pytest.mark.xfail(
    strict=True,
    raises=NotImplementedError,
    reason="PyIceberg 0.12.0 cannot stream a RecordBatchReader into a partitioned table (F5)",
)
def test_rejected_streaming_reader_into_a_partitioned_table(local_lake: Lake) -> None:
    table = sk1.create(local_lake, "sk1_rejected_stream")
    reader = pa.RecordBatchReader.from_batches(sk1.ARROW_SCHEMA, sk1.rows(DAY_1, 8).to_batches())

    table.overwrite(reader, overwrite_filter=Unit(DAY_1, 8).row_filter)

    assert len(sk1.ids(table, HOUR_8)) == 5


@pytest.mark.xfail(
    strict=True,
    reason="dynamic_partition_overwrite takes the partitions from the rows and does nothing on "
    "empty input, so it cannot empty a unit (finding F5)",
)
def test_rejected_dynamic_partition_overwrite_cannot_empty_a_unit(local_lake: Lake) -> None:
    table = sk1.create(local_lake, "sk1_rejected_dynamic")
    overwrite_unit(table, sk1.rows(DAY_1, 8), Unit(DAY_1, 8))

    table.dynamic_partition_overwrite(sk1.empty())

    assert sk1.ids(table, ONLY_DAY_1) == []


# --- Q6 ------------------------------------------------------------------------------------


def test_q6_audit_on_a_branch_then_publish(lake: Lake) -> None:
    table = sk1.create(lake, "sk1_q6")
    overwrite_unit(table, sk1.rows(DAY_1, 8), Unit(DAY_1, 8))
    overwrite_unit(table, sk1.rows(DAY_1, 9), Unit(DAY_1, 9))
    main_before = table.current_snapshot().snapshot_id
    hour_8_before = sk1.ids(table, HOUR_8)
    hour_9_before = sk1.ids(table, HOUR_9)

    # Stage on a branch cut from main.
    table.manage_snapshots().create_branch(snapshot_id=main_before, branch_name="audit").commit()
    replacement = sk1.rows(DAY_1, 8, count=2, version=2)
    with table.transaction() as transaction:
        transaction.overwrite(
            replacement, overwrite_filter=Unit(DAY_1, 8).row_filter, branch="audit"
        )
    new_ids = sorted(replacement["id"].to_pylist())

    # Main is unchanged; the branch holds the whole table with the hour replaced.
    assert table.current_snapshot().snapshot_id == main_before
    assert sk1.ids(table, HOUR_8) == hour_8_before
    assert sk1.ids(table, HOUR_8, ref="audit") == new_ids
    assert sk1.ids(table, HOUR_9, ref="audit") == hour_9_before

    # Publish: there is no fast-forward, so main is pointed at the branch head.
    table.manage_snapshots().set_current_snapshot(ref_name="audit").commit()
    assert table.current_snapshot().snapshot_id == table.refs()["audit"].snapshot_id
    assert sk1.ids(table, HOUR_8) == new_ids
    assert sk1.ids(table, HOUR_9) == hour_9_before

    table.manage_snapshots().remove_branch("audit").commit()
    assert "audit" not in table.refs()
    assert sk1.ids(lake.catalog.load_table(table.name()), HOUR_8) == new_ids
    note(
        lake,
        "Q6",
        "branch write left main unchanged; set_current_snapshot(ref_name=...) published it",
    )


# --- Q7 ------------------------------------------------------------------------------------


def test_q7_expiry_keeps_the_last_commits_and_removes_no_files(lake: Lake) -> None:
    table = sk1.create(lake, "sk1_q7")
    results = [
        overwrite_unit(table, sk1.rows(DAY_1, 8, version=version), Unit(DAY_1, 8))
        for version in range(1, 9)
    ]
    # A tag on the oldest commit: a tagged snapshot must survive.
    tagged = results[0].snapshot_id
    table.manage_snapshots().create_tag(snapshot_id=tagged, tag_name="keep-me").commit()

    commits = commits_on_main(table)
    assert [commit.commit_id for commit in commits] == [
        result.commit_id for result in reversed(results)
    ]
    assert [len(commit.snapshot_ids) for commit in commits] == [2, 2, 2, 2, 2, 2, 2, 1]
    snapshots_before = len(table.metadata.snapshots)
    files_before = storage_files(table)
    checksum_before = content_checksum(whole_table(table))

    # With the default age nothing on a fresh table is old enough.
    assert expire_snapshots(table).expired_snapshot_ids == ()

    in_a_minute = datetime.now(UTC) + timedelta(minutes=1)
    result = expire_snapshots(table, older_than=timedelta(0), keep_last_commits=5, now=in_a_minute)

    remaining = {snapshot.snapshot_id for snapshot in table.metadata.snapshots}
    kept = {snapshot_id for commit in commits[:5] for snapshot_id in commit.snapshot_ids}
    old = {snapshot_id for commit in commits[5:] for snapshot_id in commit.snapshot_ids}
    # The newest five commits are whole, the tagged snapshot survives, the rest is gone.
    assert kept <= remaining
    assert remaining == kept | {tagged}
    assert set(result.expired_snapshot_ids) == old - {tagged}
    assert [commit.commit_id for commit in result.kept_commits] == [
        commit.commit_id for commit in commits[:5]
    ]
    assert all(
        snapshot.summary[COMMIT_ID_PROPERTY]
        for snapshot in table.metadata.snapshots
        if snapshot.summary
    )
    # Current data is intact ...
    assert content_checksum(whole_table(lake.catalog.load_table(table.name()))) == checksum_before
    # ... and expiry removed no file: only metadata entries (finding F5).
    files_after = storage_files(table)
    assert files_before <= files_after
    note(
        lake,
        "Q7",
        f"snapshots {snapshots_before} -> {len(remaining)}; "
        f"files on storage {len(files_before)} -> {len(files_after)} (none removed)",
    )
