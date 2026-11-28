"""The five SEC tables as real Iceberg tables, and the units which replace their rows.

Week 3 plan, WP-05 step 5, on the local backend: an overwrite of one date, of a set of
companies and of a whole table; an empty input; rows outside the unit; and ClickHouse reading
each table through a pin, with the column types which the dbt models will rely on.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta

import pyarrow as pa
import pytest
from clickhouse_connect.driver.client import Client
from pyiceberg.table import Table

from gbmi.core.checksum import content_checksum
from gbmi.lake.commit import Scope, UnitMismatchError, overwrite_scope, unit_rows
from gbmi.lake.maintenance import commits_on_main, expire_snapshots
from gbmi.lake.tables import (
    SEC_COMPANY_FORMER_NAME,
    SEC_COMPANY_SNAPSHOT,
    SEC_FILING_DETAIL,
    SEC_FILING_INDEX,
    SEC_PARTITIONS,
    SEC_SCHEMAS,
    SEC_TABLE_KEYS,
    SEC_XBRL_FACT,
    create_sec_table_if_missing,
)
from gbmi.warehouse.pin import pin_table
from tests.integration.conftest import Lake

from .sec_rows import DAY, MATTEL, META, NVIDIA, detail, facts, former_names, index, snapshot

pytestmark = pytest.mark.integration

NEXT_DAY = DAY + timedelta(days=1)


def create(lake: Lake, name: str) -> Table:
    return create_sec_table_if_missing(lake.catalog, lake.track(name)[0], name)


def stored(table: Table, *sort: str) -> pa.Table:
    return table.scan().to_arrow().sort_by([(column, "ascending") for column in sort])


def data_files(table: Table) -> set[str]:
    return {task.file.file_path for task in table.scan().plan_files()}


def test_each_sec_table_has_its_partition_and_takes_and_returns_its_rows(local_lake: Lake) -> None:
    samples = {
        SEC_COMPANY_SNAPSHOT: (
            snapshot(DAY, [META, NVIDIA, MATTEL]),
            Scope.for_date("snapshot_date", DAY),
        ),
        SEC_COMPANY_FORMER_NAME: (former_names([META, NVIDIA]), Scope.for_table()),
        SEC_FILING_INDEX: (index(DAY, [META, NVIDIA, MATTEL]), Scope.for_date("index_date", DAY)),
        SEC_FILING_DETAIL: (detail([META, NVIDIA]), Scope.for_keys("cik", [META, NVIDIA])),
        SEC_XBRL_FACT: (facts([META, MATTEL]), Scope.for_keys("cik", [META, MATTEL])),
    }
    assert set(samples) == set(SEC_SCHEMAS)
    for name, (data, scope) in samples.items():
        table = create(local_lake, name)

        fields = [field.name for field in table.spec().fields]
        assert fields == ([SEC_PARTITIONS[name]] if SEC_PARTITIONS[name] else []), name
        result = overwrite_scope(table, data, scope, run_id="integration", pipeline_version="test")
        held = table.scan().to_arrow()

        assert held.num_rows == data.num_rows == result.rows, name
        assert content_checksum(held) == content_checksum(data), name
        keys = set(zip(*(held[column].to_pylist() for column in SEC_TABLE_KEYS[name]), strict=True))
        assert len(keys) == held.num_rows, name
        snapshot_now = table.current_snapshot()
        assert snapshot_now is not None and snapshot_now.summary is not None
        assert snapshot_now.summary["gbmi.unit"] == scope.label == result.unit, name
        # The table exists already: a second call loads the same table.
        assert create(local_lake, name).metadata_location == table.metadata_location


def test_an_overwrite_of_one_date_leaves_the_other_dates(local_lake: Lake) -> None:
    table = create(local_lake, SEC_COMPANY_SNAPSHOT)
    today, tomorrow = (Scope.for_date("snapshot_date", day) for day in (DAY, NEXT_DAY))
    overwrite_scope(table, snapshot(DAY, [META, NVIDIA, MATTEL]), today)
    overwrite_scope(table, snapshot(NEXT_DAY, [META, NVIDIA]), tomorrow)

    # The next build of the first date knows two companies, with a new name.
    result = overwrite_scope(table, snapshot(DAY, [META, MATTEL], suffix=" Inc"), today)

    held = stored(table, "snapshot_date", "cik")
    assert held["snapshot_date"].to_pylist() == [DAY, DAY, NEXT_DAY, NEXT_DAY]
    assert held["cik"].to_pylist() == [MATTEL, META, NVIDIA, META] or held["cik"].to_pylist() == [
        MATTEL,
        META,
        META,
        NVIDIA,
    ]
    assert held["name"].to_pylist()[:2] == [f"Company {MATTEL} Inc", f"Company {META} Inc"]
    assert (unit_rows(table, today), unit_rows(table, tomorrow)) == (2, 2)
    # A whole date of an identity partition is dropped without a rewrite: delete and append.
    assert result.operations == ("delete", "append")


@pytest.mark.parametrize("name", [SEC_FILING_DETAIL, SEC_XBRL_FACT])
def test_an_overwrite_of_a_set_of_companies_leaves_the_other_companies(
    local_lake: Lake, name: str
) -> None:
    build = detail if name == SEC_FILING_DETAIL else facts
    table = create(local_lake, name)
    overwrite_scope(
        table, build([META, NVIDIA, MATTEL]), Scope.for_keys("cik", [META, NVIDIA, MATTEL])
    )
    before = stored(table, "cik", "accession_number")
    files_before = data_files(table)

    # A refresh of two companies: Meta has new rows, and Nvidia has none any more.
    scope = Scope.for_keys("cik", [META, NVIDIA])
    fresh = (
        detail([META], filings=3, form="10-K")
        if name == SEC_FILING_DETAIL
        else facts([META], count=4, value="9.25")
    )
    result = overwrite_scope(table, fresh, scope)

    held = stored(table, "cik", "accession_number")
    assert sorted(set(held["cik"].to_pylist())) == [MATTEL, META]
    mattel_before = before.filter(pa.compute.equal(before["cik"], MATTEL))
    mattel_after = held.filter(pa.compute.equal(held["cik"], MATTEL))
    assert content_checksum(mattel_after) == content_checksum(mattel_before)
    assert held.filter(pa.compute.equal(held["cik"], META)).num_rows == fresh.num_rows
    assert unit_rows(table, scope) == fresh.num_rows
    if SEC_PARTITIONS[name] == "cik":
        # Change C15: one data file for each company, so a refresh rewrites no file of another
        # company. The file of Mattel is the same file as before.
        kept = files_before & data_files(table)
        assert len(files_before) == 3 and len(kept) == 1
        assert result.operations == ("delete", "append")
    else:
        # Without a partition the one data file holds every company and is written again.
        assert len(files_before) == 1 and not files_before & data_files(table)
        assert "overwrite" in result.operations


def test_an_overwrite_of_the_whole_table_replaces_every_row(local_lake: Lake) -> None:
    table = create(local_lake, SEC_COMPANY_FORMER_NAME)
    overwrite_scope(table, former_names([META, NVIDIA, MATTEL]), Scope.for_table())

    result = overwrite_scope(table, former_names([NVIDIA]), Scope.for_table())

    assert stored(table, "cik")["cik"].to_pylist() == [NVIDIA]
    assert (result.unit, result.rows) == ("table", 1)
    assert unit_rows(table, Scope.for_table()) == 1


def test_an_empty_input_empties_the_unit_and_nothing_else(local_lake: Lake) -> None:
    table = create(local_lake, SEC_FILING_INDEX)
    today, tomorrow = (Scope.for_date("index_date", day) for day in (DAY, NEXT_DAY))
    overwrite_scope(table, index(DAY, [META, NVIDIA]), today)
    overwrite_scope(table, index(NEXT_DAY, [MATTEL]), tomorrow)

    result = overwrite_scope(table, index(DAY, []), today)

    assert result.rows == 0
    assert stored(table, "cik")["index_date"].to_pylist() == [NEXT_DAY]
    assert (unit_rows(table, today), unit_rows(table, tomorrow)) == (0, 1)


def test_rows_outside_the_unit_are_refused_and_the_table_keeps_its_version(
    local_lake: Lake,
) -> None:
    table = create(local_lake, SEC_XBRL_FACT)
    overwrite_scope(table, facts([META]), Scope.for_keys("cik", [META]))
    version = table.metadata_location

    with pytest.raises(UnitMismatchError, match=f"rows of cik {MATTEL}"):
        overwrite_scope(table, facts([META, MATTEL]), Scope.for_keys("cik", [META]))
    snapshots = create(local_lake, SEC_COMPANY_SNAPSHOT)
    with pytest.raises(UnitMismatchError, match="rows of snapshot_date 2026-11-26"):
        overwrite_scope(snapshots, snapshot(NEXT_DAY, [META]), Scope.for_date("snapshot_date", DAY))

    assert local_lake.catalog.load_table(table.name()).metadata_location == version
    assert snapshots.current_snapshot() is None


def test_a_commit_of_three_snapshots_is_one_commit_for_the_maintenance(local_lake: Lake) -> None:
    # Finding RF12: an overwrite of keys can drop one data file, rewrite another and append.
    table = create(local_lake, SEC_FILING_DETAIL)
    overwrite_scope(
        table, detail([META, NVIDIA, MATTEL]), Scope.for_keys("cik", [META, NVIDIA, MATTEL])
    )
    overwrite_scope(table, detail([META], filings=3), Scope.for_keys("cik", [META]))

    # The file of Meta is dropped whole, the file with Nvidia and Mattel is written again
    # without Nvidia, and the new rows are appended: three snapshots in one catalog commit.
    result = overwrite_scope(
        table, detail([META, NVIDIA], filings=1), Scope.for_keys("cik", [META, NVIDIA])
    )

    assert result.operations == ("delete", "overwrite", "append")
    assert len(result.snapshot_ids) == 3
    current = table.current_snapshot()
    assert current is not None and result.snapshot_id == current.snapshot_id
    newest = commits_on_main(table)[0]
    assert set(newest.snapshot_ids) == set(result.snapshot_ids)
    assert newest.commit_id == result.commit_id
    held = stored(table, "cik", "accession_number")
    assert held["cik"].to_pylist() == [MATTEL, MATTEL, NVIDIA, META] or sorted(
        held["cik"].to_pylist()
    ) == sorted([MATTEL, MATTEL, NVIDIA, META])

    expiry = expire_snapshots(
        table,
        older_than=timedelta(0),
        keep_last_commits=1,
        now=datetime.now(UTC) + timedelta(minutes=5),
    )

    # The newest commit stays whole; the snapshots of the two commits before are expired.
    assert expiry.kept_commits == (newest,)
    assert expiry.remaining_snapshots == 3
    assert set(expiry.expired_snapshot_ids).isdisjoint(result.snapshot_ids)
    assert table.scan().to_arrow().num_rows == 4


@pytest.fixture
def pinned_names(clickhouse: Client) -> Iterator[dict[str, str]]:
    names = {name: f"{name}_{uuid.uuid4().hex[:8]}" for name in SEC_SCHEMAS}
    try:
        yield names
    finally:
        for pinned in names.values():
            clickhouse.command(f"DROP TABLE IF EXISTS lake.{pinned}")


#: The ClickHouse type of each kind of column, as the engine Iceberg gives the type.
CLICKHOUSE_TYPES = {
    ("long", True): "Int64",
    ("long", False): "Nullable(Int64)",
    ("int", True): "Int32",
    ("int", False): "Nullable(Int32)",
    ("string", True): "String",
    ("string", False): "Nullable(String)",
    ("date", True): "Date32",
    ("date", False): "Nullable(Date32)",
    ("boolean", True): "Bool",
    ("boolean", False): "Nullable(Bool)",
    ("timestamptz", False): "Nullable(DateTime64(6, 'UTC'))",
    ("list<string>", True): "Array(String)",
    ("decimal(38, 6)", True): "Decimal(38, 6)",
}


def test_clickhouse_reads_each_sec_table_through_a_pin_with_the_expected_types(
    shared_lake: Lake, clickhouse: Client, pinned_names: dict[str, str]
) -> None:
    if shared_lake.backend != "local":
        pytest.skip("the types are read on the local backend; S3 reads are spike SK2")
    samples = {
        SEC_COMPANY_SNAPSHOT: (
            snapshot(DAY, [META, NVIDIA, MATTEL]),
            Scope.for_date("snapshot_date", DAY),
        ),
        SEC_COMPANY_FORMER_NAME: (former_names([META, NVIDIA]), Scope.for_table()),
        SEC_FILING_INDEX: (index(DAY, [META, NVIDIA, MATTEL]), Scope.for_date("index_date", DAY)),
        SEC_FILING_DETAIL: (detail([META, NVIDIA]), Scope.for_keys("cik", [META, NVIDIA])),
        SEC_XBRL_FACT: (facts([META, MATTEL]), Scope.for_keys("cik", [META, MATTEL])),
    }
    for name, (data, scope) in samples.items():
        table = create(shared_lake, name)
        overwrite_scope(table, data, scope)
        pinned = pinned_names[name]
        pin_table(clickhouse, shared_lake.catalog.load_table(table.name()), name=pinned)

        described = clickhouse.query(f"DESCRIBE TABLE lake.{pinned}").result_rows
        count = clickhouse.query(f"SELECT count() FROM lake.{pinned}").result_rows[0][0]

        expected = [
            (field.name, CLICKHOUSE_TYPES[(str(field.field_type), field.required)])
            for field in SEC_SCHEMAS[name].fields
        ]
        assert [(row[0], row[1]) for row in described] == expected, name
        assert count == data.num_rows, name

    row = clickhouse.query(
        f"SELECT sum(length(tickers)), countIf(empty(exchanges)), "
        f"countIf(primary_exchange IS NULL), min(snapshot_date) "
        f"FROM lake.{pinned_names[SEC_COMPANY_SNAPSHOT]}"
    ).result_rows[0]
    assert row == (4, 1, 1, DAY)
    row = clickhouse.query(
        f"SELECT sum(value), countIf(is_instant), countIf(fiscal_period IS NULL), "
        f"min(period_start), uniqExact(cik) FROM lake.{pinned_names[SEC_XBRL_FACT]}"
    ).result_rows[0]
    assert (float(row[0]), row[1], row[2], row[3], row[4]) == (21.0, 2, 2, date(2026, 1, 1), 2)
    row = clickhouse.query(
        f"SELECT sum(length(items)), countIf(report_date IS NULL), max(acceptance_ts), "
        f"countIf(is_xbrl_numeric IS NULL) FROM lake.{pinned_names[SEC_FILING_DETAIL]}"
    ).result_rows[0]
    assert (row[0], row[1], row[3]) == (4, 2, 4)
    assert row[2].isoformat().startswith("2026-11-25T21:54:12")
