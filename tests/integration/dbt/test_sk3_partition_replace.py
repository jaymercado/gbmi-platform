"""Spike SK3, part A - the incremental contract (week 1 plan, WP-08; risk R2).

Can a partition-replaced mart be rebuilt idempotently, one partition at a time? Six tests,
T1 to T6, run against a real ClickHouse for a model partitioned by day and one partitioned by
month, over a source of three days in two months.

Two strategies are exercised:

* ``custom`` - the ``partition_replace`` materialisation in ``dbt/macros/materializations``.
  All six tests must pass; this is the permanent build-twice test of blueprint 7.7, item 6.
* ``stock`` - the adapter's own ``insert_overwrite`` strategy. It replaces only the partitions
  present in the new data, so T4 is a strict expected failure: if a later adapter release
  empties a partition whose rebuild returns no rows, that test passes and the suite fails.

The tests need ClickHouse and the dbt environment: ``make integration`` locally, the ``dbt``
workflow in CI.
"""

from __future__ import annotations

import json
import os
import subprocess
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from clickhouse_connect.driver.client import Client

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[3]
PROJECT_DIR = Path(__file__).resolve().parent / "project"
PROFILES_DIR = REPO_ROOT / "dbt" / "profiles"

OCT_30 = date(2026, 10, 30)
OCT_31 = date(2026, 10, 31)
NOV_01 = date(2026, 11, 1)

#: The privileges of the dbt_rw role (db/clickhouse/init/02_roles.sql), on one database.
DBT_RW = (
    "SELECT, INSERT, ALTER, CREATE TABLE, CREATE VIEW, CREATE DICTIONARY, DROP TABLE, DROP VIEW, "
    "DROP DICTIONARY, TRUNCATE, OPTIMIZE, SHOW, CREATE DATABASE"
)
#: The same without ALTER: REPLACE PARTITION needs ALTER DELETE, so the swap step fails.
NO_ALTER = (
    "SELECT, INSERT, CREATE TABLE, CREATE VIEW, CREATE DICTIONARY, DROP TABLE, DROP VIEW, "
    "DROP DICTIONARY, TRUNCATE, SHOW, CREATE DATABASE"
)


@dataclass(frozen=True)
class Grain:
    """How a model is partitioned and how its partitions are named for dbt and ClickHouse."""

    name: str

    def value(self, day: date) -> str | int:
        """The partition value passed to dbt in the ``partitions`` variable."""
        return day.isoformat() if self.name == "day" else day.year * 100 + day.month

    def partition_id(self, day: date) -> str:
        """The ClickHouse partition id of the partition that holds ``day``.

        ``slot_date`` is a Date32, as Iceberg dates are in ClickHouse, and the partition id of a
        Date32 is its day number since 1970 (20756 for 2026-10-30), not YYYYMMDD as for a Date.
        """
        if self.name == "day":
            return str((day - date(1970, 1, 1)).days)
        return day.strftime("%Y%m")


def dbt_executable() -> str:
    for candidate in (
        os.environ.get("DBT_EXECUTABLE"),
        "/opt/venvs/dbt/bin/dbt",
        str(REPO_ROOT / "dbt" / ".venv" / "bin" / "dbt"),
    ):
        if candidate and Path(candidate).exists():
            return candidate
    pytest.skip("no dbt executable: run make setup, or use make integration")


@dataclass
class Sk3:
    """One test case: a database, a source table, a model and the means to run dbt on it."""

    admin: Client
    database: str
    model: str
    grain: Grain
    host: str
    port: str
    users: dict[str, str]
    work_dir: Path

    # --- source -------------------------------------------------------------------------

    def load_source(self, rows: list[tuple[date, int, str, float]]) -> None:
        self.admin.command(f"TRUNCATE TABLE {self.database}.source_events")
        if rows:
            self.admin.insert(
                f"{self.database}.source_events",
                rows,
                column_names=["slot_date", "id", "category", "amount"],
            )

    def expected(self, where: str = "1 = 1") -> list[tuple[Any, ...]]:
        """What the model should hold, computed straight from the source."""
        return self.admin.query(
            f"SELECT slot_date, category, count(), round(sum(amount), 6), max(id) "
            f"FROM {self.database}.source_events WHERE {where} GROUP BY slot_date, category "
            f"ORDER BY slot_date, category"
        ).result_rows

    # --- model --------------------------------------------------------------------------

    def rows(self, where: str = "1 = 1") -> list[tuple[Any, ...]]:
        return self.admin.query(
            f"SELECT slot_date, category, events, round(amount, 6), max_id "
            f"FROM {self.database}.{self.model} WHERE {where} ORDER BY slot_date, category"
        ).result_rows

    def partitions(self) -> dict[str, tuple[int, int]]:
        """Row count and checksum of every partition of the model."""
        result = self.admin.query(
            f"SELECT _partition_id, count(), "
            f"sum(cityHash64(slot_date, category, events, round(amount, 6), max_id)) "
            f"FROM {self.database}.{self.model} GROUP BY _partition_id ORDER BY _partition_id"
        ).result_rows
        return {row[0]: (row[1], row[2]) for row in result}

    def staging_tables(self) -> list[str]:
        result = self.admin.query(
            "SELECT name FROM system.tables "
            "WHERE database = %(database)s AND name LIKE %(pattern)s",
            parameters={"database": self.database, "pattern": f"{self.model}__dbt_new_data%"},
        ).result_rows
        return [row[0] for row in result]

    # --- dbt ----------------------------------------------------------------------------

    def run(
        self,
        partitions: list[date] | None = None,
        *,
        full_refresh: bool = False,
        user: str = "rw",
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        variables: dict[str, Any] = {"sk3_database": self.database}
        if partitions is not None:
            variables["partitions"] = sorted({self.grain.value(day) for day in partitions}, key=str)
        command = [
            dbt_executable(), "run",
            "--project-dir", str(PROJECT_DIR),
            "--profiles-dir", str(PROFILES_DIR),
            "--select", self.model,
            "--vars", json.dumps(variables),
        ]  # fmt: skip
        if full_refresh:
            command.append("--full-refresh")
        environment = {
            **os.environ,
            "DBT_CH_HOST": self.host,
            "DBT_CH_PORT": self.port,
            "DBT_CH_USER": f"{self.database}_{user}",
            "DBT_CH_PASSWORD": self.users[user],
            "DBT_CH_SCHEMA": self.database,
            "DBT_TARGET_PATH": str(self.work_dir / "target"),
            "DBT_LOG_PATH": str(self.work_dir / "logs"),
        }
        result = subprocess.run(
            command, env=environment, capture_output=True, text=True, check=False
        )
        if check and result.returncode != 0:
            pytest.fail(f"dbt run failed:\n{result.stdout[-3000:]}\n{result.stderr[-1000:]}")
        return result


SOURCE_ROWS: list[tuple[date, int, str, float]] = [
    (OCT_30, 1, "a", 1.5), (OCT_30, 2, "a", 2.5), (OCT_30, 3, "b", 4.0),
    (OCT_31, 4, "a", 8.0), (OCT_31, 5, "c", 16.25),
    (NOV_01, 6, "b", 32.0), (NOV_01, 7, "b", 64.5), (NOV_01, 8, "c", 128.0),
]  # fmt: skip


@pytest.fixture(params=["day", "month"])
def grain(request: pytest.FixtureRequest) -> Grain:
    return Grain(request.param)


@pytest.fixture
def make_case(clickhouse_admin: Client, grain: Grain, tmp_path: Path) -> Iterator[Any]:
    """Factory for a test case with its own database and two users (with and without ALTER)."""
    host = os.environ.get("DBT_CH_HOST", "localhost")
    port = os.environ.get("DBT_CH_PORT", "8123")
    created: list[str] = []

    def build(strategy: str) -> Sk3:
        database = f"sk3_{uuid.uuid4().hex[:10]}"
        created.append(database)
        users = {"rw": uuid.uuid4().hex, "noalter": uuid.uuid4().hex}
        clickhouse_admin.command(f"CREATE DATABASE {database}")
        clickhouse_admin.command(
            f"CREATE TABLE {database}.source_events "
            f"(slot_date Date32, id UInt64, category String, amount Float64) "
            f"ENGINE = MergeTree ORDER BY (slot_date, id)"
        )
        for user, privileges in (("rw", DBT_RW), ("noalter", NO_ALTER)):
            name = f"{database}_{user}"
            clickhouse_admin.command(
                f"CREATE USER {name} IDENTIFIED WITH sha256_password BY %(password)s",
                parameters={"password": users[user]},
            )
            clickhouse_admin.command(f"GRANT {privileges} ON {database}.* TO {name}")
        case = Sk3(
            admin=clickhouse_admin,
            database=database,
            model=f"sk3_{strategy}_by_{grain.name}",
            grain=grain,
            host=host,
            port=port,
            users=users,
            work_dir=tmp_path,
        )
        case.load_source(SOURCE_ROWS)
        return case

    try:
        yield build
    finally:
        for database in created:
            clickhouse_admin.command(f"DROP DATABASE IF EXISTS {database}")
            for user in ("rw", "noalter"):
                clickhouse_admin.command(f"DROP USER IF EXISTS {database}_{user}")


@pytest.fixture
def custom(make_case: Any) -> Sk3:
    return make_case("custom")


@pytest.fixture
def stock(make_case: Any) -> Sk3:
    return make_case("stock")


# --- The partition_replace materialisation: all six must pass ---------------------------------


def test_t1_first_build_creates_every_partition(custom: Sk3) -> None:
    custom.run()

    assert custom.rows() == custom.expected()
    assert set(custom.partitions()) == {
        custom.grain.partition_id(day) for day in (OCT_30, OCT_31, NOV_01)
    }


def test_t2_identical_rebuild_changes_nothing(custom: Sk3) -> None:
    custom.run()
    first = custom.partitions()

    custom.run([OCT_30, OCT_31, NOV_01])
    second = custom.partitions()
    custom.run([NOV_01])

    # Same count and checksum per partition, and no duplicated grain.
    assert second == first
    assert custom.partitions() == first
    rows = custom.rows()
    assert rows == custom.expected()
    assert len({(row[0], row[1]) for row in rows}) == len(rows)


def test_t3_only_the_requested_partition_changes(custom: Sk3) -> None:
    custom.run()
    before = custom.partitions()
    requested = custom.grain.partition_id(NOV_01)
    not_requested = custom.grain.partition_id(OCT_30)

    # The input changes in two partitions; only one of them is requested.
    changed = [
        (day, i, cat, amount * 3 if day in (NOV_01, OCT_30) else amount)
        for day, i, cat, amount in SOURCE_ROWS
    ]
    custom.load_source([*changed, (NOV_01, 9, "z", 1.0)])
    custom.run([NOV_01])
    after = custom.partitions()

    assert after[requested] != before[requested]
    assert custom.rows("slot_date = '2026-11-01'") == custom.expected("slot_date = '2026-11-01'")
    # The partition nobody asked for keeps its old rows, although its input changed too.
    assert after[not_requested] == before[not_requested]
    assert {key: value for key, value in after.items() if key != requested} == {
        key: value for key, value in before.items() if key != requested
    }


def test_t4_partition_whose_input_became_empty_ends_empty(custom: Sk3) -> None:
    custom.run()
    before = custom.partitions()
    emptied = custom.grain.partition_id(NOV_01)

    custom.load_source([row for row in SOURCE_ROWS if row[0] != NOV_01])
    custom.run([NOV_01])
    after = custom.partitions()

    assert emptied not in after
    assert custom.rows("slot_date = '2026-11-01'") == []
    assert after == {key: value for key, value in before.items() if key != emptied}
    assert custom.staging_tables() == []


def test_t5_failed_replace_leaves_the_target_unchanged(custom: Sk3) -> None:
    custom.run()
    before = custom.partitions()
    custom.load_source([(day, i, cat, amount + 100) for day, i, cat, amount in SOURCE_ROWS])

    # A user without ALTER can build the staging table but cannot replace the partition.
    failed = custom.run([NOV_01], user="noalter", check=False)

    assert failed.returncode != 0
    assert "ACCESS_DENIED" in failed.stdout or "Not enough privileges" in failed.stdout
    assert custom.partitions() == before
    # The staging table of the failed run is left behind, under a name with its invocation id.
    left_over = custom.staging_tables()
    assert len(left_over) == 1

    # The next normal run is not disturbed by it and gives the correct result.
    custom.run([NOV_01])
    assert custom.rows("slot_date = '2026-11-01'") == custom.expected("slot_date = '2026-11-01'")
    assert custom.staging_tables() == left_over


def test_t6_full_refresh_equals_the_incremental_result(custom: Sk3) -> None:
    custom.run()
    custom.load_source([(day, i, cat, amount * 2) for day, i, cat, amount in SOURCE_ROWS])
    custom.run([OCT_30, OCT_31, NOV_01])
    incremental = (custom.rows(), custom.partitions())

    custom.run(full_refresh=True)

    assert (custom.rows(), custom.partitions()) == incremental
    assert custom.rows() == custom.expected()


def test_model_that_ignores_the_requested_partitions_is_refused(make_case: Any) -> None:
    """Without this guard, a model with no partition filter would replace every partition."""
    unfiltered: Sk3 = make_case("unfiltered")
    unfiltered.run()
    before = unfiltered.partitions()
    unfiltered.load_source([(day, i, cat, amount + 1) for day, i, cat, amount in SOURCE_ROWS])

    result = unfiltered.run([OCT_30], check=False)

    assert result.returncode != 0
    assert "outside the requested" in result.stdout
    assert unfiltered.partitions() == before
    assert unfiltered.staging_tables() == []


def test_partition_run_without_partitions_is_an_error(custom: Sk3) -> None:
    custom.run()

    result = custom.run(check=False)

    assert result.returncode != 0
    assert "needs the partitions to replace" in result.stdout


def test_empty_partition_list_is_a_no_op(custom: Sk3) -> None:
    custom.run()
    before = custom.partitions()
    custom.load_source([(day, i, cat, amount + 1) for day, i, cat, amount in SOURCE_ROWS])

    custom.run([])

    assert custom.partitions() == before


# --- The adapter's insert_overwrite: why it was not adopted -----------------------------------


def test_stock_t1_to_t3_pass(stock: Sk3) -> None:
    stock.run()
    first = stock.partitions()
    assert stock.rows() == stock.expected()

    stock.run([OCT_30, OCT_31, NOV_01])
    assert stock.partitions() == first

    stock.load_source([*SOURCE_ROWS, (NOV_01, 9, "z", 1.0)])
    stock.run([NOV_01])
    assert stock.rows() == stock.expected()


@pytest.mark.xfail(
    strict=True,
    reason="dbt-clickhouse 1.10.4 insert_overwrite replaces only the partitions present in the new "
    "data, so a partition whose rebuild returns no rows keeps its old rows (finding F4)",
)
def test_stock_t4_partition_whose_input_became_empty_ends_empty(stock: Sk3) -> None:
    stock.run()
    stock.load_source([row for row in SOURCE_ROWS if row[0] != NOV_01])

    stock.run([NOV_01])

    assert stock.rows("slot_date = '2026-11-01'") == []
