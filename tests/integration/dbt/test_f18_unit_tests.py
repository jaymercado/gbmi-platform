"""Finding F18 of the week 3 plan: dbt unit tests run on dbt-clickhouse 1.10.4 and ClickHouse 26.8.

The unit tests of the main project need the pinned lake tables, so they run in ``make demo``.
The test here gives the workflow ``dbt`` the same proof without a lake: one model of the test
project with two unit tests, built by a user who holds exactly the privileges of the role
``dbt_rw`` on one database.

The user has no grant for temporary tables. The adapter creates one temporary table for each
unit test, and ClickHouse allows the statement to a user who may create a table in a database
(the week 3 plan expected a missing grant; the run of 2026-11-28 showed that none is needed).
"""

from __future__ import annotations

import json
import os
import subprocess
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
from clickhouse_connect.driver.client import Client

from tests.integration.dbt.test_sk3_partition_replace import (
    DBT_RW,
    PROFILES_DIR,
    PROJECT_DIR,
    dbt_executable,
)

pytestmark = pytest.mark.integration

MODEL = "f18_snapshot_versions"
PASSING = "f18_a_b_a_gives_three_intervals"
FAILING = "f18_a_wrong_expectation_fails"


@dataclass(frozen=True)
class Project:
    """One database with the source table, one user, and the means to run dbt as the user."""

    database: str
    user: str
    password: str
    work_dir: Path

    def dbt(self, *arguments: str) -> tuple[int, dict[str, str], str]:
        """Run dbt; give the exit code, the status of each node and the end of the output."""
        environment = {
            **os.environ,
            "DBT_CH_HOST": os.environ.get("DBT_CH_HOST", "localhost"),
            "DBT_CH_PORT": os.environ.get("DBT_CH_PORT", "8123"),
            "DBT_CH_USER": self.user,
            "DBT_CH_PASSWORD": self.password,
            "DBT_CH_SCHEMA": self.database,
            "DBT_TARGET_PATH": str(self.work_dir / "target"),
            "DBT_LOG_PATH": str(self.work_dir / "logs"),
        }
        command = [
            dbt_executable(), *arguments,
            "--project-dir", str(PROJECT_DIR),
            "--profiles-dir", str(PROFILES_DIR),
            "--vars", json.dumps({"sk3_database": self.database}),
        ]  # fmt: skip
        result = subprocess.run(
            command, env=environment, capture_output=True, text=True, check=False
        )
        results = json.loads(
            (self.work_dir / "target" / "run_results.json").read_text(encoding="utf-8")
        )
        statuses = {
            entry["unique_id"].rsplit(".", 1)[-1]: entry["status"] for entry in results["results"]
        }
        return result.returncode, statuses, result.stdout[-3000:]


@pytest.fixture
def project(clickhouse_admin: Client, tmp_path: Path) -> Iterator[Project]:
    database = f"sk3_{uuid.uuid4().hex[:10]}"
    user, password = f"{database}_rw", uuid.uuid4().hex
    clickhouse_admin.command(f"CREATE DATABASE {database}")
    clickhouse_admin.command(
        f"CREATE TABLE {database}.source_snapshots "
        f"(cik Int64, snapshot_date Date32, attr_hash String, name String) "
        f"ENGINE = MergeTree ORDER BY (cik, snapshot_date)"
    )
    clickhouse_admin.command(
        f"CREATE USER {user} IDENTIFIED WITH sha256_password BY %(password)s",
        parameters={"password": password},
    )
    clickhouse_admin.command(f"GRANT {DBT_RW} ON {database}.* TO {user}")
    try:
        yield Project(database, user, password, tmp_path)
    finally:
        clickhouse_admin.command(f"DROP DATABASE IF EXISTS {database}")
        clickhouse_admin.command(f"DROP USER IF EXISTS {user}")


def test_a_unit_test_passes_before_its_model_is_built(
    project: Project, clickhouse_admin: Client
) -> None:
    code, statuses, output = project.dbt("build", "--select", MODEL, "--exclude", FAILING)

    assert code == 0, output
    assert statuses == {PASSING: "pass", MODEL: "success"}
    # The user of the test holds the privileges of dbt_rw and no grant for temporary tables.
    grants = clickhouse_admin.query(f"SHOW GRANTS FOR {project.user}").result_rows
    assert not [row[0] for row in grants if "TEMPORARY" in row[0]]
    # The made-up rows of the unit test never reach the table of the model.
    assert clickhouse_admin.query(
        f"SELECT count() FROM {project.database}.{MODEL}"
    ).result_rows == [(0,)]


def test_a_unit_test_with_a_wrong_expectation_fails(project: Project) -> None:
    code, statuses, output = project.dbt("test", "--select", FAILING)

    assert code != 0, output
    assert statuses == {FAILING: "fail"}


def test_a_build_can_leave_out_the_unit_tests(project: Project) -> None:
    # What the DAG marts_build does: the exclusion keeps every unit test out of a build.
    code, statuses, output = project.dbt(
        "build", "--select", MODEL, "--exclude", "resource_type:unit_test"
    )

    assert code == 0, output
    assert statuses == {MODEL: "success"}
