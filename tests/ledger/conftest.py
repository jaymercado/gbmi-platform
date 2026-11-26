"""Fixtures for the tests which need PostgreSQL.

The tests work in throwaway databases (week 2 plan 3.3). ``make test`` hands over the
administrator connection of the running stack in ``GBMI_TEST_PG_ADMIN_DSN``, and the workflow
``ci`` hands over a service container. Without the variable the tests fail with the
instruction: a skipped ledger suite would hide the largest part of the coverage gate.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

from gbmi.config import get_settings
from gbmi.ledger.db import Connection, connect
from gbmi.ledger.migrate import migrate
from gbmi.ledger.repository import Ledger
from gbmi.ledger.scratch import ADMIN_DSN_VARIABLE, scratch_database

MIGRATIONS = Path(__file__).resolve().parents[2] / "db" / "postgres" / "migrations"
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "gdelt" / "v2"


@pytest.fixture(scope="session")
def admin_dsn() -> str:
    dsn = os.environ.get(ADMIN_DSN_VARIABLE)
    if not dsn:
        pytest.fail(
            "tests/ledger needs PostgreSQL: run `make test` beside a running stack, "
            f"or set {ADMIN_DSN_VARIABLE}",
            pytrace=False,
        )
    return dsn


@pytest.fixture
def empty_db(admin_dsn: str) -> Iterator[str]:
    """A new database without the schema ops."""
    with scratch_database(admin_dsn, migrated=False) as dsn:
        yield dsn


@pytest.fixture(scope="session")
def ops_db(admin_dsn: str) -> Iterator[str]:
    """One database with every migration applied, shared by the session.

    Tests keep apart through the column ``source``: each test writes under a name of its own.
    """
    with scratch_database(admin_dsn, directory=MIGRATIONS) as dsn:
        yield dsn


@pytest.fixture
def ops(ops_db: str) -> Iterator[Connection]:
    """An autocommit connection to the migrated database."""
    with connect(ops_db, autocommit=True) as connection:
        yield connection


@pytest.fixture
def stack(empty_db: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """Settings which point at a new database, a new lake and the fixture files.

    The task entry points and the commands work on the source ``gdelt`` and read the settings,
    so a test of them takes a database of its own and a lake in a temporary directory.
    """
    migrate(empty_db, MIGRATIONS)
    monkeypatch.setenv("GBMI_PG_DSN", empty_db)
    monkeypatch.setenv("GBMI_LAKE_URI", f"file://{tmp_path}")
    monkeypatch.setenv("GBMI_CATALOG_URI", f"sqlite:///{tmp_path}/catalog/catalog.db")
    monkeypatch.setenv("GBMI_FIXTURES_DIR", str(FIXTURES))
    monkeypatch.setenv("GBMI_PIPELINE_VERSION", "test-version")
    get_settings.cache_clear()
    yield empty_db
    get_settings.cache_clear()


@pytest.fixture
def source() -> str:
    """A source name which no other test uses."""
    return f"test-{uuid.uuid4().hex[:12]}"


@pytest.fixture
def ledger(ops: Connection, source: str) -> Ledger:
    """A ledger which sees only the slots of the test."""
    return Ledger(ops, source=source, run_id="test-run")
