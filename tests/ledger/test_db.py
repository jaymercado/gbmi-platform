"""Connections to the operational database and throwaway databases."""

from __future__ import annotations

import pytest

from gbmi.config import get_settings
from gbmi.ledger.db import APPLICATION_NAME, LedgerConfigurationError, connect, resolve_dsn
from gbmi.ledger.scratch import ADMIN_DSN_VARIABLE, PREFIX, scratch_database
from gbmi.ledger.scratch import admin_dsn as read_admin_dsn


def test_a_session_runs_in_utc_under_the_application_name(ops_db: str) -> None:
    with connect(ops_db) as connection:
        row = connection.execute(
            "SELECT current_setting('TimeZone'), current_setting('application_name')"
        ).fetchone()

    assert row == ("UTC", APPLICATION_NAME)


def test_the_connection_string_comes_from_the_argument_or_the_setting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GBMI_PG_DSN", raising=False)
    get_settings.cache_clear()
    try:
        assert resolve_dsn("postgresql://given") == "postgresql://given"
        with pytest.raises(LedgerConfigurationError, match="GBMI_PG_DSN is not set"):
            resolve_dsn()

        monkeypatch.setenv("GBMI_PG_DSN", "postgresql://configured")
        get_settings.cache_clear()
        assert resolve_dsn() == "postgresql://configured"
    finally:
        get_settings.cache_clear()


def databases(admin: str) -> set[str]:
    with connect(admin, autocommit=True) as connection:
        rows = connection.execute(
            "SELECT datname FROM pg_database WHERE datname LIKE %s", (PREFIX + "%",)
        ).fetchall()
    return {str(row[0]) for row in rows}


def test_a_throwaway_database_has_the_schema_and_is_dropped_afterwards(admin_dsn: str) -> None:
    before = databases(admin_dsn)

    with scratch_database(admin_dsn) as dsn:
        created = databases(admin_dsn) - before
        assert len(created) == 1
        with connect(dsn, autocommit=True) as connection:
            row = connection.execute(
                "SELECT current_database(), to_regclass('ops.source_slot') IS NOT NULL"
            ).fetchone()
        assert row == (created.pop(), True)

    assert databases(admin_dsn) == before


def test_a_throwaway_database_is_dropped_when_the_caller_fails(admin_dsn: str) -> None:
    before = databases(admin_dsn)

    with pytest.raises(RuntimeError, match="the test body failed"), scratch_database(admin_dsn):
        raise RuntimeError("the test body failed")

    assert databases(admin_dsn) == before


def test_the_administrator_connection_comes_from_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(ADMIN_DSN_VARIABLE, "postgresql://admin@somewhere/postgres")
    assert read_admin_dsn() == "postgresql://admin@somewhere/postgres"

    monkeypatch.delenv(ADMIN_DSN_VARIABLE)
    with pytest.raises(LedgerConfigurationError, match=ADMIN_DSN_VARIABLE):
        read_admin_dsn()
    with pytest.raises(LedgerConfigurationError, match=ADMIN_DSN_VARIABLE), scratch_database():
        pass
