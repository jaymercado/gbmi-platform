"""Connections to the operational database (PostgreSQL schema ``ops``, blueprint 7.3).

One helper opens every connection, so the session settings are the same everywhere: the session
time zone is UTC (blueprint 7.11), and the application name tells an operator who holds a lock.
"""

from __future__ import annotations

import psycopg
from psycopg.rows import TupleRow

from gbmi.config import get_settings

APPLICATION_NAME = "gbmi"

Connection = psycopg.Connection[TupleRow]


class LedgerConfigurationError(RuntimeError):
    """No connection string for the operational database is configured."""


def resolve_dsn(dsn: str | None = None) -> str:
    """The connection string to use: the argument, or the setting ``GBMI_PG_DSN``."""
    if dsn is not None:
        return dsn
    configured = get_settings().pg_dsn
    if configured is None:
        raise LedgerConfigurationError(
            "GBMI_PG_DSN is not set, so the operational database is unknown"
        )
    return configured.get_secret_value()


def connect(dsn: str | None = None, *, autocommit: bool = False) -> Connection:
    """Open a connection whose session time zone is UTC."""
    return psycopg.connect(
        resolve_dsn(dsn),
        autocommit=autocommit,
        application_name=APPLICATION_NAME,
        options="-c timezone=UTC",
    )
