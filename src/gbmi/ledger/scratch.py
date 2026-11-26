"""Throwaway databases for tests, drills and scratch runs of benchmarks (week 2 plan 3.3).

No test, no drill and no scratch run writes to the database ``gbmi`` of a stack. A caller asks
for a new database, gets the schema ``ops`` from the migrations, and the database is dropped
afterwards. The caller needs an administrator connection, because only an administrator can
create a database: the variable ``GBMI_TEST_PG_ADMIN_DSN`` holds the connection string.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from psycopg import sql
from psycopg.conninfo import make_conninfo

from gbmi.ledger.db import LedgerConfigurationError, connect
from gbmi.ledger.migrate import migrate

ADMIN_DSN_VARIABLE = "GBMI_TEST_PG_ADMIN_DSN"
PREFIX = "gbmi_scratch"


def admin_dsn() -> str:
    """The administrator connection string; raises when the variable is not set."""
    value = os.environ.get(ADMIN_DSN_VARIABLE)
    if not value:
        raise LedgerConfigurationError(
            f"{ADMIN_DSN_VARIABLE} is not set: a throwaway database needs an administrator "
            "connection to PostgreSQL (make test and the toolbox container set the variable)"
        )
    return value


@contextmanager
def scratch_database(
    admin: str | None = None, *, migrated: bool = True, directory: Path | None = None
) -> Iterator[str]:
    """A new database with the schema ops; the database is dropped when the block ends."""
    admin = admin or admin_dsn()
    name = f"{PREFIX}_{uuid.uuid4().hex[:12]}"
    with connect(admin, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    dsn = make_conninfo(admin, dbname=name)
    try:
        if migrated:
            migrate(dsn, directory)
        yield dsn
    finally:
        with connect(admin, autocommit=True) as connection:
            connection.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name))
            )
