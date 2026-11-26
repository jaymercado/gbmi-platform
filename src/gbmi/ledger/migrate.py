"""Forward-only SQL migrations for the operational database (week 2 plan 3.3).

The files ``db/postgres/migrations/NNNN_name.sql`` are applied in number order. Each file runs in
one transaction together with its row in ``ops.schema_migration``, so a failing file leaves no
trace. The row holds the SHA-256 hash of the file. A file which changed after it was applied is
refused, because the database would no longer match the repository: a change to the schema is
always a new file. A session-level advisory lock makes two runners take turns, so each file is
applied once.
"""

from __future__ import annotations

import hashlib
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from gbmi.config import get_settings
from gbmi.ledger.db import Connection, connect
from gbmi.logging import get_logger

log = get_logger(__name__)

_FILE_NAME = re.compile(r"^(?P<version>\d{4})_(?P<name>[a-z0-9_]+)\.sql$")
#: Statements which would end the runner's transaction. A bare BEGIN opens a PL/pgSQL block
#: and is allowed.
_TRANSACTION_CONTROL = re.compile(
    r"^[ \t]*(begin[ \t]*;|begin[ \t]+(transaction|work|isolation)\b|start[ \t]+transaction\b"
    r"|commit\b|rollback\b)",
    re.IGNORECASE | re.MULTILINE,
)
#: The eight ASCII bytes of "gbmi_mig", read as one 64-bit number.
LOCK_KEY = 0x67626D695F6D6967

_BOOKKEEPING = b"""
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.schema_migration (
    version      integer     PRIMARY KEY,
    name         text        NOT NULL,
    sha256       text        NOT NULL,
    duration_ms  integer     NOT NULL,
    applied_by   text        NOT NULL DEFAULT current_user,
    applied_at   timestamptz NOT NULL DEFAULT clock_timestamp()
);
"""


class MigrationError(Exception):
    """The migration files and the database do not fit together, or a file is not valid."""


@dataclass(frozen=True)
class Migration:
    """One migration file."""

    version: int
    name: str
    sha256: str
    sql: str

    @property
    def file_name(self) -> str:
        return f"{self.version:04d}_{self.name}.sql"


@dataclass(frozen=True)
class Applied:
    """One row of ``ops.schema_migration``."""

    version: int
    name: str
    sha256: str
    applied_at: datetime


def default_directory() -> Path:
    """The directory of the setting ``GBMI_MIGRATIONS_DIR``."""
    return Path(get_settings().migrations_dir)


def discover(directory: Path) -> list[Migration]:
    """The migration files of a directory, in number order.

    Raises MigrationError for a file which breaks a rule of the directory.
    """
    if not directory.is_dir():
        raise MigrationError(f"there is no migration directory at {directory}")
    migrations: list[Migration] = []
    for path in sorted(directory.glob("*.sql")):
        match = _FILE_NAME.match(path.name)
        if match is None:
            raise MigrationError(f"{path.name}: a migration file is named NNNN_name.sql")
        content = path.read_bytes()
        sql = content.decode("utf-8")
        control = _TRANSACTION_CONTROL.search(sql)
        if control is not None:
            raise MigrationError(
                f"{path.name}: a migration file holds no {control.group(1).strip().upper()!r}; "
                "the runner wraps each file in one transaction"
            )
        migrations.append(
            Migration(
                version=int(match["version"]),
                name=match["name"],
                sha256=hashlib.sha256(content).hexdigest(),
                sql=sql,
            )
        )
    versions = [migration.version for migration in migrations]
    if versions != list(range(1, len(versions) + 1)):
        raise MigrationError(
            "migration files are numbered 0001, 0002, ... without a gap and without a repeat; "
            f"found {[f'{version:04d}' for version in versions]}"
        )
    return migrations


def _applied(connection: Connection) -> dict[int, Applied]:
    rows = connection.execute(
        "SELECT version, name, sha256, applied_at FROM ops.schema_migration ORDER BY version"
    ).fetchall()
    return {int(row[0]): Applied(int(row[0]), str(row[1]), str(row[2]), row[3]) for row in rows}


def _problems(migrations: list[Migration], applied: dict[int, Applied]) -> list[str]:
    """Why the files cannot be applied on top of what the database holds; empty when they can."""
    by_version = {migration.version: migration for migration in migrations}
    problems: list[str] = []
    for version, row in applied.items():
        migration = by_version.get(version)
        if migration is None:
            problems.append(
                f"the database holds migration {version:04d}_{row.name}, "
                "and the directory has no such file"
            )
        elif migration.sha256 != row.sha256:
            problems.append(
                f"{migration.file_name} changed after it was applied on "
                f"{row.applied_at:%Y-%m-%d %H:%M} UTC; migrations are forward only, "
                "so a change to the schema is a new file"
            )
    newest = max(applied, default=0)
    problems += [
        f"{migration.file_name} is not applied, and the later migration {newest:04d} is"
        for migration in migrations
        if migration.version < newest and migration.version not in applied
    ]
    return problems


def migrate(dsn: str | None = None, directory: Path | None = None) -> list[dict[str, Any]]:
    """Apply every file which the database does not hold yet.

    Returns one entry for each file applied in the call, in order.
    """
    migrations = discover(directory or default_directory())
    results: list[dict[str, Any]] = []
    with connect(dsn, autocommit=True) as connection:
        connection.execute("SELECT pg_advisory_lock(%s)", (LOCK_KEY,))
        try:
            connection.execute(_BOOKKEEPING)
            applied = _applied(connection)
            problems = _problems(migrations, applied)
            if problems:
                raise MigrationError("; ".join(problems))
            for migration in migrations:
                if migration.version in applied:
                    continue
                started = time.monotonic()
                with connection.transaction():
                    connection.execute(migration.sql.encode("utf-8"))
                    duration_ms = round((time.monotonic() - started) * 1000)
                    connection.execute(
                        "INSERT INTO ops.schema_migration (version, name, sha256, duration_ms) "
                        "VALUES (%s, %s, %s, %s)",
                        (migration.version, migration.name, migration.sha256, duration_ms),
                    )
                log.info("migration_applied", file=migration.file_name, duration_ms=duration_ms)
                results.append({"file": migration.file_name, "duration_ms": duration_ms})
        finally:
            connection.execute("SELECT pg_advisory_unlock(%s)", (LOCK_KEY,))
    return results


def status(dsn: str | None = None, directory: Path | None = None) -> list[dict[str, Any]]:
    """Each migration file and each applied migration, with its state. Changes nothing."""
    migrations = discover(directory or default_directory())
    with connect(dsn, autocommit=True) as connection:
        exists = connection.execute("SELECT to_regclass('ops.schema_migration')").fetchone()
        applied = _applied(connection) if exists is not None and exists[0] is not None else {}
    report: list[dict[str, Any]] = []
    for migration in migrations:
        row = applied.get(migration.version)
        if row is None:
            state = "pending"
        elif row.sha256 != migration.sha256:
            state = "changed"
        else:
            state = "applied"
        report.append(
            {
                "file": migration.file_name,
                "state": state,
                "applied_at": row.applied_at.isoformat() if row is not None else None,
            }
        )
    known = {migration.version for migration in migrations}
    report += [
        {
            "file": f"{version:04d}_{row.name}.sql",
            "state": "file_missing",
            "applied_at": row.applied_at.isoformat(),
        }
        for version, row in applied.items()
        if version not in known
    ]
    return report
