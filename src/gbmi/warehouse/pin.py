"""Pin a silver table for a mart build (blueprint 5.12, 7.7).

Reading "the latest" version of a lake table while it is being written would make a mart build
irreproducible. Before each build, the pipeline therefore re-creates one ClickHouse table per
silver table, bound to the exact Iceberg metadata file that the catalog names as current:

    CREATE OR REPLACE TABLE lake.<table>
    ENGINE = IcebergLocal('/lake/warehouse/silver.db/<table>/')
    SETTINGS iceberg_metadata_file_path = 'metadata/<file>.metadata.json'

``iceberg_metadata_file_path`` is an engine-level setting that overrides every other way
ClickHouse resolves the metadata file, so later commits to the lake table do not change what
the pinned table returns. dbt models read ``lake.<table>`` and never name a file themselves.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

from clickhouse_connect.driver.client import Client
from pyiceberg.table import Table

from gbmi.config import Settings, get_settings
from gbmi.lake.catalog import load_lake_catalog
from gbmi.warehouse.client import connect, quote_identifier, quote_string

LAKE_DATABASE = "lake"


@dataclass(frozen=True)
class S3Credentials:
    """Short-lived credentials for an ``IcebergS3`` table.

    On the host ClickHouse uses the instance role and no credentials appear in the statement;
    these are for a laptop reading S3 with exported SSO credentials.
    """

    access_key_id: str
    secret_access_key: str
    session_token: str | None = None


@dataclass(frozen=True)
class PinnedTable:
    """A ClickHouse table bound to one Iceberg metadata file."""

    database: str
    name: str
    table_location: str
    #: Path of the metadata file relative to the table location.
    metadata_file: str
    #: The Iceberg snapshot that metadata file names as current (None for an empty table).
    snapshot_id: int | None

    @property
    def qualified_name(self) -> str:
        return f"{self.database}.{self.name}"


def relative_metadata_file(table_location: str, metadata_location: str) -> str:
    """``metadata/<file>``: the metadata file as ClickHouse wants it, relative to the table."""
    prefix = table_location.rstrip("/") + "/"
    if not metadata_location.startswith(prefix):
        raise ValueError(
            f"metadata file {metadata_location!r} is not under table {table_location!r}"
        )
    return metadata_location[len(prefix) :]


def engine_clause(table_location: str, credentials: S3Credentials | None = None) -> str:
    """The engine for a table location: ``IcebergLocal`` for file URIs, ``IcebergS3`` for S3."""
    parts = urlsplit(table_location)
    if parts.scheme == "file":
        return f"IcebergLocal({quote_string(parts.path.rstrip('/') + '/')})"
    if parts.scheme == "s3":
        arguments = [quote_string(table_location.rstrip("/") + "/")]
        if credentials is not None:
            arguments += [
                quote_string(credentials.access_key_id),
                quote_string(credentials.secret_access_key),
            ]
            if credentials.session_token:
                arguments.append(quote_string(credentials.session_token))
        return f"IcebergS3({', '.join(arguments)})"
    raise ValueError(f"unsupported table location {table_location!r}")


def pin_statement(
    name: str,
    table_location: str,
    metadata_location: str,
    *,
    database: str = LAKE_DATABASE,
    credentials: S3Credentials | None = None,
) -> str:
    """The statement that re-creates a pinned table. It replaces the old one atomically."""
    metadata_file = relative_metadata_file(table_location, metadata_location)
    return (
        f"CREATE OR REPLACE TABLE {quote_identifier(database)}.{quote_identifier(name)} "
        f"ENGINE = {engine_clause(table_location, credentials)} "
        f"SETTINGS iceberg_metadata_file_path = {quote_string(metadata_file)}"
    )


def pin_table(
    client: Client,
    table: Table,
    *,
    name: str | None = None,
    database: str = LAKE_DATABASE,
    metadata_location: str | None = None,
    credentials: S3Credentials | None = None,
) -> PinnedTable:
    """Pin ``lake.<name>`` to the table's current metadata file, or to the one given.

    ``table`` should be freshly loaded from the catalog: its metadata location is what
    "current" means for this build.
    """
    name = name or table.name()[-1]
    location = table.location()
    metadata_location = metadata_location or table.metadata_location
    client.command(
        pin_statement(name, location, metadata_location, database=database, credentials=credentials)
    )
    snapshot = table.current_snapshot() if metadata_location == table.metadata_location else None
    return PinnedTable(
        database=database,
        name=name,
        table_location=location,
        metadata_file=relative_metadata_file(location, metadata_location),
        snapshot_id=snapshot.snapshot_id if snapshot else None,
    )


def pin_silver_table_task(name: str, settings: Settings | None = None) -> dict[str, Any]:
    """Task entry point: pin ``lake.<name>`` to the catalog's current metadata file.

    Returns the snapshot that was pinned, which the mart build records as
    ``_silver_snapshot_id`` (blueprint 7.12).
    """
    settings = settings or get_settings()
    table = load_lake_catalog(settings).load_table((settings.silver_namespace, name))
    client = connect(settings=settings)
    try:
        pinned = pin_table(client, table, name=name)
        relation = f"{quote_identifier(pinned.database)}.{quote_identifier(pinned.name)}"
        rows = client.query(f"SELECT count() FROM {relation}").result_rows[0][0]  # noqa: S608
    finally:
        client.close()
    return {
        "table": pinned.qualified_name,
        "metadata_file": pinned.metadata_file,
        "snapshot_id": pinned.snapshot_id,
        "rows": int(rows),
    }
