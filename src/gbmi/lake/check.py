"""Checks of the lake backend of a running stack (week 3 plan, WP-10 step 5).

The smoke test of a deployment runs ``gbmi lake check``. The checks use the settings of the
stack, so the same command works on the laptop (file system and SQLite catalog) and on the AWS
host (S3 and Glue):

    catalog   the catalog answers, and the silver namespace exists
    object    one object is written whole, read back and deleted below the scratch address
    pin       ClickHouse reads one silver table through the pin statement of the backend:
              on the host, through the lake role
    version   on the environment prod the pipeline version is the Git commit of the release

A check never raises: the result names the error, so one run shows every failed check.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pyarrow.fs as pafs

from gbmi.config import Environment, Settings
from gbmi.lake.catalog import load_lake_catalog
from gbmi.lake.objects import write_object
from gbmi.lake.tables import GDELT_EVENT
from gbmi.warehouse.client import connect, quote_identifier
from gbmi.warehouse.pin import LAKE_DATABASE, lake_grant, pin_silver_tables

COMMIT = re.compile(r"[0-9a-f]{40}")


@dataclass(frozen=True)
class Check:
    name: str
    passed: bool
    detail: dict[str, Any]


def _run(name: str, body: Callable[[], dict[str, Any]]) -> Check:
    try:
        return Check(name, True, body())
    except Exception as error:
        return Check(name, False, {"error": f"{type(error).__name__}: {error}"[:500]})


def check_catalog(settings: Settings) -> dict[str, Any]:
    """The catalog answers and holds the silver namespace. Nothing is created."""
    catalog = load_lake_catalog(settings)
    namespace = settings.silver_namespace
    if not catalog.namespace_exists(namespace):
        raise LookupError(f"the catalog has no namespace {namespace}")
    return {"namespace": namespace, "tables": len(catalog.list_tables(namespace))}


def check_object(scratch_uri: str) -> dict[str, Any]:
    """Write one object whole, read the object back and delete the object."""
    scratch = scratch_uri.rstrip("/")
    key = f"{uuid.uuid4().hex}.txt"
    content = f"gbmi lake check {key}".encode()
    write_object(scratch, key, content)
    filesystem, path = pafs.FileSystem.from_uri(f"{scratch}/{key}")
    try:
        with filesystem.open_input_stream(path) as stream:
            read = stream.read()
    finally:
        filesystem.delete_file(path)
    if read != content:
        raise ValueError("the object which was read differs from the object which was written")
    if filesystem.get_file_info(path).type != pafs.FileType.NotFound:
        raise ValueError("the object is still there after the delete")
    return {"bytes": len(content), "deleted": True}


def check_pin(settings: Settings, table: str) -> dict[str, Any]:
    """ClickHouse reads one silver table through the pin statement of the backend.

    The table is pinned under a name of its own and dropped again, so the pinned tables of the
    mart build stay as they are. A table which the catalog lacks is created empty first.
    """
    catalog = load_lake_catalog(settings)
    name = f"{table}_check_{uuid.uuid4().hex[:8]}"
    client = connect(settings=settings)
    try:
        try:
            pinned = pin_silver_tables(
                client,
                catalog,
                settings.silver_namespace,
                [table],
                pinned_names={table: name},
                grant=lake_grant(settings),
            )[table]
        finally:
            relation = f"{quote_identifier(LAKE_DATABASE)}.{quote_identifier(name)}"
            client.command(f"DROP TABLE IF EXISTS {relation}")
    finally:
        client.close()
    return {
        "table": table,
        "rows": pinned["rows"],
        "created": pinned["created"],
        "access": settings.ch_lake_access.value if lake_grant_needed(settings) else "none",
    }


def lake_grant_needed(settings: Settings) -> bool:
    return settings.lake_uri.startswith("s3://")


def check_version(settings: Settings, release: str | None) -> dict[str, Any]:
    """The pipeline version of the process: on the environment prod, the commit of the release."""
    version = settings.pipeline_version
    if settings.env is Environment.PROD and not COMMIT.fullmatch(version):
        raise ValueError("on the environment prod the pipeline version has to be a Git commit")
    if release is not None and version != release:
        raise ValueError(f"the pipeline version is {version}, and the release is {release}")
    return {"pipeline_version": version, "env": settings.env.value}


def run_checks(
    settings: Settings,
    *,
    scratch_uri: str | None = None,
    release: str | None = None,
    table: str = GDELT_EVENT,
) -> list[Check]:
    """Every check of the backend, in the order of the list above."""
    scratch = scratch_uri or f"{settings.lake_root}/quarantine/_check"
    return [
        _run("catalog", lambda: check_catalog(settings)),
        _run("object", lambda: check_object(scratch)),
        _run("pin", lambda: check_pin(settings, table)),
        _run("version", lambda: check_version(settings, release)),
    ]
