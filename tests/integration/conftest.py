"""Fixtures for tests that need a lake.

Each test gets its own lake, so tests never see each other's tables:

* **local** - a fresh directory with its own SQLite catalog. In the toolbox container
  (``make integration``) it is created under ``/lake/_test``, the bind mount that ClickHouse
  also sees, so the same tests exercise SQLite on the shared mount. On the laptop it falls back
  to pytest's temporary directory.
* **aws** - the Glue database ``gbmi_ci_silver`` under the bucket's ``ci/`` prefix (week 1
  plan 3.3). It runs only with ``GBMI_TEST_AWS=1``, short-lived credentials in the environment
  and ``GBMI_TEST_AWS_LAKE_BUCKET`` set; otherwise it is skipped.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
from pyiceberg.catalog import Catalog

from gbmi.config import Environment, LakeBackend, Settings
from gbmi.lake.catalog import ensure_silver_namespace, load_lake_catalog

SHARED_LAKE = Path("/lake")


@dataclass(frozen=True)
class Lake:
    """A lake backend prepared for one test."""

    backend: str
    settings: Settings
    catalog: Catalog
    namespace: str
    #: Tables created through ``track`` are dropped when the test ends.
    created: list[tuple[str, str]]

    def track(self, name: str) -> tuple[str, str]:
        identifier = (self.namespace, name)
        self.created.append(identifier)
        return identifier


def aws_enabled() -> bool:
    return os.environ.get("GBMI_TEST_AWS") == "1"


def _local_lake(tmp_path: Path) -> Iterator[Lake]:
    shared = SHARED_LAKE.is_dir() and os.access(SHARED_LAKE, os.W_OK)
    root = (SHARED_LAKE / "_test" if shared else tmp_path) / uuid.uuid4().hex[:12]
    root.mkdir(parents=True)
    settings = Settings(
        env=Environment.LOCAL,
        lake_backend=LakeBackend.LOCAL,
        lake_uri=f"file://{root}",
        catalog_uri=f"sqlite:///{root}/catalog/catalog.db",
    )
    catalog = load_lake_catalog(settings)
    namespace = ensure_silver_namespace(catalog, settings)
    try:
        yield Lake("local", settings, catalog, namespace, [])
    finally:
        shutil.rmtree(root, ignore_errors=True)


def _aws_lake() -> Iterator[Lake]:
    if not aws_enabled():
        pytest.skip("Glue/S3 half: set GBMI_TEST_AWS=1 with short-lived credentials to run it")
    bucket = os.environ.get("GBMI_TEST_AWS_LAKE_BUCKET")
    if not bucket:
        pytest.fail("GBMI_TEST_AWS=1 needs GBMI_TEST_AWS_LAKE_BUCKET (the lake bucket name)")
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    settings = Settings(
        env=Environment.CI,
        lake_backend=LakeBackend.AWS,
        lake_uri=f"s3://{bucket}/ci",
        catalog_uri=f"glue://{region}",
    )
    catalog = load_lake_catalog(settings)
    namespace = ensure_silver_namespace(catalog, settings)
    lake = Lake("aws", settings, catalog, namespace, [])
    try:
        yield lake
    finally:
        for identifier in lake.created:
            with contextlib.suppress(Exception):
                catalog.purge_table(identifier)
        # The database itself is dropped once it is empty; another test may still be using it.
        with contextlib.suppress(Exception):
            catalog.drop_namespace(namespace)


@pytest.fixture(params=["local", pytest.param("aws", marks=pytest.mark.aws)])
def lake(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Lake]:
    """A lake on each backend; the AWS one is skipped unless it is enabled."""
    yield from (_local_lake(tmp_path) if request.param == "local" else _aws_lake())


@pytest.fixture
def local_lake(tmp_path: Path) -> Iterator[Lake]:
    """A lake on the local backend only."""
    yield from _local_lake(tmp_path)
