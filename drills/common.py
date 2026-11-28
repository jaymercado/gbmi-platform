"""What the drills share: a scratch world, commands in a process of their own, and checks.

A *world* is a throwaway database with the schema ops, and a lake in a directory of its own
with its own Iceberg catalog. The recorded fixture hour (2026-11-25, 08:00 to 08:45 UTC, three
feeds) is the data of every drill.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import uuid
from collections import Counter
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from pyiceberg.table import Table

from gbmi.config import Environment, LakeBackend, SecSource, Settings
from gbmi.core.checksum import ContentChecksum, content_checksum
from gbmi.core.gdelt import Feed
from gbmi.lake.catalog import ensure_silver_namespace, load_lake_catalog
from gbmi.lake.commit import Unit
from gbmi.lake.tables import ALL_TABLE_KEYS, TABLE_KEYS
from gbmi.ledger.repository import Ledger, open_ledger
from gbmi.ledger.scratch import scratch_database
from gbmi.sources.gdelt import ingest
from gbmi.sources.gdelt.land import Bronze, Fetcher, FixtureFetcher
from gbmi.transforms.build import build_unit

DAY = date(2026, 11, 25)
HOUR = Unit(DAY, 8)
#: The clock of the drills: the files of the fixture hour are published.
AS_OF = datetime(2026, 11, 25, 9, 0, tzinfo=UTC)
#: The mount of the lake in the containers. A drill on the laptop uses a temporary directory.
SHARED_LAKE = Path("/lake")
_REPOSITORY = Path(__file__).resolve().parents[1]


#: The clock of the SEC drills: the clock of the demo, the day after the recorded index file.
SEC_AS_OF = datetime(2026, 11, 26, 12, 0, tzinfo=UTC)
#: The five demo companies: the watchlist of the recorded SEC files (week 3 plan 3.3).
SEC_WATCHLIST = (1326801, 1045810, 789019, 63276, 1075124)


def sec_fixtures_dir() -> Path:
    """The recorded SEC files: the mount of the containers, or the files of the checkout."""
    configured = os.environ.get("GBMI_SEC_FIXTURES_DIR")
    if configured and Path(configured).is_dir():
        return Path(configured)
    mounted = Path("/fixtures/sec")
    return mounted if mounted.is_dir() else _REPOSITORY / "tests" / "fixtures" / "sec"


def fixtures_dir() -> Path:
    """The recorded GDELT files: the mount of the containers, or the files of the checkout."""
    configured = os.environ.get("GBMI_FIXTURES_DIR")
    if configured:
        return Path(configured)
    mounted = Path("/fixtures/gdelt/v2")
    return mounted if mounted.is_dir() else _REPOSITORY / "tests" / "fixtures" / "gdelt" / "v2"


@dataclass
class World:
    """A throwaway database and a lake of its own."""

    name: str
    dsn: str
    settings: Settings
    ledger: Ledger

    @property
    def lake_path(self) -> Path:
        return Path(self.settings.lake_uri.removeprefix("file://"))

    def land(self, fetcher: Fetcher | None = None) -> dict[str, Any]:
        """Enqueue the fixture day and run the ingest once."""
        fetcher = fetcher or FixtureFetcher(fixtures_dir())
        lake_uri = self.settings.lake_uri
        ingest.enqueue(DAY, DAY, as_of=AS_OF, fetcher=fetcher, ledger=self.ledger)
        return ingest.run_ingest(
            as_of=AS_OF,
            fetcher=fetcher,
            ledger=self.ledger,
            bronze=Bronze(lake_uri),
            lake_uri=lake_uri,
            sleep=lambda seconds: None,
        )

    def build(self, feed: Feed, unit: Unit) -> dict[str, Any]:
        """Build one unit in the process of the drill."""
        return build_unit(feed, unit, as_of=AS_OF, ledger=self.ledger, settings=self.settings)

    def command(
        self, *arguments: str, failpoint: str | None = None
    ) -> subprocess.CompletedProcess[str]:
        """Run a gbmi command in a new process, on the database and the lake of the world.

        With ``failpoint`` the process kills itself at the named point (gbmi.core.failpoint).
        """
        environment = {
            **os.environ,
            "GBMI_PG_DSN": self.dsn,
            "GBMI_LAKE_BACKEND": "local",
            "GBMI_LAKE_URI": self.settings.lake_uri,
            "GBMI_CATALOG_URI": self.settings.catalog_uri,
            "GBMI_PIPELINE_VERSION": self.settings.pipeline_version,
            "GBMI_FIXTURES_DIR": str(fixtures_dir()),
        }
        environment.pop("GBMI_FAILPOINT", None)
        if failpoint:
            environment["GBMI_FAILPOINT"] = failpoint
        return subprocess.run(
            [sys.executable, "-c", "from gbmi.cli import app; app()", *arguments],
            env=environment,
            capture_output=True,
            text=True,
            timeout=600,
            check=False,
        )

    def table(self, name: str) -> Table:
        catalog = load_lake_catalog(self.settings)
        return catalog.load_table((self.settings.silver_namespace, name))

    def snapshot_id(self, name: str) -> int | None:
        snapshot = self.table(name).current_snapshot()
        return snapshot.snapshot_id if snapshot else None

    def checksum(self, name: str, unit: Unit) -> ContentChecksum:
        """The checksum of the rows which the table holds for the unit now."""
        return content_checksum(self.table(name).scan(row_filter=unit.row_filter).to_arrow())

    def repeated_keys(self, name: str, unit: Unit) -> int:
        """The rows of the unit whose logical key another row of the unit has too."""
        key = list(TABLE_KEYS[name])
        rows = (
            self.table(name).scan(row_filter=unit.row_filter, selected_fields=tuple(key)).to_arrow()
        )
        return int(rows.num_rows - rows.group_by(key).aggregate([]).num_rows)

    def sec_settings(self) -> Settings:
        """The settings of the world with the recorded SEC files as the source."""
        return self.settings.model_copy(
            update={
                "sec_source": SecSource.FIXTURE,
                "sec_user_agent": None,
                "sec_fixtures_dir": str(sec_fixtures_dir()),
            }
        )

    def table_checksum(self, name: str) -> ContentChecksum:
        """The checksum of every row which a table holds now."""
        return content_checksum(self.table(name).scan().to_arrow())

    def repeated_table_keys(self, name: str) -> int:
        """The rows of a table whose logical key another row of the table has too."""
        key = list(ALL_TABLE_KEYS[name])
        rows = self.table(name).scan(selected_fields=tuple(key)).to_arrow()
        return int(rows.num_rows - rows.group_by(key).aggregate([]).num_rows)

    def states(self, feed: Feed | None = None) -> dict[str, int]:
        """The number of slots in each state, without the slots which are only expected."""
        records = self.ledger.find(feeds=[feed] if feed else None)
        counts = Counter(record.state.value for record in records)
        counts.pop("expected", None)
        return dict(counts)

    def versions(self, table_name: str) -> list[dict[str, Any]]:
        """The partition versions of a table, oldest first."""
        rows = self.ledger.connection.execute(
            "SELECT unit_kind, committed_rows, file_count, fileset_hash, content_digest, "
            "snapshot_id FROM ops.silver_partition WHERE source = %s AND table_name = %s "
            "ORDER BY partition_version_id",
            (self.ledger.source, table_name),
        ).fetchall()
        names = ("unit_kind", "rows", "files", "fileset_hash", "content_digest", "snapshot_id")
        return [dict(zip(names, row, strict=True)) for row in rows]


@contextmanager
def world(name: str) -> Iterator[World]:
    """A world for one drill; the database is dropped and the lake removed at the end."""
    shared = SHARED_LAKE.is_dir() and os.access(SHARED_LAKE, os.W_OK)
    parent = SHARED_LAKE / "_test" if shared else Path(tempfile.gettempdir()) / "gbmi-drills"
    root = parent / f"{name}-{uuid.uuid4().hex[:8]}"
    root.mkdir(parents=True)
    settings = Settings(
        env=Environment.LOCAL,
        lake_backend=LakeBackend.LOCAL,
        lake_uri=f"file://{root}",
        catalog_uri=f"sqlite:///{root}/catalog/catalog.db",
    )
    ensure_silver_namespace(load_lake_catalog(settings), settings)
    try:
        with scratch_database() as dsn, open_ledger(dsn, run_id=f"drill-{name}") as ledger:
            yield World(name=name, dsn=dsn, settings=settings, ledger=ledger)
    finally:
        shutil.rmtree(root, ignore_errors=True)


@dataclass
class Drill:
    """The checks of one drill. Each check prints one line; a failed check fails the drill."""

    identifier: str
    title: str
    results: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        print(f"== {self.identifier}: {self.title}")

    def step(self, text: str) -> None:
        print(f"-- {text}")

    def check(self, statement: str, passed: bool, observed: object = None) -> bool:
        self.results.append({"check": statement, "passed": bool(passed), "observed": observed})
        suffix = "" if passed or observed is None else f"  (observed: {observed})"
        print(f"{'PASS' if passed else 'FAIL'}  {statement}{suffix}")
        return bool(passed)

    def finish(self, facts: dict[str, Any] | None = None) -> int:
        """Print the summary as JSON; the exit code is 0 only when every check passed."""
        failed = [result["check"] for result in self.results if not result["passed"]]
        summary = {
            "drill": self.identifier,
            "title": self.title,
            "checks": len(self.results),
            "failed": failed,
            "facts": facts or {},
        }
        print(json.dumps(summary, indent=2, default=str))
        print(f"== {self.identifier}: {'passed' if not failed else 'FAILED'}")
        return 0 if not failed else 1


def parse_json(output: str) -> Any:
    """The JSON which a gbmi command printed on standard output."""
    return json.loads(output) if output.strip() else None


def digests(summary: dict[str, Any], names: Sequence[str]) -> dict[str, dict[str, Any]]:
    """The content checksum of each table in the summary of a build."""
    return {name: summary["tables"][name]["checksum"] for name in names}
