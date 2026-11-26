"""Pipeline PL-1 on the fixture hour, inside the stack (week 2 plan, WP-04 step 6).

The test runs in the toolbox container: the ledger is a throwaway database of the PostgreSQL of
the stack, and the lake is a directory under ``/lake/_test``. The database ``gbmi`` and the
lake of the stack are not touched (week 2 plan 3.3).
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from gbmi.ledger.repository import Ledger, open_ledger
from gbmi.ledger.scratch import ADMIN_DSN_VARIABLE, scratch_database
from gbmi.ledger.states import State
from gbmi.sources.gdelt import ingest, reconcile
from gbmi.sources.gdelt.land import Bronze, FixtureFetcher

from ..conftest import Lake

pytestmark = pytest.mark.integration

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "gdelt" / "v2"
DAY = date(2026, 11, 25)
AS_OF = datetime(2026, 11, 25, 9, 0, tzinfo=UTC)


@pytest.fixture
def ledger() -> Iterator[Ledger]:
    import os

    if not os.environ.get(ADMIN_DSN_VARIABLE):
        pytest.skip(
            f"needs PostgreSQL: {ADMIN_DSN_VARIABLE} is not set (run with make integration)"
        )
    with scratch_database() as dsn, open_ledger(dsn, run_id="integration") as opened:
        yield opened


def test_twelve_slots_end_verified_and_a_second_run_writes_no_file(
    ledger: Ledger, local_lake: Lake
) -> None:
    lake_uri = local_lake.settings.lake_uri
    bronze = Bronze(lake_uri)
    fetcher = FixtureFetcher(FIXTURES)

    enqueued = ingest.enqueue(DAY, DAY, as_of=AS_OF, fetcher=fetcher, ledger=ledger)
    first = ingest.run_ingest(
        as_of=AS_OF, fetcher=fetcher, ledger=ledger, bronze=bronze, lake_uri=lake_uri
    )

    assert enqueued["listed"] == 12
    assert (first["verified"], first["written"], first["already_in_bronze"]) == (12, 12, 0)
    verified = ledger.find(states=[State.VERIFIED])
    assert len(verified) == 12
    assert all(record.bronze_uri and record.bronze_uri.startswith(lake_uri) for record in verified)

    second = ingest.run_ingest(
        as_of=AS_OF, fetcher=fetcher, ledger=ledger, bronze=bronze, lake_uri=lake_uri
    )

    assert (second["selected"], second["written"]) == (0, 0)
    # The ledger agrees with the bronze listing: twelve objects, each with the listed size.
    objects = bronze.sizes("raw")
    assert objects == {
        record.key.slot.bronze_key(record.key.feed): record.expected_bytes for record in verified
    }
    summary = reconcile.reconcile(as_of=AS_OF, fetcher=fetcher, ledger=ledger, bronze=bronze)
    assert (summary["bronze_differences"], summary["files_in_the_master_list"]) == (0, 12)
