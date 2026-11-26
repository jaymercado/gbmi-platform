"""An hour unit of each feed, built from the ledger (week 2 plan WP-06, step 7).

The tests run in the toolbox container: the ledger is a throwaway database of the PostgreSQL of
the stack, and the lake is a directory under ``/lake/_test``.
"""

from __future__ import annotations

import pytest

from gbmi.core.gdelt import Feed
from gbmi.lake.commit import Unit
from gbmi.lake.tables import FEED_TABLES
from gbmi.ledger.repository import Ledger, fileset_hash
from gbmi.ledger.states import State
from gbmi.transforms.build import build_unit

from ..conftest import Lake
from .helpers import AS_OF, DAY, HOUR, ROWS, land, silver, states, stored, versions

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("feed", list(Feed))
def test_two_builds_of_an_hour_give_the_same_checksum_and_a_new_snapshot(
    feed: Feed, ledger: Ledger, local_lake: Lake
) -> None:
    land(ledger, local_lake)
    settings = local_lake.settings

    first = build_unit(feed, HOUR, as_of=AS_OF, ledger=ledger, settings=settings, run_id="run-1")
    second = build_unit(feed, HOUR, as_of=AS_OF, ledger=ledger, settings=settings, run_id="run-2")

    assert (first["loaded"], second["loaded"]) == (4, 0)
    assert states(ledger, feed) == {State.LOADED: 4}
    assert (len(first["files"]), second["fileset_hash"]) == (4, first["fileset_hash"])
    for name in FEED_TABLES[feed]:
        before, after = first["tables"][name], second["tables"][name]
        assert before["rows"] == ROWS[name]
        assert after["checksum"] == before["checksum"]
        # The table holds the rows of the recorded checksum.
        assert stored(local_lake, name, HOUR).digest == after["checksum"]["digest"]
        assert stored(local_lake, name, HOUR).rows == ROWS[name]
        # A rerun is a new snapshot which replaces the unit (blueprint 5.6).
        assert after["snapshot_id"] != before["snapshot_id"]
        assert after["previous_snapshot_id"] == before["snapshot_id"]
        assert before["snapshot_operations"] == ["append"]
        assert after["snapshot_operations"] == ["delete", "append"]
        recorded = versions(ledger, name)
        assert [version["snapshot_id"] for version in recorded] == [
            before["snapshot_id"],
            after["snapshot_id"],
        ]
        assert {version["content_digest"] for version in recorded} == {before["checksum"]["digest"]}
        assert {(version["unit_kind"], version["slot_hour"]) for version in recorded} == {
            ("hour", 8)
        }
        assert {version["fileset_hash"] for version in recorded} == {first["fileset_hash"]}
    # The first table of a feed carries the line counts; the organisation table has none.
    lines = versions(ledger, FEED_TABLES[feed][0])[-1]
    assert lines["raw_lines"] == lines["parsed_rows"] + lines["quarantined_rows"]
    assert lines["committed_rows"] == lines["parsed_rows"] - lines["duplicate_rows"]
    if feed is Feed.GKG:
        assert versions(ledger, "gdelt_gkg_org")[-1]["raw_lines"] is None
    # Each slot points at the snapshot of the newest build and holds its accounting.
    records = ledger.unit_slots(feed, DAY, 8)
    assert {record.silver_snapshot_id for record in records} == {second["snapshot_id"]}
    assert sum(record.raw_lines or 0 for record in records) == first["counts"]["raw_lines"]
    assert feed not in {unit.feed for unit in ledger.pending_units(AS_OF)}


def test_an_hour_without_files_makes_no_commit_and_still_records_a_version(
    ledger: Ledger, local_lake: Lake
) -> None:
    land(ledger, local_lake)
    settings = local_lake.settings
    empty_hour = Unit(DAY, 9)

    empty = build_unit(Feed.EXPORT, empty_hour, as_of=AS_OF, ledger=ledger, settings=settings)

    assert (empty["files"], empty["loaded"], empty["counts"]["committed"]) == ([], 0, 0)
    assert empty["tables"]["gdelt_event"] == {
        "rows": 0,
        "committed": False,
        "snapshot_id": None,
        "previous_snapshot_id": None,
        "snapshot_operations": [],
        "checksum": {"rows": 0, "digest": "0" * 16},
    }
    assert silver(local_lake, "gdelt_event").current_snapshot() is None
    (version,) = versions(ledger, "gdelt_event")
    assert (version["committed_rows"], version["file_count"]) == (0, 0)
    assert (version["commit_id"], version["snapshot_id"]) == (None, None)
    assert version["fileset_hash"] == fileset_hash([])

    # The table has a snapshot now. The empty hour still needs none.
    hour = build_unit(Feed.EXPORT, HOUR, as_of=AS_OF, ledger=ledger, settings=settings)
    again = build_unit(Feed.EXPORT, empty_hour, as_of=AS_OF, ledger=ledger, settings=settings)

    assert again["tables"]["gdelt_event"]["committed"] is False
    assert again["snapshot_id"] == hour["snapshot_id"]
    current = silver(local_lake, "gdelt_event").current_snapshot()
    assert current is not None and current.snapshot_id == hour["snapshot_id"]
    assert stored(local_lake, "gdelt_event", HOUR).rows == ROWS["gdelt_event"]
