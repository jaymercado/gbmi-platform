"""Two writers on one table (blueprint 8.4: commit conflicts; review point R7).

PyIceberg 0.12 commits optimistically. A commit which lost a race is repeated on the refreshed
table when the other commit touched other data, and raises when the other commit changed the
same unit. ``commit_with_retry`` then starts again from the catalog. In each case the unit ends
with the rows of the last writer, and the commit names the snapshot it replaced.

The tests also pin which of the two paths PyIceberg 0.12.0 takes. When a later release
changes a path, the test fails and the decision of the build is looked at again.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date

import pytest
from pyiceberg.table import Table

from gbmi.lake.commit import CommitResult, DayFile, Unit, commit_with_retry, overwrite_unit
from tests.integration.conftest import Lake
from tests.integration.lake import sk1_table as sk1
from tests.integration.lake.sk1_table import DAY_1, DAY_2

pytestmark = pytest.mark.integration

HOUR_8 = "slot_date = '2026-11-24' and slot_hour = 8"
HOUR_9 = "slot_date = '2026-11-24' and slot_hour = 9"
ONLY_DAY_1 = "slot_date = '2026-11-24'"
ONLY_DAY_2 = "slot_date = '2026-11-25'"


class StaleFirst:
    """A loader which hands out a table loaded before the other writer, then fresh tables."""

    def __init__(self, lake: Lake, stale: Table) -> None:
        self.lake = lake
        self.stale = stale
        self.loads = 0

    def __call__(self) -> Table:
        self.loads += 1
        return self.stale if self.loads == 1 else self.lake.catalog.load_table(self.stale.name())


def expected_ids(hour: int, version: int, count: int = 5) -> list[int]:
    return sorted(sk1.rows(DAY_1, hour, count=count, version=version)["id"].to_pylist())


def replace_hour_8(version: int, count: int) -> Callable[[Table], CommitResult]:
    def commit(loaded: Table) -> CommitResult:
        return overwrite_unit(
            loaded, sk1.rows(DAY_1, 8, count=count, version=version), Unit(DAY_1, 8)
        )

    return commit


@pytest.mark.parametrize(
    ("other_day", "other_hour"), [(DAY_2, 9), (DAY_1, 9)], ids=["another day", "another hour"]
)
def test_a_commit_which_lost_a_race_against_another_unit_lands_after_the_other_commit(
    other_day: date, other_hour: int, local_lake: Lake
) -> None:
    table = sk1.create(local_lake, "conflict_other_unit")
    overwrite_unit(table, sk1.rows(DAY_1, 8), Unit(DAY_1, 8))
    stale = local_lake.catalog.load_table(table.name())
    other = overwrite_unit(
        local_lake.catalog.load_table(table.name()),
        sk1.rows(other_day, other_hour),
        Unit(other_day, other_hour),
    )
    load = StaleFirst(local_lake, stale)

    result = commit_with_retry(load, replace_hour_8(version=2, count=3))

    fresh = local_lake.catalog.load_table(table.name())
    assert sk1.ids(fresh, HOUR_8) == expected_ids(8, 2, count=3)
    assert len(sk1.ids(fresh)) == 3 + 5
    # The snapshot before the commit is the snapshot of the other writer, not the stale one.
    assert result.previous_snapshot_id == other.snapshot_id
    assert result.operations == ("delete", "append")
    current = fresh.current_snapshot()
    assert current is not None and current.snapshot_id == result.snapshot_id
    # PyIceberg 0.12.0 repeats the commit by itself: the other commit touched another unit.
    assert load.loads == 1


def test_a_commit_which_lost_a_race_for_the_same_unit_replaces_the_unit_of_the_other_writer(
    local_lake: Lake,
) -> None:
    table = sk1.create(local_lake, "conflict_same_unit")
    overwrite_unit(table, sk1.rows(DAY_1, 8), Unit(DAY_1, 8))
    overwrite_unit(table, sk1.rows(DAY_1, 9), Unit(DAY_1, 9))
    stale = local_lake.catalog.load_table(table.name())
    other = overwrite_unit(
        local_lake.catalog.load_table(table.name()),
        sk1.rows(DAY_1, 8, count=7, version=2),
        Unit(DAY_1, 8),
    )
    load = StaleFirst(local_lake, stale)

    result = commit_with_retry(load, replace_hour_8(version=3, count=3))

    fresh = local_lake.catalog.load_table(table.name())
    # The last writer wins for the unit; the other hour of the day is untouched.
    assert sk1.ids(fresh, HOUR_8) == expected_ids(8, 3, count=3)
    assert len(sk1.ids(fresh, HOUR_9)) == 5
    assert result.previous_snapshot_id == other.snapshot_id
    assert result.rows == 3
    # PyIceberg 0.12.0 raises ValidationException for the same unit. The second attempt of
    # commit_with_retry, on a freshly loaded table, lands.
    assert load.loads == 2


def test_a_staged_day_is_committed_after_another_writer_changed_the_day(local_lake: Lake) -> None:
    table = sk1.create(local_lake, "conflict_day")
    overwrite_unit(table, sk1.rows(DAY_1, 8), Unit(DAY_1, 8))
    overwrite_unit(table, sk1.rows(DAY_2, 8), Unit(DAY_2, 8))
    stale = local_lake.catalog.load_table(table.name())
    day = DayFile(stale, DAY_1)
    for hour in (8, 9, 10):
        day.write(sk1.rows(DAY_1, hour, version=2))
    assert day.read(["id", "slot_date"]).num_rows == 15
    # Another writer adds an hour to the day while the day file is staged.
    other = overwrite_unit(
        local_lake.catalog.load_table(table.name()), sk1.rows(DAY_1, 11), Unit(DAY_1, 11)
    )
    load = StaleFirst(local_lake, stale)

    result = commit_with_retry(load, day.commit)

    fresh = local_lake.catalog.load_table(table.name())
    # The day holds the staged rows only, in one file; the other day is untouched.
    assert sk1.ids(fresh, ONLY_DAY_1) == sorted(
        identifier for hour in (8, 9, 10) for identifier in expected_ids(hour, 2)
    )
    assert len(sk1.ids(fresh, ONLY_DAY_2)) == 5
    day_1_files = [path for path in sk1.data_files(fresh) if "slot_date=2026-11-24" in path]
    assert day_1_files == [day.path]
    assert result.previous_snapshot_id == other.snapshot_id
    assert (result.rows, result.operations) == (15, ("delete", "append"))
    # ValidationException first: the other writer changed the same day. The staged file is
    # used again by the second attempt.
    assert load.loads == 2


def test_a_day_file_without_rows_leaves_no_file_and_empties_the_day(local_lake: Lake) -> None:
    table = sk1.create(local_lake, "day_file_empty")
    overwrite_unit(table, sk1.rows(DAY_1, 8), Unit(DAY_1, 8))
    overwrite_unit(table, sk1.rows(DAY_2, 8), Unit(DAY_2, 8))
    files_before = set(sk1.data_files(table))
    day = DayFile(table, DAY_1)
    day.write(sk1.empty())

    assert day.read(["id"]).num_rows == 0
    assert not table.io.new_input(day.path).exists()
    result = day.commit()

    assert (result.rows, result.operations) == (0, ("delete",))
    assert sk1.ids(table, ONLY_DAY_1) == []
    assert len(sk1.ids(table, ONLY_DAY_2)) == 5
    assert set(sk1.data_files(table)) < files_before
    with pytest.raises(RuntimeError, match="closed"):
        day.write(sk1.rows(DAY_1, 8))
    day.discard()  # nothing to remove
