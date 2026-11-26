"""The content checksum ignores row order and run metadata, and nothing else."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pyarrow as pa
import pytest

from gbmi.core.checksum import (
    VOLATILE_COLUMNS,
    ContentChecksum,
    business_columns,
    combine_checksums,
    content_checksum,
)


def _table(rows: list[dict[str, object]]) -> pa.Table:
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
            ("score", pa.float64()),
            ("tags", pa.list_(pa.string())),
            ("seen_at", pa.timestamp("us", tz="UTC")),
            ("_run_id", pa.string()),
            ("_ingested_at", pa.timestamp("us", tz="UTC")),
        ]
    )
    return pa.Table.from_pylist(rows, schema=schema)


ROWS: list[dict[str, object]] = [
    {"id": 1, "name": "alpha", "score": 1.5, "tags": ["a", "b"],
     "seen_at": datetime(2026, 11, 25, 8, 15, tzinfo=UTC), "_run_id": "run-1",
     "_ingested_at": datetime(2026, 11, 25, 9, 0, tzinfo=UTC)},
    {"id": 2, "name": None, "score": None, "tags": None, "seen_at": None, "_run_id": "run-1",
     "_ingested_at": datetime(2026, 11, 25, 9, 0, tzinfo=UTC)},
    {"id": 3, "name": "", "score": -2.25, "tags": [], "seen_at": datetime(2026, 1, 1, tzinfo=UTC),
     "_run_id": "run-1", "_ingested_at": datetime(2026, 11, 25, 9, 0, tzinfo=UTC)},
]  # fmt: skip


def _changed(index: int, **changes: object) -> pa.Table:
    rows = [dict(row) for row in ROWS]
    rows[index].update(changes)
    return _table(rows)


def test_row_order_does_not_matter() -> None:
    forwards = content_checksum(_table(ROWS))
    backwards = content_checksum(_table(ROWS[::-1]))

    assert forwards == backwards
    assert forwards.rows == 3
    assert len(forwards.digest) == 16


def test_volatile_columns_are_ignored_by_default() -> None:
    rerun = _table(
        [
            {**row, "_run_id": "run-2", "_ingested_at": datetime(2026, 11, 26, tzinfo=UTC)}
            for row in ROWS
        ]
    )

    assert content_checksum(rerun) == content_checksum(_table(ROWS))
    assert business_columns(rerun) == ("id", "name", "score", "tags", "seen_at")
    assert {"_run_id", "_ingested_at"} <= VOLATILE_COLUMNS


def test_volatile_columns_count_when_named() -> None:
    rerun = _changed(0, _run_id="run-2")

    assert content_checksum(rerun, ["id", "_run_id"]) != content_checksum(
        _table(ROWS), ["id", "_run_id"]
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"name": "alpha "},
        {"score": 1.51},
        {"tags": ["a, b"]},
        {"tags": ["b", "a"]},
        {"seen_at": datetime(2026, 11, 25, 8, 15, 0, 1, tzinfo=UTC)},
        {"id": 10},
    ],
)
def test_any_business_change_is_detected(changes: dict[str, object]) -> None:
    assert content_checksum(_changed(0, **changes)) != content_checksum(_table(ROWS))


def test_null_and_empty_string_differ() -> None:
    assert content_checksum(_changed(1, name="")) != content_checksum(_table(ROWS))


def test_duplicate_row_changes_the_digest() -> None:
    doubled = content_checksum(_table([*ROWS, ROWS[0], ROWS[0]]))
    single = content_checksum(_table(ROWS))

    assert doubled.rows == 5
    assert doubled.digest != single.digest


def test_float_noise_below_the_rounding_is_ignored() -> None:
    assert content_checksum(_changed(0, score=1.5 + 1e-9)) == content_checksum(_table(ROWS))


def test_same_instant_in_another_time_zone_is_equal() -> None:
    manila = timezone(timedelta(hours=8))
    shifted = _table(ROWS).set_column(
        4,
        "seen_at",
        pa.array(
            [
                datetime(2026, 11, 25, 16, 15, tzinfo=manila),
                None,
                datetime(2026, 1, 1, 8, tzinfo=manila),
            ],
            pa.timestamp("us", tz="Asia/Manila"),
        ),
    )

    assert content_checksum(shifted) == content_checksum(_table(ROWS))


def test_empty_table_has_a_checksum() -> None:
    empty = content_checksum(_table([]))

    assert empty.rows == 0
    assert empty.digest == "0" * 16


def test_unknown_column_is_an_error() -> None:
    with pytest.raises(KeyError):
        content_checksum(_table(ROWS), ["id", "missing"])


@pytest.mark.parametrize("cut", [0, 1, 2, 3])
def test_the_checksum_of_a_whole_is_the_combination_of_its_parts(cut: int) -> None:
    whole = content_checksum(_table(ROWS))

    parts = [content_checksum(_table(ROWS[:cut])), content_checksum(_table(ROWS[cut:]))]

    assert combine_checksums(parts) == whole
    assert combine_checksums(reversed(parts)) == whole


def test_a_part_which_is_counted_twice_changes_the_combination() -> None:
    part = content_checksum(_table(ROWS))

    twice = combine_checksums([part, part])

    assert twice.rows == 6
    assert twice == content_checksum(_table(ROWS + ROWS))


def test_parts_over_different_columns_do_not_combine() -> None:
    table = _table(ROWS)

    with pytest.raises(ValueError, match="different columns"):
        combine_checksums([content_checksum(table), content_checksum(table, ["id"])])


def test_no_part_combines_to_no_rows() -> None:
    assert combine_checksums([]) == ContentChecksum(rows=0, digest="0" * 16, columns=())
