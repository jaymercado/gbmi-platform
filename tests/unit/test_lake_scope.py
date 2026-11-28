"""The units of work of the SEC tables: a date, a set of keys, the whole table (week 3 plan 3.3)."""

from __future__ import annotations

from datetime import date
from typing import Any

import pyarrow as pa
import pytest
from pyiceberg.expressions import AlwaysTrue, EqualTo, In

from gbmi.lake.commit import (
    KIND_DAY,
    KIND_HOUR,
    KIND_KEYS,
    KIND_TABLE,
    MAX_SCOPE_KEYS,
    Scope,
    Unit,
    UnitMismatchError,
    UnitOfWork,
)

DAY = date(2026, 11, 25)


def rows(**columns: list[object]) -> pa.Table:
    return pa.table(columns)


def test_a_gdelt_unit_and_a_scope_are_both_a_unit_of_work() -> None:
    units: list[UnitOfWork] = [
        Unit(DAY),
        Unit(DAY, 8),
        Scope.for_date("snapshot_date", DAY),
        Scope.for_keys("cik", [1326801, 1045810]),
        Scope.for_table(),
    ]

    assert [unit.kind for unit in units] == [KIND_DAY, KIND_HOUR, KIND_DAY, KIND_KEYS, KIND_TABLE]
    assert [unit.label for unit in units][:3] == ["2026-11-25", "2026-11-25T08", "2026-11-25"]
    assert units[4].label == "table"
    for unit in units:
        assert unit.row_filter is not None
        unit.check(pa.table({"anything": pa.array([], pa.int64())}))  # no rows: nothing to refuse


def test_a_scope_of_one_date_owns_the_rows_of_the_date() -> None:
    scope = Scope.for_date("index_date", DAY)

    assert isinstance(scope.row_filter, EqualTo)
    assert "index_date" in repr(scope.row_filter) and "2026-11-25" in repr(scope.row_filter)
    scope.check(rows(index_date=[DAY, DAY], cik=[1, 2]))
    with pytest.raises(UnitMismatchError, match="unit 2026-11-25 was given rows of index_date"):
        scope.check(rows(index_date=[DAY, date(2026, 11, 24)], cik=[1, 2]))


def test_a_scope_of_keys_owns_the_rows_of_the_keys_and_has_a_stable_label() -> None:
    scope = Scope.for_keys("cik", [1326801, 63276, 1045810, 63276])

    assert scope.keys == frozenset({63276, 1045810, 1326801})
    assert isinstance(scope.row_filter, In)
    assert scope.label == Scope.for_keys("cik", (63276, 1045810, 1326801)).label
    assert scope.label.startswith("cik:3:") and len(scope.label) == len("cik:3:") + 8
    assert scope.label != Scope.for_keys("cik", [63276, 1045810, 789019]).label
    assert scope == Scope.for_keys("cik", {1045810, 1326801, 63276})
    scope.check(rows(cik=[63276, 63276, 1326801]))
    with pytest.raises(UnitMismatchError, match="was given rows of cik 789019"):
        scope.check(rows(cik=[63276, 789019]))
    # A key of the unit without a row is allowed: the commit empties the key.
    scope.check(rows(cik=[63276]))


def test_one_key_gives_an_equality_filter() -> None:
    # PyIceberg drops a whole data file only when the filter proves that every row matches.
    assert isinstance(Scope.for_keys("cik", [63276]).row_filter, EqualTo)


@pytest.mark.parametrize("count", [0, MAX_SCOPE_KEYS + 1, 7988])
def test_a_scope_of_keys_holds_1_to_200_values(count: int) -> None:
    with pytest.raises(ValueError, match="1 to 200 values of cik"):
        Scope.for_keys("cik", range(1, count + 1))

    assert len(Scope.for_keys("cik", range(1, MAX_SCOPE_KEYS + 1)).keys) == MAX_SCOPE_KEYS


def test_a_scope_of_the_whole_table_owns_every_row() -> None:
    scope = Scope.for_table()

    assert isinstance(scope.row_filter, AlwaysTrue)
    scope.check(rows(cik=[1, 2, 3], former_name=["a", "b", "c"]))


def test_many_foreign_values_are_shown_in_short() -> None:
    scope = Scope.for_keys("cik", [1])

    with pytest.raises(UnitMismatchError, match=r"cik 2, 3, 4, 5, 6 \.\.\.$"):
        scope.check(rows(cik=list(range(1, 12))))


@pytest.mark.parametrize(
    "parts",
    [
        {"kind": "day", "column": "index_date"},  # no date: the filter would be "every row"
        {"kind": "day", "day": date(2026, 11, 25)},
        {"kind": "keys", "column": "cik"},
        {"kind": "keys", "keys": frozenset({1})},
        {"kind": "table", "column": "cik"},
        {"kind": "table", "keys": frozenset({1})},
        {"kind": "week"},
    ],
)
def test_a_scope_with_missing_parts_cannot_be_built(parts: dict[str, Any]) -> None:
    # A scope of one date without the date would replace the whole table (review 2, R2-14).
    with pytest.raises(ValueError, match=r"Scope\.for_date|day, keys or table"):
        Scope(**parts)
