"""Gate G3, the pre-commit audit, check by check (week 2 plan 3.3)."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pyarrow as pa
import pytest

from gbmi.lake.commit import Scope, Unit
from gbmi.quality.audit import (
    AuditError,
    UnitCounts,
    check_accounting,
    check_change,
    check_fall,
    check_keys,
    check_lists,
    check_org_rows,
    check_quarantine,
    check_rows,
    check_volume,
    list_columns,
    null_lists,
    problems,
    raise_if_failed,
)
from gbmi.quality.results import ACCEPTED, FAILED, G3, PASSED, SKIPPED, CheckResult

DAY = date(2026, 11, 25)
HOUR = Unit(DAY, 8)
SCHEMA = pa.schema(
    [
        ("slot_date", pa.date32()),
        ("slot_hour", pa.int32()),
        ("slot_ts", pa.timestamp("us", tz="UTC")),
        ("id", pa.int64()),
        ("name", pa.string()),
        ("themes", pa.list_(pa.string())),
    ]
)


def rows(*entries: tuple[int | None, str | None], hour: int = 8, day: date = DAY) -> pa.Table:
    return pa.Table.from_pylist(
        [
            {
                "slot_date": day,
                "slot_hour": hour,
                "slot_ts": datetime(day.year, day.month, day.day, hour, tzinfo=UTC),
                "id": identifier,
                "name": name,
                "themes": [],
            }
            for identifier, name in entries
        ],
        schema=SCHEMA,
    )


def statuses(results: list[CheckResult]) -> dict[str, str]:
    return {result.check_name: result.status for result in results}


def test_sound_accounting_passes_three_checks() -> None:
    results = check_accounting(UnitCounts(10, 9, 1, 2, 7), 7, HOUR, table="t", feed="export")

    assert statuses(results) == {
        "accounting_raw_lines": PASSED,
        "accounting_committed": PASSED,
        "row_count": PASSED,
    }
    assert {(result.gate, result.table_name, result.feed, result.unit) for result in results} == {
        (G3, "t", "export", "2026-11-25T08")
    }
    assert problems(results) == []


@pytest.mark.parametrize(
    ("counts", "rows_to_commit", "failed", "problem"),
    [
        (UnitCounts(11, 9, 1, 2, 7), 7, "accounting_raw_lines", "raw lines (11) != parsed (9)"),
        (UnitCounts(10, 9, 1, 1, 7), 7, "accounting_committed", "committed (7) != parsed (9)"),
        (UnitCounts(10, 9, 1, 2, 7), 8, "row_count", "8 rows to commit"),
    ],
)
def test_each_broken_identity_fails_its_own_check(
    counts: UnitCounts, rows_to_commit: int, failed: str, problem: str
) -> None:
    results = check_accounting(counts, rows_to_commit, HOUR)

    assert [result.check_name for result in results if result.status == FAILED] == [failed]
    assert problem in problems(results)[0]


def test_a_unique_key_inside_the_unit_passes() -> None:
    results = check_keys(rows((1, "a"), (2, "b")), HOUR, key=["id"])

    assert statuses(results) == {
        "key_not_null": PASSED,
        "key_unique": PASSED,
        "unit_membership": PASSED,
    }


def test_a_repeated_key_and_a_missing_key_fail() -> None:
    repeated = check_keys(rows((1, "a"), (1, "b"), (2, "c")), HOUR, key=["id"])
    missing = check_keys(rows((1, "a"), (None, "b")), HOUR, key=["id"])

    assert statuses(repeated)["key_unique"] == FAILED
    assert [result.observed_value for result in repeated] == [0, 1, None]
    assert "id is not unique: 3 rows, 2 distinct keys" in problems(repeated)
    assert statuses(missing) == {
        "key_not_null": FAILED,
        "key_unique": PASSED,
        "unit_membership": PASSED,
    }
    assert problems(missing) == ["1 rows have no id"]


def test_a_key_of_several_columns_is_unique_as_a_whole() -> None:
    table = rows((1, "a"), (1, "b"), (2, "a"))

    assert statuses(check_keys(table, HOUR, key=["id", "name"]))["key_unique"] == PASSED
    again = pa.concat_tables([table, rows((1, "b"))])
    failed = check_keys(again, HOUR, key=["id", "name"])
    assert statuses(failed)["key_unique"] == FAILED
    assert "id, name is not unique: 4 rows, 3 distinct keys" in problems(failed)
    # The slot is a part of the key: the same pair in another slot is another row.
    other_slot = pa.concat_tables([table, rows((1, "b"), hour=9)])
    assert (
        statuses(check_keys(other_slot, Unit(DAY), key=["id", "name", "slot_ts"]))["key_unique"]
        == PASSED
    )


def test_a_unit_without_rows_passes_every_check_of_the_rows() -> None:
    for key in (["id"], ["id", "name", "slot_ts"]):
        results = check_rows(rows(), HOUR, key=key)

        assert set(statuses(results).values()) == {PASSED}
        assert [result.observed_value for result in results] == [0, 0, None, 0]


def test_a_row_of_another_hour_or_another_day_does_not_belong_to_the_unit() -> None:
    other_hour = pa.concat_tables([rows((1, "a")), rows((2, "b"), hour=9)])
    other_day = pa.concat_tables([rows((1, "a")), rows((2, "b"), day=date(2026, 11, 24))])

    assert statuses(check_keys(other_hour, HOUR, key=["id"]))["unit_membership"] == FAILED
    assert statuses(check_keys(other_hour, Unit(DAY), key=["id"]))["unit_membership"] == PASSED
    failed = check_keys(other_day, Unit(DAY), key=["id"])
    assert statuses(failed)["unit_membership"] == FAILED
    assert "slot_date" in problems(failed)[0]


def test_a_null_list_fails_and_an_empty_list_passes() -> None:
    table = rows((1, "a"), (2, "b"))
    with_null = table.set_column(
        table.schema.get_field_index("themes"),
        "themes",
        pa.array([["TAX"], None], pa.list_(pa.string())),
    )

    assert list_columns(table.schema) == ["themes"]
    assert (null_lists(table), null_lists(with_null)) == (0, 1)
    assert statuses(check_rows(table, HOUR, key=["id"]))["lists_not_null"] == PASSED
    failed = check_rows(with_null, HOUR, key=["id"])
    assert statuses(failed)["lists_not_null"] == FAILED
    assert problems(failed) == ["1 NULL lists in themes"]
    assert check_lists(0, ["themes", "countries"], HOUR).status == PASSED
    # A table without a list column has no check of lists.
    assert "lists_not_null" not in statuses(
        check_rows(table.drop_columns(["themes"]), HOUR, key=["id"])
    )


def test_the_organisation_rows_equal_the_count_of_the_documents() -> None:
    same = check_org_rows(541, 541, HOUR, table="gdelt_gkg_org", feed="gkg")
    fewer = check_org_rows(540, 541, HOUR, table="gdelt_gkg_org", feed="gkg")

    assert (same.status, same.check_name) == (PASSED, "rows_equal_sum_of_org_count")
    assert fewer.status == FAILED
    assert fewer.detail["problem"] == "540 organisation rows, but the documents count 541"


def test_the_quarantine_file_holds_every_quarantined_line() -> None:
    counts = UnitCounts(10, 8, 2, 0, 8)

    assert check_quarantine(counts, 2, HOUR).status == PASSED
    short = check_quarantine(counts, 1, HOUR)
    assert short.status == FAILED
    assert short.detail["problem"] == ("1 rows for the quarantine file, but the accounting says 2")


@pytest.mark.parametrize(
    ("day_rows", "status"),
    [(50, PASSED), (100, PASSED), (200, PASSED), (49, FAILED), (201, FAILED), (0, FAILED)],
)
def test_the_volume_band_is_half_to_twice_the_median(day_rows: int, status: str) -> None:
    result = check_volume(day_rows, [100, 90, 100, 110], Unit(DAY), table="t", feed="export")

    assert (result.status, result.check_name, result.observed_value) == (
        status,
        "volume_band",
        day_rows,
    )
    assert result.detail["median"] == 100
    assert result.threshold == "between 0.5 and 2.0 times the median of 4 same weekdays"


def test_the_median_of_four_counts_is_the_mean_of_the_two_in_the_middle() -> None:
    result = check_volume(55, [80, 200, 100, 120], Unit(DAY), table="t", feed="export")

    assert (result.status, result.detail["median"]) == (PASSED, 110)
    assert (
        check_volume(54, [80, 200, 100, 120], Unit(DAY), table="t", feed="export").status == FAILED
    )


@pytest.mark.parametrize("history", [[], [100, 100, 100], [100, None, 100, 100]])
def test_the_volume_check_is_skipped_without_four_weekdays(history: list[int | None]) -> None:
    result = check_volume(1, history, Unit(DAY), table="t", feed="export")

    assert (result.status, result.detail["reason"]) == (SKIPPED, "history is short")
    assert problems([result]) == []


def test_an_operator_accepts_a_day_outside_the_band_with_a_note() -> None:
    history: list[int | None] = [100, 100, 100, 100]

    accepted = check_volume(
        10, history, Unit(DAY), table="t", feed="export", accept_note="a strike day"
    )
    inside = check_volume(
        100, history, Unit(DAY), table="t", feed="export", accept_note="a strike day"
    )

    assert (accepted.status, accepted.detail["note"]) == (ACCEPTED, "a strike day")
    assert "outside 0.5 to 2.0 times the median 100" in accepted.detail["problem"]
    assert problems([accepted]) == []
    # A note changes nothing for a day inside the band.
    assert inside.status == PASSED and "note" not in inside.detail


def test_a_failed_check_raises_with_every_problem() -> None:
    results = [
        *check_accounting(UnitCounts(11, 9, 1, 1, 7), 7, HOUR),
        check_org_rows(1, 2, HOUR, table="gdelt_gkg_org", feed="gkg"),
    ]

    with pytest.raises(AuditError) as raised:
        raise_if_failed(results, HOUR)

    message = str(raised.value)
    assert message.startswith("unit 2026-11-25T08 failed the pre-commit audit: ")
    assert message.count(";") == 2
    raise_if_failed(check_accounting(UnitCounts(10, 9, 1, 2, 7), 7, HOUR), HOUR)


# --- the units and the volume rules of the SEC tables (week 3 plan 3.3) -------------------------


def test_the_row_checks_take_any_unit_of_work() -> None:
    scope = Scope.for_keys("cik", [1326801, 1045810])
    good = pa.table({"cik": [1326801, 1045810], "accession_number": ["a", "b"]})
    foreign = pa.table({"cik": [1326801, 63276], "accession_number": ["a", "a"]})

    passed = check_rows(good, scope, key=["cik", "accession_number"], table="sec_filing_detail")
    failed = check_rows(foreign, scope, key=["cik", "accession_number"], table="sec_filing_detail")

    assert {result.status for result in passed} == {PASSED}
    assert {result.unit for result in passed} == {scope.label}
    assert [result.check_name for result in failed if result.status == FAILED] == [
        "unit_membership"
    ]
    assert "rows of cik 63276" in problems(failed)[0]
    with pytest.raises(AuditError, match=f"unit {scope.label} failed the pre-commit audit"):
        raise_if_failed(failed, scope)


def test_the_volume_band_of_an_index_file_is_wider_than_the_band_of_a_gdelt_day() -> None:
    # The index files of one quarter differ by a factor of 2.7 in size (week 3 plan, F10).
    unit = Scope.for_date("index_date", DAY)
    history = [5000, 6000, 5500, 5800]  # the median is 5650

    def check(rows: int, **options: object) -> str:
        return check_volume(
            rows, history, unit, table="sec_filing_index", feed="index", **options
        ).status  # type: ignore[arg-type]

    assert check(2000) == FAILED  # below half of the median: outside the band of phase P1
    assert check(2000, band=(0.33, 3.0)) == PASSED
    assert check(16_000, band=(0.33, 3.0)) == PASSED
    assert check(1800, band=(0.33, 3.0)) == FAILED
    assert check(17_000, band=(0.33, 3.0)) == FAILED
    result = check_volume(
        1800, history, unit, table="t", feed="index", band=(0.33, 3.0), accept_note="half day"
    )
    assert result.status == ACCEPTED and "0.33 and 3.0 times" in (result.threshold or "")
    assert check_volume(5000, [None, 1, 2, 3], unit, table="t", feed="index").status == SKIPPED


def test_the_universe_may_change_by_five_percent_in_both_directions() -> None:
    unit = Scope.for_date("snapshot_date", DAY)

    def check(rows: int, previous: int | None, note: str | None = None) -> CheckResult:
        return check_change(
            rows,
            previous,
            unit,
            table="sec_company_snapshot",
            feed="reference",
            tolerance=0.05,
            name="universe_change",
            accept_note=note,
        )

    assert check(7988, None).status == SKIPPED  # the first snapshot has no snapshot before
    assert check(7988, 7988).status == PASSED
    assert check(8387, 7988).status == PASSED  # 5% more, rounded down
    assert check(7589, 7988).status == PASSED
    assert check(8388, 7988).status == FAILED
    assert check(7588, 7988).status == FAILED
    failed = check(4000, 7988)
    assert (failed.gate, failed.check_name, failed.unit) == (G3, "universe_change", "2026-11-25")
    assert failed.detail["previous"] == 7988 and "more than 5%" in failed.detail["problem"]
    accepted = check(4000, 7988, "the SEC removed the over-the-counter companies")
    assert accepted.status == ACCEPTED and accepted.detail["note"].startswith("the SEC removed")


def test_the_facts_of_a_company_may_not_fall_by_more_than_two_percent() -> None:
    unit = Scope.for_keys("cik", [63276])

    def check(rows: int, previous: int | None) -> CheckResult:
        return check_fall(
            rows,
            previous,
            unit,
            table="sec_xbrl_fact",
            feed="companyfacts",
            max_fall=0.02,
            name="facts_not_fewer",
        )

    assert check(28_712, None).status == SKIPPED
    assert check(28_712, 28_712).status == PASSED
    assert check(40_000, 28_712).status == PASSED  # more facts always pass
    assert check(28_138, 28_712).status == PASSED  # 2% fewer, rounded up
    failed = check(27_850, 28_712)  # 3% fewer
    assert failed.status == FAILED and failed.check_name == "facts_not_fewer"
    assert "more than 2% below 28712 rows" in failed.detail["problem"]
    # An operator who knows the reason accepts the fall with a note.
    accepted = check_fall(
        27_850,
        28_712,
        unit,
        table="sec_xbrl_fact",
        feed="companyfacts",
        max_fall=0.02,
        name="facts_not_fewer",
        accept_note="the SEC removed old facts",
    )
    assert accepted.status == ACCEPTED
    assert accepted.detail["note"] == "the SEC removed old facts"
