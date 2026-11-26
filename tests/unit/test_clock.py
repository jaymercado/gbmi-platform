"""The clock helpers: a time needs a time zone, and a missing time is the real clock."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

from gbmi.core.clock import parse_day, parse_instant, resolve_as_of, resolve_day, utc_now


def test_a_time_is_read_with_its_time_zone_and_kept_in_utc() -> None:
    expected = datetime(2026, 11, 26, 8, 8, tzinfo=UTC)

    assert parse_instant("2026-11-26T08:08:00+00:00") == expected
    assert parse_instant("2026-11-26T08:08:00Z") == expected
    assert parse_instant("2026-11-26T16:08:00+08:00") == expected
    assert parse_instant("2026-11-26T16:08:00+08:00").tzinfo is UTC


@pytest.mark.parametrize("text", ["2026-11-26T08:08:00", "2026-11-26", "yesterday", ""])
def test_a_time_without_a_time_zone_is_refused(text: str) -> None:
    with pytest.raises(ValueError, match=r"time zone|ISO 8601"):
        parse_instant(text)


def test_a_missing_time_is_the_real_clock() -> None:
    before = utc_now()
    resolved = resolve_as_of(None)

    assert before <= resolved <= utc_now()
    assert resolved.utcoffset() == timedelta(0)
    assert resolve_as_of("2026-11-26T08:08:00Z") == datetime(2026, 11, 26, 8, 8, tzinfo=UTC)


def test_a_day_is_written_with_four_two_and_two_digits() -> None:
    assert parse_day("2026-11-25") == date(2026, 11, 25)
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        parse_day("25/11/2026")


def test_a_date_scoped_run_takes_the_named_day_or_the_date_of_its_logical_date() -> None:
    assert resolve_day("2026-11-25", None) == date(2026, 11, 25)
    assert resolve_day(None, "2026-11-25T08:25:00+00:00") == date(2026, 11, 25)
    # The date is the UTC date of the logical date.
    assert resolve_day(None, "2026-11-26T02:00:00+08:00") == date(2026, 11, 25)
    # The named day counts when the run has both.
    assert resolve_day("2026-11-20", "2026-11-25T08:25:00+00:00") == date(2026, 11, 20)
    # A template renders a missing value as an empty text.
    assert resolve_day("", "2026-11-25T08:25:00+00:00") == date(2026, 11, 25)


def test_a_run_without_a_day_and_without_a_logical_date_is_refused() -> None:
    for day, logical_date in ((None, None), ("", "")):
        with pytest.raises(ValueError, match="name the day with the parameter day"):
            resolve_day(day, logical_date)
    with pytest.raises(ValueError, match="a day is written YYYY-MM-DD"):
        resolve_day("25 November", None)
