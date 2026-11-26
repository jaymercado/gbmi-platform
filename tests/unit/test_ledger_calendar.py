"""The slot calendar: 96 slots for every UTC day, whatever the local clocks do (blueprint 7.11)."""

from __future__ import annotations

import itertools
from datetime import UTC, date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest
from hypothesis import given
from hypothesis import strategies as st

from gbmi.core.gdelt import Slot
from gbmi.ledger.calendar import (
    SLOT_LENGTH,
    SLOTS_PER_DAY,
    days_between,
    first_slot_of,
    last_slot_of,
    newest_expected,
    slot_at,
    slots_between,
    slots_of_day,
    slots_of_days,
)

days = st.dates(min_value=date(2015, 2, 18), max_value=date(2100, 12, 31))
moments = st.datetimes(
    min_value=datetime(2015, 2, 18),  # noqa: DTZ001 - hypothesis adds the time zone
    max_value=datetime(2100, 12, 31),  # noqa: DTZ001
    timezones=st.sampled_from(
        [UTC, ZoneInfo("Asia/Manila"), ZoneInfo("America/New_York"), timezone(timedelta(hours=-3))]
    ),
)


@pytest.mark.parametrize(
    "day",
    [
        date(2026, 11, 30),  # the end of a month
        date(2026, 12, 31),  # the turn of the year
        date(2027, 1, 1),
        date(2028, 2, 29),  # a leap day
        date(2026, 3, 8),  # clocks go forward in the United States (risk R26)
        date(2026, 11, 1),  # clocks go back in the United States
        date(2027, 3, 14),
        date(2027, 11, 7),
    ],
)
def test_every_day_has_96_slots(day: date) -> None:
    slots = slots_of_day(day)

    assert len(slots) == SLOTS_PER_DAY == 96
    assert (
        slots[0] == first_slot_of(day) == Slot(datetime(day.year, day.month, day.day, tzinfo=UTC))
    )
    assert slots[-1] == last_slot_of(day)
    assert slots[-1].start.strftime("%H:%M") == "23:45"
    assert {slot.slot_date for slot in slots} == {day}
    assert all(b.start - a.start == SLOT_LENGTH for a, b in itertools.pairwise(slots))


@given(days, st.integers(min_value=0, max_value=40))
def test_a_range_of_days_has_96_slots_for_each_day(first_day: date, extra: int) -> None:
    last_day = first_day + timedelta(days=extra)

    slots = list(slots_of_days(first_day, last_day))

    assert len(slots) == SLOTS_PER_DAY * (extra + 1)
    assert len(set(slots)) == len(slots)
    assert slots == sorted(slots)
    assert days_between(first_day, last_day) == sorted({slot.slot_date for slot in slots})


@given(moments)
def test_the_slot_of_a_moment_contains_the_moment(moment: datetime) -> None:
    slot = slot_at(moment)

    assert slot.start <= moment < slot.start + SLOT_LENGTH
    assert slot.start.tzinfo is UTC
    assert slot_at(slot.start) == slot


def test_the_slot_of_a_moment_is_counted_in_utc() -> None:
    manila = datetime(2026, 11, 25, 16, 7, 30, tzinfo=ZoneInfo("Asia/Manila"))

    assert slot_at(manila) == Slot(datetime(2026, 11, 25, 8, 0, tzinfo=UTC))
    with pytest.raises(ValueError, match="time zone"):
        slot_at(datetime(2026, 11, 25, 8, 7))  # noqa: DTZ001 - the test gives a naive time


@pytest.mark.parametrize(
    ("clock", "newest"),
    [
        ("13:50:30", "14:00"),  # GDELT uploaded the files named 14:00 at 13:50:30 (finding F1)
        ("13:44:59", "13:45"),
        ("13:45:00", "14:00"),
        ("23:52:00", "00:00"),  # the first slot of the next day
    ],
)
def test_the_calendar_expects_slots_up_to_15_minutes_ahead(clock: str, newest: str) -> None:
    as_of = datetime.fromisoformat(f"2026-11-26T{clock}+00:00")

    slot = newest_expected(as_of)

    assert slot.start.strftime("%H:%M") == newest
    assert timedelta(0) < slot.start + SLOT_LENGTH - as_of <= timedelta(minutes=30)


def test_slots_between_includes_both_ends_and_can_be_empty() -> None:
    first = Slot(datetime(2026, 12, 31, 23, 30, tzinfo=UTC))
    last = Slot(datetime(2027, 1, 1, 0, 15, tzinfo=UTC))

    assert [slot.stamp for slot in slots_between(first, last)] == [
        "20261231233000",
        "20261231234500",
        "20270101000000",
        "20270101001500",
    ]
    assert list(slots_between(last, first)) == []
    assert days_between(date(2027, 1, 2), date(2027, 1, 1)) == []
