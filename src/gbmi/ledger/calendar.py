"""The slot calendar: which source files are expected (blueprint 8.2).

Expected slots come from the calendar and not from the list of the source, so a file which the
source never lists is noticed. Every time is UTC (blueprint 7.11), so every day has 96 slots,
also the two days of a year on which clocks change in the United States.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime, time, timedelta

from gbmi.core.gdelt import SLOT_MINUTES, Slot

SLOT_LENGTH = timedelta(minutes=SLOT_MINUTES)
SLOTS_PER_DAY = 24 * 60 // SLOT_MINUTES
#: GDELT uploads a file about ten minutes before the time in the file name (week 2 plan, F1),
#: so the calendar expects the slot which starts up to 15 minutes after the clock.
LEAD = timedelta(minutes=15)


def _utc(moment: datetime) -> datetime:
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError(f"the calendar works with times which carry a time zone, got {moment!r}")
    return moment.astimezone(UTC)


def slot_at(moment: datetime) -> Slot:
    """The slot which contains the moment: the last quarter hour at or before the moment."""
    utc = _utc(moment)
    return Slot(utc.replace(minute=utc.minute - utc.minute % SLOT_MINUTES, second=0, microsecond=0))


def newest_expected(as_of: datetime, lead: timedelta = LEAD) -> Slot:
    """The newest slot which the calendar expects when the clock shows ``as_of``."""
    return slot_at(_utc(as_of) + lead)


def slots_between(first: Slot, last: Slot) -> Iterator[Slot]:
    """Every slot from ``first`` to ``last``, both included, in time order."""
    start = first.start
    while start <= last.start:
        yield Slot(start)
        start += SLOT_LENGTH


def first_slot_of(day: date) -> Slot:
    return Slot(datetime.combine(day, time(0, 0), tzinfo=UTC))


def last_slot_of(day: date) -> Slot:
    return Slot(datetime.combine(day, time(23, 45), tzinfo=UTC))


def slots_of_day(day: date) -> list[Slot]:
    """The 96 slots of one UTC day."""
    return list(slots_between(first_slot_of(day), last_slot_of(day)))


def slots_of_days(first_day: date, last_day: date) -> Iterator[Slot]:
    """The slots of every UTC day from ``first_day`` to ``last_day``, both included."""
    return slots_between(first_slot_of(first_day), last_slot_of(last_day))


def days_between(first_day: date, last_day: date) -> list[date]:
    """Every day from ``first_day`` to ``last_day``, both included."""
    return [first_day + timedelta(days=offset) for offset in range((last_day - first_day).days + 1)]
