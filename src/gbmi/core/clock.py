"""The clock as an argument (week 2 plan 3.3).

Every function which depends on the current time takes the time as the argument ``as_of``. The
command line and the DAGs are the only places which read the real clock, through the functions
here, so tests and drills control the time and no test depends on the day of the run.
"""

from __future__ import annotations

from datetime import UTC, date, datetime


def utc_now() -> datetime:
    """The real clock, in UTC."""
    return datetime.now(UTC)


def parse_instant(text: str) -> datetime:
    """A time written in ISO 8601 with a time zone, for example ``2026-11-26T08:08:00+00:00``."""
    try:
        moment = datetime.fromisoformat(text)
    except ValueError as error:
        raise ValueError(f"not a time in ISO 8601: {text!r}") from error
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError(f"the time {text!r} has no time zone; write for example {text}+00:00")
    return moment.astimezone(UTC)


def resolve_as_of(text: str | None) -> datetime:
    """The time a run works with: the given time, or the real clock when none is given."""
    return parse_instant(text) if text else utc_now()


def parse_day(text: str) -> date:
    """A UTC day written ``YYYY-MM-DD``."""
    try:
        return date.fromisoformat(text)
    except ValueError as error:
        raise ValueError(f"a day is written YYYY-MM-DD, got {text!r}") from error
