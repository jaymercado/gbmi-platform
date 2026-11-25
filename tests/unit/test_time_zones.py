"""The package brings its own time-zone database (found by the Linux rehearsal of the workflows).

Silver timestamps are ``timestamptz``. PyArrow resolves the zone through ``zoneinfo`` when it
returns such values to Python, and ``zoneinfo`` needs the IANA database: from the system, or
from the ``tzdata`` package. A minimal container has no system copy.
"""

from __future__ import annotations

import zoneinfo
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest


@pytest.fixture
def no_system_time_zones() -> Iterator[None]:
    """Hide the system's time-zone database for one test."""
    original = zoneinfo.TZPATH
    zoneinfo.reset_tzpath(to=())
    zoneinfo.ZoneInfo.clear_cache()
    try:
        yield
    finally:
        zoneinfo.reset_tzpath(to=original)
        zoneinfo.ZoneInfo.clear_cache()


@pytest.mark.usefixtures("no_system_time_zones")
def test_utc_loads_from_the_tzdata_package() -> None:
    moment = datetime(2026, 11, 25, 8, 15, tzinfo=zoneinfo.ZoneInfo("UTC"))

    assert moment.utcoffset() == timedelta(0)


@pytest.mark.usefixtures("no_system_time_zones")
def test_pyarrow_returns_timestamptz_values_without_a_system_database() -> None:
    moment = datetime(2026, 11, 25, 8, 15, tzinfo=UTC)
    column = pa.array([moment], type=pa.timestamp("us", tz="UTC"))

    assert column.to_pylist() == [moment]
