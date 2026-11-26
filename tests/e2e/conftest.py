"""What the end-to-end tests expect of the fixture hour, counted from the fixture zips."""

from __future__ import annotations

import pytest

from tests.e2e.fixture_counts import event_counts, silver_rows


@pytest.fixture(scope="session")
def expected() -> dict[str, int]:
    """Raw lines, quarantined rows, duplicate rows and committed rows of the events."""
    return event_counts()


@pytest.fixture(scope="session")
def expected_rows() -> dict[str, int]:
    """The rows of the fixture hour in each of the four silver tables."""
    return silver_rows()
