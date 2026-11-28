"""The sliding-window limiter: no second holds more than 8 grants (week 3 plan 3.3)."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from gbmi.sources.sec.limiter import MAX_REQUESTS, WINDOW_SECONDS, SlidingWindowLimiter


class Clock:
    """A clock which moves only when a test or the limiter says so."""

    def __init__(self, start: float = 100.0) -> None:
        self.value = start
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.value

    def sleep(self, seconds: float) -> None:
        assert seconds > 0
        self.sleeps.append(seconds)
        self.value += seconds


def limiter(clock: Clock, **options: float) -> SlidingWindowLimiter:
    return SlidingWindowLimiter(clock=clock, sleep=clock.sleep, **options)  # type: ignore[arg-type]


def max_in_window(grants: list[float], window: float = 1.0) -> int:
    """The largest number of grants in any window which counts its start and not its end."""
    return max(
        (sum(1 for other in grants if start <= other < start + window) for start in grants),
        default=0,
    )


def test_eight_requests_leave_at_once_and_the_ninth_waits_one_window() -> None:
    clock = Clock()
    gate = limiter(clock)

    waits = [gate.acquire() for _ in range(9)]

    assert waits[:8] == [0.0] * 8
    assert waits[8] == pytest.approx(WINDOW_SECONDS)
    assert clock.sleeps == [pytest.approx(WINDOW_SECONDS)]
    assert gate.last_grant == pytest.approx(100.0 + WINDOW_SECONDS)


def test_a_pause_saves_no_requests() -> None:
    # A token bucket would allow 16 requests in one second after a pause (difference D30).
    clock = Clock()
    gate = limiter(clock)
    clock.value += 3600
    grants = []
    for _ in range(16):
        gate.acquire()
        grants.append(clock.value)

    assert max_in_window(grants) == 8
    assert grants[8] - grants[0] == pytest.approx(WINDOW_SECONDS)


def test_the_first_request_waits_until_the_client_may_start() -> None:
    clock = Clock()
    gate = limiter(clock, not_before=clock.value + WINDOW_SECONDS)

    assert gate.last_grant is None
    assert gate.acquire() == pytest.approx(WINDOW_SECONDS)
    assert gate.acquire() == 0.0


def test_a_sleep_which_ends_early_is_followed_by_another_sleep() -> None:
    clock = Clock()

    def short_sleep(seconds: float) -> None:
        clock.sleeps.append(seconds)
        clock.value += seconds / 2

    gate = SlidingWindowLimiter(clock=clock, sleep=short_sleep, not_before=clock.value + 2.0)

    gate.acquire()

    assert len(clock.sleeps) > 1
    assert clock.value >= 102.0


@pytest.mark.parametrize("too_many", [0, MAX_REQUESTS + 1, 10, 16])
def test_the_limiter_refuses_more_than_eight_requests_in_a_window(too_many: int) -> None:
    with pytest.raises(ValueError, match="1 to 8 requests"):
        SlidingWindowLimiter(too_many)


def test_the_limiter_refuses_a_window_without_the_margin() -> None:
    with pytest.raises(ValueError, match=r"at least 1\.05 seconds"):
        SlidingWindowLimiter(window=1.0)

    assert SlidingWindowLimiter(4, 2.0).earliest(7.0) == 7.0


@settings(max_examples=300, deadline=None)
@given(
    gaps=st.lists(
        st.one_of(st.just(0.0), st.floats(min_value=0.0, max_value=3.0, allow_nan=False)),
        min_size=1,
        max_size=60,
    ),
    max_requests=st.integers(min_value=1, max_value=MAX_REQUESTS),
    head_start=st.floats(min_value=0.0, max_value=2.0, allow_nan=False),
)
def test_for_any_arrival_times_the_rule_holds_and_no_grant_waits_longer_than_needed(
    gaps: list[float], max_requests: int, head_start: float
) -> None:
    clock = Clock()
    not_before = clock.value + head_start
    gate = limiter(clock, max_requests=max_requests, not_before=not_before)
    arrivals: list[float] = []
    grants: list[float] = []
    for gap in gaps:
        clock.value += gap  # the caller comes back after the gap
        arrivals.append(clock.value)
        waited = gate.acquire()
        grants.append(clock.value)
        assert waited == pytest.approx(grants[-1] - arrivals[-1])

    # The grants keep the order of the calls.
    assert grants == sorted(grants)
    # No window of one second holds more than the ceiling.
    assert max_in_window(grants) <= max_requests <= MAX_REQUESTS
    for index in range(max_requests, len(grants)):
        assert grants[index] - grants[index - max_requests] >= WINDOW_SECONDS - 1e-9
    # No grant waits longer than the rule asks.
    for index, (arrival, grant) in enumerate(zip(arrivals, grants, strict=True)):
        needed = max(arrival, not_before)
        if index >= max_requests:
            needed = max(needed, grants[index - max_requests] + WINDOW_SECONDS)
        assert grant == pytest.approx(needed, abs=1e-6)


# --- a grant is not a departure (review 1, finding B1) -----------------------------------------


def test_a_settled_grant_counts_from_the_moment_the_answer_arrived() -> None:
    clock = Clock()
    gate = limiter(clock)
    gate.settle()  # nothing was granted: nothing moves
    assert gate.last_grant is None

    gate.acquire()
    clock.value += 0.4  # the request took 400 ms
    gate.settle()

    assert gate.last_grant == pytest.approx(100.4)
    clock.value -= 0.3  # a clock which is read earlier never moves a grant back
    gate.settle()
    assert gate.last_grant == pytest.approx(100.4)


def test_a_slow_first_request_cannot_pull_nine_departures_into_one_second() -> None:
    # The first request needs 120 ms to set up its connection, the others leave at once and
    # are answered after 50 ms. Without settle() the ninth request would leave 1.05 s after
    # the first grant, which is 0.93 s after the first departure.
    clock = Clock()
    gate = limiter(clock)
    departures = []
    for number in range(24):
        gate.acquire()
        clock.value += 0.12 if number == 0 else 0.0
        departures.append(clock.value)
        clock.value += 0.05
        gate.settle()

    assert max_in_window(departures) <= MAX_REQUESTS
    assert departures[8] - departures[0] >= WINDOW_SECONDS


@settings(max_examples=300, deadline=None)
@given(
    requests=st.lists(
        st.tuples(
            st.floats(min_value=0.0, max_value=0.4, allow_nan=False),  # until the departure
            st.floats(min_value=0.0, max_value=0.4, allow_nan=False),  # until the answer
            st.floats(min_value=0.0, max_value=1.5, allow_nan=False),  # until the next call
        ),
        min_size=1,
        max_size=60,
    ),
    max_requests=st.integers(min_value=1, max_value=MAX_REQUESTS),
)
def test_for_any_request_times_no_second_holds_more_departures_than_the_ceiling(
    requests: list[tuple[float, float, float]], max_requests: int
) -> None:
    clock = Clock()
    gate = limiter(clock, max_requests=max_requests)
    departures: list[float] = []
    arrivals: list[float] = []
    for setup, answer, pause in requests:
        gate.acquire()
        clock.value += setup
        departures.append(clock.value)  # the request leaves
        clock.value += answer / 2
        arrivals.append(clock.value)  # the request reaches the server
        clock.value += answer / 2
        gate.settle()  # the answer is back
        clock.value += pause

    # The rule holds where the requests leave and where the requests arrive.
    assert max_in_window(departures) <= max_requests
    assert max_in_window(arrivals) <= max_requests
