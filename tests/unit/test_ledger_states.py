"""The state table of the ledger: seven states, twelve transitions, no other way (blueprint 8.2)."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from gbmi.ledger.states import (
    ALLOWED,
    FINAL,
    INITIAL,
    PENDING,
    TRANSITIONS,
    IllegalTransitionError,
    State,
    can_move,
    check,
    next_states,
    replay,
)

#: The state diagram of the blueprint, written a second time, so a change of the table shows.
DIAGRAM = {
    ("expected", "listed"),
    ("listed", "downloaded"),
    ("downloaded", "verified"),
    ("downloaded", "listed"),
    ("listed", "quarantined"),
    ("verified", "loaded"),
    ("verified", "quarantined"),
    ("quarantined", "listed"),
    ("expected", "confirmed_missing"),
    ("listed", "confirmed_missing"),
    ("quarantined", "confirmed_missing"),
    ("confirmed_missing", "listed"),
}

states = st.sampled_from(sorted(State))


def test_the_table_is_the_state_diagram_of_the_blueprint() -> None:
    assert len(State) == 7
    assert len(TRANSITIONS) == 12
    assert {(a.value, b.value) for a, b in ALLOWED} == DIAGRAM
    assert all(transition.meaning for transition in TRANSITIONS)
    assert INITIAL is State.EXPECTED
    assert frozenset({State.LOADED, State.CONFIRMED_MISSING}) == FINAL
    assert frozenset(State) == PENDING | FINAL and not PENDING & FINAL


def test_nothing_leaves_loaded_and_confirmed_missing_only_reopens() -> None:
    assert next_states(State.LOADED) == frozenset()
    assert next_states(State.CONFIRMED_MISSING) == {State.LISTED}
    assert next_states(State.EXPECTED) == {State.LISTED, State.CONFIRMED_MISSING}
    assert next_states(State.DOWNLOADED) == {State.VERIFIED, State.LISTED}


def test_every_state_is_reachable_and_every_pending_state_can_finish() -> None:
    def reachable(start: State) -> set[State]:
        seen, queue = {start}, [start]
        while queue:
            for state in next_states(queue.pop()):
                if state not in seen:
                    seen.add(state)
                    queue.append(state)
        return seen

    assert reachable(INITIAL) == set(State)
    for state in PENDING:
        assert reachable(state) & FINAL, f"{state} is a dead end"


@given(st.lists(states, max_size=40))
def test_only_moves_of_the_table_succeed(requests: list[State]) -> None:
    state = INITIAL
    taken: list[tuple[State, State]] = []
    for target in requests:
        if (state.value, target.value) in DIAGRAM:
            check(state, target)
            assert can_move(state, target)
            taken.append((state, target))
            state = target
        else:
            assert not can_move(state, target)
            with pytest.raises(IllegalTransitionError):
                check(state, target)
    # No move leaves loaded, and the only move out of confirmed_missing leads to listed.
    for from_state, to_state in taken:
        assert from_state is not State.LOADED
        assert from_state is not State.CONFIRMED_MISSING or to_state is State.LISTED
    assert replay(taken) is state


@given(states, states, states)
def test_replay_refuses_a_broken_chain(first: State, second: State, third: State) -> None:
    moves = [(INITIAL, first), (second, third)]
    valid = can_move(INITIAL, first) and second is first and can_move(second, third)
    if valid:
        assert replay(moves) is third
    else:
        with pytest.raises(IllegalTransitionError):
            replay(moves)


def test_replay_of_nothing_is_the_start() -> None:
    assert replay([]) is State.EXPECTED
    assert replay([], start=State.VERIFIED) is State.VERIFIED
