"""The state table of an SEC object: six states, ten transitions, no other way (week 3 plan 3.3)."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from gbmi.ledger.object_states import (
    ALLOWED,
    CLOSED,
    FINAL,
    INITIAL,
    OPEN,
    TRANSITIONS,
    ObjectState,
    can_move,
    check,
    next_states,
    replay,
)
from gbmi.ledger.states import IllegalTransitionError

#: The ten transitions of the week 3 plan, written a second time, so a change of the table shows.
PLAN = {
    ("expected", "verified"),
    ("verified", "loaded"),
    ("expected", "quarantined"),
    ("verified", "quarantined"),
    ("quarantined", "expected"),
    ("expected", "not_published"),
    ("not_published", "expected"),
    ("expected", "superseded"),
    ("verified", "superseded"),
    ("quarantined", "superseded"),
}

states = st.sampled_from(sorted(ObjectState))


def test_the_table_is_the_table_of_the_plan() -> None:
    assert len(ObjectState) == 6
    assert len(TRANSITIONS) == 10
    assert {(a.value, b.value) for a, b in ALLOWED} == PLAN
    assert all(transition.meaning for transition in TRANSITIONS)
    assert INITIAL is ObjectState.EXPECTED
    assert (
        frozenset({ObjectState.LOADED, ObjectState.NOT_PUBLISHED, ObjectState.SUPERSEDED}) == FINAL
    )
    assert frozenset(ObjectState) == OPEN | FINAL and not OPEN & FINAL
    assert CLOSED < FINAL


def test_nothing_leaves_loaded_or_superseded_and_not_published_only_reopens() -> None:
    assert next_states(ObjectState.LOADED) == frozenset()
    assert next_states(ObjectState.SUPERSEDED) == frozenset()
    assert next_states(ObjectState.NOT_PUBLISHED) == {ObjectState.EXPECTED}
    assert next_states(ObjectState.EXPECTED) == {
        ObjectState.VERIFIED,
        ObjectState.QUARANTINED,
        ObjectState.NOT_PUBLISHED,
        ObjectState.SUPERSEDED,
    }
    assert {state for state in ObjectState if not next_states(state)} == CLOSED


def test_every_state_is_reachable_and_every_open_state_can_finish() -> None:
    def reachable(start: ObjectState) -> set[ObjectState]:
        seen = {start}
        todo = [start]
        while todo:
            for following in next_states(todo.pop()):
                if following not in seen:
                    seen.add(following)
                    todo.append(following)
        return seen

    assert reachable(INITIAL) == set(ObjectState)
    for state in OPEN:
        assert reachable(state) & FINAL


@given(states, states)
def test_a_move_is_allowed_exactly_when_the_table_holds_the_pair(
    from_state: ObjectState, to_state: ObjectState
) -> None:
    allowed = (from_state.value, to_state.value) in PLAN
    assert can_move(from_state, to_state) is allowed
    if allowed:
        check(from_state, to_state)
        assert from_state not in CLOSED
    else:
        with pytest.raises(IllegalTransitionError, match="no transition"):
            check(from_state, to_state)


@given(st.lists(st.tuples(states, states), max_size=12))
def test_replay_accepts_exactly_the_connected_paths_of_the_table(
    moves: list[tuple[ObjectState, ObjectState]],
) -> None:
    state = INITIAL
    valid = True
    for from_state, to_state in moves:
        if from_state is not state or (from_state.value, to_state.value) not in PLAN:
            valid = False
            break
        state = to_state

    if valid:
        assert replay(moves) is state
    else:
        with pytest.raises(IllegalTransitionError):
            replay(moves)


def test_replay_names_a_change_which_does_not_follow_the_state_before() -> None:
    with pytest.raises(IllegalTransitionError, match="follows the state 'verified'"):
        replay(
            [
                (ObjectState.EXPECTED, ObjectState.VERIFIED),
                (ObjectState.EXPECTED, ObjectState.NOT_PUBLISHED),
            ]
        )
