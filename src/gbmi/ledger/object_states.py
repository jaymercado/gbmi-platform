"""The state machine of an SEC object as one table (blueprint 7.3; week 3 plan 3.3).

An *object* is one file which the platform asks from the SEC: the ticker file, a submissions
document, an index file, a facts document or a bulk archive. The SEC publishes no size and no
hash for a file, so the states "listed" and "downloaded" of a GDELT slot have no meaning here.
An object is in one of six states and changes its state only through one of ten transitions.
The module is pure: it knows the table and nothing about the database.
``gbmi.ledger.objects`` asks the table before each change, and the check constraint
``object_transition_allowed`` holds the same ten pairs in the database.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum

from gbmi.ledger.states import IllegalTransitionError


class ObjectState(StrEnum):
    """The six states of an object."""

    EXPECTED = "expected"  # a pipeline wants the file
    VERIFIED = "verified"  # the bytes passed gate G1 and are in bronze
    QUARANTINED = "quarantined"  # gate G1 failed three times, or gate G2 or G3 failed
    LOADED = "loaded"  # the rows of the file are committed to silver
    NOT_PUBLISHED = "not_published"  # the SEC has no such file
    SUPERSEDED = "superseded"  # a newer object of the same name was loaded first


@dataclass(frozen=True)
class ObjectTransition:
    """One allowed change of state, with its meaning."""

    from_state: ObjectState
    to_state: ObjectState
    meaning: str


TRANSITIONS: tuple[ObjectTransition, ...] = (
    ObjectTransition(
        ObjectState.EXPECTED, ObjectState.VERIFIED, "stored in bronze, gate G1 passed"
    ),
    ObjectTransition(ObjectState.VERIFIED, ObjectState.LOADED, "silver committed"),
    ObjectTransition(ObjectState.EXPECTED, ObjectState.QUARANTINED, "gate G1 failed three times"),
    ObjectTransition(ObjectState.VERIFIED, ObjectState.QUARANTINED, "gate G2 or G3 failed"),
    ObjectTransition(ObjectState.QUARANTINED, ObjectState.EXPECTED, "operator requeues"),
    ObjectTransition(ObjectState.EXPECTED, ObjectState.NOT_PUBLISHED, "the SEC has no such file"),
    ObjectTransition(ObjectState.NOT_PUBLISHED, ObjectState.EXPECTED, "the file appears later"),
    ObjectTransition(ObjectState.EXPECTED, ObjectState.SUPERSEDED, "a newer object was loaded"),
    ObjectTransition(ObjectState.VERIFIED, ObjectState.SUPERSEDED, "a newer object was loaded"),
    ObjectTransition(ObjectState.QUARANTINED, ObjectState.SUPERSEDED, "a newer object was loaded"),
)

#: The ten pairs (from, to).
ALLOWED: frozenset[tuple[ObjectState, ObjectState]] = frozenset(
    (transition.from_state, transition.to_state) for transition in TRANSITIONS
)
#: The state of an object which a pipeline has just asked for.
INITIAL = ObjectState.EXPECTED
#: A load is complete when every object is in one of the three states (exit gate of P2, row 6).
FINAL: frozenset[ObjectState] = frozenset(
    {ObjectState.LOADED, ObjectState.NOT_PUBLISHED, ObjectState.SUPERSEDED}
)
#: The states of an object which still needs work.
OPEN: frozenset[ObjectState] = frozenset(ObjectState) - FINAL
#: No transition leaves the two states. ``not_published`` is final and still not closed: the
#: file can appear later.
CLOSED: frozenset[ObjectState] = frozenset({ObjectState.LOADED, ObjectState.SUPERSEDED})


def can_move(from_state: ObjectState, to_state: ObjectState) -> bool:
    """True when the table allows the change."""
    return (from_state, to_state) in ALLOWED


def check(from_state: ObjectState, to_state: ObjectState) -> None:
    """Raise IllegalTransitionError unless the table allows the change."""
    if not can_move(from_state, to_state):
        raise IllegalTransitionError(
            f"the object ledger has no transition from {from_state.value!r} to {to_state.value!r}"
        )


def next_states(state: ObjectState) -> frozenset[ObjectState]:
    """The states which an object in ``state`` can reach with one transition."""
    return frozenset(to_state for from_state, to_state in ALLOWED if from_state is state)


def replay(
    moves: Iterable[tuple[ObjectState, ObjectState]], start: ObjectState = INITIAL
) -> ObjectState:
    """The state after a list of recorded changes, read in order.

    Raises IllegalTransitionError when a change does not start where the change before ended,
    or when the table does not allow a change.
    """
    state = start
    for from_state, to_state in moves:
        if from_state is not state:
            raise IllegalTransitionError(
                f"a change from {from_state.value!r} follows the state {state.value!r}"
            )
        check(from_state, to_state)
        state = to_state
    return state
