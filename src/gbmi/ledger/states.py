"""The ledger state machine as one table (blueprint 8.2).

Every expected source file is a *slot* of the ledger. A slot is in one of seven states and
changes its state only through one of twelve transitions. The module is pure: it knows the
table and nothing about the database. ``gbmi.ledger.repository`` asks the table before each
change, and the check constraint ``slot_transition_allowed`` holds the same twelve pairs in the
database.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum


class State(StrEnum):
    """The seven states of a slot."""

    EXPECTED = "expected"  # the calendar says the file should exist
    LISTED = "listed"  # the source lists the file: URL, size and MD5 are known
    DOWNLOADED = "downloaded"  # the bytes arrived; gate G1 has not judged them yet
    VERIFIED = "verified"  # gate G1 passed and the file is in bronze
    LOADED = "loaded"  # the rows of the file are committed to silver
    QUARANTINED = "quarantined"  # the file failed gate G1 three times, or gate G2 as a whole
    CONFIRMED_MISSING = "confirmed_missing"  # a rule or an operator decided: no file will come


@dataclass(frozen=True)
class Transition:
    """One allowed change of state, with the label of the state diagram."""

    from_state: State
    to_state: State
    meaning: str


TRANSITIONS: tuple[Transition, ...] = (
    Transition(State.EXPECTED, State.LISTED, "found in feed index"),
    Transition(State.LISTED, State.DOWNLOADED, "file stored in bronze"),
    Transition(State.DOWNLOADED, State.VERIFIED, "size and MD5 match"),
    Transition(State.DOWNLOADED, State.LISTED, "mismatch, fetch again"),
    Transition(State.LISTED, State.QUARANTINED, "retries exhausted"),
    Transition(State.VERIFIED, State.LOADED, "silver committed"),
    Transition(State.VERIFIED, State.QUARANTINED, "parse gate failed"),
    Transition(State.QUARANTINED, State.LISTED, "operator requeues"),
    Transition(State.EXPECTED, State.CONFIRMED_MISSING, "absent from master list for 48 hours"),
    Transition(State.LISTED, State.CONFIRMED_MISSING, "file unavailable for 48 hours"),
    Transition(State.QUARANTINED, State.CONFIRMED_MISSING, "operator decision"),
    Transition(State.CONFIRMED_MISSING, State.LISTED, "slot appears later"),
)

#: The twelve pairs (from, to).
ALLOWED: frozenset[tuple[State, State]] = frozenset(
    (transition.from_state, transition.to_state) for transition in TRANSITIONS
)
#: The state of a slot which the calendar has just created.
INITIAL = State.EXPECTED
#: A day is complete when every slot of the day is in one of the two states.
FINAL: frozenset[State] = frozenset({State.LOADED, State.CONFIRMED_MISSING})
#: The states of a slot which still needs work.
PENDING: frozenset[State] = frozenset(State) - FINAL


class IllegalTransitionError(ValueError):
    """The state diagram has no transition between the two states."""


def can_move(from_state: State, to_state: State) -> bool:
    """True when the state diagram allows the change."""
    return (from_state, to_state) in ALLOWED


def check(from_state: State, to_state: State) -> None:
    """Raise IllegalTransitionError unless the state diagram allows the change."""
    if not can_move(from_state, to_state):
        raise IllegalTransitionError(
            f"the ledger has no transition from {from_state.value!r} to {to_state.value!r}"
        )


def next_states(state: State) -> frozenset[State]:
    """The states which a slot in ``state`` can reach with one transition."""
    return frozenset(to_state for from_state, to_state in ALLOWED if from_state is state)


def replay(moves: Iterable[tuple[State, State]], start: State = INITIAL) -> State:
    """The state after a list of recorded changes, read in order.

    Raises IllegalTransitionError when a change does not start where the change before ended,
    or when the state diagram does not allow a change.
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
