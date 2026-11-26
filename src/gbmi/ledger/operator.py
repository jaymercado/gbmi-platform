"""The two operator decisions which change states (blueprint 8.2).

Both need a range of UTC days and a note. The note and the name of the operator go into the
ledger and into ``ops.slot_transition``, so every decision can be read later.
"""

from __future__ import annotations

from collections.abc import Collection
from datetime import date, datetime

from gbmi.core.gdelt import Feed
from gbmi.ledger.calendar import first_slot_of, last_slot_of
from gbmi.ledger.repository import Ledger
from gbmi.ledger.states import State, can_move

#: The name which the rule of automatic confirmation writes; an operator cannot use the name.
AUTO = "auto"
#: The states from which a slot can be confirmed missing.
CONFIRMABLE: frozenset[State] = frozenset(
    state for state in State if can_move(state, State.CONFIRMED_MISSING)
)


def _decision(decided_by: str, note: str) -> tuple[str, str]:
    decided_by, note = decided_by.strip(), note.strip()
    if not note:
        raise ValueError("a note is required: say why the slots are changed")
    if not decided_by or decided_by == AUTO:
        raise ValueError(f"the name of the operator is required and cannot be {AUTO!r}")
    return decided_by, note


def confirm_missing(
    ledger: Ledger,
    *,
    first_day: date,
    last_day: date,
    states: Collection[State],
    decided_by: str,
    note: str,
    feeds: Collection[Feed] | None = None,
    as_of: datetime | None = None,
) -> dict[str, int]:
    """Confirm the slots of the range which are in one of ``states``; returns the count by state.

    The operator names the states, so a slot which only waits for its download is not confirmed
    by accident.
    """
    decided_by, note = _decision(decided_by, note)
    wrong = sorted(state.value for state in states if state not in CONFIRMABLE)
    if wrong or not states:
        allowed = sorted(state.value for state in CONFIRMABLE)
        raise ValueError(f"name one or more of the states {allowed}; got {wrong}")
    confirmed: dict[str, int] = {}
    for state in sorted(states, key=lambda state: state.value):
        records = ledger.find(
            first=first_slot_of(first_day), last=last_slot_of(last_day), feeds=feeds, states=[state]
        )
        moved = ledger.confirm_missing(
            [record.key for record in records],
            from_state=state,
            decided_by=decided_by,
            note=note,
            as_of=as_of,
        )
        confirmed[state.value] = len(moved)
    return confirmed


def requeue(
    ledger: Ledger,
    *,
    first_day: date,
    last_day: date,
    decided_by: str,
    note: str,
    feeds: Collection[Feed] | None = None,
    as_of: datetime | None = None,
) -> int:
    """Send the quarantined slots of the range back to the download; returns their number."""
    decided_by, note = _decision(decided_by, note)
    records = ledger.find(
        first=first_slot_of(first_day),
        last=last_slot_of(last_day),
        feeds=feeds,
        states=[State.QUARANTINED],
    )
    moved = ledger.requeue(
        [record.key for record in records], decided_by=decided_by, note=note, as_of=as_of
    )
    return len(moved)
