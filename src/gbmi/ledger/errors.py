"""Errors which the ledger of slots and the ledger of objects share."""

from __future__ import annotations

from enum import StrEnum
from typing import Protocol


class Labelled(Protocol):
    """A key of a ledger row: a slot or an object."""

    @property
    def label(self) -> str: ...


class StaleStateError(Exception):
    """A guarded update changed no row: the row was not in the state the caller assumed."""

    #: What the key names; the ledger of objects says "object".
    noun = "slot"

    def __init__(self, key: Labelled, from_state: StrEnum, to_state: StrEnum) -> None:
        super().__init__(
            f"{self.noun} {key.label} is not in state {from_state.value!r}, "
            f"so the change to {to_state.value!r} did not happen"
        )
        self.key = key
        self.from_state = from_state
        self.to_state = to_state
