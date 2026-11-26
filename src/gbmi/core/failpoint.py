"""Stop the process at an exact place (week 2 plan 3.3).

A crash drill needs a crash at a known point, not at a random moment. When the environment
variable ``GBMI_FAILPOINT`` names a point, the process which reaches the point kills itself
with SIGKILL: no handler runs, no file is closed, nothing is rolled back by the program. What
is left behind is what a real crash at the point would leave behind.
"""

from __future__ import annotations

import os
import signal

VARIABLE = "GBMI_FAILPOINT"

#: The downloaded bytes are in a temporary bronze file; the move to the final key has not happened.
BRONZE_BEFORE_MOVE = "bronze.before_move"
#: The unit is parsed and audited; nothing is committed to the silver table.
SILVER_BEFORE_COMMIT = "silver.before_commit"
#: The silver table holds the new rows; the ledger does not know yet.
SILVER_AFTER_COMMIT = "silver.after_commit"

POINTS = frozenset({BRONZE_BEFORE_MOVE, SILVER_BEFORE_COMMIT, SILVER_AFTER_COMMIT})


def failpoint(name: str) -> None:
    """Kill the process when ``GBMI_FAILPOINT`` names this point; do nothing otherwise."""
    if name not in POINTS:
        raise ValueError(f"unknown failpoint {name!r}")
    if os.environ.get(VARIABLE) == name:
        os.kill(os.getpid(), signal.SIGKILL)
