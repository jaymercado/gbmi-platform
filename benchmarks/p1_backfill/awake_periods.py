"""The periods in which the laptop was fully awake, from the power log of macOS (WP-09).

The live run of WP-09 needs the laptop awake. The script reads the output of
``pmset -g log`` from standard input and prints, as JSON, each period between a full wake
and the next sleep, with the cause of the sleep and the number of short wakes ("dark wakes")
inside the sleep before the period. During a dark wake the containers run for some seconds,
most of the time without a network.

The script only reads standard input. The script runs on the laptop, not in a container:
    pmset -g log | uv run python -m benchmarks.p1_backfill.awake_periods \
        --since 2026-11-26T20:14:00+00:00 --out benchmarks/p1_backfill/results/awake-periods.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

LINE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} [+-]\d{4}) ([A-Za-z ]+?)\s*\t(.*)$")
KINDS = ("Sleep", "Wake", "DarkWake")


def power(text: str) -> str:
    """The power source which a log line names."""
    lowered = text.lower()
    if "using ac" in lowered:
        return "mains"
    return "battery" if "using batt" in lowered else "unknown"


def periods(lines: Any, since: datetime) -> list[dict[str, Any]]:
    """The awake periods after ``since``. The laptop counts as awake at ``since``."""
    found: list[dict[str, Any]] = []
    awake_since: datetime | None = since
    dark_wakes = 0
    for line in lines:
        match = LINE.match(line.rstrip("\n"))
        if match is None or match.group(2).strip() not in KINDS:
            continue
        when = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S %z").astimezone(UTC)
        if when < since:
            continue
        kind, text = match.group(2).strip(), match.group(3)
        if kind == "Sleep" and awake_since is not None:
            cause = re.search(r"due to '([^']*)'", text)
            found.append(
                {
                    "awake_from": awake_since.isoformat(),
                    "awake_to": when.isoformat(),
                    "minutes": round((when - awake_since).total_seconds() / 60, 1),
                    "sleep_cause": cause.group(1) if cause else text[:60],
                    "power_at_the_sleep": power(text),
                    "dark_wakes_in_the_sleep_before": dark_wakes,
                }
            )
            awake_since, dark_wakes = None, 0
        elif kind == "Wake" and awake_since is None:
            awake_since = when
        elif kind == "DarkWake" and awake_since is None:
            dark_wakes += 1
    if awake_since is not None:
        found.append(
            {
                "awake_from": awake_since.isoformat(),
                "awake_to": None,
                "minutes": None,
                "sleep_cause": None,
                "power_at_the_sleep": None,
                "dark_wakes_in_the_sleep_before": dark_wakes,
            }
        )
    return found


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument("--since", required=True, help="start, ISO 8601 with a time zone")
    parser.add_argument("--out", type=Path, default=None)
    arguments = parser.parse_args()
    since = datetime.fromisoformat(arguments.since).astimezone(UTC)
    found = periods(sys.stdin, since)
    result = {
        "since": since.isoformat(),
        "read_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "periods": found,
    }
    text = json.dumps(result, indent=2)
    if arguments.out is not None:
        arguments.out.parent.mkdir(parents=True, exist_ok=True)
        arguments.out.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
