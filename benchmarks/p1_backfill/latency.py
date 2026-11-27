"""Latency of the live run, read from the ledger (WP-09, step 6).

For each slot of a time range which is loaded, three times in seconds:

- upload to listed: ``listed_at - published_at``. The upload time comes from the HTTP header
  Last-Modified of the download; a slot without the time is left out of the measure;
- listed to verified: ``verified_at - listed_at`` (blueprint 8.3: within 10 minutes at the
  95th percentile);
- listed to the silver commit: ``loaded_at - listed_at`` (blueprint 3.4: 45 minutes at the
  95th percentile; ``loaded_at`` is written in the transaction after the commit).

The script prints the number of slots, the median and the 95th percentile of each measure, by
feed and over all feeds. The script only reads.

Run on the Compose network of the working stack, with the first and the last slot time:
    make toolbox CMD="python -m benchmarks.p1_backfill.latency \
        --from 2026-11-27T02:00:00+00:00 --to 2026-11-27T09:00:00+00:00"
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
from typing import Any

from gbmi.core.clock import parse_instant, utc_now
from gbmi.ledger.repository import open_ledger

MEASURES = ("upload_to_listed", "listed_to_verified", "listed_to_silver")


def percentile(values: list[float], share: float) -> float:
    """The value at the given share of the sorted list (nearest rank)."""
    ordered = sorted(values)
    return ordered[max(0, math.ceil(share * len(ordered)) - 1)]


def summary(values: list[float]) -> dict[str, Any]:
    if not values:
        return {"slots": 0}
    return {
        "slots": len(values),
        "median_seconds": round(statistics.median(values), 1),
        "p95_seconds": round(percentile(values, 0.95), 1),
        "max_seconds": round(max(values), 1),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument("--from", dest="first", required=True, help="first slot time, ISO 8601")
    parser.add_argument("--to", dest="last", required=True, help="last slot time, ISO 8601")
    arguments = parser.parse_args()
    first, last = parse_instant(arguments.first), parse_instant(arguments.last)
    with open_ledger() as ledger:
        rows = ledger.connection.execute(
            "SELECT feed, state, extract(epoch FROM listed_at - published_at), "
            "extract(epoch FROM verified_at - listed_at), "
            "extract(epoch FROM loaded_at - listed_at) "
            "FROM ops.source_slot WHERE source = %s AND slot_ts >= %s AND slot_ts <= %s",
            (ledger.source, first, last),
        ).fetchall()
    states: dict[str, int] = {}
    samples: dict[str, dict[str, list[float]]] = {}
    for feed, state, upload, verified, loaded in rows:
        states[state] = states.get(state, 0) + 1
        if state != "loaded":
            continue
        for scope in (feed, "all"):
            measures = samples.setdefault(scope, {name: [] for name in MEASURES})
            for name, value in zip(MEASURES, (upload, verified, loaded), strict=True):
                if value is not None:
                    measures[name].append(float(value))
    report = {
        "slots_from": first.isoformat(),
        "slots_to": last.isoformat(),
        "evaluated_at": utc_now().isoformat(timespec="seconds"),
        "slots_by_state": states,
        "latency": {
            scope: {name: summary(values) for name, values in measures.items()}
            for scope, measures in sorted(samples.items())
        },
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
