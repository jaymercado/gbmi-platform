"""Results of quality checks, stored in ``ops.dq_result`` (blueprint 5.7 and 7.3).

Every execution of a check gives one row: the gate, the check, the status, the observed value
and the rule in words. In phase P1 a failed check also writes a log line of level error; the
webhook and the email follow in phase P4 (week 2 plan 3.3).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from psycopg.types.json import Jsonb

from gbmi.ledger.db import Connection
from gbmi.logging import get_logger

log = get_logger(__name__)

PASSED = "passed"
FAILED = "failed"
SKIPPED = "skipped"
ACCEPTED = "accepted"

G1 = "G1"  # landing: size, MD5, the zip opens
G2 = "G2"  # parse: column count, required keys, types
G3 = "G3"  # pre-commit audit
G6 = "G6"  # reconciliation between the layers


@dataclass(frozen=True)
class CheckResult:
    """One execution of one check."""

    gate: str
    check_name: str
    status: str
    observed_value: float | None = None
    #: The rule in words, for example "at most 0.01".
    threshold: str | None = None
    feed: str | None = None
    table_name: str | None = None
    #: What was checked: "2026-11-25T08", "2026-11-25", or the stamp of one slot.
    unit: str | None = None
    detail: Mapping[str, Any] = field(default_factory=dict)


def record_results(
    connection: Connection,
    results: Sequence[CheckResult],
    *,
    source: str,
    run_id: str | None = None,
) -> int:
    """Store the results; returns their number. A failed check is also logged as an error."""
    if not results:
        return 0
    for result in results:
        if result.status == FAILED:
            log.error(
                "quality_check_failed",
                gate=result.gate,
                check=result.check_name,
                feed=result.feed,
                table=result.table_name,
                unit=result.unit,
                observed=result.observed_value,
                threshold=result.threshold,
            )
    with connection.cursor() as cursor:
        cursor.executemany(
            "INSERT INTO ops.dq_result (gate, check_name, status, observed_value, threshold, "
            "source, feed, table_name, unit, run_id, detail) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            [
                (
                    result.gate,
                    result.check_name,
                    result.status,
                    result.observed_value,
                    result.threshold,
                    source,
                    result.feed,
                    result.table_name,
                    result.unit,
                    run_id,
                    Jsonb(dict(result.detail)),
                )
                for result in results
            ],
        )
    return len(results)
