"""The pre-commit audit, gate G3 (blueprint 5.7), in the size the tracer needs.

Before a unit is committed to silver, every raw line has to be accounted for and the rows to
be committed have to be sound. If a check fails nothing is committed: the previous version of
the unit stays live and the task fails (blueprint 8.1, error class "logic").

The two accounting identities (week 1 plan, WP-09):

    raw lines  =  parsed rows + quarantined rows
    committed  =  parsed rows - duplicate rows

*Parsed* means typed rows before deduplication. The row-count tolerance against the trailing
median of the same weekday needs history and arrives with the ledger in phase P1.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

import pyarrow as pa
import pyarrow.compute as pc

from gbmi.lake.commit import Unit, UnitMismatchError


class AuditError(Exception):
    """The unit failed gate G3 and must not be committed."""


@dataclass(frozen=True)
class UnitCounts:
    """Where every raw line of a unit went."""

    raw_lines: int
    parsed: int
    quarantined: int
    duplicates: int
    committed: int

    def as_dict(self) -> dict[str, int]:
        return asdict(self)


def audit_unit(counts: UnitCounts, data: pa.Table, unit: Unit, *, key: str) -> None:
    """Raise AuditError unless the unit may be committed."""
    problems: list[str] = []
    if counts.raw_lines != counts.parsed + counts.quarantined:
        problems.append(
            f"raw lines ({counts.raw_lines}) != parsed ({counts.parsed}) "
            f"+ quarantined ({counts.quarantined})"
        )
    if counts.committed != counts.parsed - counts.duplicates:
        problems.append(
            f"committed ({counts.committed}) != parsed ({counts.parsed}) "
            f"- duplicates ({counts.duplicates})"
        )
    if data.num_rows != counts.committed:
        problems.append(
            f"{data.num_rows} rows to commit, but the accounting says {counts.committed}"
        )
    keys = data[key]
    if keys.null_count:
        problems.append(f"{keys.null_count} rows have no {key}")
    distinct = pc.count_distinct(keys).as_py()
    if distinct != data.num_rows - keys.null_count:
        problems.append(f"{key} is not unique: {data.num_rows} rows, {distinct} distinct keys")
    try:
        unit.check(data)
    except UnitMismatchError as error:
        problems.append(str(error))
    if problems:
        raise AuditError(f"unit {unit.label} failed the pre-commit audit: " + "; ".join(problems))
