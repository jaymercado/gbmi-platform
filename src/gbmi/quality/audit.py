"""The pre-commit audit, gate G3 (blueprint 5.7; week 2 plan 3.3).

Before a unit is committed to silver, every raw line has to be accounted for and the rows to
be committed have to be sound. If a check fails nothing is committed: the previous version of
the unit stays live and the task fails (blueprint 8.1, error class "logic").

The two accounting identities:

    raw lines  =  parsed rows + quarantined rows
    committed  =  parsed rows - duplicate rows

*Parsed* means typed rows before deduplication. The rows of a table have a key which is unique
and not NULL, belong to the unit, and hold no NULL list. A table whose rows are not lines of a
source file (``gdelt_gkg_org``) has no line identities: its row count equals the sum of
``org_count`` over the committed documents. A day unit is also compared with the same weekday
of the four weeks before.

Each check gives one result for ``ops.dq_result``.
"""

from __future__ import annotations

import statistics
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from functools import partial
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from gbmi.lake.commit import UnitMismatchError, UnitOfWork
from gbmi.quality.results import ACCEPTED, FAILED, G3, PASSED, SKIPPED, CheckResult

#: A day is plausible between half and twice the median of the same weekday (week 2 plan, F10).
VOLUME_BAND = (0.5, 2.0)
#: The band needs the current day-level version of each of the four preceding same weekdays.
VOLUME_HISTORY = 4


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


def _result(
    name: str,
    problem: str | None,
    *,
    table: str | None,
    feed: str | None,
    unit: UnitOfWork,
    observed: float | None = None,
    threshold: str | None = None,
) -> CheckResult:
    return CheckResult(
        gate=G3,
        check_name=name,
        status=FAILED if problem else PASSED,
        observed_value=observed,
        threshold=threshold,
        feed=feed,
        table_name=table,
        unit=unit.label,
        detail={"problem": problem} if problem else {},
    )


def check_accounting(
    counts: UnitCounts,
    rows: int,
    unit: UnitOfWork,
    *,
    table: str | None = None,
    feed: str | None = None,
) -> list[CheckResult]:
    """The two accounting identities, and the rows to commit against the accounting."""
    raw = (
        f"raw lines ({counts.raw_lines}) != parsed ({counts.parsed}) "
        f"+ quarantined ({counts.quarantined})"
        if counts.raw_lines != counts.parsed + counts.quarantined
        else None
    )
    committed = (
        f"committed ({counts.committed}) != parsed ({counts.parsed}) "
        f"- duplicates ({counts.duplicates})"
        if counts.committed != counts.parsed - counts.duplicates
        else None
    )
    total = (
        f"{rows} rows to commit, but the accounting says {counts.committed}"
        if rows != counts.committed
        else None
    )
    result = partial(_result, table=table, feed=feed, unit=unit)
    return [
        result(
            "accounting_raw_lines",
            raw,
            observed=counts.raw_lines,
            threshold="raw lines = parsed rows + quarantined rows",
        ),
        result(
            "accounting_committed",
            committed,
            observed=counts.committed,
            threshold="committed rows = parsed rows - duplicate rows",
        ),
        result(
            "row_count",
            total,
            observed=rows,
            threshold="rows to commit = committed rows of the accounting",
        ),
    ]


def check_quarantine(
    counts: UnitCounts,
    quarantine_rows: int,
    unit: UnitOfWork,
    *,
    table: str | None = None,
    feed: str | None = None,
) -> CheckResult:
    """The quarantine output holds one row for each line which the accounting calls quarantined."""
    return _result(
        "quarantine_rows",
        f"{quarantine_rows} rows for the quarantine file, "
        f"but the accounting says {counts.quarantined}"
        if quarantine_rows != counts.quarantined
        else None,
        table=table,
        feed=feed,
        unit=unit,
        observed=quarantine_rows,
        threshold="rows of the quarantine file = quarantined rows of the accounting",
    )


def list_columns(schema: pa.Schema) -> list[str]:
    """The list columns of a table."""
    return [field.name for field in schema if pa.types.is_nested(field.type)]


def null_lists(data: pa.Table) -> int:
    """The number of NULL lists in the list columns of the rows."""
    return sum(data[name].null_count for name in list_columns(data.schema))


def check_keys(
    data: pa.Table,
    unit: UnitOfWork,
    *,
    key: Sequence[str],
    table: str | None = None,
    feed: str | None = None,
) -> list[CheckResult]:
    """The key is not NULL and unique, and every row belongs to the unit.

    ``data`` needs the key columns and the unit columns only.
    """
    result = partial(_result, table=table, feed=feed, unit=unit)
    label = ", ".join(key)
    nulls = sum(data[column].null_count for column in key)
    distinct = (
        pc.count_distinct(data[key[0]]).as_py()
        if len(key) == 1
        else data.group_by(list(key)).aggregate([]).num_rows
    )
    expected = data.num_rows - (nulls if len(key) == 1 else 0)
    try:
        unit.check(data)
        foreign = None
    except UnitMismatchError as error:
        foreign = str(error)
    return [
        result(
            "key_not_null",
            f"{nulls} rows have no {label}" if nulls else None,
            observed=nulls,
            threshold="0 rows without a key",
        ),
        result(
            "key_unique",
            f"{label} is not unique: {data.num_rows} rows, {distinct} distinct keys"
            if distinct != expected
            else None,
            observed=expected - distinct,
            threshold="0 rows with a repeated key",
        ),
        result("unit_membership", foreign, threshold="every row belongs to the unit"),
    ]


def check_lists(
    missing: int,
    columns: Sequence[str],
    unit: UnitOfWork,
    *,
    table: str | None = None,
    feed: str | None = None,
) -> CheckResult:
    """No list is NULL: a row without values has an empty list (week 2 plan 3.3)."""
    return _result(
        "lists_not_null",
        f"{missing} NULL lists in {', '.join(columns)}" if missing else None,
        table=table,
        feed=feed,
        unit=unit,
        observed=missing,
        threshold="0 NULL lists",
    )


def check_rows(
    data: pa.Table,
    unit: UnitOfWork,
    *,
    key: Sequence[str],
    table: str | None = None,
    feed: str | None = None,
) -> list[CheckResult]:
    """The rows themselves: the key, the unit and the lists."""
    results = check_keys(data, unit, key=key, table=table, feed=feed)
    lists = list_columns(data.schema)
    if lists:
        results.append(check_lists(null_lists(data), lists, unit, table=table, feed=feed))
    return results


def check_org_rows(
    org_rows: int, org_count_total: int, unit: UnitOfWork, *, table: str, feed: str
) -> CheckResult:
    """The organisation table: its rows are the organisation strings which the documents count."""
    return _result(
        "rows_equal_sum_of_org_count",
        f"{org_rows} organisation rows, but the documents count {org_count_total}"
        if org_rows != org_count_total
        else None,
        table=table,
        feed=feed,
        unit=unit,
        observed=org_rows,
        threshold="rows = sum of org_count over the committed documents",
    )


def check_volume(
    rows: int,
    history: Sequence[int | None],
    unit: UnitOfWork,
    *,
    table: str,
    feed: str,
    accept_note: str | None = None,
    band: tuple[float, float] = VOLUME_BAND,
) -> CheckResult:
    """The row count of a day against the median of the four preceding same weekdays.

    ``history`` holds the row count of the current day-level version of each of the four dates,
    None where a date has none. With a missing date the check is skipped. An operator can
    accept a day outside the band with a note. ``band`` is the band of the GDELT days by
    default; the index files of the SEC differ more and take a wider band (week 3 plan 3.3).
    """
    low, high = band
    threshold = f"between {low} and {high} times the median of {VOLUME_HISTORY} same weekdays"
    known = [value for value in history if value is not None]

    def outcome(status: str, detail: dict[str, Any]) -> CheckResult:
        return CheckResult(
            gate=G3,
            check_name="volume_band",
            status=status,
            observed_value=float(rows),
            threshold=threshold,
            feed=feed,
            table_name=table,
            unit=unit.label,
            detail=detail,
        )

    if len(known) < VOLUME_HISTORY:
        return outcome(SKIPPED, {"history": list(history), "reason": "history is short"})
    median = statistics.median(known)
    detail: dict[str, Any] = {"history": known, "median": median}
    if low * median <= rows <= high * median:
        return outcome(PASSED, detail)
    problem = f"{rows} rows are outside {low} to {high} times the median {median:g}"
    if accept_note:
        return outcome(ACCEPTED, {**detail, "problem": problem, "note": accept_note})
    return outcome(FAILED, {**detail, "problem": problem})


def _compared(
    name: str,
    status: str,
    rows: int,
    threshold: str,
    unit: UnitOfWork,
    table: str,
    feed: str,
    detail: dict[str, Any],
) -> CheckResult:
    return CheckResult(
        gate=G3,
        check_name=name,
        status=status,
        observed_value=float(rows),
        threshold=threshold,
        feed=feed,
        table_name=table,
        unit=unit.label,
        detail=detail,
    )


def check_change(
    rows: int,
    previous: int | None,
    unit: UnitOfWork,
    *,
    table: str,
    feed: str,
    tolerance: float,
    name: str,
    accept_note: str | None = None,
) -> CheckResult:
    """The row count of a unit against the version before: at most ``tolerance`` up or down.

    The universe of the SEC may differ by 5% from the snapshot before (blueprint 8.5). Without
    a version before, the check is skipped. An operator can accept a larger change with a note.
    """
    threshold = f"within {tolerance:.0%} of the version before"
    if previous is None:
        return _compared(
            name, SKIPPED, rows, threshold, unit, table, feed, {"reason": "no version before"}
        )
    detail: dict[str, Any] = {"previous": previous}
    if abs(rows - previous) <= tolerance * previous:
        return _compared(name, PASSED, rows, threshold, unit, table, feed, detail)
    detail["problem"] = f"{rows} rows differ by more than {tolerance:.0%} from {previous} rows"
    if accept_note:
        detail["note"] = accept_note
        return _compared(name, ACCEPTED, rows, threshold, unit, table, feed, detail)
    return _compared(name, FAILED, rows, threshold, unit, table, feed, detail)


def check_fall(
    rows: int,
    previous: int | None,
    unit: UnitOfWork,
    *,
    table: str,
    feed: str,
    max_fall: float,
    name: str,
) -> CheckResult:
    """The row count of a document against the load before: at most ``max_fall`` fewer rows.

    The facts of a company may not fall by more than 2% between two loads (blueprint 8.7): a
    document which lost facts is suspect. More rows always pass. Without a load before, the
    check is skipped.
    """
    threshold = f"at most {max_fall:.0%} fewer rows than the load before"
    if previous is None:
        return _compared(
            name, SKIPPED, rows, threshold, unit, table, feed, {"reason": "no load before"}
        )
    detail: dict[str, Any] = {"previous": previous}
    if rows >= previous * (1 - max_fall):
        return _compared(name, PASSED, rows, threshold, unit, table, feed, detail)
    detail["problem"] = f"{rows} rows are more than {max_fall:.0%} below {previous} rows"
    return _compared(name, FAILED, rows, threshold, unit, table, feed, detail)


def problems(results: Sequence[CheckResult]) -> list[str]:
    """The problems of the failed checks, in the order of the checks."""
    return [str(result.detail.get("problem")) for result in results if result.status == FAILED]


def raise_if_failed(results: Sequence[CheckResult], unit: UnitOfWork) -> None:
    """Raise AuditError when a check failed."""
    found = problems(results)
    if found:
        raise AuditError(f"unit {unit.label} failed the pre-commit audit: " + "; ".join(found))


def audit_unit(
    counts: UnitCounts, data: pa.Table, unit: UnitOfWork, *, key: str | Sequence[str]
) -> None:
    """Raise AuditError unless the unit may be committed: the accounting and the rows."""
    keys = [key] if isinstance(key, str) else list(key)
    raise_if_failed(
        [*check_accounting(counts, data.num_rows, unit), *check_rows(data, unit, key=keys)], unit
    )
