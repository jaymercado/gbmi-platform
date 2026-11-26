"""Order-independent content checksums.

A checksum is a row count plus a digest over a named list of business columns. It answers one
question: do two builds of the same unit hold the same rows? Row order does not matter, a
duplicated row does, and floating-point values are rounded first so that two engines that
differ in the last bit still agree.

Columns that change on every run by design are left out unless they are named explicitly
(week 1 plan 3.3): a rerun creates a new snapshot and new timestamps, and that is correct.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass

import duckdb
import pyarrow as pa

#: Provenance columns that differ between two runs by design.
VOLATILE_COLUMNS = frozenset(
    {"_ingested_at", "_run_id", "_built_at", "_dbt_invocation_id", "_silver_snapshot_id"}
)

_NULL = "chr(0)"  # NULL and the empty string must not collide
_SEPARATOR = "chr(31)"  # ASCII unit separator between column values
_MODULUS = 2**64


@dataclass(frozen=True)
class ContentChecksum:
    """Row count and digest of a table's business columns."""

    rows: int
    digest: str
    columns: tuple[str, ...]

    def __str__(self) -> str:
        return f"{self.rows} rows, digest {self.digest}"


def combine_checksums(parts: Iterable[ContentChecksum]) -> ContentChecksum:
    """The checksum of the rows of every part together.

    The digest is a sum over the rows, so the checksum of a whole equals the combination of
    the checksums of its parts. A day which is built hour by hour gets its checksum without
    ever being held in memory as a whole.
    """
    rows = 0
    digest = 0
    columns: tuple[str, ...] | None = None
    for part in parts:
        if columns is None:
            columns = part.columns
        elif part.columns != columns:
            raise ValueError("the parts were checksummed over different columns")
        rows += part.rows
        digest = (digest + int(part.digest, 16)) % _MODULUS
    return ContentChecksum(rows=rows, digest=f"{digest:016x}", columns=columns or ())


def business_columns(table: pa.Table) -> tuple[str, ...]:
    """Every column except the volatile provenance columns, in table order."""
    return tuple(name for name in table.column_names if name not in VOLATILE_COLUMNS)


def content_checksum(
    table: pa.Table,
    columns: Sequence[str] | None = None,
    *,
    float_digits: int = 6,
) -> ContentChecksum:
    """Checksum the given columns of an Arrow table (default: all but the volatile ones).

    Each row is rendered as text, hashed with MD5 and reduced to 64 bits; the digest is the
    sum of those values modulo 2**64. A sum, unlike an exclusive-or, changes when a row is
    duplicated. MD5 is used as a fast, stable hash, not for security.
    """
    names = tuple(columns) if columns is not None else business_columns(table)
    missing = [name for name in names if name not in table.column_names]
    if missing:
        raise KeyError(f"columns not in table: {missing}")
    if not names:
        raise ValueError("at least one column is needed for a checksum")

    rendered = ", ".join(
        _render(name, table.schema.field(name).type, float_digits) for name in names
    )
    query = (
        f"select count(*), coalesce(sum(md5_number_lower(concat_ws({_SEPARATOR}, {rendered}))), 0)"  # noqa: S608
        f" % {_MODULUS} from data"
    )
    connection = duckdb.connect(":memory:")
    try:
        connection.register("data", table)
        row = connection.execute(query).fetchone()
    finally:
        connection.close()
    if row is None:  # an aggregate without GROUP BY always returns one row
        raise RuntimeError("checksum query returned no row")
    return ContentChecksum(rows=int(row[0]), digest=f"{int(row[1]):016x}", columns=names)


def _render(name: str, arrow_type: pa.DataType, float_digits: int) -> str:
    """SQL that turns one column into text the same way on every run and in every time zone."""
    column = '"' + name.replace('"', '""') + '"'
    if pa.types.is_floating(arrow_type):
        text = f"cast(round({column}, {float_digits}) as varchar)"
    elif pa.types.is_timestamp(arrow_type):
        # Microseconds since the epoch: independent of the session time zone.
        text = f"cast(epoch_us({column}) as varchar)"
    elif pa.types.is_nested(arrow_type):
        # JSON keeps ['a, b'] and ['a', 'b'] apart, which a plain cast would not.
        text = f"cast(to_json({column}) as varchar)"
    else:
        text = f"cast({column} as varchar)"
    return f"coalesce({text}, {_NULL})"
