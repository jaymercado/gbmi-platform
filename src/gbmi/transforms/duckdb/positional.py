"""Parsing a positional GDELT feed in DuckDB: raw lines, gate G2 and typed fields.

Each file is read as text. Every row is checked against the positional schema of the feed - the
exact column count - and every typed field is validated before it is cast, so nothing is
coerced silently: a row which fails goes to quarantine with the reason (gate G2, blueprint
5.7). A file with more than 1% of failed rows is rejected as a whole and gives no row.

The SQL is generated from a feed definition (``gbmi.core.positional.FeedSchema``), the single
definition of the positional schema.
"""

from __future__ import annotations

import io
import zipfile
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

import duckdb
import pyarrow as pa

from gbmi.core.gdelt import Feed, parse_file_name
from gbmi.core.positional import FeedSchema, Kind, SilverColumn, file_is_rejected
from gbmi.quality.audit import UnitCounts
from gbmi.quality.quarantine import QUARANTINE_SCHEMA

INTEGER = "-?[0-9]+"
DOUBLE = r"-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?"

RAW_LINES_SCHEMA = pa.schema(
    [
        pa.field("source_file", pa.string()),
        pa.field("source_md5", pa.string()),
        pa.field("slot_date", pa.date32()),
        pa.field("slot_hour", pa.int32()),
        pa.field("slot_ts", pa.timestamp("us", tz="UTC")),
        pa.field("line_number", pa.int64()),
        pa.field("line", pa.string()),
    ]
)

#: An extra check of a feed: SQL which is true when the row fails, and the reason to record.
Check = tuple[str, str]


@dataclass(frozen=True)
class BronzeFile:
    """One landed zip: its bronze key, its MD5 and its bytes."""

    key: str
    md5: str
    content: bytes


@dataclass(frozen=True)
class FileCounts:
    """Where every raw line of one file went: raw lines = parsed rows + quarantined rows."""

    key: str
    md5: str
    raw_lines: int
    parsed_rows: int
    quarantined_rows: int


@dataclass(frozen=True)
class Parsed:
    """The outcome of parsing the files of one unit for one feed."""

    feed: Feed
    #: Silver table name -> the rows to commit, typed and deduplicated.
    tables: dict[str, pa.Table]
    #: The rejected rows of the accepted files, with the reason.
    quarantine: pa.Table
    #: The line accounting of the accepted files, for the first table of the feed.
    counts: UnitCounts
    #: The accepted files, each with its accounting.
    files: tuple[FileCounts, ...]
    #: The files which the 1% rule rejected as a whole; they gave no row and no quarantine row.
    rejected: tuple[FileCounts, ...] = ()
    #: Further counts of the feed, for example organisation names without a normal form.
    notes: dict[str, int] = field(default_factory=dict)

    @property
    def events(self) -> pa.Table:
        return self.tables["gdelt_event"]


def raw_lines(files: Sequence[BronzeFile]) -> pa.Table:
    """One row per line of every file, with the file it came from and its line number.

    Bytes that are not valid UTF-8 are replaced, never dropped, so the line count is exact.
    """
    tables = [pa.table({name: [] for name in RAW_LINES_SCHEMA.names}, schema=RAW_LINES_SCHEMA)]
    for file in files:
        slot, _ = parse_file_name(file.key.rsplit("/", 1)[-1])
        with zipfile.ZipFile(io.BytesIO(file.content)) as archive:
            (member,) = archive.namelist()
            text = archive.read(member).decode("utf-8", errors="replace")
        lines = text.split("\n")
        if lines and lines[-1] == "":  # the file ends with a newline
            lines.pop()
        count = len(lines)
        tables.append(
            pa.table(
                {
                    "source_file": pa.repeat(pa.scalar(file.key, pa.string()), count),
                    "source_md5": pa.repeat(pa.scalar(file.md5, pa.string()), count),
                    "slot_date": pa.repeat(pa.scalar(slot.slot_date, pa.date32()), count),
                    "slot_hour": pa.repeat(pa.scalar(slot.slot_hour, pa.int32()), count),
                    "slot_ts": pa.repeat(
                        pa.scalar(slot.start, pa.timestamp("us", tz="UTC")), count
                    ),
                    "line_number": pa.array(range(1, count + 1), pa.int64()),
                    "line": pa.array(lines, pa.string()),
                },
                schema=RAW_LINES_SCHEMA,
            )
        )
    return pa.concat_tables(tables)


def field_sql(column: SilverColumn) -> str:
    return f"f[{column.position}]"


def failure(column: SilverColumn) -> Check | None:
    """SQL that is true when the field cannot become its type, and the reason to record."""
    source = field_sql(column)
    label = f"{column.name} ({column.source})"
    match column.kind:
        case Kind.KEY:
            return (
                f"not regexp_full_match({source}, '{INTEGER}') "
                f"or try_cast({source} as bigint) is null",
                f"{label}: not an integer",
            )
        case Kind.INTEGER:
            return (
                f"{source} <> '' and (not regexp_full_match({source}, '{INTEGER}') "
                f"or try_cast({source} as integer) is null)",
                f"{label}: not an integer",
            )
        case Kind.DOUBLE:
            # A text such as 1e999 has the form of a number and is read as infinity.
            return (
                f"{source} <> '' and (not regexp_full_match({source}, '{DOUBLE}') "
                f"or try_cast({source} as double) is null "
                f"or not isfinite(try_cast({source} as double)))",
                f"{label}: not a number",
            )
        case Kind.FLAG:
            return f"{source} not in ('', '0', '1')", f"{label}: not 0 or 1"
        case Kind.DATE8:
            return (
                f"{source} <> '' and (not regexp_full_match({source}, '[0-9]{{8}}') "
                f"or try_strptime({source}, '%Y%m%d') is null)",
                f"{label}: not a date YYYYMMDD",
            )
        case Kind.TIMESTAMP14:
            return (
                f"{source} <> '' and (not regexp_full_match({source}, '[0-9]{{14}}') "
                f"or try_strptime({source}, '%Y%m%d%H%M%S') is null)",
                f"{label}: not a timestamp YYYYMMDDHHMMSS",
            )
        case Kind.REQUIRED_TIMESTAMP14:
            return (
                f"not regexp_full_match({source}, '[0-9]{{14}}') "
                f"or try_strptime({source}, '%Y%m%d%H%M%S') is null",
                f"{label}: not a timestamp YYYYMMDDHHMMSS",
            )
        case Kind.REQUIRED_TEXT:
            return f"{source} = ''", f"{label}: empty"
        case Kind.TEXT:
            return None


def typed(column: SilverColumn) -> str:
    """SQL that turns a validated field into its typed value. An empty field is NULL."""
    source = field_sql(column)
    match column.kind:
        case Kind.KEY:
            return f"cast({source} as bigint)"
        case Kind.INTEGER:
            return f"cast(nullif({source}, '') as integer)"
        case Kind.DOUBLE:
            return f"cast(nullif({source}, '') as double)"
        case Kind.FLAG:
            return f"case {source} when '1' then true when '0' then false end"
        case Kind.DATE8:
            return f"cast(strptime(nullif({source}, ''), '%Y%m%d') as date)"
        case Kind.TIMESTAMP14 | Kind.REQUIRED_TIMESTAMP14:
            return f"strptime(nullif({source}, ''), '%Y%m%d%H%M%S') at time zone 'UTC'"
        case Kind.TEXT:
            return f"nullif({source}, '')"
        case Kind.REQUIRED_TEXT:
            return source


def typed_columns(schema: FeedSchema) -> str:
    """The typed columns of a feed as a SELECT list."""
    return ",\n        ".join(f"{typed(column)} as {column.name}" for column in schema.columns)


def check_sql(schema: FeedSchema, extra: Sequence[Check] = ()) -> str:
    """Split every raw line and give it a reason if it fails gate G2, else NULL."""
    count = schema.column_count
    failures = [found for column in schema.columns if (found := failure(column)) is not None]
    cases = "\n".join(
        f"            when {test} then '{reason}'" for test, reason in [*failures, *extra]
    )
    return f"""
    create or replace temp table checked as
    with split as (
        select *, string_split(line, chr(9)) as f from raw_lines
    )
    select
        *,
        case
            when len(f) <> {count}
                then 'column_count: expected {count}, found ' || cast(len(f) as varchar)
{cases}
        end as reason
    from split
    """  # noqa: S608


def check_files(
    connection: duckdb.DuckDBPyConnection,
    files: Sequence[BronzeFile],
    schema: FeedSchema,
    *,
    extra: Sequence[Check] = (),
    file_threshold: float | None = None,
) -> tuple[tuple[FileCounts, ...], tuple[FileCounts, ...]]:
    """Gate G2: build the table ``checked`` with the lines of the accepted files.

    Returns the accepted files and the rejected files, each with its accounting. Without a
    threshold no file is rejected as a whole.
    """
    connection.register("raw_lines", raw_lines(files))
    connection.execute(check_sql(schema, extra))
    rows = connection.execute(
        "select source_file, count(*), count(*) filter (where reason is null) "
        "from checked group by source_file"
    ).fetchall()
    counted = {str(row[0]): (int(row[1]), int(row[2])) for row in rows}
    accepted: list[FileCounts] = []
    rejected: list[FileCounts] = []
    for file in files:
        total, parsed = counted.get(file.key, (0, 0))  # a file without lines has no row
        counts = FileCounts(file.key, file.md5, total, parsed, total - parsed)
        if file_threshold is not None and file_is_rejected(total, total - parsed, file_threshold):
            rejected.append(counts)
        else:
            accepted.append(counts)
    if rejected:
        connection.execute(
            "delete from checked where list_contains($files, source_file)",
            {"files": [counts.key for counts in rejected]},
        )
    return tuple(accepted), tuple(rejected)


def quarantine_rows(
    connection: duckdb.DuckDBPyConnection, *, run_id: str | None, ingested_at: Any
) -> pa.Table:
    """The rejected rows of the accepted files, in the layout of the quarantine file."""
    rows = arrow(
        connection.sql(
            "select source_file, source_md5, slot_ts, line_number, reason, line as raw_line, "
            "$run_id as _run_id, $ingested_at as _ingested_at from checked "
            "where reason is not null order by source_file, line_number",
            params={"ingested_at": ingested_at, "run_id": run_id},
        )
    )
    return rows.cast(QUARANTINE_SCHEMA)


def duplicate_rows(connection: duckdb.DuckDBPyConnection, table: str = "typed") -> int:
    """The typed rows which are not the first occurrence of their key.

    The number is counted in the typed table and not derived from the output, so the identity
    "committed rows = parsed rows - duplicate rows" of gate G3 compares two measurements.
    """
    row = connection.execute(f"select count(*) from {table} where occurrence > 1").fetchone()  # noqa: S608
    return int(row[0]) if row else 0


def line_counts(files: Sequence[FileCounts], committed: int, duplicates: int) -> UnitCounts:
    """The accounting of a unit: the lines of its files, the duplicates and the rows to commit."""
    return UnitCounts(
        raw_lines=sum(counts.raw_lines for counts in files),
        parsed=sum(counts.parsed_rows for counts in files),
        quarantined=sum(counts.quarantined_rows for counts in files),
        duplicates=duplicates,
        committed=committed,
    )


def arrow(result: Any) -> pa.Table:
    """The Arrow table of a DuckDB result (its stubs allow None, which a SELECT never gives)."""
    table: pa.Table | None = result.to_arrow_table()
    if table is None:
        raise RuntimeError("the query returned no result set")
    return table
