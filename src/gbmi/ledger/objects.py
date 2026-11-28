"""The object ledger in PostgreSQL: the files of the SEC, with guarded state changes.

One row of ``ops.source_object`` stands for one file which the platform asks from the SEC
(blueprint 7.3; week 3 plan 3.3). Every change of state is one guarded update
(``UPDATE … WHERE state = <from>``) together with one row in ``ops.object_transition``, in one
statement, so the object and its log cannot differ: the pattern of
``gbmi.ledger.repository.Ledger``. A guarded update which changes no row means: the object was
not in the state the caller assumed. The functions for one object then raise
``StaleObjectStateError``; the functions for many objects return the objects which moved.

The SEC keeps no old version of a document. A document of a day which was never loaded cannot
be fetched later: the newer document replaces the object, and the state ``superseded`` records
the fact.
"""

from __future__ import annotations

from collections.abc import Collection, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Any

from psycopg import sql
from psycopg.types.json import Jsonb

from gbmi.ledger.db import Connection, connect
from gbmi.ledger.errors import StaleStateError
from gbmi.ledger.object_states import OPEN, ObjectState, check

SOURCE = "sec"
#: Gate G1 quarantines an object after three failed tries (week 3 plan 3.3).
MAX_ATTEMPTS = 3

_COLUMNS = sql.SQL(
    "dataset, object_date, object_name, state, cik, url, bronze_uri, byte_count, md5, "
    "http_status, last_modified, attempts, last_error, verified_at, loaded_at, raw_records, "
    "parsed_rows, quarantined_rows, reason, decided_by, detail"
)
_KEYS = sql.SQL(
    "unnest(%(datasets)s::text[], %(dates)s::date[], %(names)s::text[]) "
    "AS i(dataset, object_date, object_name)"
)
_MATCH = sql.SQL(
    "o.source = %(source)s AND o.dataset = i.dataset AND o.object_date = i.object_date "
    "AND o.object_name = i.object_name AND o.state = %(from_state)s"
)


class Dataset(StrEnum):
    """The five kinds of files which the platform asks from the SEC."""

    REFERENCE = "reference"  # the ticker file
    SUBMISSIONS = "submissions"  # one submissions document of one company
    INDEX = "index"  # one daily index file, or the listing of a quarter
    COMPANYFACTS = "companyfacts"  # one facts document of one company
    BULK = "bulk"  # one bulk archive


@dataclass(frozen=True, order=True)
class ObjectKey:
    """One file of one day."""

    dataset: Dataset
    object_date: date
    name: str

    @property
    def label(self) -> str:
        return f"{self.dataset.value}/{self.object_date.isoformat()}/{self.name}"


@dataclass(frozen=True)
class Expected:
    """An object which a pipeline wants, with the address to ask."""

    key: ObjectKey
    url: str
    cik: int | None = None


@dataclass(frozen=True)
class Download:
    """The facts of an object which passed gate G1 and is in bronze.

    ``http_status`` and ``last_modified`` are None when no request was made for the object:
    bronze already held the bytes, or the bytes are a member of a bulk archive.
    """

    bronze_uri: str
    byte_count: int
    md5: str
    http_status: int | None = None
    last_modified: datetime | None = None


@dataclass(frozen=True)
class ObjectAccounting:
    """Where every record of one file went: raw records = parsed rows + quarantined rows."""

    key: ObjectKey
    raw_records: int
    parsed_rows: int
    quarantined_rows: int = 0


@dataclass(frozen=True)
class ObjectRecord:
    """One row of ``ops.source_object``."""

    key: ObjectKey
    state: ObjectState
    cik: int | None
    url: str
    bronze_uri: str | None
    byte_count: int | None
    md5: str | None
    http_status: int | None
    last_modified: datetime | None
    attempts: int
    last_error: str | None
    verified_at: datetime | None
    loaded_at: datetime | None
    raw_records: int | None
    parsed_rows: int | None
    quarantined_rows: int | None
    reason: str | None
    decided_by: str | None
    detail: dict[str, Any]


class StaleObjectStateError(StaleStateError):
    """A guarded update changed no row: the object was not in the state the caller assumed."""

    noun = "object"


def _utc(value: datetime | None) -> datetime | None:
    return value.astimezone(UTC) if value is not None else None


def _key(dataset: str, object_date: date, name: str) -> ObjectKey:
    return ObjectKey(Dataset(dataset), object_date, name)


def _record(row: tuple[Any, ...]) -> ObjectRecord:
    return ObjectRecord(
        key=_key(row[0], row[1], row[2]),
        state=ObjectState(row[3]),
        cik=row[4],
        url=row[5],
        bronze_uri=row[6],
        byte_count=row[7],
        md5=row[8],
        http_status=row[9],
        last_modified=_utc(row[10]),
        attempts=row[11],
        last_error=row[12],
        verified_at=_utc(row[13]),
        loaded_at=_utc(row[14]),
        raw_records=row[15],
        parsed_rows=row[16],
        quarantined_rows=row[17],
        reason=row[18],
        decided_by=row[19],
        detail=dict(row[20] or {}),
    )


def _key_arrays(keys: Sequence[ObjectKey]) -> dict[str, Any]:
    return {
        "datasets": [key.dataset.value for key in keys],
        "dates": [key.object_date for key in keys],
        "names": [key.name for key in keys],
    }


class ObjectLedger:
    """The object ledger of one source, on one connection.

    The connection is in autocommit mode: each function is one transaction, and a caller who
    needs several changes together wraps the calls in ``with connection.transaction():``.
    """

    def __init__(
        self, connection: Connection, *, source: str = SOURCE, run_id: str | None = None
    ) -> None:
        if not connection.autocommit:
            raise ValueError("the object ledger needs a connection in autocommit mode")
        self.connection = connection
        self.source = source
        self.run_id = run_id

    # --- changes of state -----------------------------------------------------------------

    def _move(
        self,
        from_state: ObjectState,
        to_state: ObjectState,
        update: sql.Composable,
        parameters: dict[str, Any],
        *,
        reason: str | None,
        decided_by: str | None = None,
        as_of: datetime | None = None,
    ) -> set[ObjectKey]:
        """One guarded update and its log rows, in one statement; returns what moved."""
        check(from_state, to_state)
        statement = sql.SQL(
            "WITH moved AS ({update} RETURNING o.dataset, o.object_date, o.object_name) "
            "INSERT INTO ops.object_transition (source, dataset, object_date, object_name, "
            "from_state, to_state, reason, run_id, decided_by, as_of) "
            "SELECT %(source)s, dataset, object_date, object_name, %(from_state)s, %(to_state)s, "
            "%(reason)s, %(run_id)s, %(decided_by)s, %(as_of)s FROM moved "
            "RETURNING dataset, object_date, object_name"
        ).format(update=update)
        rows = self.connection.execute(
            statement,
            {
                **parameters,
                "source": self.source,
                "from_state": from_state.value,
                "to_state": to_state.value,
                "reason": reason,
                "run_id": self.run_id,
                "decided_by": decided_by,
                "as_of": as_of,
            },
        ).fetchall()
        return {_key(row[0], row[1], row[2]) for row in rows}

    def _move_keys(
        self,
        keys: Sequence[ObjectKey],
        from_state: ObjectState,
        to_state: ObjectState,
        assignments: sql.Composable,
        parameters: dict[str, Any],
        *,
        reason: str | None,
        decided_by: str | None = None,
        as_of: datetime | None = None,
    ) -> set[ObjectKey]:
        update = sql.SQL(
            "UPDATE ops.source_object o SET state = %(to_state)s{assignments} "
            "FROM {keys} WHERE {match}"
        ).format(assignments=assignments, keys=_KEYS, match=_MATCH)
        return self._move(
            from_state,
            to_state,
            update,
            {**parameters, **_key_arrays(keys)},
            reason=reason,
            decided_by=decided_by,
            as_of=as_of,
        )

    def _move_one(
        self,
        key: ObjectKey,
        from_state: ObjectState,
        to_state: ObjectState,
        assignments: sql.Composable,
        parameters: dict[str, Any],
        *,
        reason: str | None,
        as_of: datetime | None = None,
    ) -> None:
        moved = self._move_keys(
            [key], from_state, to_state, assignments, parameters, reason=reason, as_of=as_of
        )
        if not moved:
            raise StaleObjectStateError(key, from_state, to_state)

    def ensure_expected(self, objects: Sequence[Expected]) -> int:
        """Create the rows which do not exist yet; returns the number of new rows."""
        if not objects:
            return 0
        cursor = self.connection.execute(
            "INSERT INTO ops.source_object (source, dataset, object_date, object_name, url, cik) "
            "SELECT %(source)s, i.dataset, i.object_date, i.object_name, i.url, i.cik "
            "FROM unnest(%(datasets)s::text[], %(dates)s::date[], %(names)s::text[], "
            "%(urls)s::text[], %(ciks)s::bigint[]) "
            "AS i(dataset, object_date, object_name, url, cik) ON CONFLICT DO NOTHING",
            {
                **_key_arrays([expected.key for expected in objects]),
                "urls": [expected.url for expected in objects],
                "ciks": [expected.cik for expected in objects],
                "source": self.source,
            },
        )
        return cursor.rowcount

    def record_download(
        self, key: ObjectKey, download: Download, *, as_of: datetime | None = None
    ) -> None:
        """``expected → verified``: the bytes passed gate G1 and are in bronze."""
        self._move_one(
            key,
            ObjectState.EXPECTED,
            ObjectState.VERIFIED,
            sql.SQL(
                ", bronze_uri = %(bronze_uri)s, byte_count = %(byte_count)s, md5 = %(md5)s, "
                "http_status = %(http_status)s, last_modified = %(last_modified)s, "
                "verified_at = clock_timestamp(), last_error = NULL, reason = NULL"
            ),
            {
                "bronze_uri": download.bronze_uri,
                "byte_count": download.byte_count,
                "md5": download.md5,
                "http_status": download.http_status,
                "last_modified": download.last_modified,
            },
            reason="stored in bronze, gate G1 passed",
            as_of=as_of,
        )

    def record_failed_attempt(
        self, key: ObjectKey, *, error: str, http_status: int | None = None
    ) -> int:
        """The bytes of one try failed gate G1; the object stays ``expected``.

        Returns the number of failed tries since the object was last expected.
        """
        row = self.connection.execute(
            "UPDATE ops.source_object SET attempts = attempts + 1, last_error = %(error)s, "
            "http_status = %(http_status)s WHERE source = %(source)s AND dataset = %(dataset)s "
            "AND object_date = %(object_date)s AND object_name = %(name)s "
            "AND state = 'expected' RETURNING attempts",
            {**self._one(key), "error": error, "http_status": http_status},
        ).fetchone()
        if row is None:
            raise StaleObjectStateError(key, ObjectState.EXPECTED, ObjectState.EXPECTED)
        return int(row[0])

    def quarantine(
        self,
        key: ObjectKey,
        *,
        from_state: ObjectState,
        reason: str,
        as_of: datetime | None = None,
    ) -> None:
        """``expected → quarantined`` (gate G1) or ``verified → quarantined`` (gate G2 or G3)."""
        self._move_one(
            key,
            from_state,
            ObjectState.QUARANTINED,
            sql.SQL(", last_error = %(reason)s, reason = %(reason)s"),
            {},
            reason=reason,
            as_of=as_of,
        )

    def mark_loaded(
        self,
        entries: Sequence[ObjectAccounting],
        *,
        as_of: datetime | None = None,
        reason: str = "silver committed",
    ) -> set[ObjectKey]:
        """Record a silver commit for the files of a unit; returns the objects which moved.

        ``reason`` names another way in which a pipeline used an object up: a listing and an
        archive give no row to silver.

        A verified object moves ``verified → loaded``. An object which is loaded already keeps
        its state, because a rebuild is no change of state, and takes the new accounting. Any
        other state raises StaleObjectStateError, and nothing is recorded.
        """
        if not entries:
            return set()
        inputs = sql.SQL(
            "unnest(%(datasets)s::text[], %(dates)s::date[], %(names)s::text[], "
            "%(raw)s::bigint[], %(parsed)s::bigint[], %(quarantined)s::bigint[]) "
            "AS i(dataset, object_date, object_name, raw_records, parsed_rows, quarantined_rows)"
        )
        assignments = sql.SQL(
            "raw_records = i.raw_records, parsed_rows = i.parsed_rows, "
            "quarantined_rows = i.quarantined_rows"
        )
        keys = [entry.key for entry in entries]
        parameters = {
            **_key_arrays(keys),
            "raw": [entry.raw_records for entry in entries],
            "parsed": [entry.parsed_rows for entry in entries],
            "quarantined": [entry.quarantined_rows for entry in entries],
        }
        with self.connection.transaction():
            rows = self.connection.execute(
                sql.SQL(
                    "UPDATE ops.source_object o SET {assignments} FROM {inputs} "
                    "WHERE o.source = %(source)s AND o.dataset = i.dataset "
                    "AND o.object_date = i.object_date AND o.object_name = i.object_name "
                    "AND o.state = 'loaded' RETURNING o.dataset, o.object_date, o.object_name"
                ).format(assignments=assignments, inputs=inputs),
                {**parameters, "source": self.source},
            ).fetchall()
            rebuilt = {_key(row[0], row[1], row[2]) for row in rows}
            moved = self._move(
                ObjectState.VERIFIED,
                ObjectState.LOADED,
                sql.SQL(
                    "UPDATE ops.source_object o SET state = %(to_state)s, "
                    "loaded_at = clock_timestamp(), {assignments} FROM {inputs} WHERE {match}"
                ).format(assignments=assignments, inputs=inputs, match=_MATCH),
                parameters,
                reason=reason,
                as_of=as_of,
            )
            left = sorted(set(keys) - rebuilt - moved)
            if left:
                raise StaleObjectStateError(left[0], ObjectState.VERIFIED, ObjectState.LOADED)
        return moved

    def mark_not_published(
        self,
        key: ObjectKey,
        *,
        reason: str,
        http_status: int | None = None,
        as_of: datetime | None = None,
    ) -> None:
        """``expected → not_published``: the SEC has no such file."""
        self._move_one(
            key,
            ObjectState.EXPECTED,
            ObjectState.NOT_PUBLISHED,
            sql.SQL(", reason = %(reason)s, http_status = %(http_status)s"),
            {"http_status": http_status},
            reason=reason,
            as_of=as_of,
        )

    def reopen(
        self, keys: Sequence[ObjectKey], *, reason: str, as_of: datetime | None = None
    ) -> set[ObjectKey]:
        """``not_published → expected``: the file appears later."""
        if not keys:
            return set()
        return self._move_keys(
            keys,
            ObjectState.NOT_PUBLISHED,
            ObjectState.EXPECTED,
            sql.SQL(", reason = NULL, http_status = NULL, attempts = 0, last_error = NULL"),
            {},
            reason=reason,
            as_of=as_of,
        )

    def requeue(
        self,
        keys: Sequence[ObjectKey],
        *,
        decided_by: str,
        note: str,
        as_of: datetime | None = None,
    ) -> set[ObjectKey]:
        """``quarantined → expected``: an operator sends quarantined files back to the pipeline."""
        if not keys:
            return set()
        return self._move_keys(
            keys,
            ObjectState.QUARANTINED,
            ObjectState.EXPECTED,
            sql.SQL(
                ", attempts = 0, last_error = NULL, reason = NULL, decided_by = %(decided_by)s"
            ),
            {},
            reason=note,
            decided_by=decided_by,
            as_of=as_of,
        )

    def supersede_older(self, key: ObjectKey, *, as_of: datetime | None = None) -> set[ObjectKey]:
        """Close every open object of the same dataset and name which is older than ``key``.

        The SEC keeps no old version of a file, so once ``key`` is loaded, an older object
        which was never loaded can never be loaded: ``expected | verified | quarantined →
        superseded``. The statement moves nothing unless ``key`` is loaded: ``superseded`` is
        final, so a wrong call must not close an object. Returns the objects which moved.
        """
        return self.supersede_older_many(key.dataset, key.object_date, [key.name], as_of=as_of)

    def supersede_older_many(
        self,
        dataset: Dataset,
        object_date: date,
        names: Sequence[str],
        *,
        as_of: datetime | None = None,
    ) -> set[ObjectKey]:
        """``supersede_older`` for many file names of one dataset and one date.

        One transaction with one statement for each open state, whatever the number of names:
        a bulk day loads the documents of about 8,000 companies.
        """
        if not names:
            return set()
        reason = f"the object of {object_date.isoformat()} was loaded first"
        moved: set[ObjectKey] = set()
        with self.connection.transaction():
            for from_state in sorted(OPEN):
                update = sql.SQL(
                    "UPDATE ops.source_object o SET state = %(to_state)s, reason = %(reason)s "
                    "WHERE o.source = %(source)s AND o.dataset = %(dataset)s "
                    "AND o.object_name = ANY(%(names)s) AND o.object_date < %(object_date)s "
                    "AND o.state = %(from_state)s AND EXISTS ("
                    "SELECT 1 FROM ops.source_object n WHERE n.source = o.source "
                    "AND n.dataset = o.dataset AND n.object_name = o.object_name "
                    "AND n.object_date = %(object_date)s AND n.state = 'loaded')"
                )
                moved |= self._move(
                    from_state,
                    ObjectState.SUPERSEDED,
                    update,
                    {"dataset": dataset.value, "names": list(names), "object_date": object_date},
                    reason=reason,
                    as_of=as_of,
                )
        return moved

    def set_detail(self, key: ObjectKey, detail: dict[str, Any]) -> None:
        """Merge free detail into the row of an object, for example the archive of a member."""
        self.connection.execute(
            "UPDATE ops.source_object SET detail = detail || %(detail)s "
            "WHERE source = %(source)s AND dataset = %(dataset)s "
            "AND object_date = %(object_date)s AND object_name = %(name)s",
            {**self._one(key), "detail": Jsonb(detail)},
        )

    # --- questions ------------------------------------------------------------------------

    def _one(self, key: ObjectKey) -> dict[str, Any]:
        return {
            "source": self.source,
            "dataset": key.dataset.value,
            "object_date": key.object_date,
            "name": key.name,
        }

    def get(self, key: ObjectKey) -> ObjectRecord | None:
        """The row of one object; None when the ledger has no such object."""
        row = self.connection.execute(
            sql.SQL(
                "SELECT {columns} FROM ops.source_object WHERE source = %(source)s "
                "AND dataset = %(dataset)s AND object_date = %(object_date)s "
                "AND object_name = %(name)s"
            ).format(columns=_COLUMNS),
            self._one(key),
        ).fetchone()
        return _record(row) if row is not None else None

    def pending(
        self,
        *,
        dataset: Dataset | None = None,
        first: date | None = None,
        last: date | None = None,
    ) -> list[ObjectRecord]:
        """The objects which still need work, oldest first."""
        return self.select(states=OPEN, dataset=dataset, first=first, last=last)

    def select(
        self,
        *,
        states: Collection[ObjectState] | None = None,
        dataset: Dataset | None = None,
        first: date | None = None,
        last: date | None = None,
    ) -> list[ObjectRecord]:
        """The objects of a range of days, by date, dataset and name."""
        rows = self.connection.execute(
            sql.SQL(
                "SELECT {columns} FROM ops.source_object WHERE source = %(source)s "
                "AND (%(states)s::text[] IS NULL OR state = ANY(%(states)s)) "
                "AND (%(dataset)s::text IS NULL OR dataset = %(dataset)s) "
                "AND (%(first)s::date IS NULL OR object_date >= %(first)s) "
                "AND (%(last)s::date IS NULL OR object_date <= %(last)s) "
                "ORDER BY object_date, dataset, object_name"
            ).format(columns=_COLUMNS),
            {
                "source": self.source,
                "states": sorted(state.value for state in states) if states is not None else None,
                "dataset": dataset.value if dataset is not None else None,
                "first": first,
                "last": last,
            },
        ).fetchall()
        return [_record(row) for row in rows]

    def newest(
        self,
        dataset: Dataset,
        *,
        cik: int | None = None,
        name: str | None = None,
        states: Collection[ObjectState] | None = None,
        not_after: date | None = None,
    ) -> ObjectRecord | None:
        """The newest object of a dataset for one company or one file name; None when none fits."""
        if (cik is None) == (name is None):
            raise ValueError("newest() takes a CIK or a file name, not both and not neither")
        row = self.connection.execute(
            sql.SQL(
                "SELECT {columns} FROM ops.source_object WHERE source = %(source)s "
                "AND dataset = %(dataset)s "
                "AND (%(cik)s::bigint IS NULL OR cik = %(cik)s) "
                "AND (%(name)s::text IS NULL OR object_name = %(name)s) "
                "AND (%(states)s::text[] IS NULL OR state = ANY(%(states)s)) "
                "AND (%(not_after)s::date IS NULL OR object_date <= %(not_after)s) "
                "ORDER BY object_date DESC LIMIT 1"
            ).format(columns=_COLUMNS),
            {
                "source": self.source,
                "dataset": dataset.value,
                "cik": cik,
                "name": name,
                "states": sorted(state.value for state in states) if states is not None else None,
                "not_after": not_after,
            },
        ).fetchone()
        return _record(row) if row is not None else None

    def newest_by_cik(
        self,
        dataset: Dataset,
        *,
        states: Collection[ObjectState] | None = None,
        not_after: date | None = None,
    ) -> dict[int, ObjectRecord]:
        """The newest object of a dataset for each company, in one question."""
        rows = self.connection.execute(
            sql.SQL(
                "SELECT DISTINCT ON (cik) {columns} FROM ops.source_object "
                "WHERE source = %(source)s AND dataset = %(dataset)s AND cik IS NOT NULL "
                "AND (%(states)s::text[] IS NULL OR state = ANY(%(states)s)) "
                "AND (%(not_after)s::date IS NULL OR object_date <= %(not_after)s) "
                "ORDER BY cik, object_date DESC"
            ).format(columns=_COLUMNS),
            {
                "source": self.source,
                "dataset": dataset.value,
                "states": sorted(state.value for state in states) if states is not None else None,
                "not_after": not_after,
            },
        ).fetchall()
        return {int(row[4]): _record(row) for row in rows}

    def status(
        self, *, first: date | None = None, last: date | None = None
    ) -> list[dict[str, Any]]:
        """The number of objects by dataset and state, for a range of days."""
        rows = self.connection.execute(
            "SELECT dataset, state, count(*), min(object_date), max(object_date) "
            "FROM ops.source_object WHERE source = %(source)s "
            "AND (%(first)s::date IS NULL OR object_date >= %(first)s) "
            "AND (%(last)s::date IS NULL OR object_date <= %(last)s) "
            "GROUP BY dataset, state ORDER BY dataset, state",
            {"source": self.source, "first": first, "last": last},
        ).fetchall()
        return [
            {
                "dataset": row[0],
                "state": row[1],
                "objects": int(row[2]),
                "first_date": row[3].isoformat(),
                "last_date": row[4].isoformat(),
            }
            for row in rows
        ]

    def transitions(self, key: ObjectKey) -> list[tuple[ObjectState, ObjectState]]:
        """The recorded changes of one object, in order."""
        rows = self.connection.execute(
            "SELECT from_state, to_state FROM ops.object_transition WHERE source = %(source)s "
            "AND dataset = %(dataset)s AND object_date = %(object_date)s "
            "AND object_name = %(name)s ORDER BY transition_id",
            self._one(key),
        ).fetchall()
        return [(ObjectState(row[0]), ObjectState(row[1])) for row in rows]


@contextmanager
def open_object_ledger(
    dsn: str | None = None, *, source: str = SOURCE, run_id: str | None = None
) -> Iterator[ObjectLedger]:
    """An object ledger on a new connection, which is closed when the block ends."""
    with connect(dsn, autocommit=True) as connection:
        yield ObjectLedger(connection, source=source, run_id=run_id)
