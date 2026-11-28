"""The ledger in PostgreSQL: guarded state changes, and the questions the pipelines ask.

Every change of state is one guarded update (``UPDATE … WHERE state = <from>``) together with
one row in ``ops.slot_transition``, in one statement, so the slot and its log cannot differ
(blueprint 8.2). A guarded update which changes no row means: the slot was not in the state the
caller assumed. The functions for one slot then raise ``StaleStateError``; the functions for many
slots return the slots which moved.

Times: a column such as ``listed_at`` holds the clock of the database, because the latency
numbers need real times. A rule reads the time from the argument ``as_of``, so a test controls
the clock (week 2 plan 3.3). ``ops.slot_transition.as_of`` records the clock of the run.
"""

from __future__ import annotations

import hashlib
from collections.abc import Collection, Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

from psycopg import sql

from gbmi.core.gdelt import Feed, Slot
from gbmi.ledger.calendar import (
    SLOTS_PER_DAY,
    days_between,
    first_slot_of,
    last_slot_of,
    newest_expected,
    slot_at,
)
from gbmi.ledger.db import Connection, connect
from gbmi.ledger.errors import StaleStateError as StaleStateError
from gbmi.ledger.states import State, check

SOURCE = "gdelt"
#: A slot which is not verified this long after its time is reported (blueprint 8.2).
OVERDUE_AFTER = timedelta(hours=6)
#: The silver jobs look at the pending slots of the trailing days (blueprint 8.4).
TRAILING_DAYS = 7

_COLUMNS = sql.SQL(
    "feed, slot_ts, state, url, expected_bytes, expected_md5, bronze_uri, attempts, last_error, "
    "listed_at, published_at, verified_at, unavailable_since, absent_count, loaded_at, "
    "silver_snapshot_id, raw_lines, parsed_rows, quarantined_rows, decided_by, decision_note, "
    "reserved_for"
)
_KEYS = sql.SQL("unnest(%(feeds)s::text[], %(slots)s::timestamptz[]) AS i(feed, slot_ts)")
_LISTINGS = sql.SQL(
    "unnest(%(feeds)s::text[], %(slots)s::timestamptz[], %(urls)s::text[], %(sizes)s::bigint[], "
    "%(md5s)s::text[]) AS i(feed, slot_ts, url, size, md5)"
)
_MATCH = sql.SQL(
    "s.source = %(source)s AND s.feed = i.feed AND s.slot_ts = i.slot_ts "
    "AND s.state = %(from_state)s"
)


@dataclass(frozen=True, order=True)
class SlotKey:
    """One expected file of a source: a feed and a slot."""

    feed: Feed
    slot: Slot

    @property
    def label(self) -> str:
        return f"{self.slot.stamp}.{self.feed.value}"


@dataclass(frozen=True)
class Listing:
    """What the source lists about a file."""

    key: SlotKey
    url: str
    size: int
    md5: str


@dataclass(frozen=True)
class Accounting:
    """Where every raw line of one file went: raw lines = parsed rows + quarantined rows."""

    key: SlotKey
    raw_lines: int
    parsed_rows: int
    quarantined_rows: int


@dataclass(frozen=True)
class SlotRecord:
    """One row of ``ops.source_slot``."""

    key: SlotKey
    state: State
    url: str | None
    expected_bytes: int | None
    expected_md5: str | None
    bronze_uri: str | None
    attempts: int
    last_error: str | None
    listed_at: datetime | None
    published_at: datetime | None
    verified_at: datetime | None
    unavailable_since: datetime | None
    absent_count: int
    loaded_at: datetime | None
    silver_snapshot_id: int | None
    raw_lines: int | None
    parsed_rows: int | None
    quarantined_rows: int | None
    decided_by: str | None
    decision_note: str | None
    reserved_for: str | None

    @property
    def listing(self) -> Listing:
        """The facts of the list; only a slot which the source has listed has them."""
        if self.url is None or self.expected_bytes is None or self.expected_md5 is None:
            raise ValueError(f"slot {self.key.label} was never listed")
        return Listing(self.key, self.url, self.expected_bytes, self.expected_md5)


@dataclass(frozen=True, order=True)
class PendingUnit:
    """What a silver job has to build: one slot-hour, or one slot-date (``slot_hour`` is None)."""

    slot_date: date
    feed: Feed
    slot_hour: int | None


def fileset_hash(files: Iterable[tuple[str, str]]) -> str:
    """The hash of a set of source files: SHA-256 over the lines ``<stamp>:<md5>`` in slot order.

    A partition version stores the hash of the files which went into the build (blueprint
    8.4). An equal hash means: the same files with the same bytes. Each line ends with a line
    feed, so ``shasum -a 256`` over a text file with the same lines gives the same value.
    """
    lines = "".join(f"{stamp}:{md5}\n" for stamp, md5 in sorted(files))
    return hashlib.sha256(lines.encode("ascii")).hexdigest()


def _utc(value: datetime | None) -> datetime | None:
    return value.astimezone(UTC) if value is not None else None


def _key(feed: str, slot_ts: datetime) -> SlotKey:
    return SlotKey(Feed(feed), Slot(slot_ts.astimezone(UTC)))


def _record(row: tuple[Any, ...]) -> SlotRecord:
    return SlotRecord(
        key=_key(row[0], row[1]),
        state=State(row[2]),
        url=row[3],
        expected_bytes=row[4],
        expected_md5=row[5],
        bronze_uri=row[6],
        attempts=row[7],
        last_error=row[8],
        listed_at=_utc(row[9]),
        published_at=_utc(row[10]),
        verified_at=_utc(row[11]),
        unavailable_since=_utc(row[12]),
        absent_count=row[13],
        loaded_at=_utc(row[14]),
        silver_snapshot_id=row[15],
        raw_lines=row[16],
        parsed_rows=row[17],
        quarantined_rows=row[18],
        decided_by=row[19],
        decision_note=row[20],
        reserved_for=row[21],
    )


def _key_arrays(keys: Sequence[SlotKey]) -> dict[str, Any]:
    return {
        "feeds": [key.feed.value for key in keys],
        "slots": [key.slot.start for key in keys],
    }


def _listing_arrays(listings: Sequence[Listing]) -> dict[str, Any]:
    return {
        **_key_arrays([listing.key for listing in listings]),
        "urls": [listing.url for listing in listings],
        "sizes": [listing.size for listing in listings],
        "md5s": [listing.md5 for listing in listings],
    }


class Ledger:
    """The ledger of one source, on one connection.

    The connection is in autocommit mode: each function is one transaction, and a caller who
    needs several changes together wraps the calls in ``with connection.transaction():``.
    """

    def __init__(
        self, connection: Connection, *, source: str = SOURCE, run_id: str | None = None
    ) -> None:
        if not connection.autocommit:
            raise ValueError("the ledger needs a connection in autocommit mode")
        self.connection = connection
        self.source = source
        self.run_id = run_id

    # --- changes of state -----------------------------------------------------------------

    def _move(
        self,
        from_state: State,
        to_state: State,
        update: sql.Composable,
        parameters: dict[str, Any],
        *,
        reason: str | None,
        decided_by: str | None = None,
        as_of: datetime | None = None,
    ) -> set[SlotKey]:
        """One guarded update and its log rows, in one statement; returns the slots which moved."""
        check(from_state, to_state)
        statement = sql.SQL(
            "WITH moved AS ({update} RETURNING s.feed, s.slot_ts) "
            "INSERT INTO ops.slot_transition "
            "(source, feed, slot_ts, from_state, to_state, reason, run_id, decided_by, as_of) "
            "SELECT %(source)s, feed, slot_ts, %(from_state)s, %(to_state)s, %(reason)s, "
            "%(run_id)s, %(decided_by)s, %(as_of)s FROM moved "
            "RETURNING feed, slot_ts"
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
        return {_key(row[0], row[1]) for row in rows}

    def _move_one(
        self,
        key: SlotKey,
        from_state: State,
        to_state: State,
        assignments: sql.Composable,
        parameters: dict[str, Any],
        *,
        reason: str | None,
        decided_by: str | None = None,
        as_of: datetime | None = None,
    ) -> None:
        update = sql.SQL(
            "UPDATE ops.source_slot s SET state = %(to_state)s{assignments} "
            "FROM {keys} WHERE {match}"
        ).format(assignments=assignments, keys=_KEYS, match=_MATCH)
        moved = self._move(
            from_state,
            to_state,
            update,
            {**parameters, **_key_arrays([key])},
            reason=reason,
            decided_by=decided_by,
            as_of=as_of,
        )
        if not moved:
            raise StaleStateError(key, from_state, to_state)

    def ensure_expected(self, keys: Sequence[SlotKey], *, reserve: str | None = None) -> int:
        """Create the calendar rows which do not exist yet; returns the number of new rows.

        ``reserve`` puts a claim on the new rows: routine silver jobs skip a claimed slot.
        """
        if not keys:
            return 0
        cursor = self.connection.execute(
            sql.SQL(
                "INSERT INTO ops.source_slot (source, feed, slot_ts, reserved_for) "
                "SELECT %(source)s, i.feed, i.slot_ts, %(reserve)s FROM {keys} "
                "ON CONFLICT DO NOTHING"
            ).format(keys=_KEYS),
            {**_key_arrays(keys), "source": self.source, "reserve": reserve},
        )
        return cursor.rowcount

    def mark_listed(
        self, listings: Sequence[Listing], *, as_of: datetime | None = None
    ) -> set[SlotKey]:
        """``expected → listed`` for every listed file whose slot is expected."""
        if not listings:
            return set()
        update = sql.SQL(
            "UPDATE ops.source_slot s SET state = %(to_state)s, url = i.url, "
            "expected_bytes = i.size, expected_md5 = i.md5, listed_at = clock_timestamp(), "
            "absent_count = 0 FROM {listings} WHERE {match}"
        ).format(listings=_LISTINGS, match=_MATCH)
        return self._move(
            State.EXPECTED,
            State.LISTED,
            update,
            _listing_arrays(listings),
            reason="found in feed index",
            as_of=as_of,
        )

    def update_listing(self, listings: Sequence[Listing]) -> set[SlotKey]:
        """Take a changed size or MD5 from the list, for a listed slot which is not verified yet.

        The state does not change. The count of failed downloads starts again, because the
        failures belonged to the old facts.
        """
        if not listings:
            return set()
        rows = self.connection.execute(
            sql.SQL(
                "UPDATE ops.source_slot s SET url = i.url, expected_bytes = i.size, "
                "expected_md5 = i.md5, attempts = 0, unavailable_since = NULL FROM {listings} "
                "WHERE s.source = %(source)s AND s.feed = i.feed AND s.slot_ts = i.slot_ts "
                "AND s.state = 'listed' "
                "AND (s.expected_bytes, s.expected_md5) IS DISTINCT FROM (i.size, i.md5) "
                "RETURNING s.feed, s.slot_ts"
            ).format(listings=_LISTINGS),
            {**_listing_arrays(listings), "source": self.source},
        ).fetchall()
        return {_key(row[0], row[1]) for row in rows}

    def record_download(
        self,
        key: SlotKey,
        *,
        bronze_uri: str,
        published_at: datetime | None = None,
        as_of: datetime | None = None,
    ) -> None:
        """``listed → downloaded → verified`` in one transaction: the file passed gate G1.

        ``published_at`` is the upload time of the file; None when bronze already held the file
        and no request was made.
        """
        with self.connection.transaction():
            self._move_one(
                key,
                State.LISTED,
                State.DOWNLOADED,
                sql.SQL(""),
                {},
                reason="file stored in bronze",
                as_of=as_of,
            )
            self._move_one(
                key,
                State.DOWNLOADED,
                State.VERIFIED,
                sql.SQL(
                    ", bronze_uri = %(bronze_uri)s, verified_at = clock_timestamp(), "
                    "published_at = COALESCE(%(published_at)s, s.published_at), "
                    "unavailable_since = NULL, last_error = NULL"
                ),
                {"bronze_uri": bronze_uri, "published_at": published_at},
                reason="size and MD5 match",
                as_of=as_of,
            )

    def record_failed_attempt(
        self, key: SlotKey, *, error: str, as_of: datetime | None = None
    ) -> int:
        """``listed → downloaded → listed``: the bytes failed gate G1.

        Returns the number of failed downloads since the slot was last listed.
        """
        with self.connection.transaction():
            self._move_one(
                key,
                State.LISTED,
                State.DOWNLOADED,
                sql.SQL(""),
                {},
                reason="file fetched",
                as_of=as_of,
            )
            self._move_one(
                key,
                State.DOWNLOADED,
                State.LISTED,
                sql.SQL(", attempts = s.attempts + 1, last_error = %(error)s"),
                {"error": error},
                reason=f"mismatch, fetch again: {error}",
                as_of=as_of,
            )
            record = self.get(key)
        if record is None:  # the row was just updated
            raise RuntimeError(f"slot {key.label} vanished from the ledger")
        return record.attempts

    def record_unavailable(self, key: SlotKey, *, as_of: datetime, error: str) -> datetime:
        """A listed file cannot be fetched (HTTP 404): keep the time of the first failure.

        The slot stays ``listed``. Returns the time since when the file is unavailable.
        """
        row = self.connection.execute(
            "UPDATE ops.source_slot "
            "SET unavailable_since = COALESCE(unavailable_since, %(as_of)s), "
            "last_error = %(error)s WHERE source = %(source)s AND feed = %(feed)s "
            "AND slot_ts = %(slot_ts)s AND state = 'listed' RETURNING unavailable_since",
            {
                "as_of": as_of,
                "error": error,
                "source": self.source,
                "feed": key.feed.value,
                "slot_ts": key.slot.start,
            },
        ).fetchone()
        if row is None:
            raise StaleStateError(key, State.LISTED, State.LISTED)
        since: datetime = row[0]
        return since.astimezone(UTC)

    def quarantine(
        self, key: SlotKey, *, from_state: State, reason: str, as_of: datetime | None = None
    ) -> None:
        """``listed → quarantined`` (retries exhausted) or ``verified → quarantined`` (gate G2)."""
        self._move_one(
            key,
            from_state,
            State.QUARANTINED,
            sql.SQL(", last_error = %(error)s"),
            {"error": reason},
            reason=reason,
            as_of=as_of,
        )

    def mark_loaded(
        self,
        entries: Sequence[Accounting],
        *,
        snapshot_id: int | None,
        as_of: datetime | None = None,
    ) -> set[SlotKey]:
        """Record a silver commit for the files of a unit; returns the slots which moved.

        A verified slot moves ``verified → loaded``. A slot which is loaded already keeps its
        state, because a rebuild is no change of state, and takes the new accounting. Any other
        state raises StaleStateError, and nothing is recorded.
        """
        if not entries:
            return set()
        inputs = sql.SQL(
            "unnest(%(feeds)s::text[], %(slots)s::timestamptz[], %(raw)s::bigint[], "
            "%(parsed)s::bigint[], %(quarantined)s::bigint[]) "
            "AS i(feed, slot_ts, raw_lines, parsed_rows, quarantined_rows)"
        )
        assignments = sql.SQL(
            "silver_snapshot_id = %(snapshot_id)s, raw_lines = i.raw_lines, "
            "parsed_rows = i.parsed_rows, quarantined_rows = i.quarantined_rows"
        )
        keys = [entry.key for entry in entries]
        parameters = {
            **_key_arrays(keys),
            "raw": [entry.raw_lines for entry in entries],
            "parsed": [entry.parsed_rows for entry in entries],
            "quarantined": [entry.quarantined_rows for entry in entries],
            "snapshot_id": snapshot_id,
        }
        with self.connection.transaction():
            rows = self.connection.execute(
                sql.SQL(
                    "UPDATE ops.source_slot s SET {assignments} FROM {inputs} "
                    "WHERE s.source = %(source)s AND s.feed = i.feed AND s.slot_ts = i.slot_ts "
                    "AND s.state = 'loaded' RETURNING s.feed, s.slot_ts"
                ).format(assignments=assignments, inputs=inputs),
                {**parameters, "source": self.source},
            ).fetchall()
            rebuilt = {_key(row[0], row[1]) for row in rows}
            moved = self._move(
                State.VERIFIED,
                State.LOADED,
                sql.SQL(
                    "UPDATE ops.source_slot s SET state = %(to_state)s, "
                    "loaded_at = clock_timestamp(), {assignments} FROM {inputs} WHERE {match}"
                ).format(assignments=assignments, inputs=inputs, match=_MATCH),
                parameters,
                reason="silver committed",
                as_of=as_of,
            )
            left = sorted(set(keys) - rebuilt - moved)
            if left:
                raise StaleStateError(left[0], State.VERIFIED, State.LOADED)
        return moved

    def confirm_missing(
        self,
        keys: Sequence[SlotKey],
        *,
        from_state: State,
        decided_by: str,
        note: str,
        as_of: datetime | None = None,
    ) -> set[SlotKey]:
        """``expected | listed | quarantined → confirmed_missing``, by a rule or an operator."""
        if not keys:
            return set()
        update = sql.SQL(
            "UPDATE ops.source_slot s SET state = %(to_state)s, decided_by = %(decided_by)s, "
            "decision_note = %(reason)s FROM {keys} WHERE {match}"
        ).format(keys=_KEYS, match=_MATCH)
        return self._move(
            from_state,
            State.CONFIRMED_MISSING,
            update,
            _key_arrays(keys),
            reason=note,
            decided_by=decided_by,
            as_of=as_of,
        )

    def requeue(
        self, keys: Sequence[SlotKey], *, decided_by: str, note: str, as_of: datetime | None = None
    ) -> set[SlotKey]:
        """``quarantined → listed``: an operator sends quarantined files back to the download."""
        if not keys:
            return set()
        update = sql.SQL(
            "UPDATE ops.source_slot s SET state = %(to_state)s, attempts = 0, last_error = NULL, "
            "unavailable_since = NULL FROM {keys} WHERE {match}"
        ).format(keys=_KEYS, match=_MATCH)
        return self._move(
            State.QUARANTINED,
            State.LISTED,
            update,
            _key_arrays(keys),
            reason=note,
            decided_by=decided_by,
            as_of=as_of,
        )

    def reopen(
        self, listings: Sequence[Listing], *, reason: str, as_of: datetime | None = None
    ) -> set[SlotKey]:
        """``confirmed_missing → listed``: the source lists the file after all."""
        if not listings:
            return set()
        update = sql.SQL(
            "UPDATE ops.source_slot s SET state = %(to_state)s, url = i.url, "
            "expected_bytes = i.size, expected_md5 = i.md5, listed_at = clock_timestamp(), "
            "decided_by = NULL, decision_note = NULL, absent_count = 0, attempts = 0, "
            "unavailable_since = NULL, last_error = NULL FROM {listings} WHERE {match}"
        ).format(listings=_LISTINGS, match=_MATCH)
        return self._move(
            State.CONFIRMED_MISSING,
            State.LISTED,
            update,
            _listing_arrays(listings),
            reason=reason,
            as_of=as_of,
        )

    def count_absent(self, keys: Sequence[SlotKey]) -> int:
        """One more reconciliation did not find the expected slots; returns the slots counted."""
        if not keys:
            return 0
        cursor = self.connection.execute(
            sql.SQL(
                "UPDATE ops.source_slot s SET absent_count = s.absent_count + 1 FROM {keys} "
                "WHERE s.source = %(source)s AND s.feed = i.feed AND s.slot_ts = i.slot_ts "
                "AND s.state = 'expected'"
            ).format(keys=_KEYS),
            {**_key_arrays(keys), "source": self.source},
        )
        return cursor.rowcount

    # --- questions ------------------------------------------------------------------------

    def get(self, key: SlotKey) -> SlotRecord | None:
        row = self.connection.execute(
            sql.SQL(
                "SELECT {columns} FROM ops.source_slot "
                "WHERE source = %(source)s AND feed = %(feed)s AND slot_ts = %(slot_ts)s"
            ).format(columns=_COLUMNS),
            {"source": self.source, "feed": key.feed.value, "slot_ts": key.slot.start},
        ).fetchone()
        return _record(row) if row is not None else None

    def find(
        self,
        *,
        first: Slot | None = None,
        last: Slot | None = None,
        feeds: Collection[Feed] | None = None,
        states: Collection[State] | None = None,
        limit: int | None = None,
    ) -> list[SlotRecord]:
        """The slots which match every given filter, oldest first."""
        rows = self.connection.execute(
            sql.SQL(
                "SELECT {columns} FROM ops.source_slot WHERE source = %(source)s "
                "AND (%(first)s::timestamptz IS NULL OR slot_ts >= %(first)s::timestamptz) "
                "AND (%(last)s::timestamptz IS NULL OR slot_ts <= %(last)s::timestamptz) "
                "AND (%(feeds)s::text[] IS NULL OR feed = ANY(%(feeds)s::text[])) "
                "AND (%(states)s::text[] IS NULL OR state = ANY(%(states)s::text[])) "
                "ORDER BY slot_ts, feed LIMIT %(limit)s"
            ).format(columns=_COLUMNS),
            {
                "source": self.source,
                "first": first.start if first is not None else None,
                "last": last.start if last is not None else None,
                "feeds": sorted(feed.value for feed in feeds) if feeds is not None else None,
                "states": sorted(state.value for state in states) if states is not None else None,
                "limit": limit,
            },
        ).fetchall()
        return [_record(row) for row in rows]

    def newest_slot(self, *, listed: bool = False) -> Slot | None:
        """The newest slot of the ledger; with ``listed``, the newest slot the source has listed."""
        row = self.connection.execute(
            "SELECT max(slot_ts) FROM ops.source_slot WHERE source = %(source)s "
            "AND (NOT %(listed)s OR url IS NOT NULL)",
            {"source": self.source, "listed": listed},
        ).fetchone()
        return Slot(row[0].astimezone(UTC)) if row is not None and row[0] is not None else None

    def pending_downloads(self, limit: int | None = None) -> list[SlotRecord]:
        """The listed slots, oldest first: the files which the ingest job has to fetch."""
        return self.find(states=[State.LISTED], limit=limit)

    def unit_slots(
        self, feed: Feed, slot_date: date, slot_hour: int | None = None
    ) -> list[SlotRecord]:
        """The slots which a build of the unit reads: verified or loaded, without a claim.

        The unit is one slot-hour, or the whole slot-date when ``slot_hour`` is None. The slots
        come in slot order.
        """
        if slot_hour is None:
            first, last = first_slot_of(slot_date), last_slot_of(slot_date)
        else:
            start = first_slot_of(slot_date).start + timedelta(hours=slot_hour)
            first, last = Slot(start), Slot(start + timedelta(minutes=45))
        records = self.find(
            first=first, last=last, feeds=[feed], states=[State.VERIFIED, State.LOADED]
        )
        return [record for record in records if record.reserved_for is None]

    def _days_to_finalise_again(self, first_day: date) -> set[PendingUnit]:
        """The finalised days, from ``first_day`` on, which no longer equal their day build.

        A day was finalised when a day-level partition version exists. The day has to be
        built again in two cases: the loaded files of the day are not the files of the newest
        day build (another file-set hash), or an hour unit of the day was built after the
        newest day build.
        """
        versions = self.connection.execute(
            "WITH newest AS ("
            "  SELECT feed, slot_date, max(partition_version_id) AS any_kind,"
            "         max(partition_version_id) FILTER (WHERE unit_kind = 'day') AS day_kind"
            "  FROM ops.silver_partition"
            "  WHERE source = %(source)s AND slot_date >= %(first)s GROUP BY feed, slot_date) "
            "SELECT n.feed, n.slot_date, n.any_kind > n.day_kind, v.fileset_hash "
            "FROM newest n JOIN ops.silver_partition v ON v.partition_version_id = n.day_kind",
            {"source": self.source, "first": first_day},
        ).fetchall()
        if not versions:
            return set()
        slots = self.connection.execute(
            "SELECT feed, (slot_ts AT TIME ZONE 'UTC')::date, "
            "to_char(slot_ts AT TIME ZONE 'UTC', 'YYYYMMDDHH24MISS'), expected_md5 "
            "FROM ops.source_slot WHERE source = %(source)s AND state = 'loaded' "
            "AND reserved_for IS NULL AND slot_ts >= %(first)s",
            {"source": self.source, "first": first_slot_of(first_day).start},
        ).fetchall()
        loaded: dict[tuple[str, date], list[tuple[str, str]]] = {}
        for feed, day, stamp, md5 in slots:
            loaded.setdefault((feed, day), []).append((stamp, md5))
        return {
            PendingUnit(day, Feed(feed), None)
            for feed, day, hour_after_day, recorded in versions
            if hour_after_day or fileset_hash(loaded.get((feed, day), ())) != recorded
        }

    def pending_units(self, as_of: datetime, *, days: int = TRAILING_DAYS) -> list[PendingUnit]:
        """What the silver jobs have to build (week 2 plan 3.3).

        A pending slot is a verified slot without a claim, in the trailing ``days``. A feed and
        date with pending slots gives one hour unit for each hour with a pending slot while the
        date has no day-level partition version, and one day unit afterwards. A finalised day
        which no longer equals its day build is pending as a whole day too.
        """
        rows = self.connection.execute(
            "WITH pending AS ("
            "  SELECT feed, (slot_ts AT TIME ZONE 'UTC')::date AS slot_date,"
            "         extract(hour FROM slot_ts AT TIME ZONE 'UTC')::integer AS slot_hour"
            "  FROM ops.source_slot"
            "  WHERE source = %(source)s AND state = 'verified' AND reserved_for IS NULL"
            "    AND slot_ts >= %(first)s AND slot_ts <= %(last)s"
            "  GROUP BY 1, 2, 3) "
            "SELECT p.feed, p.slot_date, p.slot_hour, EXISTS ("
            "  SELECT 1 FROM ops.silver_partition v WHERE v.source = %(source)s"
            "  AND v.feed = p.feed AND v.slot_date = p.slot_date AND v.unit_kind = 'day') "
            "FROM pending p",
            {
                "source": self.source,
                "first": slot_at(as_of - timedelta(days=days)).start,
                "last": newest_expected(as_of).start,
            },
        ).fetchall()
        units = {PendingUnit(row[1], Feed(row[0]), None if row[3] else int(row[2])) for row in rows}
        units |= self._days_to_finalise_again(slot_at(as_of - timedelta(days=days)).slot_date)
        return sorted(
            units, key=lambda unit: (unit.slot_date, unit.feed.value, unit.slot_hour or 0)
        )

    def overdue(self, as_of: datetime, *, after: timedelta = OVERDUE_AFTER) -> list[SlotRecord]:
        """The slots which are still not verified ``after`` their time (blueprint 8.2)."""
        return self.find(
            last=slot_at(as_of - after),
            states=[State.EXPECTED, State.LISTED, State.DOWNLOADED],
        )

    def status(self, first_day: date, last_day: date) -> list[dict[str, Any]]:
        """The number of slots in each state, for each feed and UTC day of the range."""
        rows = self.connection.execute(
            "SELECT (slot_ts AT TIME ZONE 'UTC')::date, feed, state, count(*) "
            "FROM ops.source_slot WHERE source = %(source)s "
            "AND slot_ts >= %(first)s AND slot_ts <= %(last)s GROUP BY 1, 2, 3 ORDER BY 1, 2, 3",
            {
                "source": self.source,
                "first": first_slot_of(first_day).start,
                "last": last_slot_of(last_day).start,
            },
        ).fetchall()
        return [
            {"slot_date": row[0].isoformat(), "feed": row[1], "state": row[2], "slots": int(row[3])}
            for row in rows
        ]

    def completeness(
        self, first_day: date, last_day: date, *, feeds: Sequence[Feed] = tuple(Feed)
    ) -> list[dict[str, Any]]:
        """Completeness of each UTC day of the range (blueprint 8.2).

        completeness = loaded slots ÷ (expected slots - confirmed-missing slots). One entry
        for each day and feed, and one entry for each day over all feeds (``feed`` is None).
        A day is complete when every slot is loaded or confirmed missing.
        """
        counts: dict[tuple[str, str], dict[str, int]] = {}
        for entry in self.status(first_day, last_day):
            counts.setdefault((entry["slot_date"], entry["feed"]), {})[entry["state"]] = entry[
                "slots"
            ]
        report: list[dict[str, Any]] = []
        for day in days_between(first_day, last_day):
            loaded_all = missing_all = 0
            for feed in feeds:
                states = counts.get((day.isoformat(), feed.value), {})
                loaded = states.get(State.LOADED.value, 0)
                missing = states.get(State.CONFIRMED_MISSING.value, 0)
                loaded_all += loaded
                missing_all += missing
                report.append(_completeness(day, feed.value, loaded, missing, SLOTS_PER_DAY))
            report.append(
                _completeness(day, None, loaded_all, missing_all, SLOTS_PER_DAY * len(feeds))
            )
        return report

    def transitions(self, key: SlotKey) -> list[tuple[State, State]]:
        """The recorded changes of one slot, oldest first."""
        rows = self.connection.execute(
            "SELECT from_state, to_state FROM ops.slot_transition WHERE source = %(source)s "
            "AND feed = %(feed)s AND slot_ts = %(slot_ts)s ORDER BY transition_id",
            {"source": self.source, "feed": key.feed.value, "slot_ts": key.slot.start},
        ).fetchall()
        return [(State(row[0]), State(row[1])) for row in rows]


def _completeness(
    day: date, feed: str | None, loaded: int, missing: int, expected: int
) -> dict[str, Any]:
    possible = expected - missing
    return {
        "slot_date": day.isoformat(),
        "feed": feed,
        "expected": expected,
        "loaded": loaded,
        "confirmed_missing": missing,
        # A day on which every slot is confirmed missing has nothing left to load.
        "completeness": round(loaded / possible, 6) if possible else 1.0,
        "complete": loaded + missing == expected,
    }


@contextmanager
def open_ledger(
    dsn: str | None = None, *, source: str = SOURCE, run_id: str | None = None
) -> Iterator[Ledger]:
    """A ledger on a new connection, which is closed when the block ends."""
    with connect(dsn, autocommit=True) as connection:
        yield Ledger(connection, source=source, run_id=run_id)
