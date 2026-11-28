"""The tables of week 3 as the migrations create them (week 3 plan, WP-02).

``0004_sec_objects.sql`` adds the object ledger, the circuit row and the request log;
``0005_partition_units.sql`` widens the partition versions. The check constraints and the
triggers are the last line of defence: a bug in the code cannot store a state, a transition or
an accounting which the rules do not allow, and cannot rewrite the request log.
"""

from __future__ import annotations

import itertools
import shutil
import uuid
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import psycopg
import pytest

from gbmi.ledger.db import Connection, connect
from gbmi.ledger.migrate import migrate
from gbmi.ledger.object_states import ALLOWED, ObjectState

from .conftest import MIGRATIONS

STATES = ("expected", "verified", "quarantined", "loaded", "not_published", "superseded")
#: The ten transitions of the week 3 plan (3.3).
TRANSITIONS = {
    ("expected", "verified"),
    ("verified", "loaded"),
    ("expected", "quarantined"),
    ("verified", "quarantined"),
    ("quarantined", "expected"),
    ("expected", "not_published"),
    ("not_published", "expected"),
    ("expected", "superseded"),
    ("verified", "superseded"),
    ("quarantined", "superseded"),
}
DAY = date(2026, 11, 25)
NOW = datetime(2026, 11, 26, 9, 0, tzinfo=UTC)
NAME = "CIK0001326801.json"
VERIFIED = {
    "bronze_uri": "file:///lake/raw/sec/submissions/dt=2026-11-25/CIK0001326801.json",
    "byte_count": 10,
    "md5": "0" * 32,
    "verified_at": NOW,
}
LOADED = {**VERIFIED, "loaded_at": NOW, "raw_records": 10, "parsed_rows": 9, "quarantined_rows": 1}


def insert(connection: Connection, table: str, **values: Any) -> None:
    columns = ", ".join(values)
    marks = ", ".join(["%s"] * len(values))
    with connection.transaction():
        connection.execute(
            f"INSERT INTO ops.{table} ({columns}) VALUES ({marks})",
            tuple(values.values()),
        )


#: The transitions which lead from a new object to each state.
PATHS = {
    "expected": [],
    "verified": ["verified"],
    "loaded": ["verified", "loaded"],
    "quarantined": ["quarantined"],
    "not_published": ["not_published"],
    "superseded": ["superseded"],
}


def move(connection: Connection, source: str, name: str = NAME, **values: Any) -> None:
    """One UPDATE of the object of the test."""
    assignments = ", ".join(f"{column} = %s" for column in values)
    with connection.transaction():
        connection.execute(
            f"UPDATE ops.source_object SET {assignments} WHERE source = %s AND object_name = %s",
            (*values.values(), source, name),
        )


def obj(
    connection: Connection, source: str, state: str = "expected", name: str = NAME, **values: Any
) -> None:
    """An object in a state: a new object is expected, and the table allows only transitions."""
    with connection.transaction():
        connection.execute(
            "INSERT INTO ops.source_object (source, dataset, object_date, object_name, url) "
            "VALUES (%s, 'submissions', %s, %s, %s) ON CONFLICT DO NOTHING",
            (source, DAY, name, f"https://data.sec.gov/submissions/{name}"),
        )
    steps = PATHS.get(state, [state])
    for step in steps[:-1]:
        # The steps on the way carry the facts of a verified object and no more.
        move(connection, source, name, state=step, **VERIFIED)
    if steps or values:
        last = {"state": steps[-1]} if steps else {}
        move(connection, source, name, **last, **values)


def request(source: str, **values: Any) -> dict[str, Any]:
    return {
        "source": source,
        "host": "data.sec.gov",
        "path": "/submissions/CIK0001326801.json",
        "method": "GET",
        "granted_at": NOW,
        "client_session": uuid.uuid4(),
        "granted_mono_ns": 1_000,
        "waited_ms": 0,
        "attempt": 1,
        **values,
    }


# --- ops.source_object ------------------------------------------------------------------------


def test_a_new_object_is_expected_and_belongs_to_a_known_dataset(
    ops: Connection, source: str
) -> None:
    insert(
        ops,
        "source_object",
        source=source,
        dataset="index",
        object_date=DAY,
        object_name="form.20261125.idx",
        url="https://www.sec.gov/Archives/edgar/daily-index/2026/QTR4/form.20261125.idx",
    )
    row = ops.execute(
        "SELECT state, attempts, detail FROM ops.source_object WHERE source = %s", (source,)
    ).fetchone()
    assert row == ("expected", 0, {})

    key = {"source": source, "object_date": DAY, "object_name": NAME}
    with pytest.raises(psycopg.errors.CheckViolation, match="source_object_dataset"):
        insert(ops, "source_object", **key, dataset="prices", url="https://x")
    with pytest.raises(psycopg.errors.NotNullViolation):
        insert(ops, "source_object", **key, dataset="submissions")
    obj(ops, source)
    for column in ("attempts", "byte_count", "raw_records", "parsed_rows", "quarantined_rows"):
        with pytest.raises(psycopg.errors.CheckViolation, match="source_object_counters"):
            move(ops, source, **{column: -1})
    for bad in ("0" * 31, "0" * 33, "G" * 32, "A" * 32):
        with pytest.raises(psycopg.errors.CheckViolation, match="source_object_md5"):
            move(ops, source, md5=bad)


def test_a_new_object_is_always_expected(ops: Connection, source: str) -> None:
    key = {
        "source": source,
        "dataset": "submissions",
        "object_date": DAY,
        "object_name": NAME,
        "url": "https://data.sec.gov/x",
    }
    for state in ("verified", "loaded", "quarantined", "not_published", "superseded", "listed"):
        # The row carries every fact which another rule could ask for: only the state is wrong.
        with pytest.raises(psycopg.errors.RaiseException, match="a new object is expected"):
            insert(ops, "source_object", **key, state=state, **LOADED, reason="none")
    insert(ops, "source_object", **key, state="expected")


def test_the_table_itself_follows_the_ten_transitions(ops: Connection, source: str) -> None:
    # A statement which is no call of the object ledger cannot move an object another way.
    stored = set()
    for number, (from_state, to_state) in enumerate(itertools.product(STATES, STATES)):
        if from_state == to_state:
            continue
        name = f"CIK{number:010d}.json"
        obj(ops, source, from_state, name, **LOADED, reason="test")
        try:
            move(ops, source, name, state=to_state)
        except psycopg.errors.RaiseException as error:
            assert f"no transition from {from_state} to {to_state}" in str(error)
            continue
        stored.add((from_state, to_state))

    assert stored == TRANSITIONS
    with pytest.raises(
        psycopg.errors.RaiseException, match="no transition from expected to listed"
    ):
        obj(ops, source, "listed", "CIK9999999999.json")


def test_the_key_of_an_object_never_changes_and_no_object_is_deleted(
    ops: Connection, source: str
) -> None:
    obj(ops, source)

    for change in ({"object_name": "other.json"}, {"object_date": NOW.date()}, {"dataset": "bulk"}):
        with pytest.raises(psycopg.errors.RaiseException, match="the key of an object never"):
            move(ops, source, **change)
    with pytest.raises(psycopg.errors.RaiseException, match="append-only"), ops.transaction():
        ops.execute("DELETE FROM ops.source_object WHERE source = %s", (source,))


@pytest.mark.parametrize("state", ["verified", "loaded"])
def test_a_verified_object_names_its_bronze_file_with_size_and_hash(
    ops: Connection, source: str, state: str
) -> None:
    for missing in ("bronze_uri", "byte_count", "md5", "verified_at"):
        with pytest.raises(psycopg.errors.CheckViolation, match="source_object_verified"):
            obj(ops, source, state, **{**LOADED, missing: None})

    obj(ops, source, state, **LOADED)


def test_a_loaded_object_accounts_for_every_record(ops: Connection, source: str) -> None:
    with pytest.raises(psycopg.errors.CheckViolation, match="source_object_loaded"):
        obj(ops, source, "loaded", **VERIFIED, loaded_at=NOW)
    with pytest.raises(psycopg.errors.CheckViolation, match="source_object_loaded"):
        obj(ops, source, "loaded", **{**LOADED, "parsed_rows": 8})

    obj(ops, source, "loaded", **LOADED)


@pytest.mark.parametrize("state", ["quarantined", "not_published", "superseded"])
def test_a_quarantine_and_a_final_state_without_a_load_say_why(
    ops: Connection, source: str, state: str
) -> None:
    with pytest.raises(psycopg.errors.CheckViolation, match="source_object_reason"):
        obj(ops, source, state)

    obj(ops, source, state, reason="HTTP 404 NoSuchKey")


def test_the_object_table_of_the_code_is_the_object_table_of_the_database() -> None:
    assert set(STATES) == {state.value for state in ObjectState}
    assert {(a.value, b.value) for a, b in ALLOWED} == TRANSITIONS


def test_only_the_ten_transitions_can_be_stored(ops: Connection, source: str) -> None:
    obj(ops, source)
    stored = set()
    for from_state, to_state in itertools.product(STATES, STATES):
        try:
            insert(
                ops,
                "object_transition",
                source=source,
                dataset="submissions",
                object_date=DAY,
                object_name=NAME,
                from_state=from_state,
                to_state=to_state,
            )
        except psycopg.errors.CheckViolation:
            continue
        stored.add((from_state, to_state))

    assert stored == TRANSITIONS
    assert len(stored) == 10
    assert not {pair for pair in stored if pair[0] in ("loaded", "superseded")}


def test_an_object_transition_belongs_to_an_object(ops: Connection, source: str) -> None:
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        insert(
            ops,
            "object_transition",
            source=source,
            dataset="submissions",
            object_date=DAY,
            object_name=NAME,
            from_state="expected",
            to_state="verified",
        )


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE ops.object_transition SET reason = 'rewritten'",
        "DELETE FROM ops.object_transition",
        "TRUNCATE ops.object_transition",
    ],
)
def test_the_object_log_is_append_only(ops: Connection, statement: str) -> None:
    with pytest.raises(psycopg.errors.RaiseException, match="append-only"), ops.transaction():
        ops.execute(statement.encode())


def test_an_update_of_an_object_moves_updated_at(ops: Connection, source: str) -> None:
    obj(ops, source)
    before = ops.execute(
        "SELECT updated_at FROM ops.source_object WHERE source = %s", (source,)
    ).fetchone()

    ops.execute("UPDATE ops.source_object SET attempts = 1 WHERE source = %s", (source,))

    after = ops.execute(
        "SELECT updated_at FROM ops.source_object WHERE source = %s", (source,)
    ).fetchone()
    assert before is not None and after is not None
    assert after[0] > before[0]


# --- ops.source_circuit -----------------------------------------------------------------------


def test_a_recorded_block_says_when_and_why_and_a_lock_needs_a_block(
    ops: Connection, source: str
) -> None:
    with pytest.raises(psycopg.errors.CheckViolation, match="source_circuit_open"):
        insert(ops, "source_circuit", source=source, blocked_until=NOW)
    with pytest.raises(psycopg.errors.CheckViolation, match="source_circuit_locked"):
        insert(ops, "source_circuit", source=source, locked=True)
    with pytest.raises(psycopg.errors.CheckViolation, match="source_circuit_counters"):
        insert(ops, "source_circuit", source=source, open_count=-1)

    insert(
        ops,
        "source_circuit",
        source=source,
        blocked_until=NOW,
        opened_at=NOW,
        reason="HTTP 429",
        locked=True,
    )
    with pytest.raises(psycopg.errors.UniqueViolation):  # one row for each source
        insert(ops, "source_circuit", source=source)


@pytest.mark.parametrize(
    "statement", ["DELETE FROM ops.source_circuit", "TRUNCATE ops.source_circuit"]
)
def test_no_circuit_row_can_be_removed(ops: Connection, statement: str) -> None:
    # A source without a row has a closed circuit: a removed row would forget a block.
    with pytest.raises(psycopg.errors.RaiseException, match="is not allowed"), ops.transaction():
        ops.execute(statement.encode())


def test_the_history_of_a_circuit_is_append_only(ops: Connection, source: str) -> None:
    insert(ops, "circuit_event", source=source, event="opened", event_at=NOW, reason="HTTP 429")
    with pytest.raises(psycopg.errors.CheckViolation, match="circuit_event_kind"):
        insert(ops, "circuit_event", source=source, event="paused", event_at=NOW)
    for statement in (
        "UPDATE ops.circuit_event SET reason = 'rewritten'",
        "DELETE FROM ops.circuit_event",
        "TRUNCATE ops.circuit_event",
    ):
        with pytest.raises(psycopg.errors.RaiseException, match="append-only"), ops.transaction():
            ops.execute(statement.encode())


# --- ops.source_request -----------------------------------------------------------------------


def test_a_request_row_keeps_its_vocabulary(ops: Connection, source: str) -> None:
    insert(ops, "source_request", **request(source))
    with pytest.raises(psycopg.errors.CheckViolation, match="source_request_method"):
        insert(ops, "source_request", **request(source, method="POST"))
    for bad in (
        {"waited_ms": -1},
        {"attempt": 0},
        {"granted_mono_ns": -1},
        {"duration_ms": -1, "outcome": "ok", "completed_at": NOW},
        {"byte_count": -1, "outcome": "ok", "completed_at": NOW},
    ):
        with pytest.raises(psycopg.errors.CheckViolation, match="source_request_counters"):
            insert(ops, "source_request", **request(source, **bad))
    row = ops.execute(
        "SELECT probe, recorded_at > now() - interval '1 minute' FROM ops.source_request "
        "WHERE source = %s",
        (source,),
    ).fetchone()
    assert row == (False, True)  # no probe unless the client says so; stamped by the database
    with pytest.raises(psycopg.errors.CheckViolation, match="source_request_outcome"):
        insert(ops, "source_request", **request(source, outcome="fine", completed_at=NOW))
    with pytest.raises(psycopg.errors.CheckViolation, match="source_request_completed"):
        insert(ops, "source_request", **request(source, outcome="ok"))

    columns = ops.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'ops' AND table_name = 'source_request'"
    ).fetchall()
    assert not {name for (name,) in columns if "agent" in name or "header" in name}


def test_the_outcome_of_a_request_is_written_once(ops: Connection, source: str) -> None:
    insert(ops, "source_request", **request(source))

    ops.execute(
        "UPDATE ops.source_request SET completed_at = %s, outcome = 'ok', http_status = 200, "
        "duration_ms = 12, byte_count = 100 WHERE source = %s",
        (NOW, source),
    )

    with pytest.raises(psycopg.errors.RaiseException, match="outcome is recorded"):
        ops.execute("UPDATE ops.source_request SET http_status = 500 WHERE source = %s", (source,))
    # The outcome is one statement: a statement which leaves completed_at empty is refused,
    # so nobody can write parts of an outcome again and again.
    insert(ops, "source_request", **request(source, path="/second"))
    with pytest.raises(psycopg.errors.RaiseException, match="written whole, with completed_at"):
        ops.execute(
            "UPDATE ops.source_request SET http_status = 500 WHERE source = %s AND path = %s",
            (source, "/second"),
        )
    row = ops.execute(
        "SELECT outcome, http_status FROM ops.source_request WHERE source = %s", (source,)
    ).fetchone()
    assert row == ("ok", 200)


@pytest.mark.parametrize(
    "assignment",
    [
        "granted_at = granted_at + interval '2 seconds'",
        "granted_mono_ns = granted_mono_ns + 1",
        "client_session = gen_random_uuid()",
        "source = 'other'",
        "host = 'www.sec.gov'",
        "path = '/other'",
        "method = 'HEAD'",
        "waited_ms = 5",
        "attempt = 2",
        "probe = true",
        "run_id = 'other'",
        "recorded_at = recorded_at - interval '1 hour'",
    ],
)
def test_the_facts_of_a_request_never_change(ops: Connection, source: str, assignment: str) -> None:
    insert(ops, "source_request", **request(source))

    # The statement also writes a whole outcome, so only the changed fact is wrong.
    with pytest.raises(psycopg.errors.RaiseException, match="only the outcome can be set"):
        ops.execute(
            f"UPDATE ops.source_request SET {assignment}, completed_at = now(), "
            f"outcome = 'ok' WHERE source = %s".encode(),
            (source,),
        )


@pytest.mark.parametrize(
    "statement", ["DELETE FROM ops.source_request", "TRUNCATE ops.source_request"]
)
def test_no_row_of_the_request_log_can_be_removed(ops: Connection, statement: str) -> None:
    with pytest.raises(psycopg.errors.RaiseException, match="append-only"), ops.transaction():
        ops.execute(statement.encode())


# --- ops.silver_partition, widened by 0005 ----------------------------------------------------


def partition(**values: Any) -> dict[str, Any]:
    return {
        "source": "sec",
        "feed": "companyfacts",
        "table_name": "sec_xbrl_fact",
        "slot_date": None,
        "unit_kind": "keys",
        "slot_hour": None,
        "unit_label": "cik:3:9f2c",
        "committed_rows": 7,
        "file_count": 3,
        "fileset_hash": "f" * 64,
        "content_digest": "0" * 16,
        "pipeline_version": "test",
        **values,
    }


def test_a_unit_of_keys_or_of_a_table_is_named_by_its_label(ops: Connection, source: str) -> None:
    insert(ops, "silver_partition", **partition(source=source))
    insert(ops, "silver_partition", **partition(source=source, unit_kind="table", unit_label="all"))
    # A date of an SEC table is a day unit, as a date of a GDELT table is.
    insert(
        ops,
        "silver_partition",
        **partition(source=source, unit_kind="day", slot_date=DAY, unit_label="2026-11-25"),
    )

    for values in (
        partition(source=source, unit_label=None),  # no label
        partition(source=source, unit_label=""),  # an empty label
        partition(source=source, slot_date=DAY),  # a keys unit has no date
        partition(source=source, slot_hour=8),  # and no hour
        partition(source=source, unit_kind="table", unit_label=None),
        partition(source=source, unit_kind="day"),  # a day unit needs the date
        partition(source=source, unit_kind="hour", slot_hour=8),  # an hour unit too
        partition(source=source, unit_kind="week"),
    ):
        with pytest.raises(psycopg.errors.CheckViolation, match="silver_partition_unit"):
            insert(ops, "silver_partition", **values)

    rows = ops.execute(
        "SELECT unit_kind, slot_date, unit_label FROM ops.silver_partition WHERE source = %s "
        "ORDER BY partition_version_id",
        (source,),
    ).fetchall()
    assert rows == [
        ("keys", None, "cik:3:9f2c"),
        ("table", None, "all"),
        ("day", DAY, "2026-11-25"),
    ]


def test_the_fifth_file_keeps_the_partition_versions_of_phase_p1(
    empty_db: str, tmp_path: Path
) -> None:
    # A database of phase P1 plus the fourth file, with partition versions: the working stack.
    before = tmp_path / "first_four"
    before.mkdir()
    for file in sorted(MIGRATIONS.glob("000[1-4]_*.sql")):
        shutil.copy(file, before / file.name)
    assert len(list(before.iterdir())) == 4
    migrate(empty_db, before)
    old = {
        "source": "gdelt",
        "feed": "export",
        "table_name": "gdelt_event",
        "slot_date": DAY,
        "committed_rows": 7,
        "file_count": 4,
        "fileset_hash": "f" * 64,
        "content_digest": "0" * 16,
        "pipeline_version": "p1",
    }
    with connect(empty_db, autocommit=True) as connection:
        insert(connection, "silver_partition", **old, unit_kind="hour", slot_hour=8)
        insert(connection, "silver_partition", **old, unit_kind="day", slot_hour=None)

    applied = migrate(empty_db, MIGRATIONS)

    assert [entry["file"] for entry in applied][:1] == ["0005_partition_units.sql"]
    with connect(empty_db, autocommit=True) as connection:
        rows = connection.execute(
            "SELECT unit_kind, slot_date, slot_hour, unit_label FROM ops.silver_partition "
            "ORDER BY partition_version_id"
        ).fetchall()
        assert rows == [("hour", DAY, 8, None), ("day", DAY, None, None)]
        insert(connection, "silver_partition", **partition())
        # The rules of phase P1 still carry the name of phase P1.
        with pytest.raises(psycopg.errors.CheckViolation, match="silver_partition_unit"):
            insert(connection, "silver_partition", **old, unit_kind="hour", slot_hour=24)
