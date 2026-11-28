"""The schema ops as the migrations of the repository create it (week 2 plan, WP-02).

The check constraints and the triggers are the last line of defence of the ledger rules: a bug in
the code cannot store a state, a transition or an accounting which the rules do not allow.
"""

from __future__ import annotations

import itertools
from datetime import UTC, datetime
from typing import Any

import psycopg
import pytest

from gbmi.ledger.db import Connection
from gbmi.ledger.migrate import status
from gbmi.ledger.states import ALLOWED, State

from .conftest import MIGRATIONS

STATES = (
    "expected",
    "listed",
    "downloaded",
    "verified",
    "loaded",
    "quarantined",
    "confirmed_missing",
)
#: The twelve transitions of the state diagram (blueprint 8.2).
TRANSITIONS = {
    ("expected", "listed"),
    ("listed", "downloaded"),
    ("downloaded", "verified"),
    ("downloaded", "listed"),
    ("listed", "quarantined"),
    ("verified", "loaded"),
    ("verified", "quarantined"),
    ("quarantined", "listed"),
    ("expected", "confirmed_missing"),
    ("listed", "confirmed_missing"),
    ("quarantined", "confirmed_missing"),
    ("confirmed_missing", "listed"),
}
SLOT = datetime(2026, 11, 25, 8, 0, tzinfo=UTC)
LISTED = {
    "url": "https://example.org/20261125080000.export.CSV.zip",
    "expected_bytes": 10,
    "expected_md5": "0" * 32,
    "listed_at": SLOT,
}
VERIFIED = {**LISTED, "bronze_uri": "file:///lake/raw/x.zip", "verified_at": SLOT}
LOADED = {**VERIFIED, "loaded_at": SLOT, "raw_lines": 10, "parsed_rows": 9, "quarantined_rows": 1}


def insert(connection: Connection, table: str, **values: Any) -> None:
    columns = ", ".join(values)
    marks = ", ".join(["%s"] * len(values))
    with connection.transaction():
        connection.execute(
            f"INSERT INTO ops.{table} ({columns}) VALUES ({marks})",
            tuple(values.values()),
        )


def slot(connection: Connection, source: str, state: str = "expected", **values: Any) -> None:
    row = {"source": source, "feed": "export", "slot_ts": SLOT, "state": state, **values}
    insert(connection, "source_slot", **row)


def test_the_migrations_create_the_eleven_tables(ops: Connection, ops_db: str) -> None:
    rows = ops.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'ops'"
    ).fetchall()

    assert {row[0] for row in rows} == {
        "schema_migration",
        "source_slot",
        "slot_transition",
        "silver_partition",
        "pipeline_run",
        "dq_result",
        # week 3 (phase P2): the objects of the SEC, the circuit and the request log
        "source_object",
        "object_transition",
        "source_circuit",
        "circuit_event",
        "source_request",
    }
    report = status(ops_db, MIGRATIONS)
    assert len(report) >= 5
    assert {entry["state"] for entry in report} == {"applied"}


def test_no_column_of_the_schema_is_an_array(ops: Connection) -> None:
    # ClickHouse cannot read a PostgreSQL relation with a nullable array column (phase P0).
    rows = ops.execute(
        "SELECT table_name, column_name FROM information_schema.columns "
        "WHERE table_schema = 'ops' AND data_type = 'ARRAY'"
    ).fetchall()

    assert rows == []


def test_a_new_slot_is_expected_and_starts_on_a_quarter_hour(ops: Connection, source: str) -> None:
    insert(ops, "source_slot", source=source, feed="export", slot_ts=SLOT)
    row = ops.execute("SELECT state FROM ops.source_slot WHERE source = %s", (source,)).fetchone()
    assert row == ("expected",)

    for bad in (SLOT.replace(minute=7), SLOT.replace(second=1), SLOT.replace(microsecond=5)):
        with pytest.raises(psycopg.errors.CheckViolation, match="source_slot_quarter_hour"):
            insert(ops, "source_slot", source=source, feed="mentions", slot_ts=bad)


def test_an_unknown_state_is_refused(ops: Connection, source: str) -> None:
    # The row carries every fact which another rule could ask for, so only the state is wrong.
    with pytest.raises(psycopg.errors.CheckViolation, match="source_slot_state"):
        slot(ops, source, "done", **LOADED, decided_by="nobody")


@pytest.mark.parametrize("state", ["listed", "downloaded", "verified", "loaded", "quarantined"])
def test_a_listed_slot_carries_the_facts_of_the_list(
    ops: Connection, source: str, state: str
) -> None:
    with pytest.raises(psycopg.errors.CheckViolation, match="source_slot_listed"):
        slot(ops, source, state)


def test_a_verified_slot_names_its_bronze_file(ops: Connection, source: str) -> None:
    with pytest.raises(psycopg.errors.CheckViolation, match="source_slot_verified"):
        slot(ops, source, "verified", **LISTED)

    slot(ops, source, "verified", **VERIFIED)


def test_a_loaded_slot_accounts_for_every_raw_line(ops: Connection, source: str) -> None:
    with pytest.raises(psycopg.errors.CheckViolation, match="source_slot_loaded"):
        slot(ops, source, "loaded", **VERIFIED, loaded_at=SLOT)
    with pytest.raises(psycopg.errors.CheckViolation, match="source_slot_loaded"):
        slot(ops, source, "loaded", **{**LOADED, "parsed_rows": 8})

    slot(ops, source, "loaded", **LOADED)


def test_a_missing_slot_is_confirmed_by_somebody(ops: Connection, source: str) -> None:
    with pytest.raises(psycopg.errors.CheckViolation, match="source_slot_confirmed"):
        slot(ops, source, "confirmed_missing")

    slot(ops, source, "confirmed_missing", decided_by="auto")


def test_the_state_table_of_the_code_is_the_state_table_of_the_database() -> None:
    # The test below shows what the database accepts; the lists here are the same lists.
    assert set(STATES) == {state.value for state in State}
    assert {(a.value, b.value) for a, b in ALLOWED} == TRANSITIONS


def test_only_the_twelve_transitions_can_be_stored(ops: Connection, source: str) -> None:
    slot(ops, source)
    stored = set()
    for from_state, to_state in itertools.product(STATES, STATES):
        try:
            insert(
                ops,
                "slot_transition",
                source=source,
                feed="export",
                slot_ts=SLOT,
                from_state=from_state,
                to_state=to_state,
            )
        except psycopg.errors.CheckViolation:
            continue
        stored.add((from_state, to_state))

    assert stored == TRANSITIONS
    assert len(stored) == 12


def test_a_transition_belongs_to_a_slot(ops: Connection, source: str) -> None:
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        insert(
            ops,
            "slot_transition",
            source=source,
            feed="export",
            slot_ts=SLOT,
            from_state="expected",
            to_state="listed",
        )


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE ops.slot_transition SET reason = 'rewritten'",
        "DELETE FROM ops.slot_transition",
        "TRUNCATE ops.slot_transition",
    ],
)
def test_the_transition_log_is_append_only(ops: Connection, statement: str) -> None:
    with pytest.raises(psycopg.errors.RaiseException, match="append-only"), ops.transaction():
        ops.execute(statement.encode())


def test_an_update_moves_updated_at(ops: Connection, source: str) -> None:
    slot(ops, source)
    before = ops.execute(
        "SELECT updated_at FROM ops.source_slot WHERE source = %s", (source,)
    ).fetchone()

    ops.execute("UPDATE ops.source_slot SET attempts = 1 WHERE source = %s", (source,))

    after = ops.execute(
        "SELECT updated_at, created_at FROM ops.source_slot WHERE source = %s", (source,)
    ).fetchone()
    assert before is not None and after is not None
    assert after[0] > before[0]
    assert after[1] < after[0]


def partition(**values: Any) -> dict[str, Any]:
    return {
        "source": "gdelt",
        "feed": "export",
        "table_name": "gdelt_event",
        "slot_date": SLOT.date(),
        "unit_kind": "hour",
        "slot_hour": 8,
        "raw_lines": 10,
        "parsed_rows": 9,
        "quarantined_rows": 1,
        "duplicate_rows": 2,
        "committed_rows": 7,
        "file_count": 4,
        "fileset_hash": "f" * 64,
        "content_digest": "0" * 16,
        "pipeline_version": "test",
        **values,
    }


def test_a_partition_version_keeps_the_rules_of_the_audit(ops: Connection, source: str) -> None:
    insert(ops, "silver_partition", **partition(source=source))
    # A table whose rows are not lines of a source file stores the committed rows only.
    without_lines = dict.fromkeys(
        ("raw_lines", "parsed_rows", "quarantined_rows", "duplicate_rows")
    )
    insert(ops, "silver_partition", **partition(source=source, **without_lines))
    # An empty unit which needed no commit has no commit and no snapshot.
    empty = partition(
        source=source, raw_lines=0, parsed_rows=0, quarantined_rows=0, duplicate_rows=0
    )
    insert(ops, "silver_partition", **{**empty, "committed_rows": 0, "file_count": 0})
    insert(ops, "silver_partition", **partition(source=source, unit_kind="day", slot_hour=None))

    refused = [
        partition(source=source, raw_lines=11),  # raw lines != parsed + quarantined
        partition(source=source, committed_rows=8),  # committed != parsed - duplicates
        partition(source=source, duplicate_rows=None),  # some counts given, one missing
    ]
    for values in refused:
        with pytest.raises(psycopg.errors.CheckViolation, match="silver_partition_accounting"):
            insert(ops, "silver_partition", **values)
    for values in (
        partition(source=source, slot_hour=24),
        partition(source=source, slot_hour=None),
        partition(source=source, unit_kind="day"),
        partition(source=source, unit_kind="week", slot_hour=None),
    ):
        with pytest.raises(psycopg.errors.CheckViolation, match="silver_partition_unit"):
            insert(ops, "silver_partition", **values)

    rows = ops.execute(
        "SELECT count(*) FROM ops.silver_partition WHERE source = %s", (source,)
    ).fetchone()
    assert rows == (4,)


def test_run_records_and_quality_results_keep_their_vocabulary(
    ops: Connection, source: str
) -> None:
    run = {
        "entry_point": f"{source}.ingest",
        "started_at": SLOT,
        "ended_at": SLOT,
        "duration_ms": 0,
        "status": "success",
        "pipeline_version": "test",
    }
    insert(ops, "pipeline_run", **run)
    with pytest.raises(psycopg.errors.CheckViolation, match="pipeline_run_status"):
        insert(ops, "pipeline_run", **{**run, "status": "running"})
    with pytest.raises(psycopg.errors.CheckViolation, match="pipeline_run_times"):
        insert(ops, "pipeline_run", **{**run, "ended_at": SLOT.replace(hour=7)})

    result = {"gate": "G1", "check_name": "md5", "status": "passed", "source": source}
    insert(ops, "dq_result", **result)
    insert(ops, "dq_result", **{**result, "gate": "G3", "status": "accepted"})
    with pytest.raises(psycopg.errors.CheckViolation, match="dq_result_gate"):
        insert(ops, "dq_result", **{**result, "gate": "G9"})
    with pytest.raises(psycopg.errors.CheckViolation, match="dq_result_status"):
        insert(ops, "dq_result", **{**result, "status": "unknown"})
