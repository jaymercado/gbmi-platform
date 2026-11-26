"""The migration runner: forward only, one transaction for each file, one runner at a time."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg
import pytest
from typer.testing import CliRunner

from gbmi.cli import app
from gbmi.config import get_settings
from gbmi.ledger.db import connect
from gbmi.ledger.migrate import MigrationError, discover, migrate, status

FIRST = "CREATE TABLE ops.first (id integer PRIMARY KEY);\n"
SECOND = "CREATE TABLE ops.second (id integer PRIMARY KEY);\nINSERT INTO ops.second VALUES (1);\n"


def write_files(directory: Path, files: dict[str, str]) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    for name, content in files.items():
        (directory / name).write_text(content, encoding="utf-8")
    return directory


def tables(dsn: str) -> set[str]:
    with connect(dsn, autocommit=True) as connection:
        rows = connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'ops'"
        ).fetchall()
    return {str(row[0]) for row in rows}


def recorded(dsn: str) -> list[tuple[int, str]]:
    with connect(dsn, autocommit=True) as connection:
        rows = connection.execute(
            "SELECT version, name FROM ops.schema_migration ORDER BY version"
        ).fetchall()
    return [(int(row[0]), str(row[1])) for row in rows]


def test_a_fresh_database_receives_every_file(empty_db: str, tmp_path: Path) -> None:
    files = write_files(tmp_path, {"0001_first.sql": FIRST, "0002_second.sql": SECOND})

    applied = migrate(empty_db, files)

    assert [entry["file"] for entry in applied] == ["0001_first.sql", "0002_second.sql"]
    assert tables(empty_db) == {"schema_migration", "first", "second"}
    assert recorded(empty_db) == [(1, "first"), (2, "second")]


def test_a_second_run_changes_nothing(empty_db: str, tmp_path: Path) -> None:
    files = write_files(tmp_path, {"0001_first.sql": FIRST, "0002_second.sql": SECOND})
    migrate(empty_db, files)
    with connect(empty_db, autocommit=True) as connection:
        before = connection.execute("SELECT * FROM ops.schema_migration ORDER BY 1").fetchall()

    assert migrate(empty_db, files) == []

    with connect(empty_db, autocommit=True) as connection:
        after = connection.execute("SELECT * FROM ops.schema_migration ORDER BY 1").fetchall()
        rows = connection.execute("SELECT count(*) FROM ops.second").fetchone()
    assert after == before
    assert rows == (1,)  # the INSERT of the second file ran once


def test_a_new_file_is_applied_on_top_of_the_applied_files(empty_db: str, tmp_path: Path) -> None:
    files = write_files(tmp_path, {"0001_first.sql": FIRST})
    migrate(empty_db, files)
    write_files(tmp_path, {"0002_second.sql": SECOND})

    applied = migrate(empty_db, files)

    assert [entry["file"] for entry in applied] == ["0002_second.sql"]
    assert recorded(empty_db) == [(1, "first"), (2, "second")]


def test_a_changed_file_is_refused(empty_db: str, tmp_path: Path) -> None:
    files = write_files(tmp_path, {"0001_first.sql": FIRST})
    migrate(empty_db, files)
    write_files(
        tmp_path, {"0001_first.sql": FIRST + "-- edited later\n", "0002_second.sql": SECOND}
    )

    with pytest.raises(MigrationError, match=r"0001_first\.sql changed after it was applied"):
        migrate(empty_db, files)

    # Nothing ran: the new file waits until the changed file is restored.
    assert tables(empty_db) == {"schema_migration", "first"}
    assert recorded(empty_db) == [(1, "first")]


def test_a_failing_file_leaves_no_trace(empty_db: str, tmp_path: Path) -> None:
    broken = "CREATE TABLE ops.second (id integer PRIMARY KEY);\nSELECT no_such_function();\n"
    files = write_files(tmp_path, {"0001_first.sql": FIRST, "0002_second.sql": broken})

    with pytest.raises(psycopg.errors.UndefinedFunction):
        migrate(empty_db, files)

    # The first file stays applied. The table of the second file went with the transaction.
    assert tables(empty_db) == {"schema_migration", "first"}
    assert recorded(empty_db) == [(1, "first")]

    # A repaired file is not a changed file, because the database never recorded the broken one.
    write_files(tmp_path, {"0002_second.sql": SECOND})
    assert [entry["file"] for entry in migrate(empty_db, files)] == ["0002_second.sql"]


def test_two_runners_at_the_same_time_apply_each_file_once(empty_db: str, tmp_path: Path) -> None:
    slow = "SELECT pg_sleep(0.4);\n" + FIRST
    files = write_files(tmp_path, {"0001_first.sql": slow, "0002_second.sql": SECOND})

    with ThreadPoolExecutor(max_workers=2) as pool:
        runs = [pool.submit(migrate, empty_db, files) for _ in range(2)]
        applied = sorted(len(run.result()) for run in runs)

    assert applied == [0, 2]  # one runner did the work, the other one waited and found nothing
    assert recorded(empty_db) == [(1, "first"), (2, "second")]


def test_an_applied_migration_without_a_file_is_refused(empty_db: str, tmp_path: Path) -> None:
    files = write_files(tmp_path, {"0001_first.sql": FIRST, "0002_second.sql": SECOND})
    migrate(empty_db, files)
    (files / "0002_second.sql").unlink()

    with pytest.raises(MigrationError, match=r"0002_second, and the directory has no such file"):
        migrate(empty_db, files)


def test_a_gap_in_the_applied_migrations_is_refused(empty_db: str, tmp_path: Path) -> None:
    files = write_files(tmp_path, {"0001_first.sql": FIRST, "0002_second.sql": SECOND})
    migrate(empty_db, files)
    with connect(empty_db, autocommit=True) as connection:
        connection.execute("DELETE FROM ops.schema_migration WHERE version = 1")

    with pytest.raises(MigrationError, match=r"0001_first\.sql is not applied"):
        migrate(empty_db, files)


@pytest.mark.parametrize(
    ("files", "message"),
    [
        ({"1_first.sql": FIRST}, "is named NNNN_name.sql"),
        ({"0001_First.sql": FIRST}, "is named NNNN_name.sql"),
        ({"0001_first.sql": FIRST, "0003_third.sql": SECOND}, "without a gap"),
        ({"0002_second.sql": SECOND}, "without a gap"),
        ({"0001_first.sql": "BEGIN;\n" + FIRST + "COMMIT;\n"}, "holds no 'BEGIN;'"),
        ({"0001_first.sql": FIRST + "commit;\n"}, "holds no 'COMMIT'"),
        ({"0001_first.sql": "START TRANSACTION;\n" + FIRST}, "holds no 'START TRANSACTION'"),
    ],
)
def test_discover_refuses_a_file_which_breaks_a_rule(
    tmp_path: Path, files: dict[str, str], message: str
) -> None:
    with pytest.raises(MigrationError, match=message):
        discover(write_files(tmp_path, files))


def test_discover_refuses_a_missing_directory(tmp_path: Path) -> None:
    with pytest.raises(MigrationError, match="no migration directory"):
        discover(tmp_path / "nowhere")


def test_a_plpgsql_block_is_not_a_transaction_statement(empty_db: str, tmp_path: Path) -> None:
    function = (
        "CREATE FUNCTION ops.one() RETURNS integer LANGUAGE plpgsql AS $$\n"
        "BEGIN\n    RETURN 1;\nEND\n$$;\n"
    )
    files = write_files(tmp_path, {"0001_function.sql": function, "notes.txt": "not a migration"})

    assert [entry["file"] for entry in migrate(empty_db, files)] == ["0001_function.sql"]


def test_status_reports_each_state_and_changes_nothing(empty_db: str, tmp_path: Path) -> None:
    files = write_files(tmp_path, {"0001_first.sql": FIRST, "0002_second.sql": SECOND})

    before = status(empty_db, files)
    assert [(entry["file"], entry["state"]) for entry in before] == [
        ("0001_first.sql", "pending"),
        ("0002_second.sql", "pending"),
    ]
    assert tables(empty_db) == set()  # status did not create the bookkeeping table

    migrate(empty_db, files)
    write_files(tmp_path, {"0002_second.sql": SECOND + "-- edited\n", "0003_third.sql": FIRST})
    after = status(empty_db, files)
    assert [(entry["file"], entry["state"]) for entry in after] == [
        ("0001_first.sql", "applied"),
        ("0002_second.sql", "changed"),
        ("0003_third.sql", "pending"),
    ]
    assert after[0]["applied_at"].endswith("+00:00")

    (files / "0002_second.sql").unlink()
    (files / "0003_third.sql").unlink()
    assert [(entry["file"], entry["state"]) for entry in status(empty_db, files)] == [
        ("0001_first.sql", "applied"),
        ("0002_second.sql", "file_missing"),
    ]


def test_the_commands_migrate_and_report(
    empty_db: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    files = write_files(tmp_path, {"0001_first.sql": FIRST})
    monkeypatch.setenv("GBMI_PG_DSN", empty_db)
    monkeypatch.setenv("GBMI_MIGRATIONS_DIR", str(files))
    get_settings.cache_clear()
    runner = CliRunner()
    try:
        migrated = runner.invoke(app, ["db", "migrate"])
        assert migrated.exit_code == 0, migrated.output
        assert [entry["file"] for entry in json.loads(migrated.stdout)["applied"]] == [
            "0001_first.sql"
        ]

        reported = runner.invoke(app, ["db", "status", "--dir", str(files)])
        assert reported.exit_code == 0, reported.output
        assert [(entry["file"], entry["state"]) for entry in json.loads(reported.stdout)] == [
            ("0001_first.sql", "applied")
        ]

        write_files(tmp_path, {"0001_first.sql": FIRST + "-- edited\n"})
        refused = runner.invoke(app, ["db", "migrate"])
        assert refused.exit_code == 1
        assert "changed after it was applied" in refused.output

        (files / "0001_first.sql").rename(files / "first.sql")
        assert runner.invoke(app, ["db", "status"]).exit_code == 1

        # Without a connection string both commands stop with one line, not with a traceback.
        (files / "first.sql").rename(files / "0001_first.sql")
        monkeypatch.delenv("GBMI_PG_DSN")
        get_settings.cache_clear()
        for command in ("migrate", "status"):
            unconfigured = runner.invoke(app, ["db", command])
            assert unconfigured.exit_code == 1
            assert "GBMI_PG_DSN is not set" in unconfigured.output
    finally:
        get_settings.cache_clear()
