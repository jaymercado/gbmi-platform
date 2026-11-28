"""The operator commands for the circuit of the SEC (review 1 of week 3, finding B2).

The error text of a locked circuit names ``gbmi sec circuit close --note``: the command has to
exist before the first live request.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest
from typer.testing import CliRunner

from gbmi.cli import app
from gbmi.ledger.db import connect
from gbmi.ops.circuit import lock_circuit, open_circuit, read_circuit
from gbmi.sources.sec.client import SOURCE

runner = CliRunner()
NOW = datetime(2026, 11, 28, 8, 30, tzinfo=UTC)
UNTIL = NOW + timedelta(minutes=15)


def run(*arguments: str) -> tuple[int, dict[str, object]]:
    result = runner.invoke(app, ["sec", "circuit", *arguments])
    return result.exit_code, json.loads(result.stdout) if result.exit_code == 0 else {}


def at(moment: datetime) -> list[str]:
    return ["--as-of", moment.isoformat()]


def test_the_status_names_the_four_states_of_the_circuit(stack: str) -> None:
    assert run("status", *at(NOW)) == (
        0,
        {
            "source": "sec",
            "state": "closed",
            "blocked_until": None,
            "opened_at": None,
            "reason": None,
            "http_status": None,
            "open_count": 0,
            "locked": False,
            "events": [],
        },
    )
    with connect(stack, autocommit=True) as connection:
        open_circuit(connection, SOURCE, until=UNTIL, reason="HTTP 429", http_status=429, now=NOW)

    code, during = run("status", *at(NOW + timedelta(minutes=5)))
    _, after = run("status", *at(UNTIL + timedelta(seconds=1)))

    assert code == 0
    assert (during["state"], during["reason"], during["open_count"]) == ("open", "HTTP 429", 1)
    assert str(during["blocked_until"]).startswith("2026-11-28 08:45:00")
    assert after["state"] == "probe"  # the block has ended: the next request is the probe
    assert [event["event"] for event in during["events"]] == ["opened"]  # type: ignore[union-attr]

    with connect(stack, autocommit=True) as connection:
        lock_circuit(connection, SOURCE, reason="probe refused", http_status=403, now=UNTIL)
    assert run("status", *at(UNTIL + timedelta(days=30)))[1]["state"] == "locked"


def test_an_operator_closes_a_locked_circuit_with_a_note(stack: str) -> None:
    with connect(stack, autocommit=True) as connection:
        open_circuit(connection, SOURCE, until=UNTIL, reason="HTTP 429", http_status=429, now=NOW)
        lock_circuit(connection, SOURCE, reason="probe refused", http_status=403, now=UNTIL)

    code, closed = run(
        "close", "--note", " the SEC lifted the block ", "--decided-by", "jay", *at(UNTIL)
    )

    assert code == 0
    assert (closed["closed"], closed["was_locked"], closed["closed_by"]) == (True, True, "jay")
    with connect(stack, autocommit=True) as connection:
        row = read_circuit(connection, SOURCE)
        stored = connection.execute(
            "SELECT closed_by, close_note FROM ops.source_circuit WHERE source = %s", (SOURCE,)
        ).fetchone()
    assert not row.recorded and not row.locked and row.open_count == 1
    assert stored == ("jay", "the SEC lifted the block")
    _, status = run("status", *at(UNTIL))
    assert status["state"] == "closed"
    assert [event["event"] for event in status["events"]] == [  # type: ignore[union-attr]
        "closed",
        "locked",
        "opened",
    ]
    # A second close finds nothing to close and says so.
    assert run("close", "--note", "again", *at(UNTIL))[1]["closed"] is False


@pytest.mark.parametrize("note", ["", "   "])
def test_a_close_without_a_reason_is_refused(stack: str, note: str) -> None:
    with connect(stack, autocommit=True) as connection:
        open_circuit(connection, SOURCE, until=UNTIL, reason="HTTP 429", http_status=429, now=NOW)

    result = runner.invoke(app, ["sec", "circuit", "close", "--note", note])

    assert result.exit_code == 1
    assert "the note is empty" in result.output
    with connect(stack, autocommit=True) as connection:
        assert read_circuit(connection, SOURCE).recorded


def test_the_commands_say_when_no_database_is_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GBMI_PG_DSN", raising=False)

    for arguments in (["status"], ["close", "--note", "x"]):
        result = runner.invoke(app, ["sec", "circuit", *arguments])
        assert result.exit_code == 1
        assert "GBMI_PG_DSN is not set" in result.output
