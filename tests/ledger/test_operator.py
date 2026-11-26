"""The operator decisions and the commands of ``gbmi ledger`` (week 2 plan, WP-03 step 5)."""

from __future__ import annotations

import json
from datetime import date

import pytest
from typer.testing import CliRunner

from gbmi.cli import app
from gbmi.config import get_settings
from gbmi.core.gdelt import Feed
from gbmi.ledger import operator
from gbmi.ledger.db import Connection
from gbmi.ledger.repository import SOURCE, Ledger
from gbmi.ledger.states import State

from .helpers import T0, key, listed, verified

DAY = date(2026, 11, 25)


def prepared(ledger: Ledger) -> None:
    ledger.ensure_expected([key(0), key(0, Feed.GKG)])
    listed(ledger, key(15))
    listed(ledger, key(30), key(30, Feed.GKG))
    ledger.quarantine(key(30), from_state=State.LISTED, reason="gate G1 failed three times")
    ledger.quarantine(key(30, Feed.GKG), from_state=State.LISTED, reason="gate G1 failed")
    verified(ledger, key(45))


def test_an_operator_confirms_only_the_named_states(ledger: Ledger, ops: Connection) -> None:
    prepared(ledger)

    confirmed = operator.confirm_missing(
        ledger,
        first_day=DAY,
        last_day=DAY,
        states=[State.QUARANTINED, State.EXPECTED],
        feeds=[Feed.EXPORT],
        decided_by=" jay ",
        note=" GDELT confirmed the outage ",
    )

    assert confirmed == {"expected": 1, "quarantined": 1}
    states = {record.key: record.state for record in ledger.find()}
    assert states[key(0)] is states[key(30)] is State.CONFIRMED_MISSING
    assert states[key(15)] is State.LISTED  # listed was not named
    assert states[key(0, Feed.GKG)] is State.EXPECTED  # another feed
    assert states[key(45)] is State.VERIFIED
    record = ledger.get(key(30))
    assert record is not None
    assert (record.decided_by, record.decision_note) == ("jay", "GDELT confirmed the outage")
    row = ops.execute(
        "SELECT decided_by, reason FROM ops.slot_transition WHERE source = %s "
        "AND to_state = 'confirmed_missing' AND from_state = 'quarantined'",
        (ledger.source,),
    ).fetchone()
    assert row == ("jay", "GDELT confirmed the outage")


def test_a_range_outside_the_slots_changes_nothing(ledger: Ledger) -> None:
    prepared(ledger)

    confirmed = operator.confirm_missing(
        ledger,
        first_day=date(2026, 11, 26),
        last_day=date(2026, 11, 27),
        states=[State.EXPECTED],
        decided_by="jay",
        note="n",
    )

    assert confirmed == {"expected": 0}
    assert (
        operator.requeue(
            ledger,
            first_day=date(2026, 11, 26),
            last_day=date(2026, 11, 27),
            decided_by="jay",
            note="n",
        )
        == 0
    )


def test_an_operator_requeues_the_quarantined_slots_of_a_range(ledger: Ledger) -> None:
    prepared(ledger)

    assert (
        operator.requeue(
            ledger,
            first_day=DAY,
            last_day=DAY,
            feeds=[Feed.GKG],
            decided_by="jay",
            note="good file",
        )
        == 1
    )

    assert ledger.get(key(30, Feed.GKG)).state is State.LISTED  # type: ignore[union-attr]
    assert ledger.get(key(30)).state is State.QUARANTINED  # type: ignore[union-attr]
    assert operator.requeue(ledger, first_day=DAY, last_day=DAY, decided_by="jay", note="n") == 1


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        ({"note": "  "}, "a note is required"),
        ({"decided_by": ""}, "name of the operator"),
        ({"decided_by": "auto"}, "cannot be 'auto'"),
        ({"states": []}, "name one or more of the states"),
        ({"states": [State.VERIFIED]}, r"got \['verified'\]"),
        ({"states": [State.EXPECTED, State.LOADED]}, r"got \['loaded'\]"),
    ],
)
def test_a_decision_needs_a_note_a_name_and_states_which_can_be_confirmed(
    ledger: Ledger, arguments: dict[str, object], message: str
) -> None:
    prepared(ledger)
    values: dict[str, object] = {
        "first_day": DAY,
        "last_day": DAY,
        "states": [State.EXPECTED],
        "decided_by": "jay",
        "note": "n",
        **arguments,
    }

    with pytest.raises(ValueError, match=message):
        operator.confirm_missing(ledger, **values)  # type: ignore[arg-type]

    assert ledger.get(key(0)).state is State.EXPECTED  # type: ignore[union-attr]


def test_the_commands_of_gbmi_ledger(empty_db: str, monkeypatch: pytest.MonkeyPatch) -> None:
    # The commands work on the source gdelt, so the test takes a database of its own.
    from gbmi.ledger.migrate import migrate

    from .conftest import MIGRATIONS

    migrate(empty_db, MIGRATIONS)
    monkeypatch.setenv("GBMI_PG_DSN", empty_db)
    monkeypatch.setenv("GBMI_OPERATOR", "jay")
    get_settings.cache_clear()
    runner = CliRunner()

    def run(*arguments: str) -> tuple[int, object, str]:
        result = runner.invoke(app, ["ledger", *arguments])
        value = json.loads(result.stdout) if result.exit_code == 0 else None
        return result.exit_code, value, result.output

    try:
        from gbmi.ledger.repository import open_ledger

        with open_ledger() as ledger:
            assert ledger.source == SOURCE
            prepared(ledger)

        code, status, _ = run("status", "--from", "2026-11-25", "--to", "2026-11-25")
        assert code == 0
        assert {
            "slot_date": "2026-11-25",
            "feed": "export",
            "state": "verified",
            "slots": 1,
        } in status  # type: ignore[operator]

        code, completeness, _ = run("completeness", "--from", "2026-11-25", "--to", "2026-11-25")
        assert code == 0 and len(completeness) == 4  # type: ignore[arg-type]

        late = (T0.replace(hour=14, minute=10)).isoformat()
        code, overdue, _ = run("overdue", "--as-of", late)
        assert code == 0
        assert overdue == [
            {"slot": "20261125080000.export", "state": "expected", "attempts": 0},
            {"slot": "20261125080000.gkg", "state": "expected", "attempts": 0},
        ]
        assert run("overdue")[0] == 0  # the real clock

        day = ("--from", "2026-11-25", "--to", "2026-11-25")
        code, confirmed, _ = run(
            "confirm-missing",
            *day,
            "--state",
            "quarantined",
            "--feed",
            "export",
            "--note",
            "outage",
        )
        assert (code, confirmed) == (0, {"confirmed_missing": {"quarantined": 1}})

        code, requeued, _ = run("requeue", *day, "--note", "the file is good now")
        assert (code, requeued) == (0, {"requeued": 1})

        with open_ledger() as ledger:
            record = ledger.get(key(30))
            assert record is not None
            assert (record.state, record.decided_by) == (State.CONFIRMED_MISSING, "jay")
            assert ledger.get(key(30, Feed.GKG)).state is State.LISTED  # type: ignore[union-attr]

        # A wrong argument is one line and exit code 1, not a traceback.
        for arguments in (
            ("status", "--from", "25/11/2026", "--to", "2026-11-25"),
            ("completeness", "--from", "2026-11-25", "--to", "soon"),
            ("overdue", "--as-of", "2026-11-25T14:10:00"),
            ("confirm-missing", *day, "--state", "loaded", "--note", "n"),
            ("confirm-missing", *day, "--state", "expected", "--note", " "),
            ("confirm-missing", *day, "--state", "expected", "--feed", "tv", "--note", "n"),
            ("requeue", *day, "--note", ""),
            ("requeue", *day, "--note", "n", "--as-of", "noon"),
        ):
            code, _, output = run(*arguments)
            assert code == 1, arguments
            assert output.startswith(f"gbmi ledger {arguments[0]}: "), output
            assert "Traceback" not in output
    finally:
        get_settings.cache_clear()
