"""The daily reconciliation against the master list (week 2 plan, WP-04 step 4)."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest
import tenacity

from gbmi.core.gdelt import Feed, Slot
from gbmi.ledger.db import Connection
from gbmi.ledger.repository import Ledger, SlotKey
from gbmi.ledger.states import State
from gbmi.ops.runs import recorded_run
from gbmi.sources.gdelt import ingest
from gbmi.sources.gdelt import reconcile as reconciliation
from gbmi.sources.gdelt.land import Bronze, HttpFetcher

from .gdelt_server import GdeltServer
from .helpers import T0, key

RUN_0753 = T0 - timedelta(minutes=7)
HOUR = timedelta(hours=1)


@pytest.fixture(autouse=True)
def no_waiting(monkeypatch: pytest.MonkeyPatch) -> None:
    for method in (HttpFetcher._get, HttpFetcher.last_update, HttpFetcher.master_tail):
        monkeypatch.setattr(method.retry, "wait", tenacity.wait_none())  # type: ignore[attr-defined]


@pytest.fixture
def server() -> GdeltServer:
    return GdeltServer()


@pytest.fixture
def lake(tmp_path: Path) -> str:
    return f"file://{tmp_path}"


def slot(minutes: int) -> Slot:
    return key(minutes).slot


def run_ingest(
    ledger: Ledger, server: GdeltServer, lake: str, as_of: datetime
) -> dict[str, object]:
    return ingest.run_ingest(
        as_of=as_of, fetcher=server.fetcher(), ledger=ledger, bronze=Bronze(lake), lake_uri=lake
    )


def reconcile(ledger: Ledger, server: GdeltServer, lake: str, as_of: datetime) -> dict[str, object]:
    return reconciliation.reconcile(
        as_of=as_of, fetcher=server.fetcher(), ledger=ledger, bronze=Bronze(lake)
    )


CHANGES = (
    "listed",
    "absent",
    "listing_changed",
    "reopened",
    "confirmed_missing_absent",
    "confirmed_missing_unavailable",
)


def changes(summary: dict[str, object]) -> dict[str, object]:
    return {name: summary[name] for name in CHANGES if summary[name]}


def test_a_file_which_is_not_served_for_48_hours_is_confirmed_missing_by_the_rule(
    ledger: Ledger, server: GdeltServer, lake: str
) -> None:
    keys = server.publish_slot(slot(0))
    missing = keys[1]
    server.always[missing.slot.file_name(missing.feed)] = 404
    run_ingest(ledger, server, lake, RUN_0753)  # the first HTTP 404: 07:53

    early = reconcile(ledger, server, lake, RUN_0753 + 48 * HOUR - timedelta(minutes=1))
    assert changes(early) == {}
    assert ledger.get(missing).state is State.LISTED  # type: ignore[union-attr]

    summary = reconcile(ledger, server, lake, RUN_0753 + 48 * HOUR)

    assert changes(summary) == {"confirmed_missing_unavailable": 1}
    record = ledger.get(missing)
    assert record is not None
    assert (record.state, record.decided_by) == (State.CONFIRMED_MISSING, "auto")
    assert record.decision_note is not None and "48 hours" in record.decision_note
    history = ledger.transitions(missing)
    assert history[-1] == (State.LISTED, State.CONFIRMED_MISSING)

    # A second reconciliation after the confirmation changes nothing: the list still names
    # the file with the same size and MD5, so the slot stays confirmed.
    again = reconcile(ledger, server, lake, RUN_0753 + 49 * HOUR)
    assert changes(again) == {}
    assert ledger.transitions(missing) == history


def test_a_slot_which_the_list_misses_twice_is_confirmed_after_48_hours(
    ledger: Ledger, server: GdeltServer, lake: str
) -> None:
    keys = [SlotKey(feed, slot(0)) for feed in Feed]
    ledger.ensure_expected(keys)
    server.publish(keys[0], keys[1])  # the source never lists the GKG file of the slot
    server.publish_slot(slot(15))  # the list goes on behind the slot
    absent = keys[2]

    first = reconcile(ledger, server, lake, T0 + HOUR)
    assert changes(first) == {"listed": 2, "absent": 1}
    second = reconcile(ledger, server, lake, T0 + 25 * HOUR)
    # Two reconciliations missed the slot, and the slot is only 25 hours old: not yet.
    assert changes(second) == {"absent": 1}
    record = ledger.get(absent)
    assert record is not None and (record.state, record.absent_count) == (State.EXPECTED, 2)

    third = reconcile(ledger, server, lake, T0 + 49 * HOUR)

    assert changes(third) == {"absent": 1, "confirmed_missing_absent": 1}
    record = ledger.get(absent)
    assert record is not None
    assert (record.state, record.decided_by, record.url) == (State.CONFIRMED_MISSING, "auto", None)
    assert changes(reconcile(ledger, server, lake, T0 + 50 * HOUR)) == {}


def test_one_reconciliation_is_not_enough_however_old_the_slot_is(
    ledger: Ledger, server: GdeltServer, lake: str
) -> None:
    ledger.ensure_expected([key(0)])
    server.publish_slot(slot(15))

    summary = reconcile(ledger, server, lake, T0 + 72 * HOUR)

    assert changes(summary) == {"absent": 1}
    assert ledger.get(key(0)).state is State.EXPECTED  # type: ignore[union-attr]


def test_a_slot_which_the_list_has_not_reached_is_not_absent(
    ledger: Ledger, server: GdeltServer, lake: str
) -> None:
    server.publish_slot(slot(0))
    ledger.ensure_expected([key(0), key(15), key(30)])

    summary = reconcile(ledger, server, lake, T0 + timedelta(minutes=23))

    # 08:15 and 08:30 are newer than the newest slot of the list.
    assert changes(summary) == {"listed": 1}
    assert {record.key: record.absent_count for record in ledger.find()} == {
        key(0): 0,
        key(15): 0,
        key(30): 0,
    }


def test_a_slot_confirmed_without_a_listing_is_reopened_when_the_file_appears(
    ledger: Ledger, server: GdeltServer, lake: str
) -> None:
    ledger.ensure_expected([key(0)])
    ledger.confirm_missing([key(0)], from_state=State.EXPECTED, decided_by="auto", note="absent")
    server.publish_slot(slot(15))
    assert changes(reconcile(ledger, server, lake, T0 + HOUR)) == {}

    server.publish(key(0))  # a late file: the line arrives at the end of the master list

    summary = reconcile(ledger, server, lake, T0 + 2 * HOUR)

    assert changes(summary) == {"reopened": 1}
    record = ledger.get(key(0))
    assert record is not None
    assert (record.state, record.decided_by, record.decision_note) == (State.LISTED, None, None)
    # The next ingest run fetches the late file, beside the newer files which the run finds.
    run_ingest(ledger, server, lake, T0 + 2 * HOUR)
    assert ledger.transitions(key(0)) == [
        (State.EXPECTED, State.CONFIRMED_MISSING),
        (State.CONFIRMED_MISSING, State.LISTED),
        (State.LISTED, State.DOWNLOADED),
        (State.DOWNLOADED, State.VERIFIED),
    ]


def test_a_slot_confirmed_after_a_listing_is_reopened_only_by_new_facts(
    ledger: Ledger, server: GdeltServer, lake: str
) -> None:
    keys = server.publish_slot(slot(0))
    unavailable, quarantined = keys[0], keys[1]
    ledger.ensure_expected(keys)
    ledger.mark_listed(ingest.listings(_lines(server)))
    ledger.confirm_missing(
        [unavailable], from_state=State.LISTED, decided_by="auto", note="not served"
    )
    ledger.quarantine(quarantined, from_state=State.LISTED, reason="gate G1 failed three times")
    ledger.confirm_missing(
        [quarantined], from_state=State.QUARANTINED, decided_by="jay", note="bad file"
    )

    # The list still names both files with the same size and MD5: both stay confirmed.
    assert changes(reconcile(ledger, server, lake, T0 + HOUR)) == {}

    server.relist(unavailable, "the source published the file again")

    summary = reconcile(ledger, server, lake, T0 + 2 * HOUR)

    assert changes(summary) == {"reopened": 1}
    record = ledger.get(unavailable)
    assert record is not None and record.state is State.LISTED
    name = unavailable.slot.file_name(unavailable.feed)
    assert record.expected_bytes == len(server.files[name])
    assert ledger.get(quarantined).state is State.CONFIRMED_MISSING  # type: ignore[union-attr]


def _lines(server: GdeltServer):  # type: ignore[no-untyped-def]
    from gbmi.sources.gdelt.index import parse_index

    return parse_index("\n".join(server.master))


def test_a_listed_slot_takes_a_changed_size_and_md5_from_the_list(
    ledger: Ledger, server: GdeltServer, lake: str
) -> None:
    keys = server.publish_slot(slot(0))
    ledger.ensure_expected(keys)
    ledger.mark_listed(ingest.listings(_lines(server)))
    before = ledger.get(keys[0])
    server.relist(keys[0], "replaced before the download")

    summary = reconcile(ledger, server, lake, T0 + HOUR)

    assert changes(summary) == {"listing_changed": 1}
    after = ledger.get(keys[0])
    assert before is not None and after is not None
    assert after.state is State.LISTED and after.expected_md5 != before.expected_md5
    assert run_ingest(ledger, server, lake, T0 + HOUR)["verified"] == 3  # the new bytes pass G1


def test_differences_between_bronze_and_the_ledger_are_reported(
    ledger: Ledger, server: GdeltServer, lake: str, tmp_path: Path, ops: Connection
) -> None:
    keys = server.publish_slot(slot(0))
    run_ingest(ledger, server, lake, RUN_0753)

    clean = reconcile(ledger, server, lake, T0 + HOUR)
    assert (clean["bronze_differences"], clean["bronze_difference_examples"]) == (0, [])

    (tmp_path / keys[0].slot.bronze_key(keys[0].feed)).unlink()
    (tmp_path / keys[2].slot.bronze_key(keys[2].feed)).write_bytes(b"cut")

    summary = reconcile(ledger, server, lake, T0 + 2 * HOUR)

    assert summary["bronze_differences"] == 2
    examples = {entry["slot"]: entry for entry in summary["bronze_difference_examples"]}  # type: ignore[union-attr]
    assert examples["20261125080000.export"]["difference"] == "missing in bronze"
    assert examples["20261125080000.gkg"]["difference"] == "size differs"
    assert examples["20261125080000.gkg"]["bronze_bytes"] == 3
    rows = ops.execute(
        "SELECT status, observed_value FROM ops.dq_result WHERE source = %s AND gate = 'G6' "
        "AND check_name = 'ledger_against_bronze' ORDER BY dq_result_id",
        (ledger.source,),
    ).fetchall()
    assert rows == [("passed", 0.0), ("failed", 2.0)]


def test_overdue_slots_are_reported_with_one_result_for_each_run(
    ledger: Ledger, ops: Connection
) -> None:
    ledger.ensure_expected([key(0), key(15)])

    on_time = reconciliation.report_overdue(as_of=T0 + 5 * HOUR, ledger=ledger)
    late = reconciliation.report_overdue(as_of=T0 + 6 * HOUR + timedelta(minutes=5), ledger=ledger)

    assert on_time == {"as_of": (T0 + 5 * HOUR).isoformat(), "overdue": 0, "slots": []}
    assert (late["overdue"], late["slots"]) == (1, ["20261125080000.export"])
    rows = ops.execute(
        "SELECT status, observed_value, unit FROM ops.dq_result WHERE source = %s "
        "AND check_name = 'overdue_slots' ORDER BY dq_result_id",
        (ledger.source,),
    ).fetchall()
    assert rows == [("passed", 0.0, "20261125130000"), ("failed", 1.0, "20261125140000")]


def test_the_reconciliation_is_due_once_in_each_utc_day_from_0800(
    ledger: Ledger, ops: Connection
) -> None:
    name = reconciliation.entry_point(ledger)
    day = T0.replace(hour=0)

    def is_due(moment: datetime) -> bool:
        return reconciliation.is_due(ops, name, moment)

    assert not is_due(day + timedelta(hours=7, minutes=59))
    assert is_due(day + timedelta(hours=8))

    # A run which was not due, and a run which failed, are no reconciliation.
    with recorded_run(ops, name, as_of=day + timedelta(hours=8, minutes=8)) as run:
        run.detail = {"reconciled": False}
    with pytest.raises(RuntimeError), recorded_run(ops, name, as_of=day + 9 * HOUR) as run:
        run.detail = {"reconciled": True}
        raise RuntimeError("the master list did not answer")
    assert is_due(day + timedelta(hours=9, minutes=8))

    with recorded_run(ops, name, as_of=day + timedelta(hours=9, minutes=8)) as run:
        run.detail = {"reconciled": True}

    assert not is_due(day + timedelta(hours=9, minutes=23))
    assert not is_due(day + timedelta(hours=23, minutes=53))
    assert not is_due(day + timedelta(days=1, hours=7))
    assert is_due(day + timedelta(days=1, hours=8, minutes=8))  # the next UTC day
