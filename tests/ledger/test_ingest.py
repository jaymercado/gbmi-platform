"""Pipeline PL-1 against a GDELT server in memory (week 2 plan, WP-04 step 6)."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import pytest
import tenacity

from gbmi.core.gdelt import Feed, Slot
from gbmi.ledger.db import Connection
from gbmi.ledger.repository import Ledger, SlotKey
from gbmi.ledger.states import State
from gbmi.sources.gdelt import ingest
from gbmi.sources.gdelt.index import parse_index
from gbmi.sources.gdelt.land import Bronze, HttpFetcher, land_file

from .gdelt_server import DAMAGED, UPLOAD_LEAD, GdeltServer
from .helpers import T0, key

RUN_0753 = T0 - timedelta(minutes=7)  # the run which finds the files named 08:00


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


def run(
    ledger: Ledger, server: GdeltServer, lake: str, as_of: datetime, **arguments: object
) -> dict[str, object]:
    return ingest.run_ingest(
        as_of=as_of,
        fetcher=server.fetcher(),
        ledger=ledger,
        bronze=Bronze(lake),
        lake_uri=lake,
        **arguments,  # type: ignore[arg-type]
    )


def slot(minutes: int) -> Slot:
    return key(minutes).slot


def states(ledger: Ledger) -> dict[SlotKey, State]:
    return {record.key: record.state for record in ledger.find()}


def quality_rows(ops: Connection, ledger: Ledger) -> list[tuple[object, ...]]:
    return ops.execute(
        "SELECT gate, check_name, status, feed, unit, detail ->> 'attempt' FROM ops.dq_result "
        "WHERE source = %s ORDER BY dq_result_id",
        (ledger.source,),
    ).fetchall()


def test_an_empty_ledger_starts_at_the_newest_slot_and_lands_its_files(
    ledger: Ledger, server: GdeltServer, lake: str, tmp_path: Path, ops: Connection
) -> None:
    keys = server.publish_slot(slot(0))

    summary = run(ledger, server, lake, RUN_0753)

    assert summary["newest_slot"] == "20261125080000"
    assert (summary["expected_created"], summary["listed"], summary["selected"]) == (3, 3, 3)
    assert (summary["verified"], summary["written"], summary["already_in_bronze"]) == (3, 3, 0)
    assert summary["bytes"] == sum(len(content) for content in server.files.values())
    assert not summary["after_a_gap"] and summary["still_listed"] == 0
    # A normal run costs one request for lastupdate.txt and none for the master list.
    assert (server.asked("lastupdate.txt"), server.asked("masterfilelist.txt")) == (1, 0)
    for slot_key in keys:
        record = ledger.get(slot_key)
        assert record is not None
        name = slot_key.slot.file_name(slot_key.feed)
        bronze_key = slot_key.slot.bronze_key(slot_key.feed)
        assert record.state is State.VERIFIED
        assert record.bronze_uri == f"{lake}/{bronze_key}"
        assert record.published_at == slot_key.slot.start - UPLOAD_LEAD  # the header Last-Modified
        assert record.listing.url == f"https://data.gdeltproject.org/gdeltv2/{name}"
        assert (tmp_path / bronze_key).read_bytes() == server.files[name]
    # Gate G1 leaves one passed result for each file.
    assert sorted(row[:5] for row in quality_rows(ops, ledger)) == [
        ("G1", "landing", "passed", feed, "20261125080000")
        for feed in ("export", "gkg", "mentions")
    ]


def test_the_next_run_takes_the_next_slot_and_a_rerun_changes_nothing(
    ledger: Ledger, server: GdeltServer, lake: str
) -> None:
    server.publish_slot(slot(0))
    run(ledger, server, lake, RUN_0753)
    server.publish_slot(slot(15))

    second = run(ledger, server, lake, RUN_0753 + timedelta(minutes=15))

    assert (second["expected_created"], second["listed"], second["verified"]) == (3, 3, 3)
    assert not second["after_a_gap"]
    assert server.asked("masterfilelist.txt") == 0
    requests_before = len(server.file_requests())

    again = run(ledger, server, lake, RUN_0753 + timedelta(minutes=15))

    assert (again["expected_created"], again["listed"], again["selected"]) == (0, 0, 0)
    assert len(server.file_requests()) == requests_before
    assert set(states(ledger).values()) == {State.VERIFIED}


def test_a_line_for_a_later_slot_waits_for_a_later_run(
    ledger: Ledger, server: GdeltServer, lake: str
) -> None:
    # The run works as if the clock showed as_of: the source is already one hour further.
    server.publish_slot(slot(60))

    summary = run(ledger, server, lake, RUN_0753)

    assert (summary["expected_created"], summary["listed"], summary["selected"]) == (3, 0, 0)
    assert set(states(ledger)) == {SlotKey(feed, slot(0)) for feed in Feed}
    assert set(states(ledger).values()) == {State.EXPECTED}


def test_a_run_after_a_gap_reads_the_master_list_and_drains_oldest_first(
    ledger: Ledger, server: GdeltServer, lake: str
) -> None:
    server.publish_slot(slot(0))
    run(ledger, server, lake, RUN_0753)
    for minutes in range(15, 105, 15):  # 08:15 to 09:30: the laptop slept
        server.publish_slot(slot(minutes))

    after = run(ledger, server, lake, T0 + timedelta(minutes=98), batch_limit=10)

    # The calendar expects 08:15 to 09:45; lastupdate.txt shows 09:30; the master list gives
    # the five slots between.
    assert after["after_a_gap"] and server.asked("masterfilelist.txt") == 1
    assert (after["expected_created"], after["listed"]) == (21, 18)
    assert (after["selected"], after["verified"], after["still_listed"]) == (10, 10, 8)
    verified = {k for k, state in states(ledger).items() if state is State.VERIFIED}
    # The three files of 08:00 and the ten oldest files after them, counted by slot time.
    by_time = sorted(states(ledger), key=lambda k: (k.slot, k.feed.value))
    assert verified == set(by_time[:13])

    rest = run(ledger, server, lake, T0 + timedelta(minutes=98), batch_limit=10)

    assert (rest["selected"], rest["verified"], rest["still_listed"]) == (8, 8, 0)
    assert not rest["after_a_gap"]
    waiting = {k for k, state in states(ledger).items() if state is State.EXPECTED}
    assert waiting == {SlotKey(feed, slot(105)) for feed in Feed}  # 09:45 is not published yet


def test_a_ledger_which_was_never_listed_reads_the_master_list(
    ledger: Ledger, server: GdeltServer, lake: str
) -> None:
    ledger.ensure_expected([SlotKey(feed, slot(0)) for feed in Feed])
    server.publish_slot(slot(0), newest=False)
    server.publish_slot(slot(15))

    summary = run(ledger, server, lake, RUN_0753 + timedelta(minutes=15))

    assert summary["after_a_gap"] and summary["verified"] == 6


def test_a_wrong_md5_three_times_quarantines_the_file_and_keeps_the_bytes(
    ledger: Ledger, server: GdeltServer, lake: str, tmp_path: Path, ops: Connection
) -> None:
    keys = server.publish_slot(slot(0))
    bad = keys[2]
    name = bad.slot.file_name(bad.feed)
    server.always[name] = DAMAGED

    summary = run(ledger, server, lake, RUN_0753)

    assert (summary["verified"], summary["rejected"], summary["quarantined"]) == (2, 3, 1)
    assert server.asked(name) == 3
    record = ledger.get(bad)
    assert record is not None
    assert (record.state, record.attempts) == (State.QUARANTINED, 3)
    assert record.last_error is not None and record.last_error.startswith("gate G1 failed 3 times")
    assert ledger.transitions(bad) == [
        (State.EXPECTED, State.LISTED),
        *[(State.LISTED, State.DOWNLOADED), (State.DOWNLOADED, State.LISTED)] * 3,
        (State.LISTED, State.QUARANTINED),
    ]
    # The damaged bytes never reach bronze; they are kept in quarantine.
    assert not (tmp_path / bad.slot.bronze_key(bad.feed)).exists()
    kept = tmp_path / "quarantine" / ledger.source / "gkg" / "dt=2026-11-25" / name
    assert kept.read_bytes() == server.files[name][:-4]
    failed = [row for row in quality_rows(ops, ledger) if row[2] == "failed"]
    assert [(row[0], row[1], row[3], row[4], row[5]) for row in failed] == [
        ("G1", "landing", "gkg", "20261125080000", str(attempt)) for attempt in (1, 2, 3)
    ]
    # The next run leaves the quarantined slot alone.
    assert run(ledger, server, lake, RUN_0753)["selected"] == 0
    assert server.asked(name) == 3


def test_a_file_which_is_damaged_once_is_fetched_again_in_the_same_run(
    ledger: Ledger, server: GdeltServer, lake: str, tmp_path: Path
) -> None:
    keys = server.publish_slot(slot(0))
    name = keys[0].slot.file_name(keys[0].feed)
    server.once[name] = [DAMAGED]

    summary = run(ledger, server, lake, RUN_0753)

    assert (summary["verified"], summary["rejected"], summary["quarantined"]) == (3, 1, 0)
    record = ledger.get(keys[0])
    assert record is not None
    assert (record.state, record.attempts, record.last_error) == (State.VERIFIED, 1, None)
    assert server.asked(name) == 2
    assert not (tmp_path / "quarantine").exists()


def test_a_file_which_the_source_does_not_serve_stays_listed(
    ledger: Ledger, server: GdeltServer, lake: str
) -> None:
    keys = server.publish_slot(slot(0))
    missing = keys[1]
    server.always[missing.slot.file_name(missing.feed)] = 404

    first = run(ledger, server, lake, RUN_0753)
    second = run(ledger, server, lake, RUN_0753 + timedelta(minutes=15))

    assert (first["verified"], first["unavailable"]) == (2, 1)
    assert (second["selected"], second["unavailable"]) == (1, 1)
    record = ledger.get(missing)
    assert record is not None
    assert record.state is State.LISTED
    assert record.unavailable_since == RUN_0753  # the first HTTP 404, not the second
    assert record.last_error is not None and "HTTP 404" in record.last_error


def test_another_refusal_leaves_the_slot_listed_for_the_next_run(
    ledger: Ledger, server: GdeltServer, lake: str
) -> None:
    keys = server.publish_slot(slot(0))
    server.always[keys[0].slot.file_name(keys[0].feed)] = 403

    summary = run(ledger, server, lake, RUN_0753)

    assert (summary["verified"], summary["errors"], summary["still_listed"]) == (2, 1, 1)
    assert states(ledger)[keys[0]] is State.LISTED


def test_after_the_first_429_the_run_goes_on_one_file_at_a_time(
    ledger: Ledger, server: GdeltServer, lake: str
) -> None:
    keys = server.publish_slot(slot(0), newest=False) + server.publish_slot(slot(15))
    ledger.ensure_expected(keys)
    ledger.mark_listed(ingest.listings(parse_index("\n".join(server.master))))
    limited = keys[0]
    server.once[limited.slot.file_name(limited.feed)] = [(429, {"Retry-After": "7"})]
    sleeps: list[float] = []

    counts = ingest.download_listed(
        ledger.pending_downloads(),
        ledger=ledger,
        bronze=Bronze(lake),
        fetcher=server.fetcher(),
        lake_uri=lake,
        as_of=RUN_0753,
        concurrency=4,
        sleep=sleeps.append,
    )

    assert sleeps == [7.0]  # the header Retry-After is honoured once
    assert (counts["verified"], counts["rate_limited"]) == (6, 1)
    assert "left_for_the_next_run" not in counts
    assert set(states(ledger).values()) == {State.VERIFIED}


def test_a_source_which_keeps_answering_429_ends_the_run_inside_its_budget(
    ledger: Ledger, server: GdeltServer, lake: str
) -> None:
    keys = server.publish_slot(slot(0))
    for slot_key in keys:
        server.always[slot_key.slot.file_name(slot_key.feed)] = 429
    sleeps: list[float] = []

    summary = run(ledger, server, lake, RUN_0753, concurrency=1, sleep=sleeps.append)

    # Without the header the pause is 60 seconds; ten pauses use the budget of the run.
    assert sleeps == [60.0] * 10
    assert (summary["verified"], summary["rate_limited"]) == (0, 1)
    assert summary["left_for_the_next_run"] == 3
    assert set(states(ledger).values()) == {State.LISTED}  # the next run goes on


def test_a_rerun_after_a_crash_completes_the_change_without_a_download(
    ledger: Ledger, server: GdeltServer, lake: str
) -> None:
    keys = server.publish_slot(slot(0))
    # The run before wrote one file to bronze and stopped before the ledger knew.
    (line,) = parse_index(server.newest[0])
    land_file(Bronze(lake), server.fetcher(), line)
    name = line.file_name
    assert server.asked(name) == 1

    summary = run(ledger, server, lake, RUN_0753)

    assert (summary["verified"], summary["written"], summary["already_in_bronze"]) == (3, 2, 1)
    assert server.asked(name) == 1  # no second download
    record = ledger.get(keys[0])
    assert record is not None
    # Nothing was fetched in the run, so the run has no upload time for the file.
    assert (record.state, record.published_at) == (State.VERIFIED, None)


def test_a_slot_which_somebody_moved_during_the_run_is_left_alone(
    ledger: Ledger, server: GdeltServer, lake: str
) -> None:
    keys = server.publish_slot(slot(0))
    ledger.ensure_expected(keys)
    ledger.mark_listed(ingest.listings(parse_index("\n".join(server.master))))
    selected = ledger.pending_downloads()
    ledger.confirm_missing([keys[0]], from_state=State.LISTED, decided_by="jay", note="outage")

    counts = ingest.download_listed(
        selected,
        ledger=ledger,
        bronze=Bronze(lake),
        fetcher=server.fetcher(),
        lake_uri=lake,
        as_of=RUN_0753,
    )

    assert (counts["verified"], counts["stale"]) == (2, 1)
    assert states(ledger)[keys[0]] is State.CONFIRMED_MISSING


def test_enqueue_creates_the_days_and_lists_what_the_master_list_holds(
    ledger: Ledger, server: GdeltServer, lake: str
) -> None:
    day = date(2026, 11, 24)
    first = Slot(T0 - timedelta(days=1, hours=8))  # 2026-11-24 00:00
    published = [
        SlotKey(feed, Slot(first.start + timedelta(minutes=15 * i)))
        for i in range(4)
        for feed in Feed
    ]
    server.publish(*published[:-1])  # the source never listed the last file
    as_of = T0 + timedelta(hours=1)

    summary = ingest.enqueue(day, day, as_of=as_of, fetcher=server.fetcher(), ledger=ledger)

    assert summary == {
        "from": "2026-11-24",
        "to": "2026-11-24",
        "slots": 288,
        "created": 288,
        "in_the_master_list": 11,
        "listed": 11,
        "reserved_for": None,
    }
    again = ingest.enqueue(day, day, as_of=as_of, fetcher=server.fetcher(), ledger=ledger)
    assert (again["created"], again["listed"]) == (0, 0)

    drained = run(ledger, server, lake, as_of, batch_limit=1500)
    assert drained["verified"] == 11
    assert states(ledger)[published[-1]] is State.EXPECTED


def test_enqueue_claims_new_rows_and_never_runs_ahead_of_the_clock(
    ledger: Ledger, server: GdeltServer
) -> None:
    today = date(2026, 11, 25)
    as_of = T0 + timedelta(minutes=8)  # 08:08: the calendar reaches 08:15

    summary = ingest.enqueue(
        today,
        date(2026, 11, 27),
        as_of=as_of,
        fetcher=server.fetcher(),
        ledger=ledger,
        reserve="spark_backfill",
    )

    assert summary["slots"] == summary["created"] == 34 * 3  # 00:00 to 08:15
    assert ledger.newest_slot() == slot(15)
    assert {record.reserved_for for record in ledger.find()} == {"spark_backfill"}
    with pytest.raises(ValueError, match="ends before it starts"):
        ingest.enqueue(
            date(2026, 11, 26), today, as_of=as_of, fetcher=server.fetcher(), ledger=ledger
        )
