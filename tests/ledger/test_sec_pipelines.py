"""The three SEC pipelines on the recorded files (week 3 plan, WP-06 step 7).

The fixture ``sec_stack`` gives a database of its own, a lake in a temporary directory and a copy
of the recorded SEC files as the source; a test changes the copy to stage a case. The clock of
the first day is the clock of the demo: 2026-11-26 12:00 UTC, a Thursday, the day after the
recorded index file. No test opens a network connection.
"""

from __future__ import annotations

import functools
import json
import shutil
from collections import Counter
from collections.abc import Callable, Iterator
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from typer.testing import CliRunner

from gbmi.cli import app
from gbmi.config import Settings, get_settings
from gbmi.lake.catalog import load_lake_catalog
from gbmi.ledger.db import Connection, connect
from gbmi.ops.partitions import PartitionVersion, record_versions
from gbmi.ops.requests import max_requests_in_window
from gbmi.sources.sec import bulk, silver, tasks
from gbmi.sources.sec import filings as filings_module
from gbmi.sources.sec import reference as reference_module
from gbmi.sources.sec.client import SecClient
from gbmi.sources.sec.errors import SecTransportError
from gbmi.sources.sec.land import Lander
from gbmi.sources.sec.models import PayloadError, Submissions
from gbmi.sources.sec.silver import PipelineError
from gbmi.sources.sec.urls import (
    SUBMISSIONS_ARCHIVE_URL,
    TICKER_FILE_URL,
    companyfacts_url,
    index_file_url,
    index_listing_url,
    split,
    submissions_url,
)

from .sec_server import FakeTime

REPO = Path(__file__).resolve().parents[2]
THURSDAY = "2026-11-26T12:00:00+00:00"
FRIDAY = "2026-11-27T12:00:00+00:00"
SUNDAY = "2026-11-29T12:00:00+00:00"
MONDAY = "2026-11-30T12:00:00+00:00"
INDEX_DAY = date(2026, 11, 25)
META, NVIDIA, MICROSOFT, MATTEL, THOMSON_REUTERS = 1326801, 1045810, 789019, 63276, 1075124
EASYJET, ALPHABET, HASBRO, AMAZON, NETFLIX = 1470419, 1652044, 46080, 1018724, 1065280
LISTING = index_listing_url(2026, 4)
INDEX_FILE = index_file_url(INDEX_DAY)

#: The rows of each table after one run of the three pipelines on the recorded files.
TABLES = {
    "sec_filing_index": 298,
    "sec_filing_detail": 293,
    "sec_company_snapshot": 10,
    "sec_company_former_name": 4,
    "sec_xbrl_fact": 12264,
}

runner = CliRunner()


def fast_client() -> Any:
    """The client of the tasks with a clock which does not wait."""
    clock = FakeTime()
    return functools.partial(SecClient, clock=clock.clock, sleep=clock.sleep, now=clock.now)


@pytest.fixture
def recorded(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A copy of the recorded SEC files which a test may change."""
    target = tmp_path_factory.mktemp("recorded") / "sec"
    shutil.copytree(REPO / "tests" / "fixtures" / "sec", target)
    return target


@pytest.fixture
def sec_stack(stack: str, recorded: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """The stack with the recorded SEC files as the source, and a clock which does not wait."""
    monkeypatch.setenv("GBMI_SEC_SOURCE", "fixture")
    monkeypatch.setenv("GBMI_SEC_FIXTURES_DIR", str(recorded))
    monkeypatch.setenv("GBMI_WATCHLIST_FILE", str(REPO / "dbt" / "seeds" / "watchlist.csv"))
    monkeypatch.setenv("GBMI_WATCHLIST_SIZE", "5")
    get_settings.cache_clear()
    monkeypatch.setattr(tasks, "SecClient", fast_client())
    yield stack
    get_settings.cache_clear()


@pytest.fixture
def database(sec_stack: str) -> Iterator[Connection]:
    with connect(sec_stack, autocommit=True) as connection:
        yield connection


def path_of(recorded: Path, url: str) -> Path:
    host, path = split(url)
    return recorded / host / path.lstrip("/")


def edit(recorded: Path, url: str, change: Callable[[Any], None]) -> None:
    """Change one recorded JSON document of the copy."""
    target = path_of(recorded, url)
    document = json.loads(target.read_bytes())
    change(document)
    target.write_bytes(json.dumps(document).encode("utf-8"))


def states(database: Connection, dataset: str | None = None) -> dict[Any, int]:
    rows = database.execute(
        "SELECT dataset, state, count(*) FROM ops.source_object WHERE source = 'sec' "
        "GROUP BY dataset, state"
    ).fetchall()
    if dataset is not None:
        return {row[1]: row[2] for row in rows if row[0] == dataset}
    return {(row[0], row[1]): row[2] for row in rows}


def state_of(database: Connection, dataset: str, day: str, name: str) -> str | None:
    row = database.execute(
        "SELECT state FROM ops.source_object WHERE source = 'sec' AND dataset = %s "
        "AND object_date = %s AND object_name = %s",
        (dataset, day, name),
    ).fetchone()
    return row[0] if row else None


def requested(database: Connection) -> list[str]:
    rows = database.execute("SELECT path FROM ops.source_request ORDER BY request_id").fetchall()
    return [row[0] for row in rows]


def rows_of(table: str) -> list[dict[str, Any]]:
    settings = get_settings()
    catalog = load_lake_catalog(settings)
    loaded = catalog.load_table((settings.silver_namespace, table))
    rows: list[dict[str, Any]] = loaded.scan().to_arrow().to_pylist()
    return rows


def by_cik(table: str) -> dict[int, list[dict[str, Any]]]:
    found: dict[int, list[dict[str, Any]]] = {}
    for row in rows_of(table):
        found.setdefault(row["cik"], []).append(row)
    return found


def load_day(as_of: str = THURSDAY) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """The three pipelines in the order of the schedules."""
    return (
        tasks.filings_task(as_of=as_of),
        tasks.reference_task(as_of=as_of),
        tasks.facts_task(as_of=as_of),
    )


def sec(*arguments: str) -> tuple[int, Any, str]:
    result = runner.invoke(app, ["sec", *arguments])
    value = json.loads(result.stdout) if result.exit_code == 0 and result.stdout.strip() else None
    return result.exit_code, value, result.output


def requeue(dataset: str, day: str, note: str = "ok") -> tuple[int, Any]:
    """The command of the operator for the quarantined objects of one dataset and one day."""
    code, value, _ = sec(
        "objects", "requeue", "--dataset", dataset, "--from", day, "--to", day, "--note", note
    )
    return code, value


# --- the recorded files, end to end -------------------------------------------------------------


def test_the_three_pipelines_load_the_recorded_files(database: Connection) -> None:
    filings, reference, facts = load_day()

    index = filings["index"]
    assert (index["weekdays"], index["listed"], index["waiting"]) == (5, 1, [])
    assert index["states"] == {"loaded": 1, "not_published": 4}
    (index_file,) = index["files"]
    assert {key: index_file[key] for key in ("raw_records", "rows", "state", "volume")} == {
        "raw_records": 300,
        "rows": 298,
        "state": "loaded",
        "volume": "skipped",
    }
    # A repeated pair is counted, and the file had no line which is no row.
    assert (index_file["duplicate_rows"], index_file["quarantined_rows"]) == (2, 0)
    assert filings["detail"]["documents"] == 5 and filings["detail"]["rows"] == 293
    assert filings["requests"]["sent"] == 7

    assert (reference["ticker_rows"], reference["companies"]) == (16, 10)
    assert (reference["documents_read"], reference["carried_forward"]) == (8, 2)
    assert reference["archive"]["state"] == "loaded"
    assert (reference["archive"]["copied"], reference["archive"]["not_in_archive"]) == (3, 2)
    assert reference["snapshot"]["rows"] == 10 and reference["universe_change"] == "skipped"
    assert reference["former_names"]["rows"] == 4
    assert reference["documents_loaded"] == 3  # the five watchlist documents have an owner

    assert (facts["selected"], facts["documents"], facts["rows"]) == (5, 5, 12264)
    assert facts["selection"] == "the first run takes every company"

    # Every object is in a final state.
    assert states(database) == {
        ("bulk", "loaded"): 1,
        ("companyfacts", "loaded"): 5,
        ("index", "loaded"): 2,
        ("index", "not_published"): 4,
        ("reference", "loaded"): 1,
        ("submissions", "loaded"): 8,
    }
    # 14 requests, and no window of one second holds more than 8.
    assert len(requested(database)) == 14
    assert max_requests_in_window(database, source="sec") <= 8
    runs = database.execute(
        "SELECT entry_point, status, row_count FROM ops.pipeline_run ORDER BY pipeline_run_id"
    ).fetchall()
    assert runs == [
        ("sec.filings", "success", 591),
        ("sec.reference", "success", 10),
        ("sec.facts", "success", 12264),
    ]
    versions = database.execute(
        "SELECT table_name, unit_kind, committed_rows, file_count FROM ops.silver_partition "
        "WHERE source = 'sec' ORDER BY partition_version_id"
    ).fetchall()
    assert versions == [
        ("sec_filing_index", "day", 298, 1),
        ("sec_filing_detail", "keys", 293, 5),
        ("sec_company_snapshot", "day", 10, 1),
        ("sec_company_former_name", "keys", 4, 8),
        ("sec_xbrl_fact", "keys", 12264, 5),
    ]


def test_the_rows_of_the_five_tables(sec_stack: str) -> None:
    load_day()

    index = rows_of("sec_filing_index")
    # A row with an earlier date stays in the unit of the file (finding F11 d).
    assert {row["index_date"] for row in index} == {INDEX_DAY}
    assert sum(1 for row in index if row["filing_date"] < INDEX_DAY) == 40
    assert len({(row["accession_number"], row["cik"]) for row in index}) == 298
    meta = next(row for row in index if row["cik"] == META)
    assert (meta["form"], meta["company_name"]) == ("4", "Meta Platforms, Inc.")
    assert meta["_source_file"] == "raw/sec/index/dt=2026-11-25/form.20261125.idx"

    detail = by_cik("sec_filing_detail")
    assert {cik: len(rows) for cik, rows in detail.items()} == {
        META: 68,
        NVIDIA: 62,
        MICROSOFT: 63,
        MATTEL: 58,
        THOMSON_REUTERS: 42,
    }
    earnings = [row for row in detail[META] if "2.02" in row["items"]]
    assert earnings and {row["form"] for row in earnings} == {"8-K"}
    assert all(row["acceptance_ts"].tzinfo is not None for row in detail[META])

    snapshot = {row["cik"]: row for row in rows_of("sec_company_snapshot")}
    assert len(snapshot) == 10
    assert snapshot[META]["name"] == "Meta Platforms, Inc."
    assert (snapshot[META]["tickers"], snapshot[META]["primary_ticker"]) == (
        ["META", "METAM"],
        "META",
    )
    assert (snapshot[META]["sic"], snapshot[META]["fiscal_year_end"]) == ("7370", "1231")
    assert snapshot[ALPHABET]["tickers"] == ["GOOGL", "GOOG", "GOOGM", "GOOGN"]
    assert snapshot[THOMSON_REUTERS]["exchanges"] == ["Nasdaq", "OTC"]
    # easyJet has a document with empty attributes; Amazon and Netflix have no document.
    assert (snapshot[EASYJET]["sic"], snapshot[EASYJET]["is_stale"]) == (None, False)
    assert snapshot[EASYJET]["attributes_as_of"] == date(2026, 11, 26)
    for cik in (AMAZON, NETFLIX):
        assert (snapshot[cik]["attributes_as_of"], snapshot[cik]["is_stale"]) == (None, True)
    assert not any(row["is_inactive"] for row in snapshot.values())
    assert len({row["attr_hash"] for row in snapshot.values()}) == 10

    former = {(row["cik"], row["former_name"]): row for row in rows_of("sec_company_former_name")}
    assert sorted(former) == [
        (HASBRO, "HASBRO INC"),
        (NVIDIA, "NVIDIA CORP/CA"),
        (THOMSON_REUTERS, "THOMSON CORP /CAN/"),
        (META, "Facebook Inc"),
    ]
    # The name change of exit-gate row 2, with the dates of the SEC.
    facebook = former[META, "Facebook Inc"]
    assert (facebook["valid_from"], facebook["valid_to"]) == (date(2005, 5, 6), date(2021, 10, 27))

    facts = by_cik("sec_xbrl_fact")
    assert {cik: len(rows) for cik, rows in facts.items()} == {
        META: 2061,
        NVIDIA: 2780,
        MICROSOFT: 3599,
        MATTEL: 3215,
        THOMSON_REUTERS: 609,
    }
    instant = [row for row in facts[MATTEL] if row["is_instant"]]
    assert instant and all(row["period_start"] == row["period_end"] for row in instant)
    assert all(row["value_text"] and row["value"] is not None for row in facts[MATTEL])


def test_a_second_run_of_the_day_sends_no_request_and_gives_the_same_content(
    database: Connection,
) -> None:
    first = load_day()
    sent = len(requested(database))

    filings = tasks.filings_task(as_of=THURSDAY)
    reference = tasks.reference_task(as_of=THURSDAY)
    facts = tasks.facts_task(as_of=THURSDAY)
    full = tasks.facts_task(as_of=THURSDAY, full=True)

    assert len(requested(database)) == sent
    assert [run["requests"]["sent"] for run in (filings, reference, facts, full)] == [0, 0, 0, 0]
    # The index file is loaded and is not built again; the other units give equal content.
    assert filings["index"]["files"] == []
    assert filings["detail"]["checksum"] == first[0]["detail"]["checksum"]
    assert reference["snapshot"]["checksum"] == first[1]["snapshot"]["checksum"]
    assert reference["former_names"]["checksum"] == first[1]["former_names"]["checksum"]
    # The archive of the day has its answer; Amazon and Netflix are not in the archive.
    assert reference["archive"] == {
        "wanted": 2,
        "copied": 0,
        "requested": False,
        "state": "loaded",
    }
    assert (facts["selected"], facts["documents"]) == (0, 0)
    assert facts["selection"] == "companies with a new filing since their last facts load"
    assert (full["selected"], full["checksum"]) == (5, first[2]["checksum"])
    # The tables themselves hold the rows of one run, not of two.
    assert {name: len(rows_of(name)) for name in TABLES} == TABLES
    assert not database.execute(
        "SELECT 1 FROM ops.source_object WHERE state IN ('expected', 'verified', 'quarantined')"
    ).fetchall()


# --- the index files ----------------------------------------------------------------------------


def test_a_weekday_without_a_file_is_not_published_and_no_request_is_sent(
    database: Connection,
) -> None:
    tasks.filings_task(as_of=THURSDAY)

    paths = requested(database)
    assert sum(1 for path in paths if path.endswith(".idx")) == 1
    assert not any("form.2026112" in path and "20261125" not in path for path in paths)
    closed = database.execute(
        "SELECT object_date::text, reason, http_status FROM ops.source_object "
        "WHERE state = 'not_published' ORDER BY object_date"
    ).fetchall()
    assert [row[0] for row in closed] == ["2026-11-19", "2026-11-20", "2026-11-23", "2026-11-24"]
    assert {row[1] for row in closed} == {
        "the listing names no file for the day and names the file of 2026-11-25"
    }
    assert {row[2] for row in closed} == {None}


def test_a_newer_weekday_waits_until_the_listing_names_a_newer_file(
    database: Connection, recorded: Path
) -> None:
    friday = tasks.filings_task(as_of=FRIDAY)

    # 2026-11-26 lies after the newest listed file: the file can still appear.
    assert friday["index"]["waiting"] == ["2026-11-26"]
    assert state_of(database, "index", "2026-11-26", "form.20261126.idx") == "expected"

    def add_files(listing: Any) -> None:
        for day in ("20261124", "20261127"):
            listing["directory"]["item"].append({"name": f"form.{day}.idx", "type": "file"})

    edit(recorded, LISTING, add_files)
    for day in (date(2026, 11, 24), date(2026, 11, 27)):
        shutil.copy(path_of(recorded, INDEX_FILE), path_of(recorded, index_file_url(day)))
    monday = tasks.filings_task(as_of=MONDAY)

    # The holiday is closed now, and a day which was closed before is open again and loaded.
    assert monday["index"]["waiting"] == []
    assert state_of(database, "index", "2026-11-26", "form.20261126.idx") == "not_published"
    assert state_of(database, "index", "2026-11-24", "form.20261124.idx") == "loaded"
    assert state_of(database, "index", "2026-11-27", "form.20261127.idx") == "loaded"
    moves = database.execute(
        "SELECT from_state, to_state FROM ops.object_transition WHERE object_name = %s "
        "ORDER BY transition_id",
        ("form.20261124.idx",),
    ).fetchall()
    assert moves == [
        ("expected", "not_published"),
        ("not_published", "expected"),
        ("expected", "verified"),
        ("verified", "loaded"),
    ]
    assert Counter(row["index_date"] for row in rows_of("sec_filing_index")) == {
        date(2026, 11, 24): 298,
        INDEX_DAY: 298,
        date(2026, 11, 27): 298,
    }
    # Three index files and the listings of the two days.
    assert states(database, "index") == {"loaded": 5, "not_published": 3}


def test_a_line_which_is_no_row_goes_to_the_quarantine_file(
    database: Connection, recorded: Path
) -> None:
    target = path_of(recorded, INDEX_FILE)
    target.write_bytes(target.read_bytes() + b"a line which is no row\n")

    filings = tasks.filings_task(as_of=THURSDAY)

    (index_file,) = filings["index"]["files"]
    assert (index_file["state"], index_file["raw_records"]) == ("loaded", 301)
    assert (index_file["quarantined_rows"], index_file["rows"]) == (1, 298)
    assert index_file["quarantine_file"] == "quarantine/sec/index/dt=2026-11-25/day/rows.parquet"
    accounting = database.execute(
        "SELECT raw_records, parsed_rows, quarantined_rows FROM ops.source_object "
        "WHERE object_name = 'form.20261125.idx'"
    ).fetchone()
    assert accounting == (301, 300, 1)
    version = database.execute(
        "SELECT raw_lines, parsed_rows, quarantined_rows, duplicate_rows, committed_rows "
        "FROM ops.silver_partition WHERE table_name = 'sec_filing_index'"
    ).fetchone()
    assert version == (301, 300, 1, 2, 298)


def test_an_index_file_with_too_many_bad_lines_is_quarantined_as_a_whole(
    database: Connection, recorded: Path, tmp_path: Path
) -> None:
    target = path_of(recorded, INDEX_FILE)
    target.write_bytes(target.read_bytes() + b"no row\n" * 4)  # 4 of 304 lines: above 1%

    filings = tasks.filings_task(as_of=THURSDAY)

    (index_file,) = filings["index"]["files"]
    assert index_file["state"] == "quarantined"
    assert index_file["reason"] == "4 of 304 lines are no row: more than 1%"
    assert index_file["kept"] == "quarantine/sec/index/dt=2026-11-25/form.20261125.idx"
    assert (tmp_path / index_file["kept"]).read_bytes() == target.read_bytes()
    assert state_of(database, "index", "2026-11-25", "form.20261125.idx") == "quarantined"
    assert filings["index"]["rows"] == 0
    failed = database.execute(
        "SELECT gate, check_name, unit FROM ops.dq_result WHERE status = 'failed'"
    ).fetchall()
    assert failed == [("G2", "file_bad_row_share", "index/2026-11-25/form.20261125.idx")]


def test_an_index_file_outside_the_volume_band_needs_the_note_of_an_operator(
    database: Connection,
) -> None:
    # Four files of the same weekday with 5,000 rows each: 298 rows are below a third. The
    # third week before has no file, as after a holiday: the rule takes the four files
    # which exist (review 2, finding R2-11).
    earlier = [
        PartitionVersion(
            feed="index",
            table_name="sec_filing_index",
            slot_date=date.fromordinal(INDEX_DAY.toordinal() - 7 * week),
            slot_hour=None,
            committed_rows=5000,
            file_count=1,
            fileset_hash="0" * 64,
            content_digest="0" * 16,
            pipeline_version="test",
        )
        for week in (1, 2, 4, 5)
    ]
    record_versions(database, earlier, source="sec")

    refused = tasks.filings_task(as_of=THURSDAY)

    (index_file,) = refused["index"]["files"]
    assert index_file["state"] == "quarantined"
    assert "298 rows are outside 0.33 to 3.0 times the median 5000" in index_file["reason"]
    assert refused["index"]["rows"] == 0

    # Gate G3 refused the file: the bytes are in bronze, and no request is needed.
    assert requeue("index", "2026-11-25", "a short day") == (
        0,
        {"requeued": ["index/2026-11-25/form.20261125.idx"], "asked_again": []},
    )
    sent = len(requested(database))
    code, accepted, _ = sec(
        "filings", "--as-of", THURSDAY, "--accept-volume", "the day before a holiday"
    )

    assert code == 0
    assert [(item["state"], item["volume"]) for item in accepted["index"]["files"]] == [
        ("loaded", "accepted")
    ]
    # The bytes stayed in bronze: the second build sent no request.
    assert len(requested(database)) == sent
    assert len(rows_of("sec_filing_index")) == 298
    note = database.execute(
        "SELECT detail ->> 'note' FROM ops.dq_result WHERE status = 'accepted'"
    ).fetchone()
    assert note == ("the day before a holiday",)


# --- the submissions documents ------------------------------------------------------------------


def test_a_document_which_does_not_open_is_asked_three_times_and_quarantined(
    database: Connection, recorded: Path, tmp_path: Path
) -> None:
    path_of(recorded, submissions_url(MATTEL)).write_bytes(b"<html>not a document</html>")

    filings = tasks.filings_task(as_of=THURSDAY)

    assert filings["detail"]["documents"] == 4
    assert filings["detail"]["not_read"] == {"quarantined": 1}
    assert filings["landing"]["failed_tries"] == 3
    assert requested(database).count("/submissions/CIK0000063276.json") == 3
    row = database.execute(
        "SELECT state, attempts, reason FROM ops.source_object WHERE cik = %s", (MATTEL,)
    ).fetchone()
    assert row is not None and row[:2] == ("quarantined", 3)
    assert row[2].startswith("gate G1 failed 3 times: the bytes are no JSON document")
    kept = tmp_path / "quarantine/sec/submissions/dt=2026-11-26/CIK0000063276.json"
    assert kept.read_bytes() == b"<html>not a document</html>"
    assert MATTEL not in by_cik("sec_filing_detail")
    assert database.execute(
        "SELECT gate, check_name FROM ops.dq_result WHERE status = 'failed'"
    ).fetchall() == [("G1", "format_opens")]
    # The bytes never reached bronze: a requeue means new requests, and the command says so.
    assert requeue("submissions", "2026-11-26", "the SEC repaired the file") == (
        0,
        {
            "requeued": ["submissions/2026-11-26/CIK0000063276.json"],
            "asked_again": ["submissions/2026-11-26/CIK0000063276.json"],
        },
    )


def test_a_document_without_a_required_field_is_quarantined(
    database: Connection, recorded: Path
) -> None:
    edit(recorded, submissions_url(NVIDIA), lambda document: document.pop("name"))

    filings = tasks.filings_task(as_of=THURSDAY)
    reference = tasks.reference_task(as_of=THURSDAY)

    assert filings["detail"]["documents"] == 4
    row = database.execute(
        "SELECT state, reason FROM ops.source_object WHERE cik = %s", (NVIDIA,)
    ).fetchone()
    assert row == ("quarantined", "Submissions: name: Field required")
    assert NVIDIA not in by_cik("sec_filing_detail")
    # The snapshot still holds the company, with the values of the ticker file only.
    nvidia = next(row for row in rows_of("sec_company_snapshot") if row["cik"] == NVIDIA)
    assert (nvidia["name"], nvidia["sic"], nvidia["is_stale"]) == ("NVIDIA CORP", None, True)
    assert reference["documents_read"] == 7


def test_a_filing_which_leaves_the_document_stays_in_the_detail_table(
    database: Connection, recorded: Path
) -> None:
    tasks.filings_task(as_of=THURSDAY)

    def drop_oldest(document: Any) -> None:
        recent = document["filings"]["recent"]
        for name in recent:
            recent[name] = recent[name][:-10]

    edit(recorded, submissions_url(META), drop_oldest)
    friday = tasks.filings_task(as_of=FRIDAY)

    assert friday["detail"]["kept_stored_rows"] == 10
    meta = by_cik("sec_filing_detail")[META]
    assert len(meta) == 68
    sources = Counter(row["_source_file"] for row in meta)
    assert sources == {
        "raw/sec/submissions/dt=2026-11-27/CIK0001326801.json": 58,
        "raw/sec/submissions/dt=2026-11-26/CIK0001326801.json": 10,
    }
    version = database.execute(
        "SELECT committed_rows, raw_lines, detail ->> 'kept_stored_rows' "
        "FROM ops.silver_partition WHERE table_name = 'sec_filing_detail' "
        "ORDER BY partition_version_id DESC LIMIT 1"
    ).fetchone()
    assert version == (293, None, "10")


def test_an_object_which_was_never_loaded_is_superseded_by_the_next_day(
    database: Connection, recorded: Path
) -> None:
    good = path_of(recorded, submissions_url(MATTEL)).read_bytes()
    edit(recorded, submissions_url(MATTEL), lambda document: document.pop("name"))
    tasks.filings_task(as_of=THURSDAY)
    assert state_of(database, "submissions", "2026-11-26", "CIK0000063276.json") == "quarantined"

    path_of(recorded, submissions_url(MATTEL)).write_bytes(good)
    tasks.filings_task(as_of=FRIDAY)

    assert state_of(database, "submissions", "2026-11-27", "CIK0000063276.json") == "loaded"
    assert state_of(database, "submissions", "2026-11-26", "CIK0000063276.json") == "superseded"
    reason = database.execute(
        "SELECT reason FROM ops.source_object WHERE state = 'superseded'"
    ).fetchall()
    assert reason == [("the object of 2026-11-27 was loaded first",)]
    assert len(by_cik("sec_filing_detail")[MATTEL]) == 58


# --- the reference pipeline ---------------------------------------------------------------------


def test_a_company_which_is_not_refreshed_for_nine_days_is_stale(database: Connection) -> None:
    load_day()

    eight = tasks.reference_task(as_of="2026-12-04T12:00:00+00:00")
    nine = tasks.reference_task(as_of="2026-12-05T12:00:00+00:00")

    # Neither day is a Sunday: no archive, and no document is newer than the snapshot before.
    assert (eight["archive"], eight["documents_read"], eight["carried_forward"]) == (None, 0, 10)
    assert (eight["stale"], nine["stale"]) == (2, 10)
    rows = [row for row in rows_of("sec_company_snapshot") if row["cik"] == META]
    by_day = {row["snapshot_date"]: row for row in rows}
    assert {day: row["is_stale"] for day, row in by_day.items()} == {
        date(2026, 11, 26): False,
        date(2026, 12, 4): False,
        date(2026, 12, 5): True,
    }
    assert {row["attributes_as_of"] for row in rows} == {date(2026, 11, 26)}
    assert len({row["attr_hash"] for row in rows}) == 1
    assert eight["universe_change"] == "passed"


def test_one_company_gives_the_same_hash_on_a_weekday_and_on_a_bulk_day(
    database: Connection, recorded: Path
) -> None:
    load_day()
    friday = tasks.reference_task(as_of=FRIDAY)
    sunday = tasks.reference_task(as_of=SUNDAY)

    def rename(document: Any) -> None:
        for row in document["data"]:
            if row[0] == HASBRO:
                row[1] = "HASBRO GAMES, INC."

    edit(recorded, TICKER_FILE_URL, rename)
    monday = tasks.reference_task(as_of=MONDAY)

    # A weekday asks for no document; a Sunday reads every document from the archive again.
    assert (friday["archive"], friday["documents_read"]) == (None, 0)
    assert (sunday["archive"]["state"], sunday["archive"]["copied"]) == ("loaded", 8)
    assert (sunday["documents_read"], sunday["documents_loaded"]) == (8, 3)
    assert monday["documents_read"] == 0
    hasbro = {
        row["snapshot_date"]: row for row in rows_of("sec_company_snapshot") if row["cik"] == HASBRO
    }
    thursday_hash = hasbro[date(2026, 11, 26)]["attr_hash"]
    assert hasbro[date(2026, 11, 27)]["attr_hash"] == thursday_hash
    assert hasbro[date(2026, 11, 29)]["attr_hash"] == thursday_hash
    assert hasbro[date(2026, 11, 29)]["attributes_as_of"] == date(2026, 11, 29)
    # A changed name gives a new hash.
    assert hasbro[date(2026, 11, 30)]["name"] == "HASBRO GAMES, INC."
    assert hasbro[date(2026, 11, 30)]["attr_hash"] != thursday_hash
    # The documents of the watchlist stay with the filings pipeline: verified, not loaded.
    assert states(database, "submissions") == {"loaded": 11, "verified": 5}
    assert state_of(database, "submissions", "2026-11-29", "CIK0001326801.json") == "verified"
    assert state_of(database, "submissions", "2026-11-29", "CIK0000046080.json") == "loaded"


def test_a_new_company_without_a_document_is_inactive_and_the_universe_rule_needs_a_note(
    database: Connection, recorded: Path
) -> None:
    load_day()
    edit(
        recorded,
        TICKER_FILE_URL,
        lambda document: document["data"].append([999999, "New Company Inc", "NEWC", "NYSE"]),
    )

    with pytest.raises(PipelineError, match="failed gate G3: 11 rows differ by more than 5%"):
        tasks.reference_task(as_of=FRIDAY)

    assert state_of(database, "reference", "2026-11-27", "company_tickers_exchange.json") == (
        "quarantined"
    )
    failed_run = database.execute(
        "SELECT status, error FROM ops.pipeline_run ORDER BY pipeline_run_id DESC LIMIT 1"
    ).fetchone()
    assert failed_run is not None and failed_run[0] == "failed"
    assert failed_run[1].startswith("PipelineError: the snapshot of 2026-11-27 failed gate G3")
    # The new company was asked for once, and the SEC has no document.
    assert requested(database).count("/submissions/CIK0000999999.json") == 1
    assert state_of(database, "submissions", "2026-11-27", "CIK0000999999.json") == "not_published"

    assert requeue("reference", "2026-11-27")[0] == 0
    code, accepted, _ = sec(
        "reference", "--as-of", FRIDAY, "--accept-volume", "one new company among ten"
    )

    assert code == 0
    assert (accepted["companies"], accepted["new_companies"]) == (11, 1)
    assert (accepted["universe_change"], accepted["inactive"]) == ("accepted", 1)
    assert accepted["requests"]["sent"] == 0
    new = next(
        row
        for row in rows_of("sec_company_snapshot")
        if row["cik"] == 999999 and row["snapshot_date"] == date(2026, 11, 27)
    )
    assert (new["is_inactive"], new["is_stale"], new["primary_ticker"]) == (True, True, "NEWC")


def test_a_run_without_the_archive_builds_the_snapshot_from_the_ticker_file(
    database: Connection, recorded: Path
) -> None:
    path_of(recorded, SUBMISSIONS_ARCHIVE_URL).write_bytes(b"not a zip archive")

    reference = tasks.reference_task(as_of=THURSDAY)

    assert reference["archive"]["state"] == "failed"
    assert reference["archive"]["error"].startswith("the archive does not open: BadZipFile")
    assert (reference["companies"], reference["documents_read"], reference["stale"]) == (10, 0, 10)
    assert "former_names" not in reference
    bulk = database.execute(
        "SELECT state, attempts FROM ops.source_object WHERE dataset = 'bulk'"
    ).fetchone()
    assert bulk == ("expected", 1)
    # A run of the same day tries the archive again; an archive which the SEC does not have
    # closes the object.
    path_of(recorded, SUBMISSIONS_ARCHIVE_URL).unlink()
    again = tasks.reference_task(as_of=THURSDAY)
    assert again["archive"]["state"] == "not_published"
    assert states(database, "bulk") == {"not_published": 1}
    assert tasks.reference_task(as_of=THURSDAY)["archive"]["state"] == "not_published"


def test_no_snapshot_without_the_ticker_file(database: Connection, recorded: Path) -> None:
    edit(recorded, TICKER_FILE_URL, lambda document: document.pop("fields"))

    with pytest.raises(PipelineError, match="the ticker file of 2026-11-26 was refused"):
        tasks.reference_task(as_of=THURSDAY)
    with pytest.raises(PipelineError, match="the ticker file of 2026-11-26 is quarantined"):
        tasks.reference_task(as_of=THURSDAY)

    assert states(database, "reference") == {"quarantined": 1}
    assert database.execute("SELECT count(*) FROM ops.silver_partition").fetchone() == (0,)


# --- the facts pipeline -------------------------------------------------------------------------


def cut_facts(share: float) -> Callable[[Any], None]:
    """Remove a share of the facts of the largest concept of a facts document."""

    def change(document: Any) -> None:
        total = sum(
            len(facts)
            for concepts in document["facts"].values()
            for concept in concepts.values()
            for facts in concept["units"].values()
        )
        largest = max(
            (
                facts
                for concepts in document["facts"].values()
                for concept in concepts.values()
                for facts in concept["units"].values()
            ),
            key=len,
        )
        del largest[: int(total * share) + 1]

    return change


def test_a_facts_document_with_fewer_facts_is_quarantined_and_the_old_facts_stay(
    database: Connection, recorded: Path
) -> None:
    load_day()
    edit(recorded, companyfacts_url(MATTEL), cut_facts(0.03))

    friday = tasks.facts_task(as_of=FRIDAY, full=True)

    assert (friday["selected"], friday["documents"]) == (5, 4)
    assert list(friday["refused"]) == [str(MATTEL)]
    assert friday["refused"][str(MATTEL)] == "3118 rows are more than 2% below 3215 rows"
    assert state_of(database, "companyfacts", "2026-11-27", "CIK0000063276.json") == "quarantined"
    facts = by_cik("sec_xbrl_fact")
    assert len(facts[MATTEL]) == 3215
    assert {row["_source_file"] for row in facts[MATTEL]} == {
        "raw/sec/companyfacts/dt=2026-11-26/CIK0000063276.json"
    }
    assert {row["_source_file"] for row in facts[META]} == {
        "raw/sec/companyfacts/dt=2026-11-27/CIK0001326801.json"
    }
    # An operator who knows the reason accepts the fall with a note.
    assert requeue("companyfacts", "2026-11-27") == (
        0,
        {"requeued": ["companyfacts/2026-11-27/CIK0000063276.json"], "asked_again": []},
    )
    code, accepted, _ = sec(
        "facts", "--as-of", FRIDAY, "--full", "--accept-volume", "the SEC removed old facts"
    )
    assert (code, accepted["documents"], accepted["refused"]) == (0, 5, {})
    assert len(by_cik("sec_xbrl_fact")[MATTEL]) == 3118
    # The accepted document is loaded: a later run of the day without the note builds the
    # same rows and does not ask for the note again (review 2, finding R2-10).
    again = tasks.facts_task(as_of=FRIDAY, full=True)
    assert (again["documents"], again["refused"], again["requests"]["sent"]) == (5, {}, 0)
    assert len(by_cik("sec_xbrl_fact")[MATTEL]) == 3118


def test_a_document_with_two_values_for_one_key_is_quarantined_alone(
    database: Connection, recorded: Path
) -> None:
    def repeat(document: Any, *, other_value: bool) -> None:
        facts = document["facts"]["us-gaap"]["NetIncomeLoss"]["units"]["USD"]
        facts.append({**facts[0], "val": facts[0]["val"] + 1} if other_value else dict(facts[0]))

    edit(recorded, companyfacts_url(NVIDIA), functools.partial(repeat, other_value=True))
    edit(recorded, companyfacts_url(MATTEL), functools.partial(repeat, other_value=False))
    tasks.filings_task(as_of=THURSDAY)

    facts = tasks.facts_task(as_of=THURSDAY)

    # An exact repeat is dropped and counted; two values for one key refuse the document.
    assert (facts["documents"], facts["exact_repeats"]) == (4, 1)
    assert facts["refused"] == {str(NVIDIA): "1 keys stand in two rows with different values"}
    assert facts["facts"] == 12264 - 2780 + 1
    loaded = by_cik("sec_xbrl_fact")
    assert NVIDIA not in loaded and len(loaded[MATTEL]) == 3215
    assert states(database, "companyfacts") == {"loaded": 4, "quarantined": 1}
    version = database.execute(
        "SELECT raw_lines, parsed_rows, quarantined_rows, duplicate_rows, committed_rows "
        "FROM ops.silver_partition WHERE table_name = 'sec_xbrl_fact'"
    ).fetchone()
    assert version == (9485, 9485, 0, 1, 9484)


def test_a_company_without_a_facts_document_is_not_published(
    database: Connection, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seed = tmp_path / "watchlist.csv"
    seed.write_text(
        "rank,cik,name_at_selection,reason,added_on\n"
        f"1,{META},Meta,test,2026-11-28\n2,{EASYJET},easyJet,test,2026-11-28\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("GBMI_WATCHLIST_FILE", str(seed))
    get_settings.cache_clear()

    first = tasks.facts_task(as_of=THURSDAY)
    second = tasks.facts_task(as_of=FRIDAY)

    assert (first["selected"], first["documents"], first["not_read"]) == (
        2,
        1,
        {"not_published": 1},
    )
    row = database.execute(
        "SELECT state, http_status, reason FROM ops.source_object WHERE cik = %s", (EASYJET,)
    ).fetchone()
    assert row == ("not_published", 404, "the SEC answered HTTP 404: no such object")
    assert set(by_cik("sec_xbrl_fact")) == {META}
    # The answer of the SEC stands: the next day asks for neither company.
    assert (second["selected"], second["requests"]["sent"]) == (0, 0)


def test_a_new_report_selects_the_company_and_a_sunday_selects_every_company(
    database: Connection, recorded: Path
) -> None:
    load_day()

    def new_report(document: Any) -> None:
        recent = document["filings"]["recent"]
        for values in recent.values():
            values.insert(0, values[0])
        recent["accessionNumber"][0] = "0001326801-30-000001"
        recent["form"][0] = "10-Q"
        recent["filingDate"][0] = "2030-01-02"
        recent["acceptanceDateTime"][0] = "2030-01-02T21:00:00.000Z"

    edit(recorded, submissions_url(META), new_report)
    tasks.filings_task(as_of=FRIDAY)
    friday = tasks.facts_task(as_of=FRIDAY)
    sunday = tasks.facts_task(as_of=SUNDAY)

    assert (friday["selected"], friday["documents"], friday["requests"]["sent"]) == (1, 1, 1)
    assert friday["unit"].startswith("cik:1:")
    assert (sunday["selected"], sunday["selection"]) == (5, "a Sunday run takes every company")
    assert len(rows_of("sec_xbrl_fact")) == 12264
    assert states(database, "companyfacts") == {"loaded": 11}


# --- more companies than one unit of keys holds -------------------------------------------------


def test_more_companies_than_a_unit_of_keys_holds_replace_the_whole_table(
    database: Connection, recorded: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A bulk day refreshes about 8,000 companies; a unit of keys holds 200. With a limit of 3
    # the eight documents of the recorded files take the same path.
    for module in (silver, reference_module):
        monkeypatch.setattr(module, "MAX_SCOPE_KEYS", 3)
    first = load_day()

    assert first[0]["detail"]["unit"] == "table"
    assert first[1]["former_names"]["unit"] == "table"
    assert first[2]["unit"] == "table"
    kinds = database.execute(
        "SELECT table_name, unit_kind, raw_lines, detail -> 'ciks' FROM ops.silver_partition "
        "WHERE unit_kind = 'table' ORDER BY partition_version_id"
    ).fetchall()
    assert [(row[0], row[1], row[2]) for row in kinds] == [
        ("sec_filing_detail", "table", None),
        ("sec_company_former_name", "table", None),
        ("sec_xbrl_fact", "table", None),
    ]
    assert kinds[1][3] is None  # the long list of companies is not stored

    # The next day refreshes one company: the rows of every other company are written again.
    def rename(document: Any) -> None:
        document["formerNames"] = [{"name": "Mattel Toys", "from": "1990-01-02T05:00:00.000Z"}]

    edit(recorded, submissions_url(MATTEL), rename)
    monkeypatch.setattr(silver, "MAX_SCOPE_KEYS", 0)
    tasks.filings_task(as_of=FRIDAY)
    friday = tasks.reference_task(as_of=FRIDAY)

    assert friday["documents_read"] == 5  # the documents of the watchlist of the day
    former = sorted((row["cik"], row["former_name"]) for row in rows_of("sec_company_former_name"))
    assert former == [
        (HASBRO, "HASBRO INC"),
        (MATTEL, "Mattel Toys"),
        (NVIDIA, "NVIDIA CORP/CA"),
        (THOMSON_REUTERS, "THOMSON CORP /CAN/"),
        (META, "Facebook Inc"),
    ]
    assert len(rows_of("sec_filing_detail")) == 293
    assert len(rows_of("sec_xbrl_fact")) == 12264


# --- documents which do not fit ------------------------------------------------------------------


def test_a_facts_document_which_does_not_fit_its_model_is_quarantined(
    database: Connection, recorded: Path
) -> None:
    edit(recorded, companyfacts_url(NVIDIA), lambda document: document.pop("entityName"))
    edit(recorded, companyfacts_url(MATTEL), lambda document: document.update(cik=1))

    def reversed_period(document: Any) -> None:
        fact = document["facts"]["us-gaap"]["NetIncomeLoss"]["units"]["USD"][0]
        fact["start"], fact["end"] = "2026-12-31", "2026-01-01"

    edit(recorded, companyfacts_url(META), reversed_period)

    facts = tasks.facts_task(as_of=THURSDAY)

    assert facts["documents"] == 2
    assert facts["refused"] == {
        str(NVIDIA): "CompanyFacts: entityName: Field required",
        str(MATTEL): "CompanyFacts: cik: the document names 1",
        str(META): "us-gaap:NetIncomeLoss: a period ends on 2026-01-01 and starts on 2026-12-31",
    }
    assert states(database, "companyfacts") == {"loaded": 2, "quarantined": 3}
    assert set(by_cik("sec_xbrl_fact")) == {MICROSOFT, THOMSON_REUTERS}


def test_a_listing_which_does_not_fit_its_model_closes_no_day(
    database: Connection, recorded: Path
) -> None:
    edit(recorded, LISTING, lambda document: document.pop("directory"))
    edit(recorded, submissions_url(MATTEL), lambda document: document.update(cik="1"))

    filings = tasks.filings_task(as_of=THURSDAY)
    again = tasks.filings_task(as_of=THURSDAY)

    # Without the listing no day is decided: every weekday waits, and nothing was asked.
    assert filings["index"]["waiting"] == [
        "2026-11-19",
        "2026-11-20",
        "2026-11-23",
        "2026-11-24",
        "2026-11-25",
    ]
    assert states(database, "index") == {"expected": 5, "quarantined": 1}
    assert again["index"]["waiting"] == filings["index"]["waiting"]
    assert again["requests"]["sent"] == 0
    # A document which names another company is refused; the other four are loaded.
    assert filings["detail"]["documents"] == 4
    assert states(database, "submissions") == {"loaded": 4, "quarantined": 1}
    reasons = database.execute(
        "SELECT reason FROM ops.source_object WHERE state = 'quarantined' ORDER BY dataset"
    ).fetchall()
    assert reasons == [
        ("IndexListing: directory: Field required",),
        ("Submissions: cik: the document names 1",),
    ]


def test_an_archive_which_does_not_arrive_does_not_stop_the_run(
    database: Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    def no_download(self: SecClient, url: str, path: Path, **options: Any) -> None:
        raise SecTransportError("ReadTimeout for www.sec.gov/Archives/.../submissions.zip")

    monkeypatch.setattr(SecClient, "download", no_download)

    reference = tasks.reference_task(as_of=THURSDAY)

    assert reference["archive"] == {
        "wanted": 10,
        "copied": 0,
        "requested": True,
        "state": "failed",
        "error": "ReadTimeout for www.sec.gov/Archives/.../submissions.zip",
        "attempts": 1,
    }
    assert (reference["companies"], reference["stale"]) == (10, 10)
    bulk = database.execute(
        "SELECT state, attempts, last_error FROM ops.source_object WHERE dataset = 'bulk'"
    ).fetchone()
    assert bulk == ("expected", 1, "ReadTimeout for www.sec.gov/Archives/.../submissions.zip")


# --- the repairs of review 2 --------------------------------------------------------------------


def test_a_listed_index_file_which_is_missing_is_asked_again_when_the_listing_still_names_it(
    database: Connection, recorded: Path
) -> None:
    # The listing names a file which the SEC does not serve (review 2, finding R2-3).
    edit(
        recorded,
        LISTING,
        lambda listing: listing["directory"]["item"].append({"name": "form.20261124.idx"}),
    )
    tasks.filings_task(as_of=THURSDAY)
    missing = database.execute(
        "SELECT state, http_status FROM ops.source_object WHERE object_name = 'form.20261124.idx'"
    ).fetchone()
    assert missing == ("not_published", 403)

    # A week later the day lies outside the days of the run, and the SEC serves the file.
    other_day = path_of(recorded, index_file_url(date(2026, 11, 24)))
    shutil.copy(path_of(recorded, INDEX_FILE), other_day)
    later = tasks.filings_task(as_of="2026-12-03T12:00:00+00:00")

    assert later["window"] == ["2026-11-26", "2026-12-02"]
    assert state_of(database, "index", "2026-11-24", "form.20261124.idx") == "loaded"
    assert [item["file"] for item in later["index"]["files"]] == ["form.20261124.idx"]
    asked = requested(database).count("/Archives/edgar/daily-index/2026/QTR4/form.20261124.idx")
    assert asked == 2


def test_a_run_which_stops_after_the_commit_is_repaired_by_the_next_run(
    database: Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = []

    def stopped(*arguments: Any, **options: Any) -> None:
        calls.append(arguments)
        raise RuntimeError("the process stopped between the commit and its record")

    with monkeypatch.context() as patched:
        patched.setattr(filings_module, "record", stopped)
        with pytest.raises(RuntimeError, match="between the commit and its record"):
            tasks.filings_task(as_of=THURSDAY)

    # The table holds the rows, and the ledger does not know yet.
    assert len(rows_of("sec_filing_index")) == 298
    assert state_of(database, "index", "2026-11-25", "form.20261125.idx") == "verified"
    assert database.execute("SELECT count(*) FROM ops.silver_partition").fetchone() == (0,)
    sent = len(requested(database))

    again = tasks.filings_task(as_of=THURSDAY)

    assert len(calls) == 1
    assert again["index"]["files"][0]["state"] == "loaded"
    assert len(rows_of("sec_filing_index")) == 298  # the same unit, replaced
    assert state_of(database, "index", "2026-11-25", "form.20261125.idx") == "loaded"
    # The files of the stopped run were in bronze: only the five documents were asked.
    assert len(requested(database)) - sent == 5


def test_a_requeued_document_of_a_company_outside_the_watchlist_is_read_again(
    database: Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The model refuses the document of Hasbro, as a model with a defect would.
    real = Submissions.parse.__func__  # type: ignore[attr-defined]

    def strict(cls: type[Submissions], content: bytes | str) -> Submissions:
        document: Submissions = real(cls, content)
        if document.cik == HASBRO:
            raise PayloadError("Submissions", ["name: the model has a defect"])
        return document

    with monkeypatch.context() as patched:
        patched.setattr(Submissions, "parse", classmethod(strict))
        first = tasks.reference_task(as_of=THURSDAY)
    assert first["documents_refused"] == 1
    assert state_of(database, "submissions", "2026-11-26", "CIK0000046080.json") == "quarantined"
    hasbro = next(row for row in rows_of("sec_company_snapshot") if row["cik"] == HASBRO)
    assert (hasbro["sic"], hasbro["is_stale"]) == (None, True)

    # The model is repaired, and the operator sends the document back.
    assert requeue("submissions", "2026-11-26", "the model is repaired") == (
        0,
        {"requeued": ["submissions/2026-11-26/CIK0000046080.json"], "asked_again": []},
    )
    sent = len(requested(database))
    second = tasks.reference_task(as_of=THURSDAY)

    # The bytes were in bronze: no request, and the snapshot of the day has the attributes.
    assert len(requested(database)) == sent
    assert (second["documents_refused"], second["archive"]["requested"]) == (0, False)
    assert state_of(database, "submissions", "2026-11-26", "CIK0000046080.json") == "loaded"
    hasbro = next(row for row in rows_of("sec_company_snapshot") if row["cik"] == HASBRO)
    assert (hasbro["sic"], hasbro["is_stale"]) == ("3944", False)
    assert sorted(row["former_name"] for row in rows_of("sec_company_former_name")) == [
        "Facebook Inc",
        "HASBRO INC",
        "NVIDIA CORP/CA",
        "THOMSON CORP /CAN/",
    ]


def test_one_member_which_cannot_be_stored_does_not_lose_the_archive(
    database: Connection, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = Lander.store

    def store(self: Lander, expected: Any, content: bytes, **options: Any) -> Any:
        if expected.cik == ALPHABET:
            raise OSError("no space left on device")
        return real(self, expected, content, **options)

    monkeypatch.setattr(Lander, "store", store)

    first = tasks.reference_task(as_of=THURSDAY)
    second = tasks.reference_task(as_of=THURSDAY)

    # The archive is used up and recorded: the second run sends no request for 1.6 GB.
    archive = first["archive"]
    assert (archive["state"], archive["copied"], archive["refused"]) == ("loaded", 7, 1)
    assert (second["archive"]["requested"], second["requests"]["sent"]) == (False, 0)
    manifest = json.loads(
        (tmp_path / "raw/sec/bulk/dt=2026-11-26/submissions.manifest.json").read_bytes()
    )
    assert manifest["refused"] == ["CIK0001652044.json"]
    assert (manifest["wanted"], len(manifest["copied"]), manifest["not_in_archive"]) == (10, 7, 2)
    accounting = database.execute(
        "SELECT state, raw_records, parsed_rows, quarantined_rows FROM ops.source_object "
        "WHERE dataset = 'bulk'"
    ).fetchone()
    assert accounting == ("loaded", 10, 7, 3)
    assert state_of(database, "submissions", "2026-11-26", "CIK0001652044.json") == "expected"


def test_after_three_failed_tries_of_a_day_no_run_asks_for_the_archive_again(
    database: Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    tries = []

    def no_download(self: SecClient, url: str, path: Path, **options: Any) -> None:
        tries.append(url)
        raise SecTransportError("ReadTimeout for www.sec.gov")

    monkeypatch.setattr(SecClient, "download", no_download)

    runs = [tasks.reference_task(as_of=THURSDAY)["archive"] for _ in range(4)]

    assert [run["state"] for run in runs] == ["failed", "failed", "failed", "quarantined"]
    assert [run.get("attempts") for run in runs] == [1, 2, 3, None]
    assert (len(tries), runs[3]["requested"]) == (3, False)
    row = database.execute(
        "SELECT state, attempts, reason FROM ops.source_object WHERE dataset = 'bulk'"
    ).fetchone()
    assert row == (
        "quarantined",
        3,
        "3 tries failed; the last one: the download failed: ReadTimeout for www.sec.gov",
    )


def test_no_download_of_the_archive_starts_while_the_sec_builds_the_archive(
    database: Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    def at(text: str) -> datetime:
        return datetime.fromisoformat(text)

    assert [
        bulk.in_build_window(at(moment))
        for moment in (
            "2026-11-29T04:29:59+00:00",
            "2026-11-29T04:30:00+00:00",
            "2026-11-29T05:29:59+00:00",
            "2026-11-29T05:30:00+00:00",
            "2026-11-29T12:45:00+08:00",  # 04:45 UTC
        )
    ] == [False, True, True, False, True]
    # The rule holds for the live source only: the recorded files have no build.
    night = at("2026-11-29T05:00:00+00:00")
    live: Any = SimpleNamespace(settings=Settings(_env_file=None, sec_source="live"))
    files: Any = SimpleNamespace(settings=Settings(_env_file=None, sec_source="fixture"))
    assert bulk.waits_for_the_build(live, night)
    assert not bulk.waits_for_the_build(files, night)
    assert not bulk.waits_for_the_build(live, at("2026-11-29T09:30:00+00:00"))

    with monkeypatch.context() as patched:
        patched.setattr(bulk, "waits_for_the_build", lambda run, moment: True)
        early = tasks.reference_task(as_of=THURSDAY)
    later = tasks.reference_task(as_of=THURSDAY)

    # The early run builds the snapshot without the archive and leaves no object behind.
    assert (early["archive"]["state"], early["archive"]["requested"]) == ("not now", False)
    assert (early["companies"], early["documents_read"]) == (10, 0)
    assert (later["archive"]["state"], later["archive"]["copied"]) == ("loaded", 8)
    assert states(database, "bulk") == {"loaded": 1}


# --- the summary of a DAG run -------------------------------------------------------------------


def test_the_summary_reads_the_record_of_the_run(database: Connection, recorded: Path) -> None:
    filings = tasks.filings_task(as_of=THURSDAY, run_id="manual__1")

    summary = tasks.summary_task("filings", THURSDAY, "manual__1")

    assert summary["pipeline"] == "sec.filings"
    assert (summary["run_id"], summary["status"], summary["day"]) == (
        "manual__1",
        "success",
        "2026-11-26",
    )
    assert (summary["rows"], summary["requests"]) == (591, filings["requests"])
    assert summary["open"] == {}
    assert {(item["dataset"], item["state"]): item["objects"] for item in summary["objects"]} == {
        ("index", "loaded"): 1,
        ("submissions", "loaded"): 5,
    }
    # A run of which nothing is recorded, and a run which failed, fail the summary.
    with pytest.raises(PipelineError, match=r"no run of sec\.reference is recorded for the run x"):
        tasks.summary_task("reference", THURSDAY, "x")
    edit(recorded, TICKER_FILE_URL, lambda document: document.pop("fields"))
    with pytest.raises(PipelineError):
        tasks.reference_task(as_of=THURSDAY, run_id="manual__2")
    with pytest.raises(
        PipelineError, match=r"the run of sec\.reference ended as failed: PipelineError"
    ):
        tasks.summary_task("reference", THURSDAY, "manual__2")
    # The refused ticker file is an object of the day which needs an operator.
    after = tasks.summary_task("filings", THURSDAY, "manual__1")
    assert after["open"] == {"reference/2026-11-26/company_tickers_exchange.json": "quarantined"}


# --- the commands -------------------------------------------------------------------------------


def test_the_commands_run_the_pipelines_and_read_the_ledger(database: Connection) -> None:
    code, filings, _ = sec(
        "filings", "--as-of", THURSDAY, "--from", "2026-11-23", "--to", "2026-11-25"
    )
    assert code == 0
    assert (filings["window"], filings["index"]["weekdays"]) == (["2026-11-23", "2026-11-25"], 3)
    assert sec("reference", "--as-of", THURSDAY, "--bulk")[0] == 0
    code, facts, _ = sec("facts", "--as-of", THURSDAY, "--full", "--run-id", "manual-1")
    assert (code, facts["rows"]) == (0, 12264)

    code, objects, _ = sec("objects", "--from", "2026-11-25", "--to", "2026-11-25")
    assert (code, objects["open"]) == (0, [])
    assert objects["states"] == [
        {
            "dataset": "index",
            "state": "loaded",
            "objects": 1,
            "first_date": "2026-11-25",
            "last_date": "2026-11-25",
        }
    ]
    code, everything, _ = sec("objects")
    assert code == 0 and sum(item["objects"] for item in everything["states"]) == 19

    code, burst, _ = sec("requests", "max-burst")
    assert code == 0
    assert (burst["ceiling"], burst["requests"]) == (8, {"ok": 14})
    assert 1 <= burst["max_requests_in_one_second"] <= 8
    code, none, _ = sec("requests", "max-burst", "--from", "2020-01-01", "--to", "2020-01-02")
    assert (code, none["max_requests_in_one_second"], none["requests"]) == (0, 0, {})
    run_ids = database.execute(
        "SELECT DISTINCT run_id FROM ops.silver_partition WHERE table_name = 'sec_xbrl_fact'"
    ).fetchall()
    assert run_ids == [("manual-1",)]


def test_a_refused_command_is_one_line(sec_stack: str, monkeypatch: pytest.MonkeyPatch) -> None:
    code, _, output = sec(
        "filings", "--as-of", THURSDAY, "--from", "2026-11-26", "--to", "2026-11-25"
    )
    assert code == 1
    assert output.strip() == "gbmi sec filings: the range starts on 2026-11-26, after 2026-11-25"
    assert sec("objects", "--from", "soon")[0] == 1
    code, _, output = sec(
        "objects",
        "requeue",
        "--dataset",
        "index",
        "--from",
        "2026-11-25",
        "--to",
        "2026-11-25",
        "--note",
        " ",
    )
    assert (code, "the note is empty" in output) == (1, True)
    assert requeue("news", "2026-11-25")[0] == 1  # no such dataset
    # The dataset is required: a requeue of every dataset could ask for the archive again.
    assert (
        sec("objects", "requeue", "--from", "2026-11-25", "--to", "2026-11-25", "--note", "x")[0]
        == 2
    )
    assert sec("requests", "max-burst", "--from", "soon")[0] == 1

    monkeypatch.setenv("GBMI_SEC_SOURCE", "off")
    get_settings.cache_clear()
    code, _, output = sec("reference", "--as-of", THURSDAY)
    assert code == 1
    assert output.strip() == (
        "gbmi sec reference: GBMI_SEC_SOURCE is off: the stack sends no request to the SEC"
    )


def test_the_day_of_a_run_is_the_utc_day_of_its_clock(database: Connection) -> None:
    late = datetime(2026, 11, 26, 23, 30, tzinfo=UTC).isoformat()

    tasks.filings_task(as_of=late)

    days = database.execute(
        "SELECT DISTINCT object_date::text FROM ops.source_object WHERE dataset = 'submissions'"
    ).fetchall()
    assert days == [("2026-11-26",)]
