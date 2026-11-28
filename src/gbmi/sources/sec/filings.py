"""Pipeline PL-4: the daily form index of every filer, and filing detail for the watchlist.

Each run (week 3 plan 3.3, with the differences D32 to D34):

1. reads the listing of each quarter which the days of the run touch;
2. expects one index file for each weekday. A file which the listing names is downloaded. A
   weekday without a file becomes ``not_published`` when the listing names a newer file, and no
   request is sent for such a day (finding F9);
3. builds each verified index file: one commit for the date of the file, one partition
   version, ``loaded``;
4. loads the submissions documents of the watchlist and replaces the rows of the companies in
   ``sec_filing_detail``. A stored row of a filing which the new document does not list any
   more stays (finding F4).

The pipeline does not depend on Airflow. A manual run takes a range of days for a backfill.

The listing of a quarter is one object of the UTC day of the run: the first run of a day asks
for the listing, and every later run of the day reads the stored one. The SEC publishes the
index file of a day at about 03:00 UTC of the next day (finding F10), so a run before that
time does not see the newest file, and the next run of the day does not either. The schedule
is 08:30 UTC; do not start a run by hand between 00:00 and 03:30 UTC (review 2, R2-5).
"""

from __future__ import annotations

import time
from collections import Counter
from collections.abc import Sequence
from datetime import date, timedelta
from typing import Any

import pyarrow as pa

from gbmi.lake.commit import Scope, Unit
from gbmi.lake.tables import SEC_FILING_DETAIL, SEC_FILING_INDEX, SEC_TABLE_KEYS
from gbmi.ledger.object_states import ObjectState
from gbmi.ledger.objects import Dataset, Expected, ObjectAccounting, ObjectKey, ObjectRecord
from gbmi.logging import get_logger
from gbmi.ops.partitions import day_rows
from gbmi.quality.audit import (
    VOLUME_HISTORY,
    UnitCounts,
    check_accounting,
    check_quarantine,
    check_rows,
    check_volume,
    problems,
    raise_if_failed,
)
from gbmi.quality.quarantine import write_quarantine
from gbmi.quality.results import FAILED, G2, G3, PASSED, CheckResult
from gbmi.sources.sec.index import parse_index
from gbmi.sources.sec.land import index_opens, json_opens
from gbmi.sources.sec.models import IndexListing, ListingItem, PayloadError, Submissions
from gbmi.sources.sec.silver import (
    SecRun,
    commit,
    company_unit,
    record,
    record_results_only,
    stored_rows,
    table_summary,
    version,
)
from gbmi.sources.sec.urls import (
    cik_file_name,
    index_file_name,
    index_file_url,
    index_listing_url,
    quarter_of,
    submissions_url,
)
from gbmi.transforms.sec import filing_detail, index_build

log = get_logger(__name__)

INDEX_FEED = Dataset.INDEX.value
SUBMISSIONS_FEED = Dataset.SUBMISSIONS.value
#: A daily run looks at the seven days before the day of the run.
TRAILING_DAYS = 7
#: The rows of an index file against the median of four files of the same weekday. The files
#: of one quarter differ by a factor of 2.7 in size (week 3 plan, finding F10).
INDEX_BAND = (0.33, 3.0)
#: The volume rule looks so many weeks back for four files of the same weekday.
LOOK_BACK_WEEKS = 8
#: A file with a larger share of lines which are no row is quarantined as a whole.
BAD_ROW_SHARE = 0.01
BAD_ROWS_RULE = f"at most {BAD_ROW_SHARE:.0%} of the lines of a file are no row"
MODEL_RULE = "the document has the shape of its model"
_READABLE = (ObjectState.VERIFIED, ObjectState.LOADED)


def listing_name(year: int, quarter: int) -> str:
    """The object name of the listing of one quarter."""
    return f"index.{year}QTR{quarter}.json"


def weekdays(first: date, last: date) -> list[date]:
    """The days from Monday to Friday in a range, both ends counted."""
    days = (first + timedelta(days=offset) for offset in range((last - first).days + 1))
    return [day for day in days if day.weekday() < 5]


def run_filings(
    run: SecRun,
    *,
    watchlist: Sequence[int],
    first: date | None = None,
    last: date | None = None,
) -> dict[str, Any]:
    """One run of the pipeline; returns a summary of built-in types.

    Without a range the run looks at the seven days before the day of the run. The index
    file of a day is published in the night after the day.
    """
    last = last if last is not None else run.day - timedelta(days=1)
    first = first if first is not None else run.day - timedelta(days=TRAILING_DAYS)
    if first > last:
        raise ValueError(f"the range starts on {first.isoformat()}, after {last.isoformat()}")
    days = weekdays(first, last)
    listed, known = _read_listings(run, days)
    # A file which the SEC lists and which answered "no such object" before is asked again,
    # also when its day lies outside the days of the run (review 2, finding R2-3).
    closed = run.ledger.select(states={ObjectState.NOT_PUBLISHED}, dataset=Dataset.INDEX)
    again = {
        item.key.object_date
        for item in closed
        if item.key.object_date in listed and item.key.name == index_file_name(item.key.object_date)
    }
    waiting = _expect_index_files(run, sorted({*days, *again}), listed, known)
    verified = run.ledger.select(states={ObjectState.VERIFIED}, dataset=Dataset.INDEX)
    files = [_build_index(run, item) for item in verified if item.key.name.startswith("form.")]
    detail = _build_detail(run, watchlist)
    states = Counter(
        item.state.value
        for item in run.ledger.select(dataset=Dataset.INDEX, first=first, last=last)
        if item.key.name.startswith("form.")
    )
    return {
        "pipeline": "sec.filings",
        "day": run.day.isoformat(),
        "window": [first.isoformat(), last.isoformat()],
        "index": {
            "weekdays": len(days),
            "listed": sum(1 for day in days if day in listed),
            "states": dict(sorted(states.items())),
            "waiting": [day.isoformat() for day in waiting],
            "files": files,
            "rows": sum(item["rows"] for item in files if item["state"] == "loaded"),
        },
        "detail": detail,
        "landing": run.lander.counts.as_dict(),
        "seconds": run.seconds(),
    }


# --- the listings and the index files ---------------------------------------------------------


def _read_listings(
    run: SecRun, days: Sequence[date]
) -> tuple[dict[date, ListingItem], set[tuple[int, int]]]:
    """The index files which the SEC lists, and the quarters about which the run knows.

    A quarter is known when its listing was read, or when the SEC has no listing for the
    quarter. A listing which was refused leaves its quarter unknown: no day of the quarter is
    closed then.
    """
    listed: dict[date, ListingItem] = {}
    known: set[tuple[int, int]] = set()
    for year, quarter in sorted({quarter_of(day) for day in days}):
        key = ObjectKey(Dataset.INDEX, run.day, listing_name(year, quarter))
        expected = Expected(key, index_listing_url(year, quarter))
        item = run.lander.land(expected, check=json_opens, listed=False)
        if item.state is ObjectState.NOT_PUBLISHED:
            known.add((year, quarter))
            continue
        if item.state not in _READABLE:
            continue
        try:
            listing = IndexListing.parse(run.lander.read(item))
        except PayloadError as error:
            if item.state is ObjectState.VERIFIED:
                run.lander.quarantine_verified(
                    item, gate=G2, check="payload_model", reason=str(error), threshold=MODEL_RULE
                )
            continue
        files = listing.index_files()
        listed.update(
            {day: file for day, file in files.items() if quarter_of(day) == (year, quarter)}
        )
        known.add((year, quarter))
        if item.state is ObjectState.VERIFIED:
            count = len(listing.directory.item)
            run.ledger.mark_loaded(
                [ObjectAccounting(key, count, count)],
                as_of=run.as_of,
                reason="the listing was read by the filings pipeline",
            )
            run.ledger.supersede_older(key, as_of=run.as_of)
    return listed, known


def _expect_index_files(
    run: SecRun,
    days: Sequence[date],
    listed: dict[date, ListingItem],
    known: set[tuple[int, int]],
) -> list[date]:
    """Create the object of each weekday and decide the weekday; returns the open days."""
    newest = max(listed, default=None)
    waiting: list[date] = []
    for day in days:
        key = ObjectKey(Dataset.INDEX, day, index_file_name(day))
        expected = Expected(key, index_file_url(day))
        run.ledger.ensure_expected([expected])
        state = _state(run, key)
        if day in listed:
            if state is ObjectState.NOT_PUBLISHED:
                run.ledger.reopen(
                    [key], reason="the listing of the quarter names the file now", as_of=run.as_of
                )
            state = run.lander.land(expected, check=index_opens, listed=True).state
        elif (
            state is ObjectState.EXPECTED
            and quarter_of(day) in known
            and newest is not None
            and newest > day
        ):
            run.ledger.mark_not_published(
                key,
                reason=(
                    "the listing names no file for the day and names the file of "
                    f"{newest.isoformat()}"
                ),
                as_of=run.as_of,
            )
            state = ObjectState.NOT_PUBLISHED
        if state is ObjectState.EXPECTED:
            waiting.append(day)
    return waiting


def _state(run: SecRun, key: ObjectKey) -> ObjectState | None:
    item = run.ledger.get(key)
    return item.state if item is not None else None


def _build_index(run: SecRun, item: ObjectRecord) -> dict[str, Any]:
    """Build one verified index file: gate G2, gate G3, one commit, one record."""
    started = time.monotonic()
    index_date = item.key.object_date
    unit = Scope.for_date("index_date", index_date)
    build = index_build(
        parse_index(run.lander.read(item)), index_date, run.source_file(item), run.facts
    )
    summary: dict[str, Any] = {
        "file": item.key.name,
        "index_date": index_date.isoformat(),
        "raw_records": build.raw_records,
        "quarantined_rows": build.quarantined_rows,
        "duplicate_rows": build.duplicate_rows,
        "earlier_dates": build.earlier_dates,
        "rows": build.table.num_rows,
    }
    if build.bad_share > BAD_ROW_SHARE:
        reason = (
            f"{build.quarantined_rows} of {build.raw_records} lines are no row: "
            f"more than {BAD_ROW_SHARE:.0%}"
        )
        kept = run.lander.quarantine_verified(
            item, gate=G2, check="file_bad_row_share", reason=reason, threshold=BAD_ROWS_RULE
        )
        return {**summary, "state": "quarantined", "reason": reason, "kept": kept}
    counts = UnitCounts(
        raw_lines=build.raw_records,
        parsed=build.parsed_rows,
        quarantined=build.quarantined_rows,
        duplicates=build.duplicate_rows,
        committed=build.table.num_rows,
    )
    # The four newest earlier files of the same weekday which exist: a holiday leaves a
    # week without a file, and the rule must not rest for four weeks (review 2, R2-11).
    same_weekday = [index_date - timedelta(weeks=week) for week in range(1, LOOK_BACK_WEEKS + 1)]
    stored = day_rows(
        run.ledger.connection, source=run.source, table_name=SEC_FILING_INDEX, dates=same_weekday
    )
    history = [rows for rows in stored if rows is not None][:VOLUME_HISTORY]
    names = {"table": SEC_FILING_INDEX, "feed": INDEX_FEED}
    key_columns = build.table.select(list(SEC_TABLE_KEYS[SEC_FILING_INDEX]))
    results = [
        CheckResult(
            gate=G2,
            check_name="file_bad_row_share",
            status=PASSED,
            observed_value=build.bad_share,
            threshold=BAD_ROWS_RULE,
            feed=INDEX_FEED,
            table_name=SEC_FILING_INDEX,
            unit=unit.label,
            detail={"raw_records": build.raw_records, "quarantined_rows": build.quarantined_rows},
        ),
        *check_accounting(counts, build.table.num_rows, unit, **names),
        check_quarantine(counts, build.quarantine.num_rows, unit, **names),
        *check_rows(key_columns, unit, key=SEC_TABLE_KEYS[SEC_FILING_INDEX], **names),
        check_volume(
            build.table.num_rows,
            history,
            unit,
            accept_note=run.accept_volume,
            band=INDEX_BAND,
            **names,
        ),
    ]
    failed = problems(results)
    if failed:
        record_results_only(run, results)
        reason = "; ".join(failed)
        kept = run.lander.quarantine_verified(
            item,
            gate=G3,
            check="pre_commit_audit",
            reason=reason,
            threshold="every check of gate G3 passes",
        )
        return {**summary, "state": "quarantined", "reason": reason, "kept": kept}
    quarantine_file = write_quarantine(
        run.settings.lake_uri, run.source, INDEX_FEED, Unit(index_date), build.quarantine
    )
    result = commit(run, SEC_FILING_INDEX, build.table, unit)
    built = version(
        run,
        name=SEC_FILING_INDEX,
        feed=INDEX_FEED,
        unit=unit,
        data=build.table,
        result=result,
        objects=[item],
        started=started,
        counts=counts,
        detail={"quarantine_file": quarantine_file, "earlier_dates": build.earlier_dates},
    )
    entry = ObjectAccounting(item.key, build.raw_records, build.parsed_rows, build.quarantined_rows)
    record(run, [built], [entry], results)
    log.info("sec_index_file_built", file=item.key.name, **counts.as_dict())
    return {
        **summary,
        "state": "loaded",
        "quarantine_file": quarantine_file,
        "volume": next(r.status for r in results if r.check_name == "volume_band"),
        **table_summary(build.table, result, unit),
    }


# --- the submissions documents of the watchlist -----------------------------------------------


def _build_detail(run: SecRun, watchlist: Sequence[int]) -> dict[str, Any]:
    """Load the document of each watchlist company and replace the rows of the companies."""
    started = time.monotonic()
    documents = []
    items: list[ObjectRecord] = []
    entries: list[ObjectAccounting] = []
    states: Counter[str] = Counter()
    for cik in watchlist:
        key = ObjectKey(Dataset.SUBMISSIONS, run.day, cik_file_name(cik))
        item = run.lander.land(
            Expected(key, submissions_url(cik), cik=cik), check=json_opens, listed=True
        )
        if item.state not in _READABLE:
            states[item.state.value] += 1
            continue
        try:
            document = Submissions.parse(run.lander.read(item))
            if document.cik != cik:
                raise PayloadError("Submissions", [f"cik: the document names {document.cik}"])
        except PayloadError as error:
            if item.state is ObjectState.VERIFIED:
                run.lander.quarantine_verified(
                    item, gate=G2, check="payload_model", reason=str(error), threshold=MODEL_RULE
                )
            states[ObjectState.QUARANTINED.value] += 1
            continue
        count = document.filings.recent.count
        documents.append((document, run.source_file(item)))
        items.append(item)
        entries.append(ObjectAccounting(key, count, count))
    summary: dict[str, Any] = {
        "companies": len(watchlist),
        "documents": len(documents),
        "not_read": dict(sorted(states.items())),
    }
    if not documents:
        return summary
    build = filing_detail(documents, run.facts)
    ciks = sorted(document.cik for document, _ in documents)
    stored = stored_rows(run, SEC_FILING_DETAIL, ciks=ciks)
    new = set(
        zip(
            build.table["cik"].to_pylist(),
            build.table["accession_number"].to_pylist(),
            strict=True,
        )
    )
    old = zip(stored["cik"].to_pylist(), stored["accession_number"].to_pylist(), strict=True)
    kept = stored.filter(pa.array([pair not in new for pair in old], pa.bool_()))
    unit, data = company_unit(run, SEC_FILING_DETAIL, pa.concat_tables([build.table, kept]), ciks)
    names = {"table": SEC_FILING_DETAIL, "feed": SUBMISSIONS_FEED}
    checked = data.select([*SEC_TABLE_KEYS[SEC_FILING_DETAIL], "items"])
    results = check_rows(checked, unit, key=SEC_TABLE_KEYS[SEC_FILING_DETAIL], **names)
    results.append(
        CheckResult(
            gate=G3,
            check_name="rows_new_and_kept",
            status=PASSED if data.num_rows >= build.table.num_rows + kept.num_rows else FAILED,
            observed_value=float(data.num_rows),
            threshold="rows to commit hold the rows of the documents and the kept stored rows",
            feed=SUBMISSIONS_FEED,
            table_name=SEC_FILING_DETAIL,
            unit=unit.label,
            detail={"from_documents": build.table.num_rows, "kept_stored_rows": kept.num_rows},
        )
    )
    if problems(results):
        record_results_only(run, results)
        raise_if_failed(results, unit)
    result = commit(run, SEC_FILING_DETAIL, data, unit)
    built = version(
        run,
        name=SEC_FILING_DETAIL,
        feed=SUBMISSIONS_FEED,
        unit=unit,
        data=data,
        result=result,
        objects=items,
        started=started,
        detail={
            "ciks": ciks,
            "documents": len(documents),
            "filings": build.filings,
            "duplicate_rows": build.duplicate_rows,
            "kept_stored_rows": kept.num_rows,
        },
    )
    record(run, [built], entries, results)
    run.ledger.supersede_older_many(
        Dataset.SUBMISSIONS, run.day, [item.key.name for item in items], as_of=run.as_of
    )
    log.info("sec_filing_detail_built", companies=len(ciks), rows=data.num_rows)
    return {
        **summary,
        "filings": build.filings,
        "duplicate_rows": build.duplicate_rows,
        "kept_stored_rows": kept.num_rows,
        **table_summary(data, result, unit),
    }
