"""Pipeline PL-3: the company universe of a day, and the former names (week 3 plan 3.3, D35).

Each run:

1. loads the ticker file. The universe of the day is every CIK of the file;
2. asks for the submissions document of each company which is new in the ticker file. On a
   Sunday and in the first run the documents of every company come from the bulk archive;
3. builds the snapshot of the day from the newest earlier snapshot. Each attribute has one
   source: the name, the tickers and the exchanges come from the ticker file; the SIC code,
   the state, the end of the fiscal year, the category and the former names come from the
   submissions document. A company without a new document keeps its attributes, with the
   date of the document. So one company gives the same ``attr_hash`` on a weekday and on a
   bulk day;
4. replaces the former names of the companies whose documents were read.

The pipeline reads documents in the states ``verified`` and ``loaded``, and marks a document
``loaded`` which no other pipeline loads: the filings pipeline owns the documents of the
watchlist.
"""

from __future__ import annotations

import time
from collections import Counter
from collections.abc import Sequence
from datetime import date
from typing import Any

from gbmi.lake.commit import MAX_SCOPE_KEYS, Scope
from gbmi.lake.tables import SEC_COMPANY_FORMER_NAME, SEC_COMPANY_SNAPSHOT, SEC_TABLE_KEYS
from gbmi.ledger.object_states import ObjectState
from gbmi.ledger.objects import Dataset, Expected, ObjectAccounting, ObjectKey, ObjectRecord
from gbmi.logging import get_logger
from gbmi.quality.audit import check_change, check_rows, problems, raise_if_failed
from gbmi.quality.results import G2, G3, PASSED, CheckResult
from gbmi.sources.sec.bulk import load_archive
from gbmi.sources.sec.land import json_opens
from gbmi.sources.sec.models import PayloadError, Submissions, TickerFile, unknown_fields
from gbmi.sources.sec.silver import (
    PipelineError,
    SecRun,
    commit,
    company_unit,
    record,
    record_results_only,
    table_summary,
    version,
)
from gbmi.sources.sec.urls import (
    TICKER_FILE_NAME,
    TICKER_FILE_URL,
    cik_file_name,
    submissions_url,
)
from gbmi.transforms.sec import (
    Attributes,
    FormerNames,
    attributes,
    known_attributes,
    snapshot,
    universe,
)

log = get_logger(__name__)

REFERENCE_FEED = Dataset.REFERENCE.value
SUBMISSIONS_FEED = Dataset.SUBMISSIONS.value
#: The universe may differ by 5% from the snapshot before (blueprint 8.5).
UNIVERSE_TOLERANCE = 0.05
#: A daily run asks for the documents of at most so many new companies. More new companies
#: wait for the archive of the next Sunday.
MAX_NEW_COMPANIES = 200
SUNDAY = 6
MODEL_RULE = "the document has the shape of its model"
_READABLE = (ObjectState.VERIFIED, ObjectState.LOADED)


def run_reference(run: SecRun, *, watchlist: Sequence[int], bulk: bool = False) -> dict[str, Any]:
    """One run of the pipeline; returns a summary of built-in types.

    ``bulk`` asks for the archive on any day. Raises PipelineError when the ticker file of the
    day cannot be used: no snapshot is built then.
    """
    started = time.monotonic()
    day = run.day
    ticker = _ticker_file(run)
    ticker_file = _parse_ticker_file(run, ticker)
    companies = universe(ticker_file)
    previous_date = _previous_snapshot_date(run)
    previous = _snapshot_before(run, previous_date)
    new = sorted(set(companies) - set(previous)) if previous_date else []

    # The documents of the day.
    readable = run.ledger.newest_by_cik(Dataset.SUBMISSIONS, states=_READABLE, not_after=day)
    with_document = {cik for cik, item in readable.items() if item.key.object_date == day}
    archive: dict[str, Any] | None = None
    asked = 0
    if bulk or previous_date is None or run.as_of.weekday() == SUNDAY:
        archive = load_archive(run, [cik for cik in companies if cik not in with_document])
    else:
        watched = set(watchlist)
        for cik in new[:MAX_NEW_COMPANIES]:
            if cik in with_document or cik in watched:
                continue
            key = ObjectKey(Dataset.SUBMISSIONS, day, cik_file_name(cik))
            # Not "listed": the ticker file names the company, not its document. A company
            # which is new today can be without a document, and three such companies in a
            # row must not open the circuit (review 2, finding R2-12).
            run.lander.land(
                Expected(key, submissions_url(cik), cik=cik), check=json_opens, listed=False
            )
            asked += 1
    # An operator can have sent quarantined documents of the day back: their bytes are in
    # bronze, and no request is needed (review 2, finding R2-2).
    waiting = run.ledger.select(
        states={ObjectState.EXPECTED}, dataset=Dataset.SUBMISSIONS, first=day, last=day
    )
    for returned in waiting:
        run.lander.recover(returned.key, check=json_opens)

    # The attributes: the snapshot before, then every document which is newer.
    known = {cik: previous[cik] for cik in companies if cik in previous}
    names = FormerNames()
    refreshed: list[int] = []
    read: list[ObjectRecord] = []
    entries: list[ObjectAccounting] = []
    refused = 0
    unknown: Counter[str] = Counter()
    watched = set(watchlist)
    readable = run.ledger.newest_by_cik(Dataset.SUBMISSIONS, states=_READABLE, not_after=day)
    for cik in sorted(companies):
        item = readable.get(cik)
        before = previous.get(cik)
        if item is None or (
            before is not None and before.as_of is not None and item.key.object_date <= before.as_of
        ):
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
            refused += 1
            continue
        known[cik] = attributes(document, item.key.object_date)
        names.add(document, run.source_file(item))
        unknown.update(unknown_fields(document))
        refreshed.append(cik)
        read.append(item)
        if item.state is ObjectState.VERIFIED and cik not in watched:
            count = document.filings.recent.count
            entries.append(ObjectAccounting(item.key, count, count))
    newest = run.ledger.newest_by_cik(Dataset.SUBMISSIONS, not_after=day)
    inactive = {
        cik for cik in companies if cik in newest and newest[cik].state is ObjectState.NOT_PUBLISHED
    }

    # The snapshot of the day.
    unit = Scope.for_date("snapshot_date", day)
    data = snapshot(day, companies, known, inactive, run.source_file(ticker), run.facts)
    labels = {"table": SEC_COMPANY_SNAPSHOT, "feed": REFERENCE_FEED}
    checked = data.select([*SEC_TABLE_KEYS[SEC_COMPANY_SNAPSHOT], "tickers", "exchanges"])
    results = [
        CheckResult(
            gate=G2,
            check_name="unknown_fields",
            status=PASSED,
            observed_value=float(sum(unknown.values())),
            threshold="a field which no model names is kept and counted",
            feed=SUBMISSIONS_FEED,
            unit=unit.label,
            detail={"documents": len(read), "fields": dict(unknown.most_common(20))},
        ),
        *check_rows(checked, unit, key=SEC_TABLE_KEYS[SEC_COMPANY_SNAPSHOT], **labels),
        check_change(
            data.num_rows,
            len(previous) if previous_date else None,
            unit,
            tolerance=UNIVERSE_TOLERANCE,
            name="universe_change",
            accept_note=run.accept_volume,
            **labels,
        ),
    ]
    failed = problems(results)
    if failed:
        record_results_only(run, results)
        reason = "; ".join(failed)
        if ticker.state is ObjectState.VERIFIED:
            run.lander.quarantine_verified(
                ticker,
                gate=G3,
                check="pre_commit_audit",
                reason=reason,
                threshold="every check of gate G3 passes",
            )
        raise PipelineError(f"the snapshot of {day.isoformat()} failed gate G3: {reason}")
    result = commit(run, SEC_COMPANY_SNAPSHOT, data, unit)
    stale = sum(data["is_stale"].to_pylist())
    detail = {
        "ticker_rows": len(ticker_file.rows),
        "companies": len(companies),
        "new_companies": len(new),
        "documents_asked": asked,
        "documents_read": len(read),
        "documents_refused": refused,
        "carried_forward": len(companies) - len(refreshed),
        "stale": stale,
        "inactive": len(inactive),
        "previous_snapshot": previous_date.isoformat() if previous_date else None,
        "archive": archive,
    }
    versions = [
        version(
            run,
            name=SEC_COMPANY_SNAPSHOT,
            feed=REFERENCE_FEED,
            unit=unit,
            data=data,
            result=result,
            objects=[ticker],
            started=started,
            detail=detail,
        )
    ]
    summary: dict[str, Any] = {
        "pipeline": "sec.reference",
        "day": day.isoformat(),
        **detail,
        "snapshot": table_summary(data, result, unit),
        "universe_change": results[-1].status,
    }

    # The former names of the companies whose documents were read.
    if refreshed:
        former_started = time.monotonic()
        former = names.build(run.facts)
        former_unit, former_data = company_unit(
            run, SEC_COMPANY_FORMER_NAME, former.table, refreshed
        )
        former_labels = {"table": SEC_COMPANY_FORMER_NAME, "feed": SUBMISSIONS_FEED}
        former_results = check_rows(
            former_data.select(list(SEC_TABLE_KEYS[SEC_COMPANY_FORMER_NAME])),
            former_unit,
            key=SEC_TABLE_KEYS[SEC_COMPANY_FORMER_NAME],
            **former_labels,
        )
        if problems(former_results):
            record_results_only(run, [*results, *former_results])
            raise_if_failed(former_results, former_unit)
        former_result = commit(run, SEC_COMPANY_FORMER_NAME, former_data, former_unit)
        results += former_results
        versions.append(
            version(
                run,
                name=SEC_COMPANY_FORMER_NAME,
                feed=SUBMISSIONS_FEED,
                unit=former_unit,
                data=former_data,
                result=former_result,
                objects=read,
                started=former_started,
                detail={
                    "companies": len(refreshed),
                    "ciks": refreshed if len(refreshed) <= MAX_SCOPE_KEYS else None,
                    "names": former.names,
                    "duplicate_rows": former.duplicate_rows,
                    "without_start": former.without_start,
                    "not_midnight": former.not_midnight,
                },
            )
        )
        summary["former_names"] = {
            "companies": len(refreshed),
            "names": former.names,
            "without_start": former.without_start,
            "not_midnight": former.not_midnight,
            **table_summary(former_data, former_result, former_unit),
        }

    ticker_rows = len(ticker_file.rows)
    record(
        run, versions, [ObjectAccounting(ticker.key, ticker_rows, ticker_rows), *entries], results
    )
    run.ledger.supersede_older(ticker.key, as_of=run.as_of)
    by_date: dict[date, list[str]] = {}
    for entry in entries:
        by_date.setdefault(entry.key.object_date, []).append(entry.key.name)
    for object_date, loaded in sorted(by_date.items()):
        run.ledger.supersede_older_many(Dataset.SUBMISSIONS, object_date, loaded, as_of=run.as_of)
    log.info(
        "sec_snapshot_built",
        day=day.isoformat(),
        companies=len(companies),
        documents_read=len(read),
        stale=stale,
    )
    return {
        **summary,
        "documents_loaded": len(entries),
        "landing": run.lander.counts.as_dict(),
        "seconds": run.seconds(),
    }


def _ticker_file(run: SecRun) -> ObjectRecord:
    key = ObjectKey(Dataset.REFERENCE, run.day, TICKER_FILE_NAME)
    item = run.lander.land(Expected(key, TICKER_FILE_URL), check=json_opens, listed=False)
    if item.state not in _READABLE:
        raise PipelineError(
            f"the ticker file of {run.day.isoformat()} is {item.state.value}: "
            "no snapshot can be built"
        )
    return item


def _parse_ticker_file(run: SecRun, item: ObjectRecord) -> TickerFile:
    try:
        return TickerFile.parse(run.lander.read(item))
    except PayloadError as error:
        if item.state is ObjectState.VERIFIED:
            run.lander.quarantine_verified(
                item, gate=G2, check="payload_model", reason=str(error), threshold=MODEL_RULE
            )
        raise PipelineError(
            f"the ticker file of {run.day.isoformat()} was refused: {error}"
        ) from None


def _previous_snapshot_date(run: SecRun) -> date | None:
    """The date of the newest snapshot before the day of the run, by the partition versions."""
    row = run.ledger.connection.execute(
        "SELECT max(slot_date) FROM ops.silver_partition WHERE source = %s AND table_name = %s "
        "AND unit_kind = 'day' AND slot_date < %s",
        (run.source, SEC_COMPANY_SNAPSHOT, run.day),
    ).fetchone()
    return row[0] if row is not None and row[0] is not None else None


def _snapshot_before(run: SecRun, snapshot_date: date | None) -> dict[int, Attributes]:
    """The attributes of each company in the snapshot before; empty in the first run."""
    if snapshot_date is None:
        return {}
    table = run.table(SEC_COMPANY_SNAPSHOT)
    if table.current_snapshot() is None:
        return {}
    unit = Scope.for_date("snapshot_date", snapshot_date)
    return known_attributes(table.scan(row_filter=unit.row_filter).to_arrow())
