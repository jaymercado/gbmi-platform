"""Pipeline PL-5: the financial facts of the watchlist companies (week 3 plan 3.3, D36).

Each run:

1. selects the watchlist companies with a filing of the forms 10-K, 10-Q, 20-F, 40-F or 6-K,
   an amendment of one of the forms, or an 8-K with item 2.02, which became known after the
   last facts load of the company. A Sunday run, the first run and a full run take every
   company;
2. loads the facts document of each selected company;
3. checks each document alone. Exact repeats of a row are dropped and counted. A document with
   two values for one key, or with more than 2% fewer facts than the load before, is
   quarantined, and the old facts of the company stay;
4. replaces the facts of the companies whose documents passed, in one commit.

One bad document holds back one company only.
"""

from __future__ import annotations

import time
from collections import Counter
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from datetime import time as clock_time
from typing import Any

import pyarrow as pa

from gbmi.lake.commit import KIND_KEYS, Scope
from gbmi.lake.tables import SEC_FILING_DETAIL, SEC_TABLE_KEYS, SEC_XBRL_FACT
from gbmi.ledger.object_states import ObjectState
from gbmi.ledger.objects import Dataset, Expected, ObjectAccounting, ObjectKey, ObjectRecord
from gbmi.logging import get_logger
from gbmi.quality.audit import (
    UnitCounts,
    check_accounting,
    check_fall,
    check_rows,
    problems,
    raise_if_failed,
)
from gbmi.quality.results import FAILED, G2, G3, CheckResult
from gbmi.sources.sec.land import json_opens
from gbmi.sources.sec.models import CompanyFacts, PayloadError
from gbmi.sources.sec.silver import (
    CIK,
    PipelineError,
    SecRun,
    commit,
    company_unit,
    record,
    record_results_only,
    stored_rows,
    table_summary,
    version,
)
from gbmi.sources.sec.urls import cik_file_name, companyfacts_url
from gbmi.transforms.sec import FactsBuild, RowsError, facts

log = get_logger(__name__)

FACTS_FEED = Dataset.COMPANYFACTS.value
#: The facts of a company may not fall by more than 2% between two loads (blueprint 8.7).
MAX_FALL = 0.02
#: The forms which start a refresh of the facts (week 3 plan 3.3 and finding F7).
TRIGGER_FORMS = frozenset({"10-K", "10-Q", "20-F", "40-F", "6-K"})
#: Item 2.02 of a form 8-K: results of operations and financial condition.
EARNINGS_ITEM = "2.02"
SUNDAY = 6
MODEL_RULE = "the document has the shape of its model and gives rows"
_READABLE = (ObjectState.VERIFIED, ObjectState.LOADED)
_ANSWERED = (ObjectState.LOADED, ObjectState.NOT_PUBLISHED)


def starts_refresh(form: str, items: Sequence[str]) -> bool:
    """Whether a filing can change the facts of its company."""
    base = form.removesuffix("/A")
    return base in TRIGGER_FORMS or (base == "8-K" and EARNINGS_ITEM in items)


def known_at(acceptance: datetime | None, filing_date: date) -> datetime:
    """The moment at which a filing became known (week 3 plan, difference D48).

    The acceptance time of the SEC; without one, 05:00 UTC of the day after the filing date,
    which is midnight in New York or later.
    """
    if acceptance is not None:
        return acceptance.astimezone(UTC)
    return datetime.combine(filing_date + timedelta(days=1), clock_time(5, 0), tzinfo=UTC)


def select_companies(
    run: SecRun, watchlist: Sequence[int], *, full: bool = False
) -> tuple[list[int], str]:
    """The companies whose facts document the run asks for, and the reason in words."""
    if full:
        return list(watchlist), "a full run was asked for"
    if run.as_of.weekday() == SUNDAY:
        return list(watchlist), "a Sunday run takes every company"
    answered = run.ledger.newest_by_cik(Dataset.COMPANYFACTS, states=_ANSWERED)
    since: dict[int, datetime] = {}
    for cik, item in answered.items():
        if item.state is ObjectState.LOADED and item.loaded_at is not None:
            since[cik] = item.loaded_at
        else:
            # The SEC had no facts document on the day: every later filing counts.
            since[cik] = datetime.combine(item.key.object_date, clock_time.max, tzinfo=UTC)
    if not since:
        return list(watchlist), "the first run takes every company"
    filings = stored_rows(
        run,
        SEC_FILING_DETAIL,
        ciks=list(watchlist),
        columns=[CIK, "form", "items", "acceptance_ts", "filing_date"],
    ).to_pylist()
    newest: dict[int, datetime] = {}
    for filing in filings:
        if starts_refresh(filing["form"], filing["items"] or []):
            moment = known_at(filing["acceptance_ts"], filing["filing_date"])
            newest[filing[CIK]] = max(moment, newest.get(filing[CIK], moment))
    selected = [
        cik for cik in watchlist if cik not in since or (cik in newest and newest[cik] > since[cik])
    ]
    return selected, "companies with a new filing since their last facts load"


def run_facts(run: SecRun, *, watchlist: Sequence[int], full: bool = False) -> dict[str, Any]:
    """One run of the pipeline; returns a summary of built-in types."""
    started = time.monotonic()
    selected, why = select_companies(run, watchlist, full=full)
    builds: dict[int, FactsBuild] = {}
    items: list[ObjectRecord] = []
    entries: list[ObjectAccounting] = []
    # The checks of each document alone. A refused document fails its own check and does
    # not stop the commit of the other companies.
    documents: list[CheckResult] = []
    states: Counter[str] = Counter()
    refused: dict[str, str] = {}
    for cik in sorted(selected):
        key = ObjectKey(Dataset.COMPANYFACTS, run.day, cik_file_name(cik))
        item = run.lander.land(
            Expected(key, companyfacts_url(cik), cik=cik), check=json_opens, listed=False
        )
        if item.state not in _READABLE:
            states[item.state.value] += 1
            continue
        build, reason = _check_document(run, cik, item, documents)
        if build is None:
            refused[str(cik)] = reason
            continue
        builds[cik] = build
        items.append(item)
        entries.append(ObjectAccounting(key, build.facts, build.facts))
    summary: dict[str, Any] = {
        "pipeline": "sec.facts",
        "day": run.day.isoformat(),
        "selection": why,
        "selected": len(selected),
        "documents": len(builds),
        "not_read": dict(sorted(states.items())),
        "refused": refused,
    }
    if not builds:
        record_results_only(run, documents)
        return {**summary, "landing": run.lander.counts.as_dict(), "seconds": run.seconds()}
    ciks = sorted(builds)
    unit, data = company_unit(
        run, SEC_XBRL_FACT, pa.concat_tables([builds[cik].table for cik in ciks]), ciks
    )
    total = sum(build.facts for build in builds.values())
    repeats = sum(build.exact_repeats for build in builds.values())
    # A unit of the whole table also holds the stored rows of the other companies.
    counts = (
        UnitCounts(
            raw_lines=total,
            parsed=total,
            quarantined=0,
            duplicates=repeats,
            committed=data.num_rows,
        )
        if unit.kind == KIND_KEYS
        else None
    )
    labels = {"table": SEC_XBRL_FACT, "feed": FACTS_FEED}
    results: list[CheckResult] = []
    if counts is not None:
        results += check_accounting(counts, data.num_rows, unit, **labels)
    results += check_rows(
        data.select(list(SEC_TABLE_KEYS[SEC_XBRL_FACT])),
        unit,
        key=SEC_TABLE_KEYS[SEC_XBRL_FACT],
        **labels,
    )
    if problems(results):
        record_results_only(run, [*documents, *results])
        raise_if_failed(results, unit)
    result = commit(run, SEC_XBRL_FACT, data, unit)
    late = sum(build.filed_before_period_end for build in builds.values())
    rounded = sum(build.rounded_values for build in builds.values())
    built = version(
        run,
        name=SEC_XBRL_FACT,
        feed=FACTS_FEED,
        unit=unit,
        data=data,
        result=result,
        objects=items,
        started=started,
        counts=counts,
        detail={
            "ciks": ciks,
            "facts": {str(cik): builds[cik].facts for cik in ciks},
            "filed_before_period_end": late,
            "rounded_values": rounded,
            "refused": refused,
        },
    )
    record(run, [built], entries, [*documents, *results])
    run.ledger.supersede_older_many(
        Dataset.COMPANYFACTS, run.day, [item.key.name for item in items], as_of=run.as_of
    )
    log.info("sec_facts_built", companies=len(ciks), rows=data.num_rows, refused=len(refused))
    return {
        **summary,
        "facts": total,
        "exact_repeats": repeats,
        "filed_before_period_end": late,
        "rounded_values": rounded,
        **table_summary(data, result, unit),
        "landing": run.lander.counts.as_dict(),
        "seconds": run.seconds(),
    }


def _check_document(
    run: SecRun, cik: int, item: ObjectRecord, results: list[CheckResult]
) -> tuple[FactsBuild | None, str]:
    """Check one facts document alone; a refused document is quarantined and gives no rows."""
    gate, check, reason = G2, "payload_model", ""
    build: FactsBuild | None = None
    try:
        document = CompanyFacts.parse(run.lander.read(item))
        if document.cik != cik:
            raise PayloadError("CompanyFacts", [f"cik: the document names {document.cik}"])
        build = facts(document, run.source_file(item), run.facts)
    except (PayloadError, RowsError) as error:
        reason = str(error)
    if build is not None:
        before = run.ledger.newest(
            Dataset.COMPANYFACTS,
            cik=cik,
            states={ObjectState.LOADED},
            not_after=run.day - timedelta(days=1),
        )
        fall = check_fall(
            build.facts,
            before.raw_records if before is not None else None,
            Scope.for_keys(CIK, [cik]),
            table=SEC_XBRL_FACT,
            feed=FACTS_FEED,
            max_fall=MAX_FALL,
            name="facts_fall",
            accept_note=run.accept_volume,
        )
        results.append(fall)
        if build.repeated_keys:
            gate, check = G3, "key_unique"
            reason = f"{build.repeated_keys} keys stand in two rows with different values"
        elif fall.status == FAILED:
            gate, check = G3, "facts_fall"
            reason = str(fall.detail["problem"])
    if not reason:
        return build, ""
    if item.state is not ObjectState.VERIFIED:
        raise PipelineError(f"the loaded facts document of CIK {cik} is refused now: {reason}")
    run.lander.quarantine_verified(
        item, gate=gate, check=check, reason=reason, threshold=MODEL_RULE
    )
    return None, reason
