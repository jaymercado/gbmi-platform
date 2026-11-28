"""Rows for the five SEC tables from the validated documents (week 3 plan 3.3, WP-06).

Pure functions: a validated document goes in, and an Arrow table with the schema of the silver
table comes out, together with the counts which the accounting needs. No function reads a
file, a table or a clock. The pipelines of ``gbmi.sources.sec`` decide what is read and what
is committed.
"""

from __future__ import annotations

import decimal
import hashlib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow as pa

from gbmi.lake.tables import (
    SEC_COMPANY_FORMER_NAME,
    SEC_COMPANY_SNAPSHOT,
    SEC_FILING_DETAIL,
    SEC_FILING_INDEX,
    SEC_SCHEMAS,
    SEC_XBRL_FACT,
)
from gbmi.quality.quarantine import QUARANTINE_SCHEMA
from gbmi.sources.sec.index import ParsedIndex
from gbmi.sources.sec.models import CompanyFacts, Submissions, TickerFile

#: The times of a former name are midnight in New York (week 3 plan, finding F3).
NEW_YORK = ZoneInfo("America/New_York")
#: A company whose submissions document is older counts as stale (week 3 plan 3.3).
STALE_AFTER_DAYS = 8
#: The value of a fact has six decimal places and at most 32 digits before the point.
_SIX_PLACES = Decimal("0.000001")
_VALUE_LIMIT = Decimal(10) ** 32
_WIDE = decimal.Context(prec=80, rounding=decimal.ROUND_HALF_EVEN)
_NULL, _SEPARATOR = "\x00", "\x1f"


class RowsError(ValueError):
    """A validated document cannot give rows: a value breaks a rule of the table."""


@dataclass(frozen=True)
class SourceFile:
    """The bronze object which carried a row: its key below the lake root, and its MD5."""

    key: str
    md5: str


@dataclass(frozen=True)
class RunFacts:
    """What a run writes into the provenance columns of each row."""

    ingested_at: datetime
    run_id: str | None
    pipeline_version: str


def _table(name: str, columns: Mapping[str, list[Any]]) -> pa.Table:
    return pa.Table.from_pydict(dict(columns), schema=SEC_SCHEMAS[name].as_arrow())


def _provenance(sources: Sequence[SourceFile], run: RunFacts) -> dict[str, list[Any]]:
    count = len(sources)
    return {
        "_source_file": [source.key for source in sources],
        "_source_md5": [source.md5 for source in sources],
        "_ingested_at": [run.ingested_at] * count,
        "_run_id": [run.run_id] * count,
        "_pipeline_version": [run.pipeline_version] * count,
    }


# --- sec_filing_index -------------------------------------------------------------------------


@dataclass(frozen=True)
class IndexBuild:
    """The rows of one index file, and where every record of the file went."""

    table: pa.Table
    #: The lines which are no row, in the schema of a quarantine file.
    quarantine: pa.Table
    raw_records: int
    parsed_rows: int
    quarantined_rows: int
    #: Rows which repeat a pair of accession number and CIK (finding F11 c).
    duplicate_rows: int
    #: Rows whose date lies before the date of the file (finding F11 d).
    earlier_dates: int

    @property
    def bad_share(self) -> float:
        return self.quarantined_rows / self.raw_records if self.raw_records else 0.0


def index_build(
    parsed: ParsedIndex, index_date: date, source: SourceFile, run: RunFacts
) -> IndexBuild:
    """The rows of one index file: one row for each filing and filer, the first line wins."""
    seen: set[tuple[str, int]] = set()
    kept = []
    for row in parsed.rows:
        pair = (row.accession_number, row.cik)
        if pair not in seen:
            seen.add(pair)
            kept.append(row)
    table = _table(
        SEC_FILING_INDEX,
        {
            "index_date": [index_date] * len(kept),
            "accession_number": [row.accession_number for row in kept],
            "cik": [row.cik for row in kept],
            "form": [row.form for row in kept],
            "company_name": [row.company_name for row in kept],
            "filing_date": [row.filing_date for row in kept],
            "file_name": [row.file_name for row in kept],
            "line_number": [row.line_number for row in kept],
            **_provenance([source] * len(kept), run),
        },
    )
    rejected = parsed.rejected
    quarantine = pa.Table.from_pydict(
        {
            "source_file": [source.key] * len(rejected),
            "source_md5": [source.md5] * len(rejected),
            "slot_ts": [None] * len(rejected),
            "line_number": [line.line_number for line in rejected],
            "reason": [line.reason for line in rejected],
            "raw_line": [line.text for line in rejected],
            "_run_id": [run.run_id] * len(rejected),
            "_ingested_at": [run.ingested_at] * len(rejected),
        },
        schema=QUARANTINE_SCHEMA,
    )
    return IndexBuild(
        table=table,
        quarantine=quarantine,
        raw_records=parsed.raw_records,
        parsed_rows=len(parsed.rows),
        quarantined_rows=len(rejected),
        duplicate_rows=len(parsed.rows) - len(kept),
        earlier_dates=sum(1 for row in kept if row.filing_date < index_date),
    )


# --- sec_filing_detail ------------------------------------------------------------------------


@dataclass(frozen=True)
class DetailBuild:
    table: pa.Table
    #: The filings which the documents list.
    filings: int
    #: Filings which repeat an accession number inside one document.
    duplicate_rows: int


def split_items(items: str | None) -> list[str]:
    """The item numbers of a form 8-K: the SEC writes one text with commas (finding F4)."""
    return [item.strip() for item in (items or "").split(",") if item.strip()]


def _flag(value: int | None) -> bool | None:
    return None if value is None else bool(value)


def filing_detail(
    documents: Iterable[tuple[Submissions, SourceFile]], run: RunFacts
) -> DetailBuild:
    """One row for each filing which the submissions documents list."""
    rows: list[tuple[Any, ...]] = []
    sources: list[SourceFile] = []
    filings = 0
    for document, source in documents:
        seen: set[str] = set()
        for filing in document.filings.recent.filings():
            filings += 1
            if filing.accession_number in seen:
                continue
            seen.add(filing.accession_number)
            sources.append(source)
            rows.append(
                (
                    document.cik,
                    filing.accession_number,
                    filing.form,
                    filing.filing_date,
                    filing.report_date,
                    filing.acceptance_time,
                    filing.act,
                    filing.file_number,
                    filing.film_number,
                    split_items(filing.items),
                    filing.core_type,
                    filing.size,
                    _flag(filing.is_xbrl),
                    _flag(filing.is_inline_xbrl),
                    _flag(filing.is_xbrl_numeric),
                    filing.primary_document,
                    filing.primary_doc_description,
                )
            )
    names = [field.name for field in SEC_SCHEMAS[SEC_FILING_DETAIL].fields][:17]
    columns = {name: [row[position] for row in rows] for position, name in enumerate(names)}
    table = _table(SEC_FILING_DETAIL, {**columns, **_provenance(sources, run)})
    return DetailBuild(table=table, filings=filings, duplicate_rows=filings - len(rows))


# --- sec_company_snapshot ---------------------------------------------------------------------


@dataclass(frozen=True)
class Listed:
    """What the ticker file says about one company (finding F2)."""

    cik: int
    name: str
    tickers: tuple[str, ...]
    exchanges: tuple[str, ...]
    primary_ticker: str | None
    primary_exchange: str | None


@dataclass(frozen=True)
class Attributes:
    """What the submissions document says about one company, and the date of the document."""

    sic: str | None = None
    sic_description: str | None = None
    state_of_incorporation: str | None = None
    fiscal_year_end: str | None = None
    category: str | None = None
    entity_type: str | None = None
    lei: str | None = None
    as_of: date | None = None


def universe(ticker_file: TickerFile) -> dict[int, Listed]:
    """The companies of the ticker file, in file order. The first row of a CIK is primary."""
    names: dict[int, str] = {}
    tickers: dict[int, list[str]] = {}
    exchanges: dict[int, list[str]] = {}
    primary: dict[int, tuple[str | None, str | None]] = {}
    for row in ticker_file.rows:
        if row.cik not in names:
            names[row.cik] = row.name
            primary[row.cik] = (row.ticker, row.exchange)
            tickers[row.cik], exchanges[row.cik] = [], []
        if row.ticker is not None and row.ticker not in tickers[row.cik]:
            tickers[row.cik].append(row.ticker)
        if row.exchange is not None and row.exchange not in exchanges[row.cik]:
            exchanges[row.cik].append(row.exchange)
    return {
        cik: Listed(
            cik=cik,
            name=name,
            tickers=tuple(tickers[cik]),
            exchanges=tuple(exchanges[cik]),
            primary_ticker=primary[cik][0],
            primary_exchange=primary[cik][1],
        )
        for cik, name in names.items()
    }


def attributes(document: Submissions, as_of: date) -> Attributes:
    return Attributes(
        sic=document.sic,
        sic_description=document.sic_description,
        state_of_incorporation=document.state_of_incorporation,
        fiscal_year_end=document.fiscal_year_end,
        category=document.category,
        entity_type=document.entity_type,
        lei=document.lei,
        as_of=as_of,
    )


def attr_hash(listed: Listed, known: Attributes) -> str:
    """The first 16 hexadecimal characters of the SHA-256 hash over six attributes.

    The name, the primary ticker, the primary exchange, the SIC code, the state and the end
    of the fiscal year (blueprint 7.10). No value and an empty text give different hashes.
    """
    values = (
        listed.name,
        listed.primary_ticker,
        listed.primary_exchange,
        known.sic,
        known.state_of_incorporation,
        known.fiscal_year_end,
    )
    text = _SEPARATOR.join(_NULL if value is None else value for value in values)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def is_stale(known: Attributes, snapshot_date: date) -> bool:
    """True when the document is older than 8 days on the snapshot date, or unknown."""
    return known.as_of is None or (snapshot_date - known.as_of).days > STALE_AFTER_DAYS


def snapshot(
    snapshot_date: date,
    companies: Mapping[int, Listed],
    known: Mapping[int, Attributes],
    inactive: frozenset[int] | set[int],
    source: SourceFile,
    run: RunFacts,
) -> pa.Table:
    """One row for each company of the ticker file of the day, in the order of the CIK."""
    order = sorted(companies)
    listed = [companies[cik] for cik in order]
    facts = [known.get(cik, Attributes()) for cik in order]
    return _table(
        SEC_COMPANY_SNAPSHOT,
        {
            "snapshot_date": [snapshot_date] * len(order),
            "cik": order,
            "name": [company.name for company in listed],
            "tickers": [list(company.tickers) for company in listed],
            "exchanges": [list(company.exchanges) for company in listed],
            "primary_ticker": [company.primary_ticker for company in listed],
            "primary_exchange": [company.primary_exchange for company in listed],
            "sic": [fact.sic for fact in facts],
            "sic_description": [fact.sic_description for fact in facts],
            "state_of_incorporation": [fact.state_of_incorporation for fact in facts],
            "fiscal_year_end": [fact.fiscal_year_end for fact in facts],
            "category": [fact.category for fact in facts],
            "entity_type": [fact.entity_type for fact in facts],
            "lei": [fact.lei for fact in facts],
            "attr_hash": [
                attr_hash(company, fact) for company, fact in zip(listed, facts, strict=True)
            ],
            "attributes_as_of": [fact.as_of for fact in facts],
            "is_stale": [is_stale(fact, snapshot_date) for fact in facts],
            "is_inactive": [cik in inactive for cik in order],
            **_provenance([source] * len(order), run),
        },
    )


def known_attributes(rows: pa.Table) -> dict[int, Attributes]:
    """The attributes of each company in the rows of a stored snapshot."""
    columns = rows.select(
        [
            "cik",
            "sic",
            "sic_description",
            "state_of_incorporation",
            "fiscal_year_end",
            "category",
            "entity_type",
            "lei",
            "attributes_as_of",
        ]
    ).to_pydict()
    names = [name for name in columns if name != "cik"]
    return {
        cik: Attributes(*(columns[name][position] for name in names))
        for position, cik in enumerate(columns["cik"])
    }


# --- sec_company_former_name ------------------------------------------------------------------


@dataclass(frozen=True)
class FormerNamesBuild:
    table: pa.Table
    #: The former names which the documents list.
    names: int
    #: Exact repeats of a name and its first day inside one document.
    duplicate_rows: int
    #: Names without a first day: the key of the table needs the day.
    without_start: int


class FormerNames:
    """Collects the former names of documents which are read one by one.

    A bulk day reads the documents of about 8,000 companies. The collector keeps the few
    values of each document, so no document has to stay in memory.
    """

    def __init__(self) -> None:
        self._rows: list[tuple[int, str, date, date | None]] = []
        self._sources: list[SourceFile] = []
        self.names = 0
        self.without_start = 0

    def add(self, document: Submissions, source: SourceFile) -> None:
        """The former names of one document, with the two times of the SEC as days in New York."""
        seen: set[tuple[str, date]] = set()
        for former in document.former_names:
            self.names += 1
            if former.since is None:
                self.without_start += 1
                continue
            start = former.since.astimezone(NEW_YORK).date()
            end = former.until.astimezone(NEW_YORK).date() if former.until else None
            if (former.name, start) in seen:
                continue
            seen.add((former.name, start))
            self._sources.append(source)
            self._rows.append((document.cik, former.name, start, end))

    def build(self, run: RunFacts) -> FormerNamesBuild:
        rows = self._rows
        table = _table(
            SEC_COMPANY_FORMER_NAME,
            {
                "cik": [row[0] for row in rows],
                "former_name": [row[1] for row in rows],
                "valid_from": [row[2] for row in rows],
                "valid_to": [row[3] for row in rows],
                **_provenance(self._sources, run),
            },
        )
        return FormerNamesBuild(
            table=table,
            names=self.names,
            duplicate_rows=self.names - self.without_start - len(rows),
            without_start=self.without_start,
        )


def former_names(
    documents: Iterable[tuple[Submissions, SourceFile]], run: RunFacts
) -> FormerNamesBuild:
    """One row for each former name of the documents."""
    collected = FormerNames()
    for document, source in documents:
        collected.add(document, source)
    return collected.build(run)


# --- sec_xbrl_fact ----------------------------------------------------------------------------


@dataclass(frozen=True)
class FactsBuild:
    """The rows of one facts document, and what the checks of the document need."""

    table: pa.Table
    #: The facts which the document lists.
    facts: int
    #: Facts which repeat another fact of the document in every value; dropped.
    exact_repeats: int
    #: Keys which stand in two rows with different values. A document with such a key is
    #: refused as a whole (week 3 plan 3.3).
    repeated_keys: int
    #: Facts which were filed before the end of their period: counted, not refused.
    filed_before_period_end: int
    #: Values with more than six decimal places: ``value`` is rounded, ``value_text`` is not.
    rounded_values: int


def fact_value(value: Decimal) -> Decimal:
    """The value with six decimal places. Raises RowsError when the value does not fit."""
    rounded = _WIDE.quantize(value, _SIX_PLACES)
    # copy_abs does not round; abs() would round to the 28 digits of the default context.
    if rounded.copy_abs() >= _VALUE_LIMIT:
        raise RowsError("a value has more than 32 digits before the decimal point")
    return rounded


def facts(document: CompanyFacts, source: SourceFile, run: RunFacts) -> FactsBuild:
    """Flatten taxonomy, concept and unit into rows (blueprint 8.7, stage 3).

    A fact at one point in time receives a period start equal to the period end, and a flag
    (difference D39). Raises RowsError when a period ends before the period starts.
    """
    rows: dict[tuple[Any, ...], tuple[Any, ...]] = {}
    keys: set[tuple[Any, ...]] = set()
    count = repeats = late = rounded = 0
    for record in document.records():
        fact = record.fact
        count += 1
        start = fact.start if fact.start is not None else fact.end
        if start > fact.end:
            raise RowsError(
                f"{record.taxonomy}:{record.concept}: a period ends on {fact.end.isoformat()} "
                f"and starts on {start.isoformat()}"
            )
        value = fact_value(fact.val)
        key = (record.taxonomy, record.concept, record.unit, start, fact.end, fact.accn)
        row = (
            *key,
            fact.start is None,
            value,
            str(fact.val),
            fact.fy,
            fact.fp,
            fact.form,
            fact.filed,
            fact.frame,
        )
        if row in rows:
            repeats += 1
            continue
        rows[row] = row
        keys.add(key)
        late += fact.filed < fact.end
        rounded += value != fact.val
    kept = list(rows)
    names = [field.name for field in SEC_SCHEMAS[SEC_XBRL_FACT].fields][1:15]
    columns = {name: [row[position] for row in kept] for position, name in enumerate(names)}
    table = _table(
        SEC_XBRL_FACT,
        {"cik": [document.cik] * len(kept), **columns, **_provenance([source] * len(kept), run)},
    )
    return FactsBuild(
        table=table,
        facts=count,
        exact_repeats=repeats,
        repeated_keys=len(kept) - len(keys),
        filed_before_period_end=late,
        rounded_values=rounded,
    )
