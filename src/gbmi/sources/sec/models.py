"""The shapes of the SEC files as code (blueprint 9.6, contract level; week 3 plan, WP-04).

Four documents of the SEC arrive as JSON: the ticker file, the submissions document of a
company, the facts document of a company, and the listing of the daily index files of one
quarter. A model names the fields which the platform reads. A field which the findings F2 to
F5 of the week 3 plan saw empty or null is optional, and an empty text counts as no value. A
field which a model does not name is kept and counted: :func:`unknown_fields` gives the count
by path, so a new field of the SEC shows in the quality results and breaks nothing (blueprint
7.8).

A number of a facts document is read as a decimal from the text of the JSON document and
never through a binary float: the value of a fact keeps every digit which the SEC wrote.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Iterator, Mapping
from datetime import date
from decimal import Decimal
from typing import Annotated, NamedTuple, Self

from pydantic import (
    AwareDatetime,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
    model_validator,
)

#: The identifier of a filing at the SEC.
ACCESSION_PATTERN = r"^\d{10}-\d{2}-\d{6}$"
#: A payload error names at most so many problems.
MAX_PROBLEMS = 5
_INDEX_FILE = re.compile(r"^form\.(\d{4})(\d{2})(\d{2})\.idx$")


class PayloadError(ValueError):
    """A document of the SEC does not have the shape of its model."""

    def __init__(self, model: str, problems: list[str]) -> None:
        super().__init__(f"{model}: " + "; ".join(problems))
        self.model = model
        self.problems = problems


def _blank_is_none(value: object) -> object:
    return None if isinstance(value, str) and not value.strip() else value


def _blanks_are_none(values: object) -> object:
    return [_blank_is_none(value) for value in values] if isinstance(values, list) else values


def _one_is_a_list(value: object) -> object:
    return [value] if isinstance(value, Mapping) else value


NonBlank = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
Accession = Annotated[str, StringConstraints(pattern=ACCESSION_PATTERN)]
Cik = Annotated[int, Field(gt=0, lt=10**10)]
#: A text field of the SEC: an empty text and null both mean "no value".
Text = Annotated[str | None, BeforeValidator(_blank_is_none)]
Day = Annotated[date | None, BeforeValidator(_blank_is_none)]
Moment = Annotated[AwareDatetime | None, BeforeValidator(_blank_is_none)]
Texts = Annotated[list[str | None], BeforeValidator(_blanks_are_none)]
Days = Annotated[list[date | None], BeforeValidator(_blanks_are_none)]
Moments = Annotated[list[AwareDatetime | None], BeforeValidator(_blanks_are_none)]
Numbers = list[int | None]


class Payload(BaseModel):
    """A document of the SEC, or a part of one."""

    model_config = ConfigDict(extra="allow", frozen=True, populate_by_name=True)

    @classmethod
    def parse(cls, content: bytes | str) -> Self:
        """Read one JSON document; raises PayloadError when the document does not fit."""
        try:
            document = json.loads(content, parse_float=Decimal)
        except ValueError as error:
            raise PayloadError(cls.__name__, [f"no JSON document: {error}"]) from None
        try:
            return cls.model_validate(document)
        except ValidationError as error:
            raise PayloadError(cls.__name__, _problems(error)) from None


def _problems(error: ValidationError) -> list[str]:
    """The first problems of a failed validation, each with its path and without the input."""
    items = error.errors(include_url=False, include_context=False, include_input=False)
    found = [
        f"{'.'.join(str(part) for part in item['loc']) or 'document'}: {item['msg']}"
        for item in items[:MAX_PROBLEMS]
    ]
    more = len(items) - len(found)
    return [*found, f"and {more} more"] if more else found


def unknown_fields(payload: BaseModel) -> Counter[str]:
    """The fields of a document which no model names, counted by path.

    A path names the fields from the top of the document as the SEC writes the names. A list
    is written as `[]` and a mapping whose keys are data, for example the concepts of a
    facts document, as `*`: `facts.*.*.units.*[].newField`.
    """
    found: Counter[str] = Counter()
    _collect(payload, "", found)
    return found


def _collect(value: object, path: str, found: Counter[str]) -> None:
    if isinstance(value, BaseModel):
        prefix = f"{path}." if path else ""
        for name in value.model_extra or {}:
            found[f"{prefix}{name}"] += 1
        for name, field in type(value).model_fields.items():
            _collect(getattr(value, name), f"{prefix}{field.alias or name}", found)
    elif isinstance(value, Mapping):
        for item in value.values():
            _collect(item, f"{path}.*", found)
    elif isinstance(value, list):
        for item in value:
            if isinstance(item, BaseModel | Mapping | list):
                _collect(item, f"{path}[]", found)


# --- the ticker file ------------------------------------------------------------------------


class TickerRow(Payload):
    """One ticker of one company."""

    cik: Cik
    name: NonBlank
    ticker: Text = None
    exchange: Text = None


class TickerFile(Payload):
    """`company_tickers_exchange.json`: one row for each ticker of a company (finding F2).

    The file names its columns once and writes each row as a list. The rows keep the order of
    the file: the first row of a CIK carries the primary ticker of the company.
    """

    columns: list[str] = Field(alias="fields")
    rows: list[TickerRow] = Field(alias="data")

    @model_validator(mode="before")
    @classmethod
    def _rows_by_column(cls, document: object) -> object:
        if not isinstance(document, Mapping):
            return document
        columns, data = document.get("fields"), document.get("data")
        if not isinstance(columns, list) or not isinstance(data, list):
            return document
        rows = []
        for number, row in enumerate(data):
            if not isinstance(row, list) or len(row) != len(columns):
                raise ValueError(f"row {number} does not hold one value for each of the fields")
            rows.append(dict(zip(columns, row, strict=True)))
        return {**document, "data": rows}


# --- the submissions document ---------------------------------------------------------------


class FormerName(Payload):
    """An earlier name of a company with the two times of the SEC (finding F3)."""

    name: NonBlank
    since: Moment = Field(default=None, alias="from")
    until: Moment = Field(default=None, alias="to")


class Address(Payload):
    street1: Text = None
    street2: Text = None
    city: Text = None
    state_or_country: Text = Field(default=None, alias="stateOrCountry")
    zip_code: Text = Field(default=None, alias="zipCode")
    state_or_country_description: Text = Field(default=None, alias="stateOrCountryDescription")
    is_foreign_location: int | None = Field(default=None, alias="isForeignLocation")
    foreign_state_territory: Text = Field(default=None, alias="foreignStateTerritory")
    country: Text = None
    country_code: Text = Field(default=None, alias="countryCode")


class FilingFile(Payload):
    """A file with older filings of a company. Phase P2 reads no such file (finding F4)."""

    name: NonBlank
    filing_count: int | None = Field(default=None, alias="filingCount")
    filing_from: Day = Field(default=None, alias="filingFrom")
    filing_to: Day = Field(default=None, alias="filingTo")


class Filing(NamedTuple):
    """One filing of a submissions document. The field names are the names of RecentFilings."""

    accession_number: str
    form: str
    filing_date: date
    report_date: date | None
    acceptance_time: AwareDatetime | None
    act: str | None
    file_number: str | None
    film_number: str | None
    items: str | None
    core_type: str | None
    size: int | None
    is_xbrl: int | None
    is_inline_xbrl: int | None
    is_xbrl_numeric: int | None
    primary_document: str | None
    primary_doc_description: str | None


class RecentFilings(Payload):
    """The newest filings of a company as parallel lists: position n of each list is one filing.

    The accession number, the form and the filing date are required for each filing. Every
    other value can be empty (finding F4).
    """

    accession_number: list[Accession] = Field(alias="accessionNumber")
    form: list[NonBlank]
    filing_date: list[date] = Field(alias="filingDate")
    report_date: Days | None = Field(default=None, alias="reportDate")
    acceptance_time: Moments | None = Field(default=None, alias="acceptanceDateTime")
    act: Texts | None = None
    file_number: Texts | None = Field(default=None, alias="fileNumber")
    film_number: Texts | None = Field(default=None, alias="filmNumber")
    items: Texts | None = None
    core_type: Texts | None = None
    size: Numbers | None = None
    is_xbrl: Numbers | None = Field(default=None, alias="isXBRL")
    is_inline_xbrl: Numbers | None = Field(default=None, alias="isInlineXBRL")
    is_xbrl_numeric: Numbers | None = Field(default=None, alias="isXBRLNumeric")
    primary_document: Texts | None = Field(default=None, alias="primaryDocument")
    primary_doc_description: Texts | None = Field(default=None, alias="primaryDocDescription")

    @model_validator(mode="after")
    def _one_length(self) -> Self:
        count = len(self.accession_number)
        lists = {name: getattr(self, name) for name in type(self).model_fields}
        lists.update(self.model_extra or {})
        wrong = sorted(
            f"{name} has {len(values)}"
            for name, values in lists.items()
            if isinstance(values, list) and len(values) != count
        )
        if wrong:
            raise ValueError(
                f"the lists of the filings must have one length; {count} accession numbers, "
                + ", ".join(wrong)
            )
        return self

    @property
    def count(self) -> int:
        return len(self.accession_number)

    def filings(self) -> Iterator[Filing]:
        """Each filing as one record, in the order of the document."""
        absent = [None] * self.count
        optional = [getattr(self, name) or absent for name in Filing._fields[3:]]
        rows = zip(self.accession_number, self.form, self.filing_date, *optional, strict=True)
        return (Filing(*row) for row in rows)


class Filings(Payload):
    recent: RecentFilings
    files: list[FilingFile] = Field(default_factory=list)


class Submissions(Payload):
    """The submissions document of one company (findings F3 and F4)."""

    cik: Cik
    name: NonBlank
    entity_type: Text = Field(default=None, alias="entityType")
    sic: Text = None
    sic_description: Text = Field(default=None, alias="sicDescription")
    owner_org: Text = Field(default=None, alias="ownerOrg")
    insider_transaction_for_owner_exists: int | None = Field(
        default=None, alias="insiderTransactionForOwnerExists"
    )
    insider_transaction_for_issuer_exists: int | None = Field(
        default=None, alias="insiderTransactionForIssuerExists"
    )
    tickers: list[str] = Field(default_factory=list)
    exchanges: list[str | None] = Field(default_factory=list)
    ein: Text = None
    lei: Text = None
    description: Text = None
    website: Text = None
    investor_website: Text = Field(default=None, alias="investorWebsite")
    category: Text = None
    fiscal_year_end: Text = Field(default=None, alias="fiscalYearEnd")
    state_of_incorporation: Text = Field(default=None, alias="stateOfIncorporation")
    state_of_incorporation_description: Text = Field(
        default=None, alias="stateOfIncorporationDescription"
    )
    addresses: dict[str, Address | None] = Field(default_factory=dict)
    phone: Text = None
    flags: Text = None
    former_names: list[FormerName] = Field(default_factory=list, alias="formerNames")
    filings: Filings


# --- the facts document ---------------------------------------------------------------------


class Fact(Payload):
    """One reported value (finding F5).

    `start` is missing for a value at one point in time. `fy` and `fp` describe the
    filing which reported the value, not the period of the value (finding F6), and both are
    null for some forms.
    """

    end: date
    val: Decimal
    accn: Accession
    form: NonBlank
    filed: date
    start: date | None = None
    fy: int | None = None
    fp: Text = None
    frame: Text = None


class Concept(Payload):
    label: Text = None
    description: Text = None
    units: dict[str, list[Fact]] = Field(default_factory=dict)


class FactRecord(NamedTuple):
    taxonomy: str
    concept: str
    unit: str
    fact: Fact


class CompanyFacts(Payload):
    """The facts document of one company: taxonomy, concept, unit, facts (finding F5)."""

    cik: Cik
    entity_name: NonBlank = Field(alias="entityName")
    facts: dict[str, dict[str, Concept]] = Field(default_factory=dict)

    def records(self) -> Iterator[FactRecord]:
        """Each fact with its taxonomy, its concept and its unit, in the order of the document."""
        for taxonomy, concepts in self.facts.items():
            for name, concept in concepts.items():
                for unit, facts in concept.units.items():
                    for fact in facts:
                        yield FactRecord(taxonomy, name, unit, fact)


# --- the listing of a quarter ---------------------------------------------------------------


class ListingItem(Payload):
    """One file of a folder of the SEC. The time carries no time zone (finding F10)."""

    name: NonBlank
    kind: Text = Field(default=None, alias="type")
    href: Text = None
    size: Text = None
    last_modified: Text = Field(default=None, alias="last-modified")


class Directory(Payload):
    item: Annotated[list[ListingItem], BeforeValidator(_one_is_a_list)] = Field(
        default_factory=list
    )
    name: Text = None
    parent_dir: Text = Field(default=None, alias="parent-dir")


class IndexListing(Payload):
    """The listing of the daily index files of one quarter (finding F10)."""

    directory: Directory

    def index_files(self) -> dict[date, ListingItem]:
        """The form index files which the listing names, by the date in the file name."""
        found: dict[date, ListingItem] = {}
        for item in self.directory.item:
            match = _INDEX_FILE.match(item.name)
            if match is None:
                continue
            try:
                day = date(int(match[1]), int(match[2]), int(match[3]))
            except ValueError:
                continue  # the digits of the name are no date
            found[day] = item
        return found
