"""The payload models on made-up documents (week 3 plan, WP-04).

`tests/contract/test_sec_payloads.py` holds the recorded files against the models. The tests
here show what a model does with a document which the SEC has not sent yet: a new field, an
empty value, a missing field.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest

from gbmi.sources.sec.models import (
    MAX_PROBLEMS,
    CompanyFacts,
    IndexListing,
    PayloadError,
    Submissions,
    TickerFile,
    unknown_fields,
)

FIRST, SECOND = "0001234567-26-000001", "0001234567-26-000002"


def submissions(**changes: Any) -> dict[str, Any]:
    """The smallest submissions document: the CIK, the name and two filings."""
    document: dict[str, Any] = {
        "cik": "0001234567",
        "name": "Example Holdings, Inc.",
        "filings": {
            "recent": {
                "accessionNumber": [FIRST, SECOND],
                "form": ["8-K", "4"],
                "filingDate": ["2026-11-25", "2026-11-24"],
            }
        },
    }
    document.update(changes)
    return document


def recent(**lists: Any) -> dict[str, Any]:
    document = submissions()
    document["filings"]["recent"].update(lists)
    return document


def fact(**changes: Any) -> dict[str, Any]:
    values: dict[str, Any] = {
        "end": "2026-09-30",
        "val": 100,
        "accn": FIRST,
        "form": "10-Q",
        "filed": "2026-10-30",
    }
    values.update(changes)
    return {key: value for key, value in values.items() if value is not ...}


def company_facts(*facts: dict[str, Any], **changes: Any) -> str:
    document = {
        "cik": 1234567,
        "entityName": "Example Holdings, Inc.",
        "facts": {"us-gaap": {"Revenues": {"units": {"USD": list(facts)}}}},
    }
    document.update(changes)
    return json.dumps(document)


def problems(model: type[Any], document: object) -> list[str]:
    text = document if isinstance(document, str) else json.dumps(document)
    with pytest.raises(PayloadError) as raised:
        model.parse(text)
    assert raised.value.model == model.__name__
    assert str(raised.value).startswith(f"{model.__name__}: ")
    found: list[str] = raised.value.problems
    return found


# --- the submissions document -----------------------------------------------------------------


def test_the_smallest_submissions_document_is_the_cik_the_name_and_the_filings() -> None:
    document = Submissions.parse(json.dumps(submissions()))

    assert (document.cik, document.name) == (1234567, "Example Holdings, Inc.")
    assert (document.sic, document.tickers, document.former_names) == (None, [], [])
    assert document.filings.recent.count == 2
    first, second = document.filings.recent.filings()
    assert first[:3] == (FIRST, "8-K", date(2026, 11, 25))
    assert second[:3] == (SECOND, "4", date(2026, 11, 24))
    assert set(first[3:]) == {None}  # every other value of a filing is optional
    assert unknown_fields(document) == {}


def test_an_empty_text_counts_as_no_value() -> None:
    document = Submissions.parse(
        json.dumps(
            recent(
                reportDate=["2026-11-20", ""],
                acceptanceDateTime=["2026-11-25T21:54:12.000Z", ""],
                items=["2.02,9.01", ""],
                isXBRLNumeric=[1, None],
                size=[1024, 12],
            )
            | {
                "sic": "",
                "fiscalYearEnd": None,
                "category": "   ",
                "formerNames": [{"name": "Old Name Inc", "from": "", "to": None}],
                "addresses": {"mailing": {"street1": "1 Main St", "street2": ""}, "business": None},
            }
        )
    )

    assert (document.sic, document.fiscal_year_end, document.category) == (None, None, None)
    former = document.former_names[0]
    assert (former.name, former.since, former.until) == ("Old Name Inc", None, None)
    mailing = document.addresses["mailing"]
    assert mailing is not None and (mailing.street1, mailing.street2) == ("1 Main St", None)
    assert document.addresses["business"] is None
    first, second = document.filings.recent.filings()
    assert (first.report_date, first.items, first.is_xbrl_numeric, first.size) == (
        date(2026, 11, 20),
        "2.02,9.01",
        1,
        1024,
    )
    assert first.acceptance_time == datetime(2026, 11, 25, 21, 54, 12, tzinfo=UTC)
    assert (second.report_date, second.acceptance_time, second.items) == (None, None, None)
    assert second.is_xbrl_numeric is None


def test_the_lists_of_the_filings_must_have_one_length() -> None:
    short = problems(Submissions, recent(reportDate=["2026-11-20"]))
    unknown = problems(Submissions, recent(newList=[1, 2, 3]))

    assert len(short) == 1 and short[0].startswith("filings.recent: ")
    assert "one length; 2 accession numbers, report_date has 1" in short[0]
    assert "newList has 3" in unknown[0]
    # A new list of the right length is kept and counted.
    kept = Submissions.parse(json.dumps(recent(newList=["a", "b"])))
    assert unknown_fields(kept) == {"filings.recent.newList": 1}


@pytest.mark.parametrize(
    ("document", "path"),
    [
        (submissions(name=""), "name"),
        (submissions(name=None), "name"),
        (submissions(cik="0"), "cik"),
        (submissions(cik="not a number"), "cik"),
        ({key: value for key, value in submissions().items() if key != "filings"}, "filings"),
        (recent(accessionNumber=[FIRST, "123"]), "filings.recent.accessionNumber.1"),
        (recent(form=["8-K", ""]), "filings.recent.form.1"),
        (recent(filingDate=["2026-11-25", "soon"]), "filings.recent.filingDate.1"),
        # A time without a time zone has no meaning.
        (
            recent(acceptanceDateTime=["2026-11-25T21:54:12", ""]),
            "filings.recent.acceptanceDateTime.0",
        ),
        ([1, 2, 3], "document"),
    ],
)
def test_a_submissions_document_without_a_required_value_is_refused(
    document: object, path: str
) -> None:
    found = problems(Submissions, document)

    assert len(found) == 1
    assert found[0].split(":")[0].startswith(path)


def test_a_payload_error_is_short_and_shows_no_value_of_the_document() -> None:
    marker = "SECRET-LOOKING-VALUE"
    many = recent(filingDate=[marker, marker], form=["", ""], accessionNumber=[marker, marker])

    found = problems(Submissions, many)

    assert len(found) == MAX_PROBLEMS + 1
    assert found[-1] == "and 1 more"
    assert marker not in " ".join(found)
    assert problems(Submissions, "{ no json")[0].startswith("no JSON document: ")
    assert problems(Submissions, b"\xff\xfe\xfd".decode("latin-1"))[0].startswith("no JSON")


def test_unknown_fields_are_kept_and_counted_by_path() -> None:
    document = submissions(
        newTopLevel=1,
        formerNames=[{"name": "A", "note": 1}, {"name": "B", "note": 2}],
        addresses={"mailing": {"street3": "x"}, "business": {"street3": "y", "floor": 2}},
    )
    document["filings"]["files"] = [{"name": "CIK0001234567-submissions-001.json", "pages": 3}]
    document["filings"]["extra"] = {"anything": True}

    parsed = Submissions.parse(json.dumps(document))

    assert unknown_fields(parsed) == {
        "newTopLevel": 1,
        "formerNames[].note": 2,
        "addresses.*.street3": 2,
        "addresses.*.floor": 1,
        "filings.files[].pages": 1,
        "filings.extra": 1,
    }
    assert parsed.model_extra == {"newTopLevel": 1}


# --- the ticker file ----------------------------------------------------------------------------


def test_the_ticker_file_names_its_columns_once() -> None:
    document = {
        "fields": ["cik", "name", "ticker", "exchange", "newColumn"],
        "data": [
            [1234567, "Example Holdings, Inc.", "EXH", "NYSE", "a"],
            [1234567, "Example Holdings, Inc.", "EXHWS", None, "b"],
            [7654321, "No Ticker Corp", "", "", "c"],
        ],
    }

    parsed = TickerFile.parse(json.dumps(document))

    assert [(row.cik, row.ticker, row.exchange) for row in parsed.rows] == [
        (1234567, "EXH", "NYSE"),
        (1234567, "EXHWS", None),
        (7654321, None, None),
    ]
    assert unknown_fields(parsed) == {"data[].newColumn": 3}


@pytest.mark.parametrize(
    ("document", "problem"),
    [
        ({"fields": ["cik", "name"], "data": [[1, "A", "extra"]]}, "row 0 does not hold one"),
        ({"fields": ["cik", "name"], "data": [[1, "A"], "no list"]}, "row 1 does not hold one"),
        ({"fields": ["cik"], "data": [[1]]}, "data.0.name: Field required"),
        ({"fields": ["cik", "name"], "data": [[0, "A"]]}, "data.0.cik: "),
        ({"data": [[1, "A"]]}, "fields: Field required"),
        ({"fields": "cik,name", "data": [[1, "A"]]}, "fields: "),
        ('"a JSON text, no object"', "document: Input should be"),
    ],
)
def test_a_ticker_file_of_another_shape_is_refused(document: object, problem: str) -> None:
    assert problem in problems(TickerFile, document)[0]


# --- the facts document -------------------------------------------------------------------------


def test_a_value_keeps_every_digit_which_the_sec_wrote() -> None:
    text = company_facts(fact(), fact(accn=SECOND)).replace("100", "1.10", 1)
    text = text.replace("100", "123456789012345678901234567890.123456", 1)

    values = [record.fact.val for record in CompanyFacts.parse(text).records()]

    # A binary float would give 1.1 and would lose the digits of the second value.
    assert [str(value) for value in values] == ["1.10", "123456789012345678901234567890.123456"]
    assert values[0] == Decimal("1.1")
    whole = CompanyFacts.parse(company_facts(fact(val=-5), fact(accn=SECOND, val=10**30)))
    assert [record.fact.val for record in whole.records()] == [Decimal(-5), Decimal(10**30)]


def test_a_fact_at_one_point_in_time_and_a_fact_without_a_period_are_accepted() -> None:
    document = CompanyFacts.parse(
        company_facts(
            fact(start="2026-07-01", fy=2026, fp="Q3", frame="CY2026Q3"),
            fact(accn=SECOND, fy=None, fp=None, form="DEF 14A"),
            fact(accn="0001234567-26-000003", fp=""),
        )
    )

    first, second, third = (record.fact for record in document.records())
    assert (first.start, first.fy, first.fp, first.frame) == (
        date(2026, 7, 1),
        2026,
        "Q3",
        "CY2026Q3",
    )
    assert (second.start, second.fy, second.fp, second.frame) == (None, None, None, None)
    assert third.fp is None
    assert [record[:3] for record in document.records()] == [("us-gaap", "Revenues", "USD")] * 3
    assert unknown_fields(document) == {}


@pytest.mark.parametrize("field", ["end", "val", "accn", "form", "filed"])
def test_a_fact_without_a_required_value_is_refused(field: str) -> None:
    found = problems(CompanyFacts, company_facts(fact(), fact(**{field: ...})))

    assert found == [f"facts.us-gaap.Revenues.units.USD.1.{field}: Field required"]


def test_a_facts_document_of_another_shape_is_refused() -> None:
    assert problems(CompanyFacts, company_facts(fact(val="many")))[0].startswith(
        "facts.us-gaap.Revenues.units.USD.0.val: "
    )
    assert problems(CompanyFacts, company_facts(fact(end="2026-02-30")))[0].startswith(
        "facts.us-gaap.Revenues.units.USD.0.end: "
    )
    assert problems(CompanyFacts, company_facts(entityName=""))[0].startswith("entityName: ")
    assert problems(CompanyFacts, company_facts(cik=None))[0].startswith("cik: ")


def test_a_company_without_facts_and_a_concept_without_a_label_are_accepted() -> None:
    empty = CompanyFacts.parse(json.dumps({"cik": 5, "entityName": "Empty Corp"}))
    unlabelled = CompanyFacts.parse(
        json.dumps(
            {
                "cik": 5,
                "entityName": "Foreign Corp",
                "facts": {
                    "ifrs-full": {
                        "Revenue": {
                            "label": None,
                            "description": None,
                            "units": {"USD": [fact(newField=1)], "GBP": [fact(newField=2)]},
                            "newConceptField": True,
                        }
                    }
                },
            }
        )
    )

    assert list(empty.records()) == []
    assert unlabelled.facts["ifrs-full"]["Revenue"].label is None
    assert unknown_fields(unlabelled) == {
        "facts.*.*.newConceptField": 1,
        "facts.*.*.units.*[].newField": 2,
    }


# --- the listing of a quarter ---------------------------------------------------------------------


def test_the_listing_gives_the_form_index_files_by_date() -> None:
    names = [
        "company.20261125.idx",
        "form.20261124.idx",
        "form.20261125.idx",
        "form.20261345.idx",  # the digits are no date
        "form.20261125.idx.bak",
        "master.20261125.idx",
    ]
    document = {
        "directory": {
            "item": [{"name": name, "type": "file", "size": "1 KB", "added": 1} for name in names],
            "name": "daily-index/2026/QTR4/",
            "parent-dir": "../",
        }
    }

    listing = IndexListing.parse(json.dumps(document))

    assert sorted(listing.index_files()) == [date(2026, 11, 24), date(2026, 11, 25)]
    assert listing.index_files()[date(2026, 11, 24)].name == "form.20261124.idx"
    assert (listing.directory.name, listing.directory.parent_dir) == (
        "daily-index/2026/QTR4/",
        "../",
    )
    assert unknown_fields(listing) == {"directory.item[].added": 6}


def test_a_folder_with_one_file_or_without_a_file_is_a_listing() -> None:
    one = IndexListing.parse(json.dumps({"directory": {"item": {"name": "form.20261001.idx"}}}))
    none = IndexListing.parse(json.dumps({"directory": {"name": "daily-index/2027/QTR1/"}}))

    assert list(one.index_files()) == [date(2026, 10, 1)]
    assert none.index_files() == {}
    assert problems(IndexListing, {"folder": {}}) == ["directory: Field required"]
    assert problems(IndexListing, {"directory": {"item": [{"type": "file"}]}}) == [
        "directory.item.0.name: Field required"
    ]
