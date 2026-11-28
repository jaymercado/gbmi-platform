"""The rows of the five SEC tables from made-up documents (week 3 plan 3.3, WP-06).

``tests/ledger/test_sec_pipelines.py`` builds the tables from the recorded files. The tests here
hold the rules of the pure functions, one case each.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest

from gbmi.lake.tables import SEC_SCHEMAS
from gbmi.sources.sec.facts import known_at, starts_refresh
from gbmi.sources.sec.filings import listing_name, weekdays
from gbmi.sources.sec.index import parse_index
from gbmi.sources.sec.models import CompanyFacts, Submissions, TickerFile
from gbmi.transforms import sec
from gbmi.transforms.sec import Attributes, Listed, RowsError, RunFacts, SourceFile

RUN = RunFacts(datetime(2026, 11, 26, 12, 0, tzinfo=UTC), "run-1", "test-version")
SOURCE = SourceFile("raw/sec/test/dt=2026-11-26/object", "a" * 32)
DAY = date(2026, 11, 25)
FIRST, SECOND = "0001234567-26-000001", "0001234567-26-000002"
HEADER = "Form Type   Company Name      CIK   Date Filed  File Name\n" + "-" * 60 + "\n"


def index_line(form: str, name: str, cik: int, accession: str, day: str = "20261125") -> str:
    return f"{form:<17}{name:<62}{cik:<12}{day:<12}edgar/data/{cik}/{accession}.txt"


def submissions(cik: int = 1234567, **changes: Any) -> Submissions:
    document: dict[str, Any] = {
        "cik": str(cik),
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
    return Submissions.parse(json.dumps(document))


def company_facts(*facts: dict[str, Any]) -> CompanyFacts:
    document = {
        "cik": 1234567,
        "entityName": "Example Holdings, Inc.",
        "facts": {"us-gaap": {"Revenues": {"units": {"USD": list(facts)}}}},
    }
    return CompanyFacts.parse(json.dumps(document))


def fact(**changes: Any) -> dict[str, Any]:
    values: dict[str, Any] = {
        "start": "2026-07-01",
        "end": "2026-09-30",
        "val": 100,
        "accn": FIRST,
        "fy": 2026,
        "fp": "Q3",
        "form": "10-Q",
        "filed": "2026-10-30",
    }
    values.update(changes)
    return {name: value for name, value in values.items() if value is not ...}


# --- sec_filing_index ---------------------------------------------------------------------------


def test_an_index_file_gives_one_row_for_each_filing_and_filer() -> None:
    lines = [
        index_line("4", "First Filer", 1, FIRST),
        index_line("4", "Second Filer", 2, FIRST),  # the same filing, another filer
        index_line("4", "First Filer", 1, FIRST),  # a repeated pair: the first line wins
        "a line which is no row",
        index_line("DRS", "Early Filer", 3, SECOND, day="20251201"),
    ]
    parsed = parse_index((HEADER + "\n".join(lines) + "\n").encode())

    build = sec.index_build(parsed, DAY, SOURCE, RUN)

    assert build.table.schema.names == SEC_SCHEMAS["sec_filing_index"].as_arrow().names
    assert (build.raw_records, build.parsed_rows, build.quarantined_rows) == (5, 4, 1)
    assert (build.duplicate_rows, build.earlier_dates, build.table.num_rows) == (1, 1, 3)
    assert build.bad_share == pytest.approx(0.2)
    rows = build.table.to_pylist()
    assert [(row["cik"], row["line_number"], row["index_date"]) for row in rows] == [
        (1, 3, DAY),
        (2, 4, DAY),
        (3, 7, DAY),
    ]
    assert rows[2]["filing_date"] == date(2025, 12, 1)
    assert {row["_source_file"] for row in rows} == {SOURCE.key}
    assert {row["_run_id"] for row in rows} == {"run-1"}
    assert build.quarantine.to_pylist() == [
        {
            "source_file": SOURCE.key,
            "source_md5": SOURCE.md5,
            "slot_ts": None,
            "line_number": 6,
            "reason": "the line does not end with a CIK, a date of 8 digits and a file name",
            "raw_line": "a line which is no row",
            "_run_id": "run-1",
            "_ingested_at": RUN.ingested_at,
        }
    ]
    empty = sec.index_build(parse_index(HEADER.encode()), DAY, SOURCE, RUN)
    assert (empty.table.num_rows, empty.bad_share) == (0, 0.0)


def test_the_days_and_the_listing_of_a_run() -> None:
    assert weekdays(date(2026, 11, 19), date(2026, 11, 25)) == [
        date(2026, 11, 19),
        date(2026, 11, 20),
        date(2026, 11, 23),
        date(2026, 11, 24),
        date(2026, 11, 25),
    ]
    assert weekdays(date(2026, 11, 28), date(2026, 11, 29)) == []
    assert listing_name(2026, 4) == "index.2026QTR4.json"


# --- sec_filing_detail --------------------------------------------------------------------------


def test_a_submissions_document_gives_one_row_for_each_filing() -> None:
    document = submissions(
        filings={
            "recent": {
                "accessionNumber": [FIRST, SECOND, FIRST],
                "form": ["8-K", "4", "8-K"],
                "filingDate": ["2026-11-25", "2026-11-24", "2026-11-25"],
                "reportDate": ["2026-11-20", "", "2026-11-20"],
                "acceptanceDateTime": ["2026-11-25T21:54:12.000Z", "", ""],
                "items": ["2.02, 9.01", "", "2.02"],
                "size": [1024, None, 1],
                "isXBRL": [1, 0, None],
                "isInlineXBRL": [1, None, 0],
            }
        }
    )

    build = sec.filing_detail([(document, SOURCE)], RUN)

    assert build.table.schema.names == SEC_SCHEMAS["sec_filing_detail"].as_arrow().names
    assert (build.filings, build.duplicate_rows, build.table.num_rows) == (3, 1, 2)
    first, second = build.table.to_pylist()
    assert (first["cik"], first["accession_number"], first["form"]) == (1234567, FIRST, "8-K")
    assert first["items"] == ["2.02", "9.01"]
    assert first["acceptance_ts"] == datetime(2026, 11, 25, 21, 54, 12, tzinfo=UTC)
    assert (first["size_bytes"], first["is_xbrl"], first["is_inline_xbrl"]) == (1024, True, True)
    assert (second["items"], second["report_date"], second["acceptance_ts"]) == ([], None, None)
    assert (second["size_bytes"], second["is_xbrl"], second["is_inline_xbrl"]) == (
        None,
        False,
        None,
    )
    assert second["is_xbrl_numeric"] is None
    assert sec.filing_detail([], RUN).table.num_rows == 0


@pytest.mark.parametrize(
    ("items", "expected"),
    [
        ("2.02,9.01", ["2.02", "9.01"]),
        (" 8.01 ", ["8.01"]),
        ("", []),
        (None, []),
        ("1.01,,", ["1.01"]),
    ],
)
def test_the_items_of_a_form_8k_are_split_at_the_commas(
    items: str | None, expected: list[str]
) -> None:
    assert sec.split_items(items) == expected


# --- sec_company_snapshot -----------------------------------------------------------------------


def ticker_file(*rows: list[Any]) -> TickerFile:
    document = {"fields": ["cik", "name", "ticker", "exchange"], "data": list(rows)}
    return TickerFile.parse(json.dumps(document))


def test_the_first_row_of_a_company_is_primary_and_the_lists_keep_the_file_order() -> None:
    companies = sec.universe(
        ticker_file(
            [2, "Second Corp", "SEC", None],
            [1, "First Corp", "FST", "Nasdaq"],
            [1, "First Corp", "FSTW", "Nasdaq"],
            [1, "First Corp", "FSTOF", "OTC"],
            [2, "Second Corp", "SECX", "NYSE"],
            [3, "No Ticker Corp", "", ""],
        )
    )

    assert list(companies) == [2, 1, 3]
    assert companies[1] == Listed(
        1, "First Corp", ("FST", "FSTW", "FSTOF"), ("Nasdaq", "OTC"), "FST", "Nasdaq"
    )
    # The first row of the second company names no exchange: the primary exchange is unknown.
    assert companies[2] == Listed(2, "Second Corp", ("SEC", "SECX"), ("NYSE",), "SEC", None)
    assert companies[3] == Listed(3, "No Ticker Corp", (), (), None, None)


def test_the_hash_covers_six_attributes_and_nothing_else() -> None:
    listed = Listed(1, "First Corp", ("FST",), ("Nasdaq",), "FST", "Nasdaq")
    known = Attributes(
        "7370", "Services", "DE", "1231", "Large accelerated filer", "operating", None
    )
    base = sec.attr_hash(listed, known)

    assert len(base) == 16 and int(base, 16) >= 0
    # The six attributes of the blueprint (7.10).
    changed = [
        sec.attr_hash(
            Listed(1, "First Corporation", ("FST",), ("Nasdaq",), "FST", "Nasdaq"), known
        ),
        sec.attr_hash(Listed(1, "First Corp", ("FST",), ("Nasdaq",), "FSTX", "Nasdaq"), known),
        sec.attr_hash(Listed(1, "First Corp", ("FST",), ("Nasdaq",), "FST", "NYSE"), known),
        sec.attr_hash(listed, Attributes("7372", "Services", "DE", "1231")),
        sec.attr_hash(listed, Attributes("7370", "Services", "NV", "1231")),
        sec.attr_hash(listed, Attributes("7370", "Services", "DE", "0630")),
    ]
    assert len({base, *changed}) == 7
    # Every other value leaves the hash alone.
    same = [
        sec.attr_hash(
            Listed(1, "First Corp", ("FST", "FSTW"), ("Nasdaq", "OTC"), "FST", "Nasdaq"), known
        ),
        sec.attr_hash(listed, Attributes("7370", "Other text", "DE", "1231", "x", "y", "z", DAY)),
    ]
    assert set(same) == {base}
    # No value and an empty text are two things.
    none = sec.attr_hash(listed, Attributes(None, None, "DE", "1231"))
    empty = sec.attr_hash(listed, Attributes("", None, "DE", "1231"))
    assert none != empty


def test_a_snapshot_row_carries_the_date_of_its_document() -> None:
    companies = sec.universe(
        ticker_file(
            [3, "Third", "THR", "NYSE"], [1, "First", "FST", "Nasdaq"], [2, "Second", "SND", None]
        )
    )
    fresh = sec.attributes(
        submissions(1, sic="7370", fiscalYearEnd="1231", lei=""), date(2026, 11, 18)
    )
    known = {1: fresh, 2: Attributes(sic="1000", as_of=date(2026, 11, 17))}

    table = sec.snapshot(date(2026, 11, 26), companies, known, {3}, SOURCE, RUN)

    assert table.schema.names == SEC_SCHEMAS["sec_company_snapshot"].as_arrow().names
    first, second, third = table.to_pylist()
    assert [row["cik"] for row in (first, second, third)] == [1, 2, 3]
    assert (first["sic"], first["fiscal_year_end"], first["lei"]) == ("7370", "1231", None)
    # Eight days are not stale, nine days are, and so is a company without a document.
    assert (first["attributes_as_of"], first["is_stale"]) == (date(2026, 11, 18), False)
    assert (second["attributes_as_of"], second["is_stale"]) == (date(2026, 11, 17), True)
    assert (third["attributes_as_of"], third["is_stale"], third["is_inactive"]) == (
        None,
        True,
        True,
    )
    assert (first["is_inactive"], second["exchanges"], second["primary_exchange"]) == (
        False,
        [],
        None,
    )
    assert first["attr_hash"] == sec.attr_hash(companies[1], fresh)
    # The attributes of a stored snapshot come back as they went in.
    assert sec.known_attributes(table) == {1: fresh, 2: known[2], 3: Attributes()}


# --- sec_company_former_name --------------------------------------------------------------------


def test_the_day_of_a_former_name_is_the_utc_date_of_its_time() -> None:
    document = submissions(
        formerNames=[
            # The SEC writes midnight in New York (04:00 UTC in summer, 05:00 UTC in winter) or
            # midnight UTC. Each form lies on the day which the SEC means.
            {
                "name": "Summer Name Inc",
                "from": "2005-05-06T04:00:00.000Z",
                "to": "2021-10-27T04:00:00.000Z",
            },
            {
                "name": "Winter Name Inc",
                "from": "2002-01-18T05:00:00.000Z",
                "to": "2008-04-08T04:00:00.000Z",
            },
            {
                "name": "Plain Date Corp",
                "from": "1995-03-21T00:00:00.000Z",
                "to": "2026-04-29T00:00:00.000Z",
            },
            {"name": "Summer Name Inc", "from": "2005-05-06T04:00:00.000Z", "to": None},
            {"name": "Open Name Inc", "from": "2021-10-27T04:00:00.000Z"},
            {"name": "No Start Inc", "to": "2001-01-01T05:00:00.000Z"},
        ]
    )

    build = sec.former_names([(document, SOURCE)], RUN)

    assert build.table.schema.names == SEC_SCHEMAS["sec_company_former_name"].as_arrow().names
    assert (build.names, build.duplicate_rows, build.without_start) == (6, 1, 1)
    rows = [
        (row["former_name"], row["valid_from"], row["valid_to"]) for row in build.table.to_pylist()
    ]
    assert rows == [
        ("Summer Name Inc", date(2005, 5, 6), date(2021, 10, 27)),
        ("Winter Name Inc", date(2002, 1, 18), date(2008, 4, 8)),
        # A date in New York would be 1995-03-20: the day before the day the SEC means.
        ("Plain Date Corp", date(1995, 3, 21), date(2026, 4, 29)),
        ("Open Name Inc", date(2021, 10, 27), None),
    ]
    assert set(build.table["cik"].to_pylist()) == {1234567}
    assert build.odd_times == 0
    assert sec.former_names([(submissions(), SOURCE)], RUN).table.num_rows == 0


def test_a_time_of_another_form_is_counted() -> None:
    # Neither midnight UTC nor midnight in New York: the UTC date is a guess, and the count
    # says how often the guess was made (review 2, finding R2-13).
    document = submissions(
        formerNames=[
            {
                "name": "Odd Start Inc",
                "from": "2010-06-01T12:30:00.000Z",
                "to": "2012-03-01T05:00:00.000Z",
            },
            {
                "name": "Odd End Inc",
                "from": "2012-03-01T00:00:00.000Z",
                "to": "2015-01-01T23:00:00.000Z",
            },
        ]
    )

    build = sec.former_names([(document, SOURCE)], RUN)

    rows = [
        (row["former_name"], row["valid_from"], row["valid_to"]) for row in build.table.to_pylist()
    ]
    assert rows == [
        ("Odd Start Inc", date(2010, 6, 1), date(2012, 3, 1)),
        ("Odd End Inc", date(2012, 3, 1), date(2015, 1, 1)),
    ]
    assert build.odd_times == 2


# --- sec_xbrl_fact ------------------------------------------------------------------------------


def test_a_facts_document_is_flattened_into_rows() -> None:
    document = company_facts(
        fact(frame="CY2026Q3"),
        fact(start=..., accn=SECOND, val=5, fy=None, fp=None, form="DEF 14A"),
    )

    build = sec.facts(document, SOURCE, RUN)

    assert build.table.schema.names == SEC_SCHEMAS["sec_xbrl_fact"].as_arrow().names
    assert (build.facts, build.exact_repeats, build.repeated_keys) == (2, 0, 0)
    period, instant = build.table.to_pylist()
    assert (period["cik"], period["taxonomy"], period["concept"], period["unit"]) == (
        1234567,
        "us-gaap",
        "Revenues",
        "USD",
    )
    assert (period["period_start"], period["period_end"], period["is_instant"]) == (
        date(2026, 7, 1),
        date(2026, 9, 30),
        False,
    )
    assert (period["value"], period["value_text"], period["frame"]) == (
        Decimal(100),
        "100",
        "CY2026Q3",
    )
    assert (period["fiscal_year"], period["fiscal_period"], period["filed_date"]) == (
        2026,
        "Q3",
        date(2026, 10, 30),
    )
    # A value at one point in time: the period start equals the period end (difference D39).
    assert (instant["period_start"], instant["period_end"], instant["is_instant"]) == (
        date(2026, 9, 30),
        date(2026, 9, 30),
        True,
    )
    assert (instant["fiscal_year"], instant["fiscal_period"], instant["form"]) == (
        None,
        None,
        "DEF 14A",
    )


def test_exact_repeats_are_dropped_and_two_values_for_one_key_are_counted() -> None:
    build = sec.facts(
        company_facts(
            fact(),
            fact(),  # an exact repeat
            fact(val=101),  # the same key with another value
            fact(accn=SECOND, filed="2026-09-01"),  # filed before the end of the period
        ),
        SOURCE,
        RUN,
    )

    assert (build.facts, build.exact_repeats, build.repeated_keys) == (4, 1, 1)
    assert (build.table.num_rows, build.filed_before_period_end) == (3, 1)


def test_the_value_keeps_six_decimal_places_and_the_text_keeps_every_digit() -> None:
    text = json.dumps(
        {
            "cik": 1,
            "entityName": "Decimal Corp",
            "facts": {"us-gaap": {"Ratio": {"units": {"pure": [fact(), fact(accn=SECOND)]}}}},
        }
    )
    text = text.replace("100", "0.33333333333", 1).replace("100", "1.10", 1)

    build = sec.facts(CompanyFacts.parse(text), SOURCE, RUN)

    rows = build.table.to_pylist()
    assert [(row["value"], row["value_text"]) for row in rows] == [
        (Decimal("0.333333"), "0.33333333333"),
        (Decimal("1.100000"), "1.10"),
    ]
    assert build.rounded_values == 1
    assert sec.fact_value(Decimal("0.0000005")) == Decimal("0.000000")  # half to even
    assert sec.fact_value(Decimal("0.0000015")) == Decimal("0.000002")
    assert sec.fact_value(Decimal(10**32 - 1)) == Decimal(10**32 - 1)
    with pytest.raises(RowsError, match="more than 32 digits"):
        sec.fact_value(Decimal(10**32))
    with pytest.raises(RowsError, match="more than 32 digits"):
        sec.fact_value(Decimal(-(10**33)))
    # A number far beyond any amount is refused like any other number which does not fit;
    # the first version raised an error of the decimal module (review 2, finding R2-6).
    for huge in ("1E+74", "-9E+500", "NaN", "Infinity"):
        with pytest.raises(RowsError, match="more than 32 digits"):
            sec.fact_value(Decimal(huge))
    # A rounding which reaches the limit is refused, too.
    with pytest.raises(RowsError, match="more than 32 digits"):
        sec.fact_value(Decimal("99999999999999999999999999999999.9999999"))


def test_the_text_of_a_value_has_no_exponent() -> None:
    # str() of a decimal writes 0.0000004 as 4E-7 (review 2, finding R2-7).
    assert sec.plain_text(Decimal("0.0000004")) == "0.0000004"
    assert sec.plain_text(Decimal("1E+2")) == "100"
    assert sec.plain_text(Decimal("1.10")) == "1.10"
    assert sec.plain_text(Decimal("-12345678901234567890")) == "-12345678901234567890"
    text = json.dumps(
        {
            "cik": 1,
            "entityName": "Small Corp",
            "facts": {"us-gaap": {"Ratio": {"units": {"pure": [fact(), fact(accn=SECOND)]}}}},
        }
    )
    text = text.replace("100", "0.0000004", 1).replace("100", "1e2", 1)

    rows = sec.facts(CompanyFacts.parse(text), SOURCE, RUN).table.to_pylist()

    assert [(row["value"], row["value_text"]) for row in rows] == [
        (Decimal("0.000000"), "0.0000004"),
        (Decimal("100.000000"), "100"),
    ]


def test_a_period_which_ends_before_it_starts_gives_no_rows() -> None:
    with pytest.raises(RowsError, match="us-gaap:Revenues: a period ends on 2026-06-30"):
        sec.facts(company_facts(fact(end="2026-06-30")), SOURCE, RUN)


# --- which filing starts a refresh of the facts -------------------------------------------------


@pytest.mark.parametrize(
    ("form", "items", "expected"),
    [
        ("10-K", [], True),
        ("10-Q/A", [], True),
        ("20-F", [], True),
        ("40-F", [], True),
        ("6-K", [], True),
        ("8-K", ["2.02", "9.01"], True),
        ("8-K/A", ["2.02"], True),
        ("8-K", ["8.01"], False),
        ("8-K", [], False),
        ("4", ["2.02"], False),
        ("10-KT", [], False),
        ("DEF 14A", [], False),
    ],
)
def test_the_forms_which_start_a_refresh_of_the_facts(
    form: str, items: list[str], expected: bool
) -> None:
    assert starts_refresh(form, items) is expected


def test_a_filing_is_known_at_its_acceptance_or_on_the_morning_after() -> None:
    accepted = datetime(2026, 11, 23, 21, 54, 12, tzinfo=UTC)

    assert known_at(accepted, date(2026, 11, 23)) == accepted
    # Without an acceptance time: 05:00 UTC of the next day (difference D48).
    assert known_at(None, date(2026, 11, 23)) == datetime(2026, 11, 24, 5, 0, tzinfo=UTC)
