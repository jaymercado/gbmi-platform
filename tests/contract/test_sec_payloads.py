"""The recorded SEC files against the payload models (blueprint 9.6; week 3 plan, WP-04).

The files of `tests/fixtures/sec` are shortened copies of the answers of the SEC of 2026-11-28
(`ATTRIBUTION.md` in the folder). A model which stops to fit a recorded file fails here, and
so does a recorded file which was changed by hand.
"""

from __future__ import annotations

import hashlib
import io
import re
import zipfile
from collections import Counter
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from gbmi.sources.sec.index import NAME_POSITION, parse_index
from gbmi.sources.sec.models import (
    CompanyFacts,
    IndexListing,
    Submissions,
    TickerFile,
    unknown_fields,
)
from gbmi.sources.sec.urls import (
    SUBMISSIONS_ARCHIVE_URL,
    TICKER_FILE_URL,
    cik_file_name,
    companyfacts_url,
    index_file_url,
    index_listing_url,
    split,
    submissions_url,
)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "sec"
META, NVIDIA, MICROSOFT, MATTEL, THOMSON_REUTERS = 1326801, 1045810, 789019, 63276, 1075124
EASYJET, ALPHABET, HASBRO, AMAZON, NETFLIX = 1470419, 1652044, 46080, 1018724, 1065280
#: The five demo companies: a submissions document and a facts document each.
DEMO = (META, NVIDIA, MICROSOFT, MATTEL, THOMSON_REUTERS)
#: Every company with a submissions document.
DOCUMENTED = (*DEMO, EASYJET, ALPHABET, HASBRO)
INDEX_DAY = date(2026, 11, 25)
_TABLE_ROW = re.compile(r"^\| `(?P<name>[^`]+)` \| (?P<size>[\d,]+) \| `(?P<md5>[0-9a-f]{32})` \|$")


def recorded(url: str) -> bytes:
    host, path = split(url)
    return (FIXTURES / host / path.lstrip("/")).read_bytes()


def submissions(cik: int) -> Submissions:
    return Submissions.parse(recorded(submissions_url(cik)))


def facts(cik: int) -> CompanyFacts:
    return CompanyFacts.parse(recorded(companyfacts_url(cik)))


def test_the_folder_holds_the_files_of_the_attribution_file_unchanged() -> None:
    text = (FIXTURES / "ATTRIBUTION.md").read_text(encoding="utf-8")
    table = text.split("## The files of the folder", 1)[1]
    listed = {
        match["name"]: (int(match["size"].replace(",", "")), match["md5"])
        for match in map(_TABLE_ROW.match, table.splitlines())
        if match
    }
    found = {
        str(path.relative_to(FIXTURES)): (
            path.stat().st_size,
            hashlib.md5(path.read_bytes(), usedforsecurity=False).hexdigest(),
        )
        for path in FIXTURES.rglob("*")
        if path.is_file() and path.name != "ATTRIBUTION.md"
    }

    assert len(listed) == 19
    assert found == listed


def test_no_recorded_file_holds_a_field_which_the_models_do_not_name() -> None:
    documents = [
        TickerFile.parse(recorded(TICKER_FILE_URL)),
        IndexListing.parse(recorded(index_listing_url(2026, 4))),
        *(submissions(cik) for cik in DOCUMENTED),
        *(facts(cik) for cik in DEMO),
    ]

    assert len(documents) == 15
    assert [dict(unknown_fields(document)) for document in documents] == [{}] * 15


def test_the_ticker_file_keeps_the_rows_of_ten_companies_in_file_order() -> None:
    ticker_file = TickerFile.parse(recorded(TICKER_FILE_URL))

    assert ticker_file.columns == ["cik", "name", "ticker", "exchange"]
    assert {row.cik for row in ticker_file.rows} == {*DOCUMENTED, AMAZON, NETFLIX}
    first: dict[int, str | None] = {}
    for row in ticker_file.rows:
        first.setdefault(row.cik, row.ticker)
    # The first row of a CIK carries the primary ticker (finding F2).
    assert (first[META], first[ALPHABET], first[THOMSON_REUTERS]) == ("META", "GOOGL", "TRI")
    assert Counter(row.cik for row in ticker_file.rows)[ALPHABET] == 4
    assert {row.exchange for row in ticker_file.rows} == {"Nasdaq", "OTC"}


def test_a_submissions_document_gives_the_former_names_with_the_times_of_the_sec() -> None:
    meta, reuters, mattel = submissions(META), submissions(THOMSON_REUTERS), submissions(MATTEL)

    # The name change of exit-gate row 2 (finding F3). 04:00 UTC is midnight in New York.
    assert [(name.name, name.since, name.until) for name in meta.former_names] == [
        (
            "Facebook Inc",
            datetime(2005, 5, 6, 4, 0, tzinfo=UTC),
            datetime(2021, 10, 27, 4, 0, tzinfo=UTC),
        )
    ]
    assert (meta.cik, meta.name, meta.sic, meta.fiscal_year_end) == (
        META,
        "Meta Platforms, Inc.",
        "7370",
        "1231",
    )
    assert meta.tickers == ["META", "METAM"]
    assert [name.name for name in reuters.former_names] == ["THOMSON CORP /CAN/"]
    assert mattel.former_names == []


def test_the_fields_which_the_sec_leaves_empty_arrive_as_no_value() -> None:
    easyjet = submissions(EASYJET)

    # Finding F3: an empty text for the SIC code, null for the end of the fiscal year.
    assert (easyjet.sic, easyjet.fiscal_year_end, easyjet.category, easyjet.lei) == (
        None,
        None,
        None,
        None,
    )
    assert easyjet.filings.recent.count == 8
    assert easyjet.filings.files == []
    for cik in DOCUMENTED:
        document = submissions(cik)
        assert document.lei is None
        assert set(document.addresses) == {"mailing", "business"}


def test_the_lists_of_the_filings_have_one_length_and_give_one_record_for_each_filing() -> None:
    for cik in DOCUMENTED:
        recent = submissions(cik).filings.recent
        records = list(recent.filings())

        assert len(records) == recent.count == len(set(recent.accession_number))
        assert all(record.acceptance_time is not None for record in records)
        assert all(
            record.acceptance_time.tzinfo is not None  # type: ignore[union-attr]
            for record in records
        )
    meta = list(submissions(META).filings.recent.filings())
    newest = meta[0]
    # Finding F4: the items of a form 8-K are one text with commas; other forms have none.
    assert (newest.form, newest.filing_date, newest.items) == ("4", INDEX_DAY, None)
    assert {record.items for record in meta if record.form == "8-K"} > {"2.02,9.01"}
    assert any(record.report_date is None for record in meta)
    # The shortened document keeps the periodic reports of two years.
    assert Counter(record.form for record in meta)["10-Q"] == 6
    assert Counter(record.form for record in meta)["10-K"] == 2


def test_the_small_archive_holds_the_eight_submissions_documents() -> None:
    with zipfile.ZipFile(io.BytesIO(recorded(SUBMISSIONS_ARCHIVE_URL))) as archive:
        members = {name: archive.read(name) for name in archive.namelist()}

    assert sorted(members) == sorted(cik_file_name(cik) for cik in DOCUMENTED)
    for cik in DOCUMENTED:
        assert members[cik_file_name(cik)] == recorded(submissions_url(cik))


@pytest.mark.parametrize("cik", DEMO)
def test_the_key_of_a_fact_is_unique_in_a_facts_document(cik: int) -> None:
    # Blueprint 7.4: taxonomy, concept, unit, period start, period end, accession number.
    document = facts(cik)
    keys = Counter(
        (
            record.taxonomy,
            record.concept,
            record.unit,
            record.fact.start,
            record.fact.end,
            record.fact.accn,
        )
        for record in document.records()
    )

    assert document.cik == cik
    assert len(keys) > 600
    assert max(keys.values()) == 1


def test_a_facts_document_keeps_the_special_cases_of_the_findings() -> None:
    mattel = list(facts(MATTEL).records())
    reuters = list(facts(THOMSON_REUTERS).records())

    # Finding F5: a value at one point in time has no start; DEF 14A facts have no period.
    assert sum(1 for record in mattel if record.fact.start is None) > 500
    without_period = [record.fact for record in mattel if record.fact.fp is None]
    assert {fact.form for fact in without_period} == {"DEF 14A"}
    assert {fact.fy for fact in without_period} == {None}
    # A value is a whole number or a decimal number, and a decimal keeps its digits.
    per_share = [r.fact.val for r in mattel if r.unit == "USD/shares"]
    assert all(isinstance(value, Decimal) for value in per_share)
    assert any(value != value.to_integral_value() for value in per_share)
    # Finding F7: a foreign company reports in ifrs-full, on the forms 40-F and 6-K.
    assert {record.taxonomy for record in reuters} == {"ifrs-full"}
    assert {record.fact.form for record in reuters} == {"40-F", "6-K"}
    assert {record.fact.fp for record in reuters} == {"FY", "Q2"}
    assert {record.taxonomy for record in mattel} == {"us-gaap", "ecd"}


def test_fy_and_fp_describe_the_filing_and_not_the_period() -> None:
    # Finding F6: the annual report of Nvidia for fiscal year 2022 repeats the revenue of the
    # year which ended in January 2021 as a comparison, with fy 2022 and fp FY.
    found = [
        record.fact
        for record in facts(NVIDIA).records()
        if (record.taxonomy, record.concept, record.unit) == ("us-gaap", "Revenues", "USD")
        and record.fact.accn == "0001045810-22-000036"
        and (record.fact.start, record.fact.end) == (date(2020, 1, 27), date(2021, 1, 31))
    ]

    assert len(found) == 1
    fact = found[0]
    assert (fact.fy, fact.fp, fact.form, fact.filed) == (2022, "FY", "10-K", date(2022, 3, 18))
    assert fact.val == Decimal(16_675_000_000)


def test_one_quarterly_report_gives_two_durations_for_one_period_end() -> None:
    # Finding F6: nine months and three months with the same end and the same fp.
    by_filing: dict[tuple[str, date, str | None], set[int]] = {}
    for record in facts(MATTEL).records():
        fact = record.fact
        if record.concept == "NetIncomeLoss" and fact.start is not None and fact.form == "10-Q":
            days = (fact.end - fact.start).days
            by_filing.setdefault((fact.accn, fact.end, fact.fp), set()).add(days // 10)

    lengths = [sorted(days) for days in by_filing.values() if len(days) > 1]
    assert lengths
    assert any(shortest <= 9 and longest >= 26 for shortest, *_, longest in lengths)


def test_the_listing_names_the_one_recorded_index_file() -> None:
    listing = IndexListing.parse(recorded(index_listing_url(2026, 4)))

    files = listing.index_files()

    assert list(files) == [INDEX_DAY]
    assert files[INDEX_DAY].name == "form.20261125.idx"
    assert files[INDEX_DAY].last_modified == "11/25/2026 10:01:49 PM"
    # The listing names four more files of the day, which are no form index.
    assert len(listing.directory.item) == 5


def test_the_recorded_index_file_gives_300_rows_and_no_rejected_line() -> None:
    content = recorded(index_file_url(INDEX_DAY))

    parsed = parse_index(content)

    assert (len(parsed.rows), parsed.rejected, parsed.raw_records) == (300, (), 300)
    numbers = [row.line_number for row in parsed.rows]
    assert numbers == sorted(set(numbers)) and numbers[0] == 12
    lines = content.decode("latin-1").split("\n")
    for row in parsed.rows:
        line = lines[row.line_number - 1]
        # Finding F11 (a) and (e): the positions of a row, and the CIK of the file name.
        assert line[NAME_POSITION - 1] == " " and line[NAME_POSITION] != " "
        assert line.startswith(row.form) and line[NAME_POSITION:].startswith(row.company_name)
        assert row.file_name == f"edgar/data/{row.cik}/{row.accession_number}.txt"
    # The SEC wrote the raw file without a line feed at its end: the rows are the same.
    assert parse_index(content.rstrip(b"\n")) == parsed


def test_the_recorded_index_file_keeps_the_cases_of_finding_f11() -> None:
    rows = parse_index(recorded(index_file_url(INDEX_DAY))).rows

    # (b) One filing has one row for each filer.
    filers = Counter(row.accession_number for row in rows)
    assert sum(1 for count in filers.values() if count > 1) >= 3
    # (c) A pair of accession number and CIK can stand in two rows.
    pairs = Counter((row.accession_number, row.cik) for row in rows)
    assert sorted(count for count in pairs.values() if count > 1) == [2, 2]
    # (d) A row can carry an earlier date than the file: 40 rows on 29 dates, back to
    # 2025-12-01. The forms are drafts, letters and notices which the SEC publishes later.
    earlier = Counter(row.filing_date for row in rows if row.filing_date != INDEX_DAY)
    assert (sum(earlier.values()), len(earlier)) == (40, 29)
    assert (min(earlier), max(earlier)) == (date(2025, 12, 1), date(2026, 11, 24))
    assert earlier.most_common(2) == [(date(2026, 11, 24), 11), (date(2026, 10, 28), 2)]
    assert {row.form for row in rows if row.filing_date != INDEX_DAY} == {
        "ATS-N/CA",
        "CORRESP",
        "DRS",
        "DRS/A",
        "EFFECT",
        "QUALIF",
        "TA-1",
        "TA-1/A",
        "UPLOAD",
    }
    # A form can hold a space: the position cuts the form from the name, not the first space.
    assert Counter(row.form for row in rows)["SCHEDULE 13D/A"] == 5
    assert max(len(row.form) for row in rows) == 14


def test_the_forms_of_the_index_are_the_forms_of_the_submissions_documents() -> None:
    # The cut at position 17 gives the form which the submissions document of the company
    # names for the same filing.
    rows = parse_index(recorded(index_file_url(INDEX_DAY))).rows
    compared = []
    for cik in DOCUMENTED:
        by_accession = {
            filing.accession_number: filing for filing in submissions(cik).filings.recent.filings()
        }
        for row in rows:
            if row.cik == cik:
                filing = by_accession[row.accession_number]
                assert (row.form, row.filing_date) == (filing.form, filing.filing_date)
                compared.append((cik, row.form))

    assert sorted(compared) == [(HASBRO, "4"), (MICROSOFT, "PX14A6G"), (META, "4")]
