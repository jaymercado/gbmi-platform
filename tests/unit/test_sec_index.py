"""The parser of the daily form index on made-up lines (week 3 plan, finding F11 and WP-04)."""

from __future__ import annotations

from datetime import date

import pytest

from gbmi.sources.sec.index import (
    MAX_TEXT,
    IndexFormatError,
    IndexRow,
    parse_index,
)

HEADER = (
    "Description:           Daily Index of EDGAR Dissemination Feed by Form Type\n"
    "Last Data Received:    Nov 25, 2026\n"
    " \n"
    "Form Type   Company Name                                                  CIK\n"
    "      Date Filed  File Name\n"
    "-----------------------------------------------------------------------------------\n"
)
ACCESSION = "0001234567-26-000001"


def line(
    form: str,
    name: str,
    cik: int,
    day: str = "20261125",
    accession: str = ACCESSION,
    path_cik: int | None = None,
) -> str:
    """One row in the layout of the SEC: the name at 17, the CIK at 79, the date at 91."""
    file_name = f"edgar/data/{path_cik or cik}/{accession}.txt"
    return f"{form:<17}{name:<62}{cik:<12}{day:<12}{file_name}"


def index(*lines: str, end: str = "\n") -> bytes:
    return (HEADER + "\n".join(lines) + end).encode("latin-1")


def test_a_row_is_read_from_its_right_end() -> None:
    parsed = parse_index(index(line("10-K", "Example Holdings, Inc.", 1234567)))

    assert parsed.rejected == ()
    assert parsed.rows == (
        IndexRow(
            line_number=7,
            form="10-K",
            company_name="Example Holdings, Inc.",
            cik=1234567,
            filing_date=date(2026, 11, 25),
            accession_number=ACCESSION,
            file_name=f"edgar/data/1234567/{ACCESSION}.txt",
        ),
    )
    assert parsed.raw_records == 1


@pytest.mark.parametrize("form", ["4", "SCHEDULE 13D/A", "SEC STAFF ACTION", "DEF 14A"])
def test_position_17_cuts_the_form_from_the_name(form: str) -> None:
    # A form can hold a space and can fill its 16 characters.
    parsed = parse_index(index(line(form, "A B C Fund", 42)))

    assert [(row.form, row.company_name) for row in parsed.rows] == [(form, "A B C Fund")]


@pytest.mark.parametrize(
    "name",
    [
        "Trust 123 20250101",  # digits which look like a CIK and a date
        "Series   2026-1   Notes",  # several spaces inside the name
        "N" * 62,  # a name which fills its column: one space before the CIK
        "edgar/data/1/0000000000-00-000000.txt Corp",  # a name which looks like a file name
    ],
)
def test_the_company_name_stays_whole(name: str) -> None:
    text = line("8-K", name, 1234567)
    if len(name) == 62:
        text = text.replace("N1234567", "N 1234567")

    parsed = parse_index(index(text))

    assert [(row.company_name, row.cik) for row in parsed.rows] == [(name, 1234567)]
    assert parsed.rejected == ()


def test_a_line_which_does_not_fit_is_returned_with_a_reason() -> None:
    good = line("10-Q", "Good Company", 7)
    cases = {
        "no file name at the end": "the line does not end with a CIK",
        line("10-K", "", 7): "the company name is empty",
        line("ABCDEFGHIJKLMNOPQRS", "Name", 7): "the form does not end before position 17",
        line("", "Only A Name", 7): "the form is empty",
        line("10-K", "Other Folder", 7, path_cik=8): "the CIK of the file name differs",
        line("10-K", "Bad Day", 7, day="20261345"): "20261345 is no date",
        line("10-K", "Short Day", 7, day="2026112"): "the line does not end with a CIK",
        line("10-K", "Bad Number", 7, accession="123-26-1"): "the line does not end with a CIK",
    }

    parsed = parse_index(index(good, *cases, good))

    assert [row.line_number for row in parsed.rows] == [7, 7 + len(cases) + 1]
    assert [rejected.line_number for rejected in parsed.rejected] == list(range(8, 8 + len(cases)))
    for rejected, (text, reason) in zip(parsed.rejected, cases.items(), strict=True):
        assert rejected.reason.startswith(reason)
        assert rejected.text == text[:MAX_TEXT]
    assert parsed.raw_records == len(cases) + 2


def test_a_rejected_line_keeps_its_start_only() -> None:
    parsed = parse_index(index("x" * 5000))

    assert len(parsed.rejected[0].text) == MAX_TEXT


def test_empty_lines_and_the_end_of_the_file_are_no_records() -> None:
    first, second = line("3", "First Filer", 1), line("4", "Second Filer", 2)
    expected = parse_index(index(first, second))

    assert len(expected.rows) == 2
    assert parse_index(index(first, second, end="")) == expected  # no line feed at the end
    assert parse_index(index(first, second, end="\n\n   \n")) == expected
    spaced = parse_index(index(first, "", "   ", second))
    assert [row.line_number for row in spaced.rows] == [7, 10]
    assert spaced.raw_records == 2
    # Lines which end with a carriage return and a line feed give the same rows.
    windows = (HEADER + first + "\n" + second + "\n").replace("\n", "\r\n").encode("latin-1")
    assert parse_index(windows) == expected


def test_a_byte_above_127_in_a_name_is_a_character() -> None:
    content = index(line("6-K", "Soci\xe9t\xe9 G\xe9n\xe9rale", 9))

    assert b"\xe9" in content
    assert parse_index(content).rows[0].company_name == "Soci\xe9t\xe9 G\xe9n\xe9rale"


def test_the_header_is_never_read() -> None:
    # A header line which looks like a row is no row: the rows start after the dashed line.
    looks_like_a_row = line("10-K", "Header Company", 5)
    content = (looks_like_a_row + "\n" + HEADER + line("10-K", "Real Company", 6) + "\n").encode()

    parsed = parse_index(content)

    assert [row.company_name for row in parsed.rows] == ["Real Company"]
    assert parsed.rows[0].line_number == 8


def test_bytes_without_a_dashed_line_are_no_index_file() -> None:
    for content in (b"", b"<html>Access Denied</html>", line("10-K", "No Header", 5).encode()):
        with pytest.raises(IndexFormatError, match="no dashed line"):
            parse_index(content)
    assert parse_index(HEADER.encode()).raw_records == 0
