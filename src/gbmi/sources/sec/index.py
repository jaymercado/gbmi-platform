"""The daily form index of the SEC as rows (week 3 plan, finding F11 and WP-04).

Pure code: bytes in, rows out. The header of an index file does not give the positions of
the columns, so the parser never reads the header. Each row is read from the right end, where
the three last columns have a fixed form: the CIK, a date of 8 digits and the file name
`edgar/data/<cik>/<accession>.txt`. The rest of the row holds the form and the company name:
the form ends before position 17, and the name starts at position 17. A form can hold a space
(`SCHEDULE 13D/A`), so the position decides and not the first space.

A line which does not fit is returned with a reason and counts as a record of the file. An
empty line is no record.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date

#: Every byte is a character: an index file can hold bytes above 127 in a company name.
ENCODING = "latin-1"
#: The company name of a row starts at the position, counted from 0; the form ends before it.
NAME_POSITION = 17
#: A rejected line keeps at most so many characters.
MAX_TEXT = 200

_DASHES = re.compile(r"^-{5,}\s*$")
_RIGHT_END = re.compile(
    r"^(?P<head>.*?)\s+(?P<cik>\d{1,10})\s+(?P<date>\d{8})\s+"
    r"(?P<file>edgar/data/(?P<path_cik>\d{1,10})/(?P<accession>\d{10}-\d{2}-\d{6})\.txt)\s*$"
)


class IndexFormatError(ValueError):
    """The bytes are no index file: the dashed line which ends the header is missing."""


@dataclass(frozen=True, slots=True)
class IndexRow:
    """One filing and one filer (difference D34)."""

    #: The line of the row in the file, counted from 1.
    line_number: int
    form: str
    company_name: str
    cik: int
    #: The date column of the row. The date can lie before the date of the file (F11 d).
    filing_date: date
    accession_number: str
    file_name: str


@dataclass(frozen=True, slots=True)
class RejectedLine:
    line_number: int
    reason: str
    #: The start of the line.
    text: str


@dataclass(frozen=True, slots=True)
class ParsedIndex:
    rows: tuple[IndexRow, ...]
    rejected: tuple[RejectedLine, ...]

    @property
    def raw_records(self) -> int:
        """The lines of the file after the header which are not empty."""
        return len(self.rows) + len(self.rejected)


def parse_index(content: bytes) -> ParsedIndex:
    """Read one index file. Raises IndexFormatError when the file has no dashed line."""
    lines = content.decode(ENCODING).split("\n")
    dashes = next((number for number, line in enumerate(lines) if _DASHES.match(line)), None)
    if dashes is None:
        raise IndexFormatError("the index file has no dashed line after its header")
    rows: list[IndexRow] = []
    rejected: list[RejectedLine] = []
    # The dashed line is line dashes + 1, counted from 1.
    for number, line in enumerate(lines[dashes + 1 :], start=dashes + 2):
        text = line.rstrip("\r")
        if not text.strip():
            continue
        parsed = _row(number, text)
        if isinstance(parsed, IndexRow):
            rows.append(parsed)
        else:
            rejected.append(parsed)
    return ParsedIndex(tuple(rows), tuple(rejected))


def _row(number: int, text: str) -> IndexRow | RejectedLine:
    match = _RIGHT_END.match(text)
    if match is None:
        reason = "the line does not end with a CIK, a date of 8 digits and a file name"
        return RejectedLine(number, reason, text[:MAX_TEXT])
    head, digits = match["head"], match["date"]
    form, name = head[:NAME_POSITION].strip(), head[NAME_POSITION:].strip()
    reason = ""
    if len(head) >= NAME_POSITION and head[NAME_POSITION - 1] != " ":
        reason = f"the form does not end before position {NAME_POSITION}"
    elif not form:
        reason = "the form is empty"
    elif not name:
        reason = "the company name is empty"
    elif int(match["path_cik"]) != int(match["cik"]):
        reason = "the CIK of the file name differs from the CIK column"
    if reason:
        return RejectedLine(number, reason, text[:MAX_TEXT])
    try:
        day = date(int(digits[:4]), int(digits[4:6]), int(digits[6:]))
    except ValueError:
        return RejectedLine(number, f"{digits} is no date", text[:MAX_TEXT])
    return IndexRow(
        line_number=number,
        form=form,
        company_name=name,
        cik=int(match["cik"]),
        filing_date=day,
        accession_number=match["accession"],
        file_name=match["file"],
    )
