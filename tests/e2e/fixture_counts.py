"""Row counts of the fixture hour, computed without the code of the pipeline.

The end-to-end tests compare the four silver tables with the counts of this module. The
module reads the recorded zip files with the standard library only and states each rule of
the week 2 plan (§3.3) a second time, so the counts cannot inherit a mistake of the pipeline.
"""

from __future__ import annotations

import os
import re
import unicodedata
import zipfile
from pathlib import Path

FIXTURES = Path(os.environ.get("GBMI_FIXTURES_DIR", "/fixtures/gdelt/v2"))
HOUR_STAMPS = ("20261125080000", "20261125081500", "20261125083000", "20261125084500")
FILE_NAMES = {
    "export": "{}.export.CSV.zip",
    "mentions": "{}.mentions.CSV.zip",
    "gkg": "{}.gkg.csv.zip",
}
COLUMNS = {"export": 61, "mentions": 16, "gkg": 27}

_RECORD_ID = re.compile(r"[0-9]{14}-T?[0-9]+")


def lines_of(feed: str, stamp: str) -> list[str]:
    """The lines of one recorded file."""
    with zipfile.ZipFile(FIXTURES / feed / FILE_NAMES[feed].format(stamp)) as archive:
        (member,) = archive.namelist()
        lines = archive.read(member).decode("utf-8", errors="replace").split("\n")
    if lines[-1] == "":
        lines.pop()
    return lines


def _is_integer(text: str) -> bool:
    return text.removeprefix("-").isdigit()


def _is_number(text: str) -> bool:
    try:
        float(text)
    except ValueError:
        return False
    return True


def _is_stamp(text: str) -> bool:
    return len(text) == 14 and text.isdigit()


_EVENT_INTEGERS = (25, 29, 31, 32, 33, 51)  # flags and counts which are empty or an integer
_EVENT_NUMBERS = (30, 34)  # Goldstein scale, average tone
_EVENT_DAY, _EVENT_ADDED = 1, 59


def _event_is_valid(fields: list[str]) -> bool:
    """61 columns; an event identifier; typed fields which are empty or well formed."""
    if len(fields) != COLUMNS["export"] or not _is_integer(fields[0]):
        return False
    if any(fields[index] and not _is_integer(fields[index]) for index in _EVENT_INTEGERS):
        return False
    if any(fields[index] and not _is_number(fields[index]) for index in _EVENT_NUMBERS):
        return False
    day, added = fields[_EVENT_DAY], fields[_EVENT_ADDED]
    return (not day or (len(day) == 8 and day.isdigit())) and (not added or _is_stamp(added))


def event_counts() -> dict[str, int]:
    """Raw lines, quarantined rows, duplicate rows and committed rows of the export files."""
    raw_lines = quarantined = duplicates = 0
    seen: set[str] = set()
    for stamp in HOUR_STAMPS:
        for line in lines_of("export", stamp):
            raw_lines += 1
            fields = line.split("\t")
            if not _event_is_valid(fields):
                quarantined += 1
            elif fields[0] in seen:
                duplicates += 1
            else:
                seen.add(fields[0])
    return {
        "raw_lines": raw_lines,
        "quarantined": quarantined,
        "duplicates": duplicates,
        "committed": raw_lines - quarantined - duplicates,
    }


def _mention_is_valid(fields: list[str]) -> bool:
    """16 columns; an event identifier; an article; typed fields which are empty or well formed."""
    if len(fields) != COLUMNS["mentions"] or not _is_integer(fields[0]) or not fields[5]:
        return False
    event_time, mention_type, confidence, tone = fields[1], fields[3], fields[11], fields[13]
    return (
        (not event_time or _is_stamp(event_time))
        and (not mention_type or _is_integer(mention_type))
        and (not confidence or _is_integer(confidence))
        and (not tone or _is_number(tone))
    )


def _document_is_valid(fields: list[str]) -> bool:
    """27 columns; a record identifier; a date; an address; a tone of seven numbers."""
    if len(fields) != COLUMNS["gkg"]:
        return False
    record_id, date, collection, address, tone = (
        fields[0],
        fields[1],
        fields[2],
        fields[4],
        fields[15],
    )
    numbers = tone.split(",")
    return (
        _RECORD_ID.fullmatch(record_id) is not None
        and _is_stamp(date)
        and (not collection or _is_integer(collection))
        and bool(address)
        and len(numbers) == 7
        and all(_is_number(number) for number in numbers)
        and _is_integer(numbers[6])
    )


def normal_form(name: str) -> str:
    """Version 1 of the text normalisation, stated a second time (week 2 plan 3.3)."""
    while True:
        folded = unicodedata.normalize("NFKC", name).casefold()
        characters = []
        for character in unicodedata.normalize("NFKD", folded):
            category = unicodedata.category(character)
            if category[0] == "L" or category == "Nd":
                characters.append(character)
            elif category[0] != "M":
                characters.append(" ")
        following = " ".join("".join(characters).split())
        if following == name:
            return following
        name = following


def _organisations(fields: list[str]) -> set[str]:
    """The normal forms of the organisation names of one document.

    The names come from V2ENHANCEDORGANIZATIONS (name and offset, separated by a comma) and,
    when that column is empty, from V1ORGANIZATIONS.
    """
    enhanced, plain = fields[14], fields[13]
    if enhanced:
        names = [re.sub(r",[0-9]+$", "", entry) for entry in enhanced.split(";")]
    else:
        names = plain.split(";")
    return {form for form in (normal_form(name.strip()) for name in names if name.strip()) if form}


def silver_rows() -> dict[str, int]:
    """The rows which the fixture hour gives to each of the four silver tables."""
    mentions = documents = organisations = 0
    seen_documents: set[str] = set()
    for stamp in HOUR_STAMPS:
        # A mention is one row for each event, article and slot.
        pairs = set()
        for line in lines_of("mentions", stamp):
            fields = line.split("\t")
            if _mention_is_valid(fields):
                pairs.add((fields[0], fields[5]))
        mentions += len(pairs)
        # A document is one row for each record identifier; the first occurrence stays.
        for line in lines_of("gkg", stamp):
            fields = line.split("\t")
            if _document_is_valid(fields) and fields[0] not in seen_documents:
                seen_documents.add(fields[0])
                documents += 1
                organisations += len(_organisations(fields))
    return {
        "gdelt_event": event_counts()["committed"],
        "gdelt_mention": mentions,
        "gdelt_gkg_doc": documents,
        "gdelt_gkg_org": organisations,
    }
