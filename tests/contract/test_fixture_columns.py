"""The positional schemas against the recorded GDELT slots (blueprint 9.6 and 7.8).

Each feed has an exact column count: 61, 16 and 27. Every line of every recorded file has to
have that count, and the typed positions have to hold what the codebooks say.
"""

from __future__ import annotations

import re
import zipfile
from pathlib import Path

import pytest

from gbmi.core.gdelt import Feed
from gbmi.core.gdelt_export import EXPORT
from gbmi.core.gdelt_gkg import GKG, RECORD_ID_PATTERN, TONE_FIELDS
from gbmi.core.gdelt_mentions import MENTIONS
from gbmi.core.positional import FeedSchema

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "gdelt" / "v2"
SCHEMAS: dict[Feed, FeedSchema] = {Feed.EXPORT: EXPORT, Feed.MENTIONS: MENTIONS, Feed.GKG: GKG}
STAMP = re.compile(r"[0-9]{14}")


def rows(feed: Feed) -> list[tuple[str, list[str]]]:
    """Every line of the recorded files of a feed, split at the tabs, with the stamp of its file."""
    found = []
    for path in sorted((FIXTURES / feed.value).glob("*.zip")):
        with zipfile.ZipFile(path) as archive:
            (member,) = archive.namelist()
            text = archive.read(member).decode("utf-8")
        found += [(path.name[:14], line.split("\t")) for line in text.split("\n") if line]
    return found


@pytest.mark.parametrize(
    ("feed", "count", "lines"),
    [(Feed.EXPORT, 61, 4023), (Feed.MENTIONS, 16, 7882), (Feed.GKG, 27, 240)],
)
def test_every_recorded_line_has_the_column_count_of_its_feed(
    feed: Feed, count: int, lines: int
) -> None:
    schema = SCHEMAS[feed]
    recorded = rows(feed)

    assert schema.column_count == count == len(schema.source_columns)
    assert len(recorded) == lines
    assert {len(fields) for _, fields in recorded} == {count}


def value(schema: FeedSchema, fields: list[str], source: str) -> str:
    return fields[schema.position(source) - 1]


def test_the_batch_time_of_every_row_is_the_time_in_the_file_name() -> None:
    # Week 2 plan, F8: DATEADDED, MentionTimeDate and V2.1DATE equal the stamp of the file.
    for feed, source in (
        (Feed.EXPORT, "DATEADDED"),
        (Feed.MENTIONS, "MentionTimeDate"),
        (Feed.GKG, "V2.1DATE"),
    ):
        schema = SCHEMAS[feed]
        assert all(value(schema, fields, source) == stamp for stamp, fields in rows(feed)), feed


def test_the_typed_positions_of_mentions_hold_what_the_codebook_says() -> None:
    for _, fields in rows(Feed.MENTIONS):
        assert value(MENTIONS, fields, "GLOBALEVENTID").isdigit()
        assert STAMP.fullmatch(value(MENTIONS, fields, "EventTimeDate"))
        assert value(MENTIONS, fields, "MentionType") in {"1", "2", "3", "4", "5", "6"}
        assert value(MENTIONS, fields, "MentionIdentifier")
        assert 0 < int(value(MENTIONS, fields, "Confidence")) <= 100
        float(value(MENTIONS, fields, "MentionDocTone"))


def test_the_typed_positions_of_the_gkg_hold_what_the_codebook_says() -> None:
    for stamp, fields in rows(Feed.GKG):
        record_id = value(GKG, fields, "GKGRECORDID")
        assert re.fullmatch(RECORD_ID_PATTERN, record_id) and record_id.startswith(stamp)
        assert value(GKG, fields, "V2SOURCECOLLECTIONIDENTIFIER") == "1"  # the open web
        assert value(GKG, fields, "V2DOCUMENTIDENTIFIER").startswith("http")
        tone = value(GKG, fields, "V1.5TONE").split(",")
        assert len(tone) == len(TONE_FIELDS) == 7
        assert all(float(number) is not None for number in tone) and tone[6].isdigit()
        assert "<PAGE_TITLE>" in value(GKG, fields, "V2EXTRASXML")
        for block in filter(None, value(GKG, fields, "V1LOCATIONS").split(";")):
            assert re.fullmatch("[A-Z]{2}", block.split("#")[-5]), block


def test_a_typo_in_a_feed_definition_stops_the_import() -> None:
    from gbmi.core.positional import Kind, feed_schema

    with pytest.raises(AssertionError, match="2 distinct columns, not 3"):
        feed_schema("broken", ("A", "B", "C"), 2, ())
    with pytest.raises(AssertionError):
        feed_schema("repeated", ("A", "A"), 2, ())
    with pytest.raises(ValueError, match="not in tuple"):
        feed_schema("unknown", ("A", "B"), 2, (("x", "C", Kind.TEXT),))
    schema = feed_schema("fine", ("A", "B"), 2, (("b", "B", Kind.INTEGER),))
    assert (schema.columns[0].position, schema.position("A"), schema.column_count) == (2, 1, 2)
