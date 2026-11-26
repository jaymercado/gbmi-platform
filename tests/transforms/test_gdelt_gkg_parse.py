"""DuckDB parsing of GDELT GKG files: gate G2, the derived columns and the organisations (WP-05)."""

from __future__ import annotations

from datetime import UTC, datetime

import pyarrow.compute as pc
import pytest

from gbmi.core.gdelt import Feed, Slot
from gbmi.core.gdelt_gkg import GKG_COLUMNS
from gbmi.core.text import normalise_text, text_id
from gbmi.lake.tables import (
    GDELT_GKG_DOC,
    GDELT_GKG_DOC_SCHEMA,
    GDELT_GKG_ORG,
    GDELT_GKG_ORG_SCHEMA,
)
from gbmi.quality.audit import UnitCounts
from gbmi.transforms.duckdb.gdelt_gkg import parse_gkg
from gbmi.transforms.duckdb.positional import BronzeFile, Parsed

from .helpers import INGESTED_AT, SLOT, bronze_file, recorded, recorded_rows

TITLE = "OpenTable To Buy Me&amp;u"
EXTRAS = f"<PAGE_LINKS>https://example.org/x</PAGE_LINKS><PAGE_TITLE>{TITLE}</PAGE_TITLE>"


def gkg_row(**overrides: str) -> str:
    """One GKG row with 27 fields. Keyword names stand for the columns of the codebook."""
    names = {
        "record_id": "GKGRECORDID",
        "date": "V2.1DATE",
        "collection": "V2SOURCECOLLECTIONIDENTIFIER",
        "source": "V2SOURCECOMMONNAME",
        "url": "V2DOCUMENTIDENTIFIER",
        "themes": "V1THEMES",
        "locations": "V1LOCATIONS",
        "v1_orgs": "V1ORGANIZATIONS",
        "v2_orgs": "V2ENHANCEDORGANIZATIONS",
        "tone": "V1.5TONE",
        "gcam": "V2GCAM",
        "extras": "V2EXTRASXML",
    }
    values = {
        "record_id": "20261125084500-0",
        "date": "20261125084500",
        "collection": "1",
        "source": "rttnews.com",
        "url": "https://www.rttnews.com/3717754/opentable-to-buy-me-u.aspx",
        "themes": "WB_678_DIGITAL_GOVERNMENT;TAX_ETHNICITY;TAX_ETHNICITY;",
        "locations": "1#Australia#AS#AS#-25#135#AS",
        "v1_orgs": "holdings inc;opentable inc",
        "v2_orgs": "Holdings Inc,70;Opentable Inc,14",
        "tone": "0,0,0,0,27.1186440677966,3.38983050847458,96",
        "gcam": "wc:96,c12.1:4",
        "extras": EXTRAS,
        **overrides,
    }
    fields = dict.fromkeys(GKG_COLUMNS, "")
    fields.update({names[key]: value for key, value in values.items()})
    return "\t".join(fields[name] for name in GKG_COLUMNS)


def parse(files: list[BronzeFile], **arguments: float) -> Parsed:
    return parse_gkg(
        files, run_id="test-run", pipeline_version="test", ingested_at=INGESTED_AT, **arguments
    )


def parse_lines(*lines: str) -> Parsed:
    return parse([bronze_file(Feed.GKG, list(lines))])


def docs(parsed: Parsed) -> list[dict[str, object]]:
    return parsed.tables[GDELT_GKG_DOC].to_pylist()


def orgs(parsed: Parsed) -> list[dict[str, object]]:
    return parsed.tables[GDELT_GKG_ORG].to_pylist()


def test_a_good_row_becomes_one_document_and_its_organisations() -> None:
    parsed = parse_lines(gkg_row())

    (doc,) = docs(parsed)
    assert parsed.tables[GDELT_GKG_DOC].schema == GDELT_GKG_DOC_SCHEMA.as_arrow()
    assert parsed.tables[GDELT_GKG_ORG].schema == GDELT_GKG_ORG_SCHEMA.as_arrow()
    assert doc == {
        "gkg_record_id": "20261125084500-0",
        "slot_date": SLOT.slot_date,
        "slot_hour": 8,
        "slot_ts": SLOT.start,
        "doc_date": datetime(2026, 11, 25, 8, 45, tzinfo=UTC),
        "source_collection_identifier": 1,
        "source_common_name": "rttnews.com",
        "document_url": "https://www.rttnews.com/3717754/opentable-to-buy-me-u.aspx",
        "page_title": "OpenTable To Buy Me&u",  # the character reference is resolved
        "story_id": text_id("opentable to buy me u"),
        "tone": 0.0,
        "positive_score": 0.0,
        "negative_score": 0.0,
        "polarity": 0.0,
        "activity_reference_density": pytest.approx(27.1186440677966),
        "self_group_reference_density": pytest.approx(3.38983050847458),
        "word_count": 96,
        "themes": ["TAX_ETHNICITY", "WB_678_DIGITAL_GOVERNMENT"],  # distinct and sorted
        "countries": ["AS"],
        "org_count": 2,
        "is_translated": False,
        "_source_file": SLOT.bronze_key(Feed.GKG),
        "_source_md5": parsed.files[0].md5,
        "_slot_ts": SLOT.start,
        "_ingested_at": INGESTED_AT,
        "_run_id": "test-run",
        "_pipeline_version": "test",
    }
    assert [
        (row["org_string_raw"], row["org_string_norm"], row["first_offset"], row["mention_count"])
        for row in sorted(orgs(parsed), key=lambda row: row["first_offset"])  # type: ignore[arg-type, return-value]
    ] == [("Opentable Inc", "opentable inc", 14, 1), ("Holdings Inc", "holdings inc", 70, 1)]
    for row in orgs(parsed):
        assert row["org_string_id"] == text_id(str(row["org_string_norm"]))
        assert (row["gkg_record_id"], row["slot_ts"]) == ("20261125084500-0", SLOT.start)
        assert (row["_source_file"], row["_run_id"]) == (SLOT.bronze_key(Feed.GKG), "test-run")
    assert parsed.counts == UnitCounts(
        raw_lines=1, parsed=1, quarantined=0, duplicates=0, committed=1
    )
    assert parsed.notes == {"organisation_names_without_a_normal_form": 0}


@pytest.mark.parametrize(
    ("line", "reason"),
    [
        ("\t".join(["x"] * 26), "column_count: expected 27, found 26"),
        (gkg_row(record_id=""), "gkg_record_id (GKGRECORDID): empty"),
        (
            gkg_row(record_id="2026112508450-0"),
            "gkg_record_id (GKGRECORDID): not 14 digits, a hyphen and a number",
        ),
        (
            gkg_row(record_id="20261125084500_0"),
            "gkg_record_id (GKGRECORDID): not 14 digits, a hyphen and a number",
        ),
        (gkg_row(date="0"), "doc_date (V2.1DATE): not a timestamp YYYYMMDDHHMMSS"),
        (gkg_row(date="20261332084500"), "doc_date (V2.1DATE): not a timestamp YYYYMMDDHHMMSS"),
        (
            gkg_row(collection="web"),
            "source_collection_identifier (V2SOURCECOLLECTIONIDENTIFIER): not an integer",
        ),
        (gkg_row(url=""), "document_url (V2DOCUMENTIDENTIFIER): empty"),
        (gkg_row(tone="0,0,0,0,27.1,3.4"), "tone (V1.5TONE): not seven numbers"),
        (gkg_row(tone=""), "tone (V1.5TONE): not seven numbers"),
        (gkg_row(tone="0,0,0,high,27.1,3.4,96"), "tone (V1.5TONE): not seven numbers"),
        (gkg_row(tone="0,0,0,0,27.1,3.4,96.5"), "word_count (V1.5TONE): not an integer"),
    ],
)
def test_each_failure_of_gate_g2_is_quarantined_with_its_reason(line: str, reason: str) -> None:
    parsed = parse_lines(gkg_row(), line)

    assert parsed.counts == UnitCounts(
        raw_lines=2, parsed=1, quarantined=1, duplicates=0, committed=1
    )
    (quarantined,) = parsed.quarantine.to_pylist()
    assert (quarantined["line_number"], quarantined["reason"]) == (2, reason)
    assert len(docs(parsed)) == 1 and len(orgs(parsed)) == 2


def test_a_translated_record_is_marked() -> None:
    (doc,) = docs(parse_lines(gkg_row(record_id="20261125084500-T52")))

    assert (doc["gkg_record_id"], doc["is_translated"]) == ("20261125084500-T52", True)


def test_titles_are_cleaned_and_give_the_story() -> None:
    def extras(title: str) -> str:
        return f"<PAGE_AUTHORS>x</PAGE_AUTHORS><PAGE_TITLE>{title}</PAGE_TITLE>"

    parsed = parse_lines(
        gkg_row(record_id="20261125084500-0", extras=extras("Budget adds &#xA3;2bn  for the NHS")),
        # Syndication: the same story with another case and another punctuation.
        gkg_row(
            record_id="20261125084500-1",
            extras=extras("BUDGET ADDS \u00a32BN FOR THE NHS!"),
            url="https://b.example/2",
        ),
        gkg_row(
            record_id="20261125084500-2",
            extras="<PAGE_LINKS>no title here</PAGE_LINKS>",
            url="https://c.example/3",
        ),
        gkg_row(record_id="20261125084500-3", extras=extras("???"), url="https://d.example/4"),
        gkg_row(record_id="20261125084500-4", extras=extras(""), url="https://e.example/5"),
    )

    by_id = {doc["gkg_record_id"]: doc for doc in docs(parsed)}
    first, second, untitled, unusable, empty = (by_id[f"20261125084500-{i}"] for i in range(5))
    assert first["page_title"] == "Budget adds \u00a32bn for the NHS"
    assert first["story_id"] == second["story_id"] == text_id("budget adds 2bn for the nhs")
    # No title: the document is a story of its own, named by its URL.
    assert (untitled["page_title"], untitled["story_id"]) == (
        None,
        text_id("url:https://c.example/3"),
    )
    # A title without a letter or a digit is kept as text and is not usable for the story.
    assert (unusable["page_title"], unusable["story_id"]) == (
        "???",
        text_id("url:https://d.example/4"),
    )
    assert (empty["page_title"], empty["story_id"]) == (None, text_id("url:https://e.example/5"))


def test_themes_and_countries_are_distinct_sorted_and_never_null() -> None:
    parsed = parse_lines(
        gkg_row(
            record_id="20261125084500-0",
            themes="ECON_IPO;TAX_FNCACT;ECON_IPO;",
            # The second block holds the separator inside the place name (week 2 plan, F6).
            locations="4#Paris, France#FR#FRA8#48.8667#2.33333#-1456928;"
            "0#Georgia, , Georgia#GG#GG#42#.5#1#GG;1#Australia#AS#AS#-25#135#AS;"
            "1#France#FR#FR#46#2#FR",
        ),
        gkg_row(record_id="20261125084500-1", themes="", locations=""),
        gkg_row(record_id="20261125084500-2", locations="broken;1#Nowhere#xx#XX#0#0#XX"),
    )

    by_id = {doc["gkg_record_id"]: doc for doc in docs(parsed)}
    assert by_id["20261125084500-0"]["themes"] == ["ECON_IPO", "TAX_FNCACT"]
    assert by_id["20261125084500-0"]["countries"] == ["AS", "FR", "GG"]
    # An empty field means no values, and the list is empty, not NULL.
    assert (by_id["20261125084500-1"]["themes"], by_id["20261125084500-1"]["countries"]) == ([], [])
    # A block without a two-letter code in the fifth field from the right gives no country.
    assert by_id["20261125084500-2"]["countries"] == []
    assert parsed.tables[GDELT_GKG_DOC]["themes"].null_count == 0
    assert parsed.tables[GDELT_GKG_DOC]["countries"].null_count == 0


def test_organisations_come_from_the_v2_column_with_offsets_and_counts() -> None:
    parsed = parse_lines(
        gkg_row(
            v1_orgs="ignored because the v2 column is filled",
            v2_orgs="Acme, Inc,300;ACME INC.,120;Acme Inc,450;Beta Corp,40;!!!,7;",
        )
    )

    rows = {row["org_string_norm"]: row for row in orgs(parsed)}
    assert set(rows) == {"acme inc", "beta corp"}
    acme = rows["acme inc"]
    # Three raw forms have one normal form: the form with the smallest offset names the row.
    assert (acme["org_string_raw"], acme["first_offset"], acme["mention_count"]) == (
        "ACME INC.",
        120,
        3,
    )
    assert acme["org_string_id"] == text_id("acme inc")
    assert (rows["beta corp"]["first_offset"], rows["beta corp"]["mention_count"]) == (40, 1)
    # A name without a letter or a digit has no normal form: dropped and counted.
    assert parsed.notes == {"organisation_names_without_a_normal_form": 1}
    assert docs(parsed)[0]["org_count"] == 2


def test_a_record_with_names_only_in_the_v1_column_uses_them() -> None:
    parsed = parse_lines(
        gkg_row(record_id="20261125084500-0", v2_orgs="", v1_orgs="acme inc;beta corp;ACME, Inc."),
        gkg_row(record_id="20261125084500-1", v2_orgs="", v1_orgs=""),
    )

    rows = sorted(orgs(parsed), key=lambda row: str(row["org_string_norm"]))
    # No offset, and a count of 1 for each name (D20). The first raw form names the row.
    assert [
        (row["org_string_raw"], row["org_string_norm"], row["first_offset"], row["mention_count"])
        for row in rows
    ] == [("acme inc", "acme inc", None, 1), ("beta corp", "beta corp", None, 1)]
    counts = {doc["gkg_record_id"]: doc["org_count"] for doc in docs(parsed)}
    assert counts == {"20261125084500-0": 2, "20261125084500-1": 0}


def test_a_repeated_record_keeps_its_first_occurrence_and_its_organisations() -> None:
    later = Slot.parse("20261125090000")
    files = [
        bronze_file(
            Feed.GKG, [gkg_row(v2_orgs="First Corp,10"), gkg_row(v2_orgs="Second Corp,10")]
        ),
        bronze_file(Feed.GKG, [gkg_row(v2_orgs="Third Corp,10")], later),
    ]

    parsed = parse(files)

    assert parsed.counts == UnitCounts(
        raw_lines=3, parsed=3, quarantined=0, duplicates=2, committed=1
    )
    assert [row["org_string_raw"] for row in orgs(parsed)] == ["First Corp"]
    assert docs(parsed)[0]["org_count"] == 1


def expected_organisations(fields: list[str]) -> set[str]:
    """The normal forms of the organisation names of one record, counted with plain Python."""
    enhanced, plain = fields[14], fields[13]
    if enhanced:
        names = [entry.rsplit(",", 1)[0] for entry in enhanced.split(";") if entry]
    else:
        names = [entry for entry in plain.split(";") if entry]
    return {normalise_text(name) for name in names} - {""}


def test_the_recorded_hour_parses_completely() -> None:
    rows = recorded_rows(Feed.GKG)
    expected_orgs = sum(len(expected_organisations(fields)) for _, fields in rows)
    assert len(rows) == 240 and len({fields[0] for _, fields in rows}) == 240

    parsed = parse(recorded(Feed.GKG))

    documents, organisations = parsed.tables[GDELT_GKG_DOC], parsed.tables[GDELT_GKG_ORG]
    assert parsed.counts == UnitCounts(
        raw_lines=240, parsed=240, quarantined=0, duplicates=0, committed=240
    )
    assert parsed.quarantine.num_rows == 0 and parsed.rejected == ()
    assert documents.schema == GDELT_GKG_DOC_SCHEMA.as_arrow()
    assert organisations.schema == GDELT_GKG_ORG_SCHEMA.as_arrow()
    # The organisation table: its row count is the sum of org_count, and its key is unique.
    assert organisations.num_rows == expected_orgs == pc.sum(documents["org_count"]).as_py()
    keys = set(
        zip(
            organisations["gkg_record_id"].to_pylist(),
            organisations["org_string_id"].to_pylist(),
            strict=True,
        )
    )
    assert len(keys) == organisations.num_rows
    by_record = {fields[0]: fields for _, fields in rows}
    for document in documents.to_pylist():
        fields = by_record[document["gkg_record_id"]]
        assert document["org_count"] == len(expected_organisations(fields))
        assert document["document_url"] == fields[4]
        themes = sorted({theme for theme in fields[7].split(";") if theme})
        assert document["themes"] == themes
        assert document["word_count"] == int(fields[15].split(",")[6])
    assert pc.unique(documents["slot_hour"]).to_pylist() == [8]
    assert str(documents.schema.field("doc_date").type) == "timestamp[us, tz=UTC]"
    assert documents["story_id"].null_count == 0


def test_the_one_percent_rule_rejects_a_whole_gkg_file() -> None:
    good = Slot.parse("20261125080000")
    lines = [gkg_row(record_id=f"20261125084500-{index}") for index in range(98)]
    lines += [gkg_row(record_id="bad"), gkg_row(tone="")]
    files = [
        bronze_file(Feed.GKG, [gkg_row(record_id="20261125080000-0", date="20261125080000")], good),
        bronze_file(Feed.GKG, lines),
    ]

    parsed = parse(files, file_threshold=0.01)

    assert parsed.counts == UnitCounts(
        raw_lines=1, parsed=1, quarantined=0, duplicates=0, committed=1
    )
    assert [counts.key for counts in parsed.rejected] == [SLOT.bronze_key(Feed.GKG)]
    assert (parsed.rejected[0].raw_lines, parsed.rejected[0].quarantined_rows) == (100, 2)
    assert [doc["gkg_record_id"] for doc in docs(parsed)] == ["20261125080000-0"]
    assert {row["gkg_record_id"] for row in orgs(parsed)} == {"20261125080000-0"}
    assert parsed.quarantine.num_rows == 0


def test_files_without_lines_give_two_empty_tables_with_the_right_schemas() -> None:
    parsed = parse([bronze_file(Feed.GKG, [])])

    assert parsed.counts == UnitCounts(0, 0, 0, 0, 0)
    assert parsed.tables[GDELT_GKG_DOC].schema == GDELT_GKG_DOC_SCHEMA.as_arrow()
    assert parsed.tables[GDELT_GKG_ORG].schema == GDELT_GKG_ORG_SCHEMA.as_arrow()
    assert (parsed.tables[GDELT_GKG_DOC].num_rows, parsed.tables[GDELT_GKG_ORG].num_rows) == (0, 0)
