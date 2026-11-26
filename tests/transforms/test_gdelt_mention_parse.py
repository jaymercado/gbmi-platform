"""DuckDB parsing of GDELT mentions files: gate G2, the key and the duplicates (WP-05)."""

from __future__ import annotations

from datetime import UTC, datetime

import pyarrow.compute as pc
import pytest

from gbmi.core.gdelt import Feed, Slot
from gbmi.core.gdelt_mentions import MENTIONS_COLUMNS
from gbmi.lake.tables import GDELT_MENTION, GDELT_MENTION_SCHEMA
from gbmi.quality.audit import UnitCounts
from gbmi.transforms.duckdb.gdelt_mention import parse_mentions
from gbmi.transforms.duckdb.positional import BronzeFile, Parsed

from .helpers import INGESTED_AT, SLOT, bronze_file, recorded, recorded_rows


def mention_row(**overrides: str) -> str:
    """One mentions row with 16 fields."""
    fields = dict.fromkeys(MENTIONS_COLUMNS, "")
    fields.update(
        GLOBALEVENTID="1334932590", EventTimeDate="20261125084500",
        MentionTimeDate="20261125084500", MentionType="1",
        MentionSourceName="example.org", MentionIdentifier="https://example.org/a-story",
        SentenceID="1", Actor1CharOffset="317", Actor2CharOffset="-1", ActionCharOffset="176",
        InRawText="0", Confidence="20", MentionDocLen="1587", MentionDocTone="-4.16666666666667",
    )  # fmt: skip
    fields.update(overrides)
    return "\t".join(fields[name] for name in MENTIONS_COLUMNS)


def parse(files: list[BronzeFile], **arguments: float) -> Parsed:
    return parse_mentions(
        files, run_id="test-run", pipeline_version="test", ingested_at=INGESTED_AT, **arguments
    )


def mentions(parsed: Parsed) -> list[dict[str, object]]:
    return parsed.tables[GDELT_MENTION].to_pylist()


def test_a_good_row_becomes_a_typed_row_in_utc_with_provenance() -> None:
    parsed = parse([bronze_file(Feed.MENTIONS, [mention_row()])])

    (row,) = mentions(parsed)
    assert parsed.tables[GDELT_MENTION].schema == GDELT_MENTION_SCHEMA.as_arrow()
    assert row == {
        "global_event_id": 1334932590,
        "mention_identifier": "https://example.org/a-story",
        "slot_date": SLOT.slot_date,
        "slot_hour": 8,
        "slot_ts": SLOT.start,
        "event_time_date": datetime(2026, 11, 25, 8, 45, tzinfo=UTC),
        "mention_type": 1,
        "mention_source_name": "example.org",
        "confidence": 20,
        "mention_doc_tone": pytest.approx(-4.16666666666667),
        "_source_file": SLOT.bronze_key(Feed.MENTIONS),
        "_source_md5": parsed.files[0].md5,
        "_slot_ts": SLOT.start,
        "_ingested_at": INGESTED_AT,
        "_run_id": "test-run",
        "_pipeline_version": "test",
    }
    assert parsed.counts == UnitCounts(
        raw_lines=1, parsed=1, quarantined=0, duplicates=0, committed=1
    )


@pytest.mark.parametrize(
    ("line", "reason"),
    [
        ("\t".join(["1"] * 15), "column_count: expected 16, found 15"),
        (mention_row(GLOBALEVENTID="abc"), "global_event_id (GLOBALEVENTID): not an integer"),
        (mention_row(GLOBALEVENTID=""), "global_event_id (GLOBALEVENTID): not an integer"),
        (mention_row(MentionIdentifier=""), "mention_identifier (MentionIdentifier): empty"),
        (
            mention_row(EventTimeDate="20261332084500"),
            "event_time_date (EventTimeDate): not a timestamp YYYYMMDDHHMMSS",
        ),
        (mention_row(MentionType="web"), "mention_type (MentionType): not an integer"),
        (mention_row(Confidence="high"), "confidence (Confidence): not an integer"),
        (mention_row(MentionDocTone="sad"), "mention_doc_tone (MentionDocTone): not a number"),
    ],
)
def test_each_failure_of_gate_g2_is_quarantined_with_its_reason(line: str, reason: str) -> None:
    parsed = parse([bronze_file(Feed.MENTIONS, [mention_row(), line])])

    assert parsed.counts == UnitCounts(
        raw_lines=2, parsed=1, quarantined=1, duplicates=0, committed=1
    )
    (quarantined,) = parsed.quarantine.to_pylist()
    assert (quarantined["line_number"], quarantined["reason"]) == (2, reason)
    assert quarantined["raw_line"] == line


def test_an_empty_optional_field_is_null_not_a_failure() -> None:
    line = mention_row(
        EventTimeDate="", MentionType="", MentionSourceName="", Confidence="", MentionDocTone=""
    )

    (row,) = mentions(parse([bronze_file(Feed.MENTIONS, [line])]))

    assert [row[name] for name in ("event_time_date", "mention_type", "mention_source_name")] == [
        None,
        None,
        None,
    ]
    assert (row["confidence"], row["mention_doc_tone"]) == (None, None)


def test_a_repeated_pair_keeps_the_row_with_the_highest_confidence() -> None:
    # One file holds the same event and article three times; the rows differ in the offsets
    # and in the confidence (week 2 plan, F5).
    lines = [
        mention_row(Confidence="20", Actor1CharOffset="317"),
        mention_row(Confidence="50", Actor1CharOffset="304"),
        mention_row(Confidence="30", Actor1CharOffset="290"),
        mention_row(GLOBALEVENTID="1334932591", Confidence="40"),
        mention_row(GLOBALEVENTID="1334932591", Confidence="40", MentionDocTone="1.5"),
        mention_row(GLOBALEVENTID="1334932592", Confidence=""),
        mention_row(GLOBALEVENTID="1334932592", Confidence="10"),
    ]

    parsed = parse([bronze_file(Feed.MENTIONS, lines)])

    assert parsed.counts == UnitCounts(
        raw_lines=7, parsed=7, quarantined=0, duplicates=4, committed=3
    )
    kept = {row["global_event_id"]: row for row in mentions(parsed)}
    assert kept[1334932590]["confidence"] == 50
    # Equal confidence: the first line stays.
    assert kept[1334932591]["mention_doc_tone"] == pytest.approx(-4.16666666666667)
    # A row without a confidence is the last choice.
    assert kept[1334932592]["confidence"] == 10


def test_the_same_pair_in_two_slots_is_two_rows() -> None:
    later = Slot.parse("20261125090000")
    files = [
        bronze_file(Feed.MENTIONS, [mention_row()]),
        bronze_file(Feed.MENTIONS, [mention_row(MentionTimeDate="20261125090000")], later),
    ]

    parsed = parse(files)

    assert parsed.counts.duplicates == 0
    assert [row["slot_ts"] for row in mentions(parsed)] == [SLOT.start, later.start]


def test_the_recorded_hour_parses_completely() -> None:
    rows = recorded_rows(Feed.MENTIONS)
    # Counted with plain Python: the distinct triples of event, article and slot.
    distinct = {(fields[0], fields[5], stamp) for stamp, fields in rows}
    assert (len(rows), len(distinct)) == (7882, 7762)

    parsed = parse(recorded(Feed.MENTIONS))

    table = parsed.tables[GDELT_MENTION]
    assert parsed.counts == UnitCounts(
        raw_lines=7882, parsed=7882, quarantined=0, duplicates=120, committed=7762
    )
    assert parsed.quarantine.num_rows == 0 and parsed.rejected == ()
    assert table.schema == GDELT_MENTION_SCHEMA.as_arrow()
    assert [(counts.raw_lines, counts.parsed_rows) for counts in parsed.files] == [
        (2091, 2091),
        (1898, 1898),
        (1970, 1970),
        (1923, 1923),
    ]
    assert pc.unique(table["slot_hour"]).to_pylist() == [8]
    assert str(table.schema.field("slot_ts").type) == "timestamp[us, tz=UTC]"
    # Each surviving row is the best row of its key: no key keeps a lower confidence.
    best: dict[tuple[str, str, str], int] = {}
    for stamp, fields in rows:
        key = (fields[0], fields[5], stamp)
        best[key] = max(best.get(key, 0), int(fields[11]))
    for row in table.to_pylist():
        key = (
            str(row["global_event_id"]),
            row["mention_identifier"],
            row["slot_ts"].strftime("%Y%m%d%H%M%S"),
        )
        assert row["confidence"] == best[key]


def test_the_one_percent_rule_rejects_a_whole_file() -> None:
    good = Slot.parse("20261125080000")
    bad_lines = [mention_row(GLOBALEVENTID=str(1000 + index)) for index in range(98)]
    bad_lines += [mention_row(GLOBALEVENTID="x"), mention_row(MentionIdentifier="")]
    files = [
        bronze_file(Feed.MENTIONS, [mention_row(MentionTimeDate="20261125080000")], good),
        bronze_file(Feed.MENTIONS, bad_lines),
    ]

    without_rule = parse(files)
    with_rule = parse(files, file_threshold=0.01)

    # Without the file rule, the two bad rows go to quarantine and the 98 good rows stay.
    assert without_rule.counts == UnitCounts(
        raw_lines=101, parsed=99, quarantined=2, duplicates=0, committed=99
    )
    assert without_rule.rejected == ()
    # With the rule, 2 failed rows of 100 are more than 1%: the file gives no row at all.
    assert with_rule.counts == UnitCounts(
        raw_lines=1, parsed=1, quarantined=0, duplicates=0, committed=1
    )
    assert with_rule.quarantine.num_rows == 0
    assert [counts.key for counts in with_rule.files] == [good.bronze_key(Feed.MENTIONS)]
    (rejected,) = with_rule.rejected
    assert (rejected.key, rejected.raw_lines, rejected.quarantined_rows) == (
        SLOT.bronze_key(Feed.MENTIONS),
        100,
        2,
    )
    assert [row["slot_ts"] for row in mentions(with_rule)] == [good.start]


def test_a_file_without_lines_gives_no_row_and_is_not_rejected() -> None:
    parsed = parse([bronze_file(Feed.MENTIONS, [])], file_threshold=0.01)

    assert parsed.counts == UnitCounts(0, 0, 0, 0, 0)
    assert parsed.tables[GDELT_MENTION].num_rows == 0
    assert parsed.tables[GDELT_MENTION].schema == GDELT_MENTION_SCHEMA.as_arrow()
    assert [(counts.raw_lines, counts.parsed_rows) for counts in parsed.files] == [(0, 0)]
    assert parse([]).counts == UnitCounts(0, 0, 0, 0, 0)
