"""Slots, file names, bronze keys and index lines of GDELT 2.0."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone

import pytest

from gbmi.core.gdelt import Feed, Slot, parse_file_name, parse_hour, slots_of_hour
from gbmi.core.gdelt_export import EVENT_COLUMNS, EXPORT_COLUMNS
from gbmi.sources.gdelt.index import lines_for_hour, parse_index, parse_index_line

LINE = (
    "76194 ef823dc41a6cd740f4ccdb42cf144b26 "
    "http://data.gdeltproject.org/gdeltv2/20261125084500.export.CSV.zip"
)


def test_slot_round_trip_and_bronze_key() -> None:
    slot = Slot.parse("20261125084500")

    assert slot.start == datetime(2026, 11, 25, 8, 45, tzinfo=UTC)
    assert (slot.slot_date, slot.slot_hour, slot.stamp) == (date(2026, 11, 25), 8, "20261125084500")
    assert slot.file_name(Feed.EXPORT) == "20261125084500.export.CSV.zip"
    assert slot.file_name(Feed.GKG) == "20261125084500.gkg.csv.zip"
    # The key of blueprint 7.1.
    assert slot.bronze_key(Feed.EXPORT) == (
        "raw/gdelt/v2/export/dt=2026-11-25/20261125084500.export.CSV.zip"
    )


@pytest.mark.parametrize(
    "start",
    [
        datetime(2026, 11, 25, 8, 45),  # noqa: DTZ001 - no time zone, on purpose
        datetime(2026, 11, 25, 8, 45, tzinfo=timezone(timedelta(hours=8))),  # not UTC
        datetime(2026, 11, 25, 8, 50, tzinfo=UTC),  # not on a quarter hour
        datetime(2026, 11, 25, 8, 45, 1, tzinfo=UTC),
    ],
)
def test_slot_must_be_a_utc_quarter_hour(start: datetime) -> None:
    with pytest.raises(ValueError, match="a slot starts"):
        Slot(start)


def test_day_boundary_is_the_utc_day_of_the_slot() -> None:
    last, first = Slot.parse("20261125234500"), Slot.parse("20261126000000")

    assert (last.slot_date, last.slot_hour) == (date(2026, 11, 25), 23)
    assert (first.slot_date, first.slot_hour) == (date(2026, 11, 26), 0)


def test_hour_and_its_four_slots() -> None:
    assert parse_hour("2026-11-25T08") == (date(2026, 11, 25), 8)
    assert [slot.stamp for slot in slots_of_hour(date(2026, 11, 25), 8)] == [
        "20261125080000", "20261125081500", "20261125083000", "20261125084500",
    ]  # fmt: skip
    for wrong in ("2026-11-25", "2026-11-25T8", "2026-11-25T24", "20261125T08"):
        with pytest.raises(ValueError, match="hour"):
            parse_hour(wrong)


def test_file_names() -> None:
    assert parse_file_name("20261125084500.gkg.csv.zip") == (Slot.parse("20261125084500"), Feed.GKG)
    for wrong in (
        "20261125084500.translation.export.CSV.zip",
        "lastupdate.txt",
        "2026112508.export.CSV.zip",
    ):
        with pytest.raises(ValueError, match=r"not a GDELT 2\.0 file name"):
            parse_file_name(wrong)


def test_index_line_is_parsed_and_its_url_made_https() -> None:
    line = parse_index_line(LINE)

    assert line is not None
    assert (line.size, line.md5, line.feed) == (
        76194,
        "ef823dc41a6cd740f4ccdb42cf144b26",
        Feed.EXPORT,
    )
    assert line.url == "https://data.gdeltproject.org/gdeltv2/20261125084500.export.CSV.zip"
    assert line.file_name == "20261125084500.export.CSV.zip"
    assert line.slot == Slot.parse("20261125084500")


def test_lines_that_are_not_gdelt_files_are_skipped() -> None:
    text = "\n".join(
        [
            LINE,
            "",
            "not an index line",
            "12 abc http://data.gdeltproject.org/gdeltv2/20261125084500.export.CSV.zip",  # bad MD5
            "5 ef823dc41a6cd740f4ccdb42cf144b26 http://data.gdeltproject.org/gdeltv2/readme.txt",
            LINE.replace("0845", "0900").replace("export.CSV", "gkg.csv"),
        ]
    )

    lines = parse_index(text)

    assert [(line.slot.stamp, line.feed) for line in lines] == [
        ("20261125084500", Feed.EXPORT),
        ("20261125090000", Feed.GKG),
    ]
    assert [
        line.slot.stamp for line in lines_for_hour(lines, date(2026, 11, 25), 8, Feed.EXPORT)
    ] == ["20261125084500"]
    assert lines_for_hour(lines, date(2026, 11, 25), 8, Feed.GKG) == []


def test_export_schema_has_61_columns_and_known_positions() -> None:
    assert len(EXPORT_COLUMNS) == 61
    assert len(set(EXPORT_COLUMNS)) == 61
    positions = {column.name: column.position for column in EVENT_COLUMNS}
    # Positions from the GDELT Event Codebook V2.0, one-based.
    assert positions["global_event_id"] == 1
    assert positions["event_date"] == 2
    assert positions["actor1_type1_code"] == 13
    assert positions["event_root_code"] == 29
    assert positions["avg_tone"] == 35
    assert positions["action_geo_country_code"] == 54
    assert positions["date_added"] == 60
    assert positions["source_url"] == 61
