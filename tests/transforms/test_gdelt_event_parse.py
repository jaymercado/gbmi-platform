"""DuckDB parsing of GDELT export files: gate G2 and the accounting identities.

Real slots rarely contain a bad row, so the quarantine path is proved with a synthetic file
of three lines - one good row, one with 60 columns, one with a non-numeric id (week 1 plan,
WP-09) - and the recorded fixture hour proves the schema against real data.
"""

from __future__ import annotations

import io
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pytest

from gbmi.core.gdelt import Feed, Slot
from gbmi.core.gdelt_export import EVENT_KEY, EXPORT_COLUMNS
from gbmi.lake.commit import Unit
from gbmi.lake.tables import GDELT_EVENT_SCHEMA
from gbmi.quality.audit import AuditError, UnitCounts, audit_unit
from gbmi.sources.gdelt.land import md5_hex
from gbmi.transforms.duckdb.gdelt_event import BronzeFile, parse_events, raw_lines

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "gdelt" / "v2" / "export"
SLOT = Slot.parse("20261125084500")
INGESTED_AT = datetime(2026, 11, 25, 9, 0, tzinfo=UTC)


def export_row(**overrides: str) -> str:
    """One export row with 61 fields; anything not given is empty or a plausible default."""
    fields = dict.fromkeys(EXPORT_COLUMNS, "")
    fields.update(
        GLOBALEVENTID="1334927362", SQLDATE="20261125", MonthYear="202611", Year="2026",
        FractionDate="2026.8904", Actor1Code="BUS", Actor1Name="COMPANY", Actor1Type1Code="BUS",
        IsRootEvent="1", EventCode="0841", EventBaseCode="084", EventRootCode="08",
        QuadClass="2", GoldsteinScale="7.0", NumMentions="5", NumSources="1", NumArticles="5",
        AvgTone="2.46753246753247", ActionGeo_Type="3",
        ActionGeo_FullName="Decatur, Alabama, United States", ActionGeo_CountryCode="US",
        DATEADDED="20261125084500", SOURCEURL="https://example.org/a-story",
    )  # fmt: skip
    fields.update(overrides)
    return "\t".join(fields[name] for name in EXPORT_COLUMNS)


def bronze_file(lines: list[str], slot: Slot = SLOT) -> BronzeFile:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            slot.file_name(Feed.EXPORT).removesuffix(".zip"), "".join(f"{line}\n" for line in lines)
        )
    content = buffer.getvalue()
    return BronzeFile(key=slot.bronze_key(Feed.EXPORT), md5=md5_hex(content), content=content)


def parse(files: list[BronzeFile]):  # type: ignore[no-untyped-def]
    return parse_events(files, run_id="test-run", pipeline_version="test", ingested_at=INGESTED_AT)


def test_three_line_file_one_good_two_quarantined() -> None:
    good = export_row()
    sixty_columns = "\t".join(export_row(GLOBALEVENTID="2").split("\t")[:-1])
    non_numeric_id = export_row(GLOBALEVENTID="not-a-number")

    parsed = parse([bronze_file([good, sixty_columns, non_numeric_id])])

    # raw lines = parsed + quarantined, and committed = parsed - duplicates.
    assert parsed.counts == UnitCounts(
        raw_lines=3, parsed=1, quarantined=2, duplicates=0, committed=1
    )
    assert parsed.events[EVENT_KEY].to_pylist() == [1334927362]
    quarantined = parsed.quarantine.to_pylist()
    assert [(row["line_number"], row["reason"]) for row in quarantined] == [
        (2, "column_count: expected 61, found 60"),
        (3, "global_event_id (GLOBALEVENTID): not an integer"),
    ]
    # A quarantined row keeps the raw line, its source file and the run that rejected it.
    assert quarantined[0]["raw_line"] == sixty_columns
    assert quarantined[1]["source_file"] == SLOT.bronze_key(Feed.EXPORT)
    assert {row["_run_id"] for row in quarantined} == {"test-run"}
    audit_unit(parsed.counts, parsed.events, Unit(date(2026, 11, 25), 8), key=EVENT_KEY)


def test_good_row_is_typed_and_carries_provenance() -> None:
    file = bronze_file([export_row()])

    (row,) = parse([file]).events.to_pylist()

    assert row["global_event_id"] == 1334927362
    assert (row["slot_date"], row["slot_hour"]) == (date(2026, 11, 25), 8)
    assert row["slot_ts"] == datetime(2026, 11, 25, 8, 45, tzinfo=UTC)
    assert row["event_date"] == date(2026, 11, 25)
    assert row["date_added"] == datetime(2026, 11, 25, 8, 45, tzinfo=UTC)
    assert row["is_root_event"] is True
    assert (row["event_code"], row["event_base_code"], row["event_root_code"]) == (
        "0841",
        "084",
        "08",
    )
    assert (row["quad_class"], row["num_mentions"], row["num_sources"], row["num_articles"]) == (
        2,
        5,
        1,
        5,
    )
    assert row["goldstein_scale"] == 7.0
    assert row["avg_tone"] == pytest.approx(2.46753246753247)
    assert row["actor1_type1_code"] == "BUS"
    # An empty field is NULL, not an empty string.
    assert row["actor2_code"] is None
    assert row["action_geo_country_code"] == "US"
    assert row["_source_file"] == file.key
    assert row["_source_md5"] == file.md5
    assert row["_slot_ts"] == row["slot_ts"]
    assert (row["_ingested_at"], row["_run_id"], row["_pipeline_version"]) == (
        INGESTED_AT,
        "test-run",
        "test",
    )


def test_output_has_exactly_the_silver_schema_in_utc() -> None:
    events = parse([bronze_file([export_row()])]).events

    assert events.schema.names == [field.name for field in GDELT_EVENT_SCHEMA.fields]
    assert events.schema.equals(GDELT_EVENT_SCHEMA.as_arrow(), check_metadata=False)
    for name in ("slot_ts", "date_added", "_slot_ts", "_ingested_at"):
        assert events.schema.field(name).type == pa.timestamp("us", tz="UTC")


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"GLOBALEVENTID": ""}, "global_event_id (GLOBALEVENTID): not an integer"),
        ({"GLOBALEVENTID": "12.5"}, "global_event_id (GLOBALEVENTID): not an integer"),
        ({"SQLDATE": "2026112"}, "event_date (SQLDATE): not a date YYYYMMDD"),
        ({"SQLDATE": "20261341"}, "event_date (SQLDATE): not a date YYYYMMDD"),
        ({"IsRootEvent": "2"}, "is_root_event (IsRootEvent): not 0 or 1"),
        ({"QuadClass": "two"}, "quad_class (QuadClass): not an integer"),
        ({"NumMentions": "5.5"}, "num_mentions (NumMentions): not an integer"),
        ({"NumArticles": "99999999999"}, "num_articles (NumArticles): not an integer"),
        ({"GoldsteinScale": "high"}, "goldstein_scale (GoldsteinScale): not a number"),
        ({"AvgTone": "1,5"}, "avg_tone (AvgTone): not a number"),
        (
            {"DATEADDED": "2026-11-25 08:45"},
            "date_added (DATEADDED): not a timestamp YYYYMMDDHHMMSS",
        ),
    ],
)
def test_a_field_that_cannot_be_typed_quarantines_the_row(
    overrides: dict[str, str], reason: str
) -> None:
    parsed = parse([bronze_file([export_row(), export_row(**{"GLOBALEVENTID": "7", **overrides})])])

    assert parsed.counts.quarantined == 1
    assert parsed.quarantine["reason"].to_pylist() == [reason]
    assert parsed.events[EVENT_KEY].to_pylist() == [1334927362]


def test_empty_optional_fields_are_null_and_exponents_are_numbers() -> None:
    row = export_row(
        QuadClass="",
        GoldsteinScale="",
        AvgTone="-1.5e-05",
        SQLDATE="",
        DATEADDED="",
        IsRootEvent="",
    )

    parsed = parse([bronze_file([row])])

    assert parsed.counts.quarantined == 0
    (event,) = parsed.events.to_pylist()
    assert event["quad_class"] is None
    assert event["goldstein_scale"] is None
    assert event["event_date"] is None
    assert event["date_added"] is None
    assert event["is_root_event"] is None
    assert event["avg_tone"] == pytest.approx(-1.5e-05)


def test_duplicates_keep_the_first_occurrence_in_slot_and_line_order() -> None:
    earlier, later = Slot.parse("20261125080000"), Slot.parse("20261125081500")
    files = [
        # Given out of order on purpose: the result must not depend on file order.
        bronze_file(
            [export_row(GLOBALEVENTID="1", NumMentions="9"), export_row(GLOBALEVENTID="2")], later
        ),
        bronze_file(
            [
                export_row(GLOBALEVENTID="1", NumMentions="3"),
                export_row(GLOBALEVENTID="1", NumMentions="4"),
            ],
            earlier,
        ),
    ]

    parsed = parse(files)

    assert parsed.counts == UnitCounts(
        raw_lines=4, parsed=4, quarantined=0, duplicates=2, committed=2
    )
    events = {row[EVENT_KEY]: row for row in parsed.events.to_pylist()}
    assert events[1]["num_mentions"] == 3
    assert events[1]["slot_ts"] == earlier.start
    audit_unit(parsed.counts, parsed.events, Unit(date(2026, 11, 25), 8), key=EVENT_KEY)


def test_blank_line_and_invalid_bytes_are_counted_not_lost() -> None:
    buffer = io.BytesIO()
    payload = (
        export_row().encode()
        + b"\n\n"
        + export_row(GLOBALEVENTID="5", Actor1Name="A\xffB").encode("latin-1")
        + b"\n"
    )
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("20261125084500.export.CSV", payload)
    file = BronzeFile(SLOT.bronze_key(Feed.EXPORT), md5_hex(buffer.getvalue()), buffer.getvalue())

    assert raw_lines([file]).num_rows == 3
    parsed = parse([file])

    assert parsed.counts == UnitCounts(
        raw_lines=3, parsed=2, quarantined=1, duplicates=0, committed=2
    )
    assert parsed.quarantine["reason"].to_pylist() == ["column_count: expected 61, found 1"]
    assert "\ufffd" in {row[EVENT_KEY]: row for row in parsed.events.to_pylist()}[5]["actor1_name"]


def test_no_files_gives_empty_tables_with_the_right_schema() -> None:
    parsed = parse([])

    assert parsed.counts == UnitCounts(0, 0, 0, 0, 0)
    assert parsed.events.num_rows == 0
    assert parsed.events.schema.names == [field.name for field in GDELT_EVENT_SCHEMA.fields]


def test_fixture_hour_parses_completely() -> None:
    files = [
        BronzeFile(
            Slot.parse(path.name[:14]).bronze_key(Feed.EXPORT),
            md5_hex(path.read_bytes()),
            path.read_bytes(),
        )
        for path in sorted(FIXTURES.glob("*.export.CSV.zip"))
    ]

    parsed = parse(files)

    assert len(files) == 4
    assert parsed.counts == UnitCounts(
        raw_lines=4023, parsed=4023, quarantined=0, duplicates=0, committed=4023
    )
    assert set(parsed.events["slot_hour"].to_pylist()) == {8}
    assert set(parsed.events["slot_date"].to_pylist()) == {date(2026, 11, 25)}
    audit_unit(parsed.counts, parsed.events, Unit(date(2026, 11, 25), 8), key=EVENT_KEY)


def test_audit_refuses_broken_accounting_duplicate_keys_and_foreign_rows() -> None:
    events = parse([bronze_file([export_row(), export_row(GLOBALEVENTID="2")])]).events
    unit = Unit(date(2026, 11, 25), 8)
    good = UnitCounts(raw_lines=2, parsed=2, quarantined=0, duplicates=0, committed=2)

    audit_unit(good, events, unit, key=EVENT_KEY)
    with pytest.raises(AuditError, match="raw lines"):
        audit_unit(UnitCounts(3, 2, 0, 0, 2), events, unit, key=EVENT_KEY)
    with pytest.raises(AuditError, match="committed"):
        audit_unit(UnitCounts(2, 2, 0, 1, 2), events, unit, key=EVENT_KEY)
    with pytest.raises(AuditError, match="not unique"):
        audit_unit(
            UnitCounts(4, 4, 0, 0, 4), pa.concat_tables([events, events]), unit, key=EVENT_KEY
        )
    with pytest.raises(AuditError, match="slot_hour"):
        audit_unit(good, events, Unit(date(2026, 11, 25), 9), key=EVENT_KEY)
