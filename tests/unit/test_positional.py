"""The file-level rule of gate G2 as a pure function, and the three feed definitions."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from gbmi.core.gdelt_export import EVENT_COLUMNS, EXPORT
from gbmi.core.gdelt_gkg import DOC_COLUMNS, GKG
from gbmi.core.gdelt_mentions import MENTION_COLUMNS, MENTIONS
from gbmi.core.positional import FILE_THRESHOLD, Kind, file_is_rejected


@pytest.mark.parametrize(
    ("raw_lines", "failed", "rejected"),
    [
        (0, 0, False),  # a file without lines
        (100, 0, False),
        (100, 1, False),  # exactly 1% is not more than 1%
        (100, 2, True),
        (99, 1, True),  # 1.01%
        (1000, 10, False),
        (1000, 11, True),
        (1, 1, True),
    ],
)
def test_a_file_with_more_than_one_percent_of_failed_rows_is_rejected(
    raw_lines: int, failed: int, rejected: bool
) -> None:
    assert FILE_THRESHOLD == 0.01
    assert file_is_rejected(raw_lines, failed) is rejected


def test_the_threshold_is_an_argument_and_impossible_counts_are_refused() -> None:
    assert file_is_rejected(100, 5, threshold=0.05) is False
    assert file_is_rejected(100, 6, threshold=0.05) is True
    with pytest.raises(ValueError, match="cannot come from"):
        file_is_rejected(10, 11)
    with pytest.raises(ValueError, match="cannot come from"):
        file_is_rejected(10, -1)


@given(st.integers(min_value=0, max_value=10**6), st.data())
def test_more_failed_rows_never_save_a_file(raw_lines: int, data: st.DataObject) -> None:
    failed = data.draw(st.integers(min_value=0, max_value=raw_lines))
    more = data.draw(st.integers(min_value=failed, max_value=raw_lines))

    if file_is_rejected(raw_lines, failed):
        assert file_is_rejected(raw_lines, more)


def test_the_three_feeds_have_61_16_and_27_columns() -> None:
    assert (EXPORT.column_count, MENTIONS.column_count, GKG.column_count) == (61, 16, 27)
    assert (EXPORT.feed, MENTIONS.feed, GKG.feed) == ("export", "mentions", "gkg")


def test_the_typed_columns_sit_at_the_positions_of_the_codebooks() -> None:
    # One-based positions, from the GDELT Event Codebook V2.0 and the GKG Codebook V2.1.
    mention = {column.name: (column.position, column.kind) for column in MENTION_COLUMNS}
    assert mention == {
        "global_event_id": (1, Kind.KEY),
        "event_time_date": (2, Kind.TIMESTAMP14),
        "mention_type": (4, Kind.INTEGER),
        "mention_source_name": (5, Kind.TEXT),
        "mention_identifier": (6, Kind.REQUIRED_TEXT),
        "confidence": (12, Kind.INTEGER),
        "mention_doc_tone": (14, Kind.DOUBLE),
    }
    document = {column.name: (column.position, column.kind) for column in DOC_COLUMNS}
    assert document == {
        "gkg_record_id": (1, Kind.REQUIRED_TEXT),
        "doc_date": (2, Kind.REQUIRED_TIMESTAMP14),
        "source_collection_identifier": (3, Kind.INTEGER),
        "source_common_name": (4, Kind.TEXT),
        "document_url": (5, Kind.REQUIRED_TEXT),
    }
    assert [GKG.position(name) for name in ("V1THEMES", "V1LOCATIONS", "V1ORGANIZATIONS")] == [
        8,
        10,
        14,
    ]
    assert [GKG.position(name) for name in ("V2ENHANCEDORGANIZATIONS", "V1.5TONE", "V2GCAM")] == [
        15,
        16,
        18,
    ]
    assert GKG.position("V2EXTRASXML") == 27
    assert {column.name: column.position for column in EVENT_COLUMNS}["source_url"] == 61
