"""The readers of the GDELT file lists: how much is read, and which lines are trusted."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from gbmi.core.gdelt import Feed, Slot
from gbmi.sources.gdelt.index import (
    BYTES_PER_SLOT,
    MIN_TAIL_BYTES,
    IndexTail,
    latest,
    parse_index,
    parse_tail,
    read_last_update,
    read_master_list,
    tail_bytes_for,
)

T0 = datetime(2026, 11, 25, 8, 0, tzinfo=UTC)


def text_line(
    minutes: int, feed: Feed = Feed.EXPORT, size: int = 81069, md5: str = "a" * 32
) -> str:
    slot = Slot(T0 + timedelta(minutes=minutes))
    return f"{size} {md5} http://data.gdeltproject.org/gdeltv2/{slot.file_name(feed)}"


class ListSource:
    """A master list in memory which serves tails and remembers what was asked."""

    def __init__(self, lines: list[str], last_update: str = "") -> None:
        self.text = "\n".join(lines) + "\n"
        self.asked: list[int] = []
        self._last_update = last_update

    def last_update(self) -> str:
        return self._last_update

    def master_tail(self, tail_bytes: int) -> IndexTail:
        self.asked.append(tail_bytes)
        if tail_bytes >= len(self.text):
            return IndexTail(self.text, complete=True)
        return IndexTail(self.text[-tail_bytes:], complete=False)


def test_the_first_line_of_a_cut_tail_is_never_trusted() -> None:
    whole = text_line(0) + "\n" + text_line(15) + "\n"
    # The cut removes the first digit of the size: the rest still looks like a line.
    cut = whole[1:]
    assert parse_index(cut)[0].size == 1069  # the wrong size which a naive reader would take

    lines = parse_tail(IndexTail(cut, complete=False))

    assert [(line.slot.stamp, line.size) for line in lines] == [("20261125081500", 81069)]
    assert len(parse_tail(IndexTail(whole, complete=True))) == 2


def test_the_tail_is_sized_from_the_oldest_wanted_slot() -> None:
    as_of = T0 + timedelta(days=30)

    assert tail_bytes_for(Slot(as_of - timedelta(hours=1)), as_of) == MIN_TAIL_BYTES
    # Thirty days need 1,383,360 bytes with the margin: the smallest tail covers them.
    assert int((30 * 96 + 2) * BYTES_PER_SLOT * 1.5) == 1_383_360 < MIN_TAIL_BYTES
    assert tail_bytes_for(Slot(T0), as_of) == MIN_TAIL_BYTES
    # Ninety days need more than the smallest tail.
    assert tail_bytes_for(Slot(T0), as_of + timedelta(days=60)) == int(
        (90 * 96 + 2) * BYTES_PER_SLOT * 1.5
    )
    # A slot ahead of the clock asks for the smallest tail.
    assert tail_bytes_for(Slot(as_of + timedelta(hours=1)), as_of) == MIN_TAIL_BYTES


def test_the_reader_doubles_the_tail_until_it_reaches_behind_the_oldest_slot() -> None:
    # 40,000 slots of one feed: about 4 MB of text, more than the smallest tail.
    lines = [text_line(15 * index) for index in range(40_000)]
    source = ListSource(lines)
    oldest = Slot(T0 + timedelta(minutes=15 * 10_000))
    as_of = T0 + timedelta(minutes=15 * 10_050)  # the clock suggests a small tail

    found = read_master_list(source, oldest=oldest, as_of=as_of)

    assert source.asked == [MIN_TAIL_BYTES, 2 * MIN_TAIL_BYTES]
    assert len(found) == 30_000
    assert min(line.slot for line in found) == oldest


def test_the_reader_stops_at_the_whole_list_and_never_assumes_sorted_lines() -> None:
    # Slot 02:45 stands after slot 04:30, as in the real list (week 2 plan, F3).
    lines = [text_line(0), text_line(30), text_line(45), text_line(15)]
    source = ListSource(lines)

    found = read_master_list(source, oldest=Slot(T0 + timedelta(minutes=15)), as_of=T0)

    assert source.asked == [MIN_TAIL_BYTES]  # the whole list came back: no second request
    assert [line.slot.stamp for line in found] == [
        "20261125083000",
        "20261125084500",
        "20261125081500",
    ]
    older = read_master_list(source, oldest=Slot(T0 - timedelta(days=1)), as_of=T0)
    assert len(older) == 4


def test_a_file_listed_twice_counts_with_its_later_line() -> None:
    lines = parse_index(
        "\n".join(
            [text_line(0, size=10), text_line(0, Feed.GKG), text_line(0, size=12, md5="b" * 32)]
        )
    )

    by_file = latest(lines)

    assert len(by_file) == 2
    assert by_file[Feed.EXPORT, Slot(T0)].size == 12


def test_last_update_gives_the_lines_of_the_newest_slot() -> None:
    source = ListSource([], last_update="\n".join(text_line(45, feed) for feed in Feed))

    assert [(line.feed, line.slot.stamp) for line in read_last_update(source)] == [
        (Feed.EXPORT, "20261125084500"),
        (Feed.MENTIONS, "20261125084500"),
        (Feed.GKG, "20261125084500"),
    ]
