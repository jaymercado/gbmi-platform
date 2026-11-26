"""Record the GDELT fixture hour used by ``make demo`` and the tests (week 2 plan, WP-04).

The files of one UTC hour are fetched from the live source and checked against the master file
list with the gate of the pipeline (size, MD5, the zip opens). Events and Mentions files are
stored unchanged. A GKG file has several megabytes, so the fixture keeps the first records of
each GKG file, zipped again; the fixture index then gives the size and the MD5 of the shortened
file, and the attribution file says so.

The fixture directory gets the files, ``masterfilelist.txt`` with one line for each file in the
form GDELT uses, and ``lastupdate.txt`` with the lines of the newest slot of the hour.

Data: The GDELT Project, https://www.gdeltproject.org/

Usage:
    uv run python benchmarks/source_profile/record_gdelt_fixtures.py --hour 2026-11-25T08 \\
        --feeds mentions,gkg --gkg-records 60
"""

from __future__ import annotations

import argparse
import io
import zipfile
from datetime import UTC, datetime
from pathlib import Path

from gbmi.core.clock import utc_now
from gbmi.core.gdelt import Feed, Slot, parse_file_name, parse_hour, slots_of_hour
from gbmi.sources.gdelt.index import IndexLine, lines_for_hour, read_master_list
from gbmi.sources.gdelt.land import (
    FIXTURE_INDEX,
    FIXTURE_LAST_UPDATE,
    HttpFetcher,
    md5_hex,
    verify,
)

#: GDELT lists the three files of a slot in this order (week 2 plan, F3).
FEED_ORDER = (Feed.EXPORT, Feed.MENTIONS, Feed.GKG)


def shorten(content: bytes, records: int, slot: Slot) -> bytes:
    """A zip with the first ``records`` lines of the one member of ``content``."""
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        (member,) = archive.namelist()
        lines = archive.read(member).split(b"\n")
    kept = b"\n".join(lines[:records]) + b"\n"
    start = slot.start
    info = zipfile.ZipInfo(
        member, date_time=(start.year, start.month, start.day, start.hour, start.minute, 0)
    )
    info.compress_type = zipfile.ZIP_DEFLATED
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(info, kept)
    return buffer.getvalue()


def as_listed(line: IndexLine, content: bytes) -> str:
    """The index line for ``content``, in the form of the GDELT lists (an http:// URL)."""
    url = "http://" + line.url.removeprefix("https://")
    return f"{len(content)} {md5_hex(content)} {url}"


def order(text_line: str) -> tuple[Slot, int]:
    slot, feed = parse_file_name(text_line.rsplit("/", 1)[-1])
    return slot, FEED_ORDER.index(feed)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--hour", default="2026-11-25T08", help="UTC hour, YYYY-MM-DDTHH")
    parser.add_argument("--out", type=Path, default=Path("tests/fixtures/gdelt/v2"))
    parser.add_argument(
        "--feeds", default="export,mentions,gkg", help="feeds to record, separated by commas"
    )
    parser.add_argument(
        "--gkg-records", type=int, default=60, help="records kept of each GKG file; 0 keeps all"
    )
    args = parser.parse_args()

    slot_date, slot_hour = parse_hour(args.hour)
    feeds = [Feed(name) for name in args.feeds.split(",")]
    first = Slot(datetime(slot_date.year, slot_date.month, slot_date.day, slot_hour, tzinfo=UTC))
    fetcher = HttpFetcher()
    listed = read_master_list(fetcher, oldest=first, as_of=utc_now())

    index_path = args.out / FIXTURE_INDEX
    recorded = set()
    lines: dict[str, str] = {}
    if index_path.exists():  # keep the lines of the feeds which are not recorded now
        for text_line in index_path.read_text(encoding="utf-8").splitlines():
            lines[text_line.rsplit("/", 1)[-1]] = text_line

    for feed in feeds:
        wanted = lines_for_hour(listed, slot_date, slot_hour, feed)
        if len(wanted) != len(slots_of_hour(slot_date, slot_hour)):
            raise SystemExit(f"the master list holds {len(wanted)} {feed.value} files of the hour")
        target = args.out / feed.value
        target.mkdir(parents=True, exist_ok=True)
        for line in wanted:
            content = fetcher.fetch(line).content
            verify(content, line)  # the real file passes the gate before anything is kept
            if feed is Feed.GKG and args.gkg_records:
                content = shorten(content, args.gkg_records, line.slot)
            (target / line.file_name).write_bytes(content)
            lines[line.file_name] = as_listed(line, content)
            recorded.add(line.file_name)
            print(f"{line.file_name}  {len(content):>8} bytes  md5 {md5_hex(content)}")

    ordered = sorted(lines.values(), key=order)
    index_path.write_text("\n".join(ordered) + "\n", encoding="utf-8")
    newest = max(order(text_line)[0] for text_line in ordered)
    last_update = [text_line for text_line in ordered if order(text_line)[0] == newest]
    (args.out / FIXTURE_LAST_UPDATE).write_text("\n".join(last_update) + "\n", encoding="utf-8")
    print(
        f"{len(recorded)} files recorded under {args.out}; the index holds {len(ordered)} lines, "
        f"and {FIXTURE_LAST_UPDATE} the {len(last_update)} lines of slot {newest.stamp}"
    )


if __name__ == "__main__":
    main()
