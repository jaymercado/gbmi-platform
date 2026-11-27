"""The source profile of the GDELT 2.0 master file list, from one stream of the list (WP-10).

The script reads ``masterfilelist.txt`` once, line by line, and keeps no copy. The list has one
line for each file which GDELT serves: the size in bytes, the MD5 hash and the address. The
profile gives:

- files and compressed bytes by month and feed;
- the slots of each feed: listed, listed more than once, first, last, and the slots of the
  15-minute calendar between the first and the last slot which the list does not hold;
- lines out of order: a line whose slot is older than a slot which stands before the line;
- slots which do not hold all three feeds; lines which are not a file of GDELT 2.0.

One request goes to the GDELT servers, with the default client name of the library. The
script writes only the result file.

Data: The GDELT Project, https://www.gdeltproject.org/

Run on the Compose network:
    make toolbox CMD="python -m benchmarks.source_profile.profile_master_list"
"""

from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

from gbmi.core.clock import utc_now
from gbmi.core.gdelt import SLOT_MINUTES, Feed
from gbmi.sources.gdelt.index import MASTER_LIST_URL, parse_index_line

RESULTS = Path(__file__).resolve().parent / "results"
STEP = timedelta(minutes=SLOT_MINUTES)
EXAMPLES = 10


class FeedProfile:
    """What the list says about one feed."""

    def __init__(self) -> None:
        self.slots: set[datetime] = set()
        self.lines = 0
        self.bytes = 0
        self.newest: datetime | None = None
        self.out_of_order = 0
        self.examples: list[dict[str, str]] = []

    def add(self, start: datetime, size: int) -> None:
        self.lines += 1
        self.bytes += size
        self.slots.add(start)
        if self.newest is not None and start < self.newest:
            self.out_of_order += 1
            self.examples.append(
                {"slot": f"{start:%Y-%m-%d %H:%M}", "stands_after": f"{self.newest:%Y-%m-%d %H:%M}"}
            )
            self.examples = self.examples[-EXAMPLES:]
        else:
            self.newest = start

    def summary(self) -> dict[str, Any]:
        first, last = min(self.slots), max(self.slots)
        calendar = int((last - first) / STEP) + 1
        missing = calendar - len(self.slots)
        by_year: Counter[int] = Counter()
        newest_missing: list[str] = []
        if missing:
            slot = first
            while slot <= last:
                if slot not in self.slots:
                    by_year[slot.year] += 1
                    newest_missing.append(f"{slot:%Y-%m-%d %H:%M}")
                slot += STEP
        return {
            "lines": self.lines,
            "compressed_bytes": self.bytes,
            "distinct_slots": len(self.slots),
            "lines_for_a_slot_listed_before": self.lines - len(self.slots),
            "first_slot": first.isoformat(),
            "last_slot": last.isoformat(),
            "calendar_slots_from_first_to_last": calendar,
            "calendar_slots_not_in_the_list": missing,
            "share_of_calendar_slots_in_the_list": round(len(self.slots) / calendar, 6),
            "calendar_slots_not_in_the_list_by_year": {
                str(k): v for k, v in sorted(by_year.items())
            },
            "newest_calendar_slots_not_in_the_list": newest_missing[-EXAMPLES:],
            "lines_out_of_order": self.out_of_order,
            "last_lines_out_of_order": self.examples,
        }


def profile(lines: Any) -> dict[str, Any]:
    """Profile an iterable of lines of the master list."""
    feeds = {feed: FeedProfile() for feed in Feed}
    months: dict[str, dict[str, list[int]]] = {}
    other_lines = 0
    other_examples: list[str] = []
    total = 0
    for line in lines:
        total += 1
        parsed = parse_index_line(line)
        if parsed is None:
            if line.strip():
                other_lines += 1
                if len(other_examples) < EXAMPLES:
                    other_examples.append(line.strip()[:120])
            continue
        start = parsed.slot.start
        feeds[parsed.feed].add(start, parsed.size)
        month = months.setdefault(f"{start:%Y-%m}", {})
        cell = month.setdefault(parsed.feed.value, [0, 0])
        cell[0] += 1
        cell[1] += parsed.size
    every = set.union(*(feed.slots for feed in feeds.values()))
    in_all = set.intersection(*(feed.slots for feed in feeds.values()))
    return {
        "lines": total,
        "lines_which_are_no_file_of_gdelt_2": other_lines,
        "examples_of_other_lines": other_examples,
        "slots_in_one_feed_or_more": len(every),
        "slots_in_all_three_feeds": len(in_all),
        "feeds": {feed.value: profile.summary() for feed, profile in feeds.items()},
        "by_month": {
            month: {
                feed: {"files": cell[0], "compressed_bytes": cell[1]}
                for feed, cell in sorted(cells.items())
            }
            for month, cells in sorted(months.items())
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument(
        "--file", type=Path, default=None, help="read a saved copy, send no request"
    )
    parser.add_argument("--out", type=Path, default=None)
    arguments = parser.parse_args()
    measured_at = utc_now()
    out = arguments.out or RESULTS / f"master-list-{measured_at:%Y-%m-%d}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    if arguments.file is not None:
        with arguments.file.open(encoding="utf-8") as handle:
            result = profile(handle)
        source: dict[str, Any] = {"file": str(arguments.file)}
    else:
        with (
            httpx.Client(timeout=120, follow_redirects=True) as client,
            client.stream("GET", MASTER_LIST_URL) as response,
        ):
            response.raise_for_status()
            result = profile(response.iter_lines())
            source = {
                "url": MASTER_LIST_URL,
                "status": response.status_code,
                "content_length": response.headers.get("Content-Length"),
                "last_modified": response.headers.get("Last-Modified"),
                "etag": response.headers.get("ETag"),
                "bytes_read": response.num_bytes_downloaded,
            }
    seconds = time.perf_counter() - started
    result = {
        "measured_at": measured_at.isoformat(timespec="seconds"),
        "source": source,
        "seconds": round(seconds, 1),
        **result,
    }
    out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(
        json.dumps({k: v for k, v in result.items() if k != "by_month"}, indent=2, sort_keys=True)
    )
    print(f"months: {len(result['by_month'])}; the profile is in {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
