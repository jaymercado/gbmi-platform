"""Record the GDELT fixture hour used by ``make demo`` and the tests (week 1 plan, WP-09).

The four ``export`` slots of one UTC hour are landed from the live source with the same code
that the pipeline uses - which is also the proof of the live download path - and then copied,
unchanged, into the fixture directory together with their index lines exactly as GDELT served
them.

Data: The GDELT Project, https://www.gdeltproject.org/

Usage:
    uv run python benchmarks/source_profile/record_gdelt_fixtures.py --hour 2026-11-25T08
"""

from __future__ import annotations

import argparse
import shutil
import tempfile
from pathlib import Path

from gbmi.core.gdelt import Feed, parse_hour, slots_of_hour
from gbmi.sources.gdelt.land import FIXTURE_INDEX, Bronze, HttpFetcher, land_hour


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--hour", default="2026-11-25T08", help="UTC hour, YYYY-MM-DDTHH")
    parser.add_argument("--out", type=Path, default=Path("tests/fixtures/gdelt/v2"))
    args = parser.parse_args()

    slot_date, slot_hour = parse_hour(args.hour)
    fetcher = HttpFetcher()
    wanted = {slot.file_name(Feed.EXPORT) for slot in slots_of_hour(slot_date, slot_hour)}
    # The index lines are kept verbatim, http:// URLs included.
    raw_lines = [
        line for line in fetcher.index_text().splitlines() if line.rsplit("/", 1)[-1] in wanted
    ]

    with tempfile.TemporaryDirectory() as scratch:
        _, landed = land_hour(
            Bronze(f"file://{scratch}"), fetcher, slot_date, slot_hour, Feed.EXPORT
        )
        target = args.out / Feed.EXPORT.value
        target.mkdir(parents=True, exist_ok=True)
        for file in landed:
            shutil.copyfile(Path(scratch) / file.key, target / file.key.rsplit("/", 1)[-1])
            print(f"{file.key.rsplit('/', 1)[-1]}  {file.size:>7} bytes  md5 {file.md5}")

    if len(landed) != len(wanted) or len(raw_lines) != len(wanted):
        raise SystemExit(
            f"expected {len(wanted)} slots, landed {len(landed)}, listed {len(raw_lines)}"
        )
    (args.out / FIXTURE_INDEX).write_text("\n".join(raw_lines) + "\n", encoding="utf-8")
    print(f"{len(landed)} files and their index lines recorded under {args.out}")


if __name__ == "__main__":
    main()
