"""The source profile of a window of days, from the bronze files and the silver tables (WP-10).

No file is downloaded: the script reads the zip files which the backfill landed, the silver
tables which the day builds wrote, and the ledger. For each feed the script gives:

- files, bytes (zipped and as text) and lines of each day;
- the bytes of each column, summed over the window, and the share of the column;
- lines whose number of columns differs from the codebook (61, 16 and 27).

And the facts which the week 2 plan lists (WP-10, step 1):

- Events: values of GLOBALEVENTID which occur in more than one slot of a day;
- Mentions: the share of repeated pairs of event and article in one slot (finding F5);
- GKG: the share of the column V2GCAM; documents with a title; copies of a normalised title
  within a day; distinct organisation strings for each day; documents whose organisations
  come from the V1 column only.

The script only reads. One day of the largest feed takes some seconds.

Data: The GDELT Project, https://www.gdeltproject.org/

Run on the Compose network of the working stack:
    make toolbox CMD="python -m benchmarks.source_profile.profile_window \
        --from 2026-10-26 --to 2026-11-24"
"""

from __future__ import annotations

import argparse
import hashlib
import json
import resource
import statistics
import time
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow as pa

from gbmi.config import get_settings
from gbmi.core.clock import parse_day, utc_now
from gbmi.core.gdelt import Feed, Slot, slots_of_hour
from gbmi.core.gdelt_export import EXPORT_COLUMNS
from gbmi.core.gdelt_gkg import GKG, GKG_COLUMNS
from gbmi.core.gdelt_mentions import MENTIONS_COLUMNS
from gbmi.lake.catalog import load_lake_catalog
from gbmi.lake.commit import Unit
from gbmi.ledger.calendar import days_between
from gbmi.ledger.repository import open_ledger
from gbmi.transforms.duckdb.positional import BronzeFile, arrow, raw_lines
from gbmi.transforms.duckdb.runner import connect

RESULTS = Path(__file__).resolve().parent / "results"
COLUMNS: dict[Feed, tuple[str, ...]] = {
    Feed.EXPORT: tuple(EXPORT_COLUMNS),
    Feed.MENTIONS: tuple(MENTIONS_COLUMNS),
    Feed.GKG: tuple(GKG_COLUMNS),
}


def lake_path() -> Path:
    return Path(get_settings().lake_uri.removeprefix("file://"))


def hour_files(feed: Feed, day: date, hour: int) -> list[BronzeFile]:
    """The landed files of one hour, read from bronze."""
    files = []
    for slot in slots_of_hour(day, hour):
        key = slot.bronze_key(feed)
        path = lake_path() / key
        if path.is_file():
            content = path.read_bytes()
            md5 = hashlib.md5(content, usedforsecurity=False).hexdigest()
            files.append(BronzeFile(key=key, md5=md5, content=content))
    return files


def gkg_line_facts(connection: Any) -> dict[str, int]:
    """Lines of one hour by the organisation column which is filled, and lines with a title tag.

    Silver takes organisations from the V2 column and falls back to the V1 column (difference
    D20 of the week 2 plan), so a line which fills only the V1 column is a "V1-only" record.
    """
    v1 = f"f[{GKG.position('V1ORGANIZATIONS')}]"
    v2 = f"f[{GKG.position('V2ENHANCEDORGANIZATIONS')}]"
    extras = f"f[{GKG.position('V2EXTRASXML')}]"
    row = connection.execute(
        f"select count(*) filter (where {v2} <> ''), "
        f"count(*) filter (where {v2} = '' and {v1} <> ''), "
        f"count(*) filter (where {v2} = '' and {v1} = ''), "
        f"count(*) filter (where contains({extras}, '<PAGE_TITLE>')) "
        f"from split where len(f) = {GKG.column_count}"
    ).fetchone()
    assert row is not None
    return {
        "with_v2_organisations": int(row[0]),
        "with_v1_organisations_only": int(row[1]),
        "without_organisations": int(row[2]),
        "with_a_title_tag": int(row[3]),
    }


def profile_feed(feed: Feed, days: list[date]) -> dict[str, Any]:
    """Lines and bytes of each column of one feed, over the days."""
    names = COLUMNS[feed]
    positions = range(1, len(names) + 1)
    sums = ", ".join(f"sum(strlen(f[{index}]))" for index in positions)
    filled = ", ".join(f"count(*) filter (where f[{index}] <> '')" for index in positions)
    column_bytes = [0] * len(names)
    column_filled = [0] * len(names)
    gkg_lines: Counter[str] = Counter()
    by_day: dict[str, dict[str, int]] = {}
    odd_lines: Counter[int] = Counter()
    repeated_ids: dict[str, int] = {}
    for day in days:
        totals = Counter({"files": 0, "zip_bytes": 0, "text_bytes": 0, "lines": 0})
        slots_of_ids: list[pa.Table] = []
        for hour in range(24):
            files = hour_files(feed, day, hour)
            if not files:
                continue
            connection = connect()
            try:
                connection.register("raw_lines", raw_lines(files))
                connection.execute(
                    "create temp table split as "
                    "select slot_ts, strlen(line) as bytes, string_split(line, chr(9)) as f "
                    "from raw_lines"
                )
                counted = connection.execute(
                    f"select count(*), coalesce(sum(bytes), 0), {sums}, {filled} from split "
                    f"where len(f) = {len(names)}"
                ).fetchone()
                assert counted is not None
                if feed is Feed.GKG:
                    gkg_lines.update(gkg_line_facts(connection))
                for width, lines in connection.execute(
                    f"select len(f), count(*) from split where len(f) <> {len(names)} group by 1"
                ).fetchall():
                    odd_lines[int(width)] += int(lines)
                    totals["lines"] += int(lines)
                if feed is Feed.EXPORT:
                    slots_of_ids.append(
                        arrow(connection.sql("select distinct f[1] as id, slot_ts from split"))
                    )
            finally:
                connection.close()
            totals["files"] += len(files)
            totals["zip_bytes"] += sum(len(file.content) for file in files)
            totals["lines"] += int(counted[0])
            totals["text_bytes"] += int(counted[1])
            for index in range(len(names)):
                column_bytes[index] += int(counted[2 + index] or 0)
                column_filled[index] += int(counted[2 + len(names) + index] or 0)
        by_day[day.isoformat()] = dict(totals)
        if feed is Feed.EXPORT and slots_of_ids:
            connection = connect()
            try:
                connection.register("ids", pa.concat_tables(slots_of_ids))
                repeated = connection.execute(
                    "select count(*) from (select id from ids group by id having count(*) > 1)"
                ).fetchone()
            finally:
                connection.close()
            repeated_ids[day.isoformat()] = int(repeated[0]) if repeated else 0
    fields = sum(column_bytes)
    profile: dict[str, Any] = {
        "days": len(by_day),
        "files": sum(day["files"] for day in by_day.values()),
        "zip_bytes": sum(day["zip_bytes"] for day in by_day.values()),
        "text_bytes": sum(day["text_bytes"] for day in by_day.values()),
        "lines": sum(day["lines"] for day in by_day.values()),
        "lines_with_another_column_count": {str(k): v for k, v in sorted(odd_lines.items())},
        "columns": [
            {
                "position": index + 1,
                "name": name,
                "bytes": column_bytes[index],
                "filled_lines": column_filled[index],
                "share_of_field_bytes": round(column_bytes[index] / fields, 6) if fields else None,
            }
            for index, name in enumerate(names)
        ],
        "by_day": by_day,
    }
    if feed is Feed.GKG:
        profile["lines_by_organisation_column"] = dict(gkg_lines)
    if feed is Feed.EXPORT:
        profile["event_ids_in_more_than_one_slot_of_a_day"] = {
            "total": sum(repeated_ids.values()),
            "days_with_one_or_more": sum(1 for value in repeated_ids.values() if value),
            "largest_day": max(repeated_ids.values(), default=0),
        }
    return profile


def mention_pairs(first_day: date, last_day: date) -> dict[str, Any]:
    """Repeated pairs of event and article in one slot, from the day builds (finding F5)."""
    with open_ledger() as ledger:
        rows = ledger.connection.execute(
            "SELECT DISTINCT ON (slot_date) slot_date, parsed_rows, duplicate_rows "
            "FROM ops.silver_partition WHERE source = %s AND table_name = 'gdelt_mention' "
            "AND unit_kind = 'day' AND slot_date >= %s AND slot_date <= %s "
            "ORDER BY slot_date, partition_version_id DESC",
            (ledger.source, first_day, last_day),
        ).fetchall()
    shares = [row[2] / row[1] for row in rows if row[1]]
    return {
        "days": len(rows),
        "parsed_rows": sum(int(row[1]) for row in rows),
        "repeated_rows": sum(int(row[2]) for row in rows),
        "share_median": round(statistics.median(shares), 6) if shares else None,
        "share_min": round(min(shares), 6) if shares else None,
        "share_max": round(max(shares), 6) if shares else None,
    }


def gkg_facts(days: list[date]) -> dict[str, Any]:
    """Titles, stories and organisation strings of each day, from the two silver tables."""
    settings = get_settings()
    catalog = load_lake_catalog(settings)
    documents = catalog.load_table((settings.silver_namespace, "gdelt_gkg_doc"))
    organisations = catalog.load_table((settings.silver_namespace, "gdelt_gkg_org"))
    by_day: dict[str, dict[str, Any]] = {}
    for day in days:
        row_filter = Unit(day).row_filter
        docs = documents.scan(
            row_filter=row_filter, selected_fields=("gkg_record_id", "page_title", "story_id")
        ).to_arrow()
        orgs = organisations.scan(
            row_filter=row_filter,
            selected_fields=("gkg_record_id", "org_string_id", "first_offset"),
        ).to_arrow()
        connection = connect()
        try:
            connection.register("docs", docs)
            connection.register("orgs", orgs)
            titles = connection.execute(
                "select count(*), count(page_title), "
                "count(distinct story_id) filter (where page_title is not null) from docs"
            ).fetchone()
            strings = connection.execute(
                "select count(*), count(distinct org_string_id), count(distinct gkg_record_id) "
                "from orgs"
            ).fetchone()
            v1_only = connection.execute(
                "select count(*) from (select gkg_record_id from orgs group by gkg_record_id "
                "having count(first_offset) = 0)"
            ).fetchone()
        finally:
            connection.close()
        assert titles is not None and strings is not None and v1_only is not None
        by_day[day.isoformat()] = {
            "documents": int(titles[0]),
            "documents_with_a_title": int(titles[1]),
            "distinct_normalised_titles": int(titles[2]),
            "copies_of_a_title": int(titles[1]) - int(titles[2]),
            "organisation_rows": int(strings[0]),
            "distinct_organisation_strings": int(strings[1]),
            "documents_with_an_organisation": int(strings[2]),
            "documents_with_v1_names_only": int(v1_only[0]),
        }
    total: Counter[str] = Counter()
    for facts in by_day.values():
        total.update(
            {
                name: value
                for name, value in facts.items()
                if name != "distinct_organisation_strings"
            }
        )
    distinct = [facts["distinct_organisation_strings"] for facts in by_day.values()]
    return {
        "days": len(by_day),
        "totals": dict(total),
        "share_of_documents_with_a_title": round(
            total["documents_with_a_title"] / total["documents"], 6
        )
        if total["documents"]
        else None,
        "share_of_titled_documents_which_copy_a_title": round(
            total["copies_of_a_title"] / total["documents_with_a_title"], 6
        )
        if total["documents_with_a_title"]
        else None,
        "share_of_documents_with_v1_names_only": round(
            total["documents_with_v1_names_only"] / total["documents_with_an_organisation"], 6
        )
        if total["documents_with_an_organisation"]
        else None,
        "distinct_organisation_strings_in_a_day": {
            "median": statistics.median(distinct) if distinct else None,
            "min": min(distinct, default=None),
            "max": max(distinct, default=None),
        },
        "by_day": by_day,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument("--from", dest="first", required=True, help="first UTC day, YYYY-MM-DD")
    parser.add_argument("--to", dest="last", required=True, help="last UTC day, YYYY-MM-DD")
    parser.add_argument("--feed", action="append", choices=[feed.value for feed in Feed])
    parser.add_argument(
        "--bronze-only", action="store_true", help="skip the silver and ledger facts"
    )
    parser.add_argument("--out", type=Path, default=None)
    arguments = parser.parse_args()
    first_day, last_day = parse_day(arguments.first), parse_day(arguments.last)
    days = list(days_between(first_day, last_day))
    feeds = [Feed(name) for name in arguments.feed] if arguments.feed else list(Feed)
    out = arguments.out or RESULTS / f"profile-{first_day}-to-{last_day}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    profile: dict[str, Any] = {
        "window": {"from": first_day.isoformat(), "to": last_day.isoformat(), "days": len(days)},
        "measured_at": utc_now().isoformat(timespec="seconds"),
        "newest_slot_of_the_window": Slot.parse(f"{last_day:%Y%m%d}234500").stamp,
        "feeds": {},
    }
    for feed in feeds:
        started = time.perf_counter()
        profile["feeds"][feed.value] = profile_feed(feed, days)
        profile["feeds"][feed.value]["seconds"] = round(time.perf_counter() - started, 1)
        print(f"{feed.value}: {profile['feeds'][feed.value]['seconds']} s", flush=True)
    if not arguments.bronze_only:
        profile["mentions_repeated_pairs"] = mention_pairs(first_day, last_day)
        profile["gkg"] = gkg_facts(days)
    # Linux reports the peak resident memory of the process in KiB; the field is MiB.
    profile["peak_resident_mib"] = round(
        resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1
    )
    out.write_text(json.dumps(profile, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary = {
        feed: {name: value for name, value in facts.items() if name not in ("by_day", "columns")}
        for feed, facts in profile["feeds"].items()
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    print(f"the profile is in {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
