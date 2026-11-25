"""Draw the first labelling sheet for the entity-resolution gold set.

This is a profiling script, not package code (week 1 plan, WP-04). It downloads eight
GDELT 2.0 Global Knowledge Graph (GKG) slots, verifies each file against the size and MD5
in the master file list, counts the documents that mention each organisation string, and
writes a sheet of 200 distinct strings: the 100 most frequent and 100 drawn at random.

The sheet is blind by construction: no matching system exists yet, so nothing but the
string, its volume and a few article titles can influence the label (blueprint 8.8).

Data: The GDELT Project, https://www.gdeltproject.org/ - article titles are metadata;
no article text is fetched.

Usage:
    uv run python benchmarks/source_profile/sample_org_strings.py
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import html
import io
import random
import re
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

import httpx

MASTER_LIST_URL = "https://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
#: Eight slots: 2026-11-25 08:00 to 09:45 UTC.
SLOTS = (
    "20261125080000",
    "20261125081500",
    "20261125083000",
    "20261125084500",
    "20261125090000",
    "20261125091500",
    "20261125093000",
    "20261125094500",
)
SEED = 20261125
TOP_N = 100
RANDOM_N = 100
MAX_TITLES = 5

# GKG 2.1 has 27 tab-separated columns (codebook V2.1). Zero-based positions used here:
GKG_COLUMNS = 27
COL_RECORD_ID = 0
COL_V1_ORGANIZATIONS = 13
COL_V2_ORGANIZATIONS = 14
COL_EXTRAS_XML = 26

PAGE_TITLE = re.compile(r"<PAGE_TITLE>(.*?)</PAGE_TITLE>", re.DOTALL)
WHITESPACE = re.compile(r"\s+")


@dataclass(frozen=True)
class IndexLine:
    """One line of the GDELT file list: size in bytes, MD5 and URL."""

    size: int
    md5: str
    url: str

    @property
    def name(self) -> str:
        return self.url.rsplit("/", 1)[-1]


def fetch_index_lines(client: httpx.Client, tail_bytes: int) -> dict[str, IndexLine]:
    """Read the tail of the master list with a range request and keep the wanted GKG lines."""
    response = client.get(MASTER_LIST_URL, headers={"Range": f"bytes=-{tail_bytes}"})
    response.raise_for_status()
    found: dict[str, IndexLine] = {}
    for line in response.text.splitlines():
        parts = line.split(" ")
        if len(parts) != 3 or not parts[2].endswith(".gkg.csv.zip"):
            continue
        slot = parts[2].rsplit("/", 1)[-1].split(".", 1)[0]
        if slot in SLOTS:
            # The list still gives http:// URLs; the server redirects them to https://.
            found[slot] = IndexLine(
                int(parts[0]), parts[1], parts[2].replace("http://", "https://")
            )
    missing = [slot for slot in SLOTS if slot not in found]
    if missing:
        raise SystemExit(f"slots not in the last {tail_bytes} bytes of the master list: {missing}")
    return found


def download_verified(client: httpx.Client, line: IndexLine, directory: Path) -> Path:
    """Download one file unless a verified copy exists; check size and MD5 against the index."""
    target = directory / line.name
    if not (target.exists() and _matches(target.read_bytes(), line)):
        response = client.get(line.url)
        response.raise_for_status()
        if not _matches(response.content, line):
            raise SystemExit(f"{line.name}: size or MD5 differs from the index line")
        target.write_bytes(response.content)
    return target


def _matches(content: bytes, line: IndexLine) -> bool:
    # MD5 is the checksum GDELT publishes; it is used for integrity, not for security.
    digest = hashlib.md5(content, usedforsecurity=False).hexdigest()
    return len(content) == line.size and digest == line.md5


def organisations(columns: list[str]) -> set[str]:
    """Distinct organisation strings of one record, exactly as GDELT gives them."""
    enhanced = columns[COL_V2_ORGANIZATIONS]
    if enhanced:
        # "name,offset;name,offset": the offset follows the last comma.
        names = (entry.rsplit(",", 1)[0] for entry in enhanced.split(";") if entry)
    else:
        names = (entry for entry in columns[COL_V1_ORGANIZATIONS].split(";") if entry)
    return {name.strip() for name in names if name.strip()}


def page_title(columns: list[str]) -> str | None:
    match = PAGE_TITLE.search(columns[COL_EXTRAS_XML])
    if not match:
        return None
    # The extras block is XML: titles carry character references such as &#x2022;.
    title = WHITESPACE.sub(" ", html.unescape(match.group(1))).strip()
    return title or None


def profile(
    paths: list[Path],
) -> tuple[Counter[str], dict[str, list[tuple[str, str]]], dict[str, int]]:
    """Count documents per organisation string and collect (record id, title) pairs."""
    doc_count: Counter[str] = Counter()
    titles: dict[str, list[tuple[str, str]]] = defaultdict(list)
    stats = {"records": 0, "malformed": 0, "with_organisations": 0, "with_title": 0}
    for path in paths:
        with zipfile.ZipFile(path) as archive:
            (member,) = archive.namelist()
            text = archive.read(member).decode("utf-8", errors="replace")
        for raw in text.split("\n"):
            if not raw:
                continue
            columns = raw.rstrip("\r").split("\t")
            if len(columns) != GKG_COLUMNS:
                stats["malformed"] += 1
                continue
            stats["records"] += 1
            title = page_title(columns)
            stats["with_title"] += title is not None
            names = organisations(columns)
            stats["with_organisations"] += bool(names)
            for name in names:
                doc_count[name] += 1
                if title is not None:
                    titles[name].append((columns[COL_RECORD_ID], title))
    return doc_count, titles, stats


def draw(doc_count: Counter[str]) -> list[tuple[str, str]]:
    """The 100 most frequent strings, then 100 of the rest drawn with a fixed seed."""
    ranked = sorted(doc_count, key=lambda name: (-doc_count[name], name))
    top = ranked[:TOP_N]
    rest = sorted(ranked[TOP_N:])
    sampled = random.Random(SEED).sample(rest, min(RANDOM_N, len(rest)))  # noqa: S311
    return [(name, "top") for name in top] + [(name, "random") for name in sampled]


def sample_titles(pairs: list[tuple[str, str]]) -> list[str]:
    """Up to five distinct titles, in record-identifier order so that reruns agree."""
    chosen: list[str] = []
    for _, title in sorted(pairs):
        if title not in chosen:
            chosen.append(title)
        if len(chosen) == MAX_TITLES:
            break
    return chosen


def write_sheet(
    path: Path,
    rows: list[tuple[str, str]],
    doc_count: Counter[str],
    titles: dict[str, list[tuple[str, str]]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(
        ["org_string", "stratum", "doc_count",
         *(f"title_{i}" for i in range(1, MAX_TITLES + 1)),
         "label", "cik", "note"]
    )  # fmt: skip
    for name, stratum in rows:
        picked = sample_titles(titles.get(name, []))
        picked += [""] * (MAX_TITLES - len(picked))
        writer.writerow([name, stratum, doc_count[name], *picked, "", "", ""])
    path.write_text(buffer.getvalue(), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--out", type=Path, default=Path("config/labelling/gold_sheet_v0.csv"))
    parser.add_argument("--scratch", type=Path, default=Path("data/scratch/source_profile"))
    parser.add_argument("--tail-bytes", type=int, default=2_000_000,
                        help="how much of the end of the master list to read")  # fmt: skip
    args = parser.parse_args()

    args.scratch.mkdir(parents=True, exist_ok=True)
    with httpx.Client(timeout=120, follow_redirects=True) as client:
        index = fetch_index_lines(client, args.tail_bytes)
        paths = [download_verified(client, index[slot], args.scratch) for slot in SLOTS]

    doc_count, titles, stats = profile(paths)
    rows = draw(doc_count)
    write_sheet(args.out, rows, doc_count, titles)

    compressed_mb = sum(path.stat().st_size for path in paths) / 1e6
    with_orgs = stats["with_organisations"]
    strata = Counter(stratum for _, stratum in rows)
    print(f"slots: {len(paths)} ({compressed_mb:.1f} MB compressed, verified against the index)")
    print(f"records: {stats['records']} (malformed lines: {stats['malformed']})")
    print(f"records naming an organisation: {with_orgs} ({with_orgs / stats['records']:.1%})")
    print(f"records with a page title: {stats['with_title']}")
    print(f"distinct organisation strings: {len(doc_count)}")
    print(
        f"sheet: {args.out} - {len(rows)} strings ({strata['top']} top, {strata['random']} random)"
    )


if __name__ == "__main__":
    main()
