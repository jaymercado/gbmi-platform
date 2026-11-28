"""Draw version 0 of the watchlist (week 3 plan 3.3 and WP-06 step 9).

A profile, not a matcher: entity resolution does not exist before phase P3. The script reads
what the platform holds and sends no request:

* the newest snapshot of the universe (``sec_company_snapshot``): the CIK and the name which
  the SEC gives each company;
* the submissions documents in bronze, and the loaded index files: which company filed a
  periodic report;
* the organisation strings of the GKG files of the 30 days of phase P1 (``gdelt_gkg_org``).

The list:

    ranks 1 to 5   the five demo companies of the week 3 plan;
    next           the other companies of the gold sheet with the label company_in_universe,
                   in the order of the documents of the sheet;
    up to 50       the universe companies whose name, without its legal form, is exactly an
                   organisation string of the GKG files, by the number of documents, without
                   the names which are common words.

A company joins the list only when the company filed a periodic report: an annual report (form
10-K, 20-F or 40-F) among the filings of its submissions document, or a periodic report in a
loaded index file. A company without such a report has no financial facts (finding F8).

Usage, in the toolbox of the working stack, after ``gbmi sec reference --bulk``:

    DRAW="python -m benchmarks.source_profile.watchlist_candidates"
    make toolbox CMD="$DRAW"            # shows the list and writes nothing
    make toolbox CMD="$DRAW --write"    # writes the seed and the result file
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.exceptions import NoSuchTableError

from gbmi.config import get_settings
from gbmi.core.text import normalise_text
from gbmi.lake.catalog import load_lake_catalog
from gbmi.lake.commit import Unit
from gbmi.lake.tables import GDELT_GKG_ORG, SEC_COMPANY_SNAPSHOT, SEC_FILING_INDEX
from gbmi.ledger.object_states import ObjectState
from gbmi.ledger.objects import Dataset, open_object_ledger
from gbmi.sources.gdelt.land import Bronze
from gbmi.sources.sec.land import bronze_key
from gbmi.sources.sec.models import PayloadError, Submissions
from gbmi.sources.sec.watchlist import COLUMNS, read_watchlist

REPO = Path(__file__).resolve().parents[2]
SEED = REPO / "dbt" / "seeds" / "watchlist.csv"
GOLD_SHEET = REPO / "config" / "labelling" / "gold_sheet_v0.csv"
RESULT = REPO / "benchmarks" / "source_profile" / "results" / "watchlist-v0.json"
SIZE = 50
#: The 30 days of phase P1.
FIRST_DAY, LAST_DAY = date(2026, 10, 26), date(2026, 11, 24)
#: The five demo companies (week 3 plan 3.3), with the reason of each.
DEMO = {
    1326801: "demo company: the name change of exit-gate row 2",
    1045810: "demo company: a fiscal year which ends in January",
    789019: "demo company: a fiscal year which ends in June",
    63276: "demo company: a calendar year",
    1075124: "demo company: a foreign company with the forms 40-F and 6-K",
}
ANNUAL_FORMS = frozenset({"10-K", "20-F", "40-F"})
PERIODIC_FORMS = ANNUAL_FORMS | {"10-Q"}
#: The words of a legal form at the end of a name of the SEC.
LEGAL_FORMS = frozenset(
    {
        "inc",
        "incorporated",
        "corp",
        "corporation",
        "co",
        "company",
        "ltd",
        "limited",
        "plc",
        "llc",
        "lp",
        "sa",
        "ag",
        "nv",
        "se",
    }
)
#: Names which are also common words: the news strings of such a name mean something else
#: too often. Each entry was read in the result of a dry run; the value says why.
COMMON_WORDS: dict[str, str] = {
    "shell": "the name is a common word; the gold sheet labels the string 'shell' as ambiguous",
    "visa": "the name is a common word: a visa is also a travel document",
    "oracle": "the name is a common word",
    "intel": "the name is a common short form of the word intelligence",
}
#: The SEC ends some names with a place between slashes: "MATTEL INC /DE/".
_PLACE = re.compile(r"\s*[/\\][A-Za-z]{2,4}[/\\]?\s*$")
_READABLE = (ObjectState.VERIFIED, ObjectState.LOADED)


def alias(name: str) -> str:
    """The name of the SEC as a news string would write the name: no place, no legal form."""
    words = normalise_text(_PLACE.sub("", name)).split()
    while len(words) > 1 and words[-1] in LEGAL_FORMS:
        words.pop()
    return " ".join(words)


def newest_snapshot(catalog: Any, namespace: str) -> tuple[date, dict[int, str]]:
    """The date of the newest snapshot, and the name of each company in the snapshot."""
    table = catalog.load_table((namespace, SEC_COMPANY_SNAPSHOT))
    rows = table.scan(selected_fields=("snapshot_date", "cik", "name")).to_arrow()
    if rows.num_rows == 0:
        raise SystemExit("sec_company_snapshot is empty: run gbmi sec reference --bulk first")
    newest = pc.max(rows["snapshot_date"]).as_py()
    kept = rows.filter(pc.equal(rows["snapshot_date"], newest)).to_pylist()
    return newest, {row["cik"]: row["name"] for row in kept}


def documents_by_string(catalog: Any, namespace: str, wanted: set[str]) -> Counter[str]:
    """The number of GKG documents of the 30 days which name each wanted string."""
    found: Counter[str] = Counter()
    try:
        table = catalog.load_table((namespace, GDELT_GKG_ORG))
    except NoSuchTableError:
        return found  # a stack without GDELT data: no news volume
    values = sorted(wanted)
    day = FIRST_DAY
    while day <= LAST_DAY:
        strings = table.scan(
            row_filter=Unit(day).row_filter, selected_fields=("org_string_norm",)
        ).to_arrow()["org_string_norm"]
        wanted_here = pc.is_in(strings, value_set=pa.array(values, strings.type))
        counted = pc.value_counts(strings.filter(wanted_here))
        for item in counted.to_pylist():
            found[item["values"]] += item["counts"]
        day = date.fromordinal(day.toordinal() + 1)
    return found


def periodic_filers(catalog: Any, namespace: str) -> set[int]:
    """The companies with a periodic report in a loaded index file."""
    table = catalog.load_table((namespace, SEC_FILING_INDEX))
    rows = table.scan(selected_fields=("cik", "form")).to_arrow().to_pylist()
    return {row["cik"] for row in rows if row["form"].removesuffix("/A") in PERIODIC_FORMS}


class Reports:
    """Says by which rule a company filed a periodic report; reads bronze and the ledger."""

    def __init__(self, ledger: Any, bronze: Bronze, in_index: set[int]) -> None:
        self._ledger = ledger
        self._bronze = bronze
        self._in_index = in_index

    def rule(self, cik: int) -> str | None:
        item = self._ledger.newest(Dataset.SUBMISSIONS, cik=cik, states=_READABLE)
        content = self._bronze.read(bronze_key(item.key)) if item is not None else None
        if content is not None:
            try:
                forms = set(Submissions.parse(content).filings.recent.form)
            except PayloadError:
                forms = set()
            if forms & ANNUAL_FORMS:
                return "an annual report in the submissions document"
        if cik in self._in_index:
            return "a periodic report in a loaded index file"
        return None


def gold_sheet() -> list[tuple[int, str, int]]:
    """CIK, string and documents of the sheet rows with the label company_in_universe."""
    with GOLD_SHEET.open(encoding="utf-8", newline="") as stream:
        rows = [row for row in csv.DictReader(stream) if row["label"] == "company_in_universe"]
    found = [(int(row["cik"]), row["org_string"], int(row["doc_count"])) for row in rows]
    return sorted(found, key=lambda item: -item[2])


def draw() -> dict[str, Any]:
    settings = get_settings()
    catalog = load_lake_catalog(settings)
    namespace = settings.silver_namespace
    snapshot_date, names = newest_snapshot(catalog, namespace)
    aliases = {cik: alias(name) for cik, name in names.items()}
    by_alias: dict[str, list[int]] = {}
    for cik, text in aliases.items():
        by_alias.setdefault(text, []).append(cik)
    documents = documents_by_string(catalog, namespace, set(by_alias))
    chosen: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    with open_object_ledger() as ledger:
        reports = Reports(ledger, Bronze(settings.lake_uri), periodic_filers(catalog, namespace))

        def consider(cik: int, reason: str, *, needs_report: bool = True) -> None:
            if len(chosen) >= SIZE or any(row["cik"] == cik for row in chosen):
                return
            entry = {
                "cik": cik,
                "name": names.get(cik),
                "alias": aliases.get(cik),
                "documents": documents.get(aliases.get(cik, ""), 0),
            }
            if any(item.get("cik") == cik for item in skipped):
                return
            if cik not in names:
                skipped.append({**entry, "why": "the CIK is not in the snapshot of the universe"})
                return
            rule = reports.rule(cik)
            if rule is None and needs_report:
                skipped.append({**entry, "why": "no periodic report is known"})
                return
            chosen.append({**entry, "reason": reason, "report": rule})

        for cik, reason in DEMO.items():
            consider(cik, reason, needs_report=False)
        for cik, string, count in gold_sheet():
            consider(cik, f"gold sheet: the string '{string}' with {count} documents")
        for text, count in documents.most_common():
            if len(chosen) >= SIZE:
                break
            ciks = by_alias[text]
            entry = {"alias": text, "documents": count, "ciks": ciks}
            if text in COMMON_WORDS:
                skipped.append({**entry, "why": COMMON_WORDS[text]})
            elif len(ciks) > 1:
                skipped.append({**entry, "why": "several companies share the name"})
            else:
                consider(ciks[0], f"news volume: '{text}' in {count} GKG documents of 30 days")
    return {
        "drawn_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "snapshot_date": snapshot_date.isoformat(),
        "universe": len(names),
        "window": [FIRST_DAY.isoformat(), LAST_DAY.isoformat()],
        "strings_with_documents": len(documents),
        "rows": [{"rank": rank, **row} for rank, row in enumerate(chosen, start=1)],
        "skipped": skipped,
    }


def write(result: dict[str, Any]) -> None:
    """Write the seed and the result file. A company keeps the date of its first draw."""
    added = {entry.cik: entry.added_on for entry in read_watchlist(SEED)} if SEED.exists() else {}
    today = datetime.now(UTC).date()
    with SEED.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(COLUMNS)
        for row in result["rows"]:
            day = added.get(row["cik"], today).isoformat()
            writer.writerow([row["rank"], row["cik"], row["name"], row["reason"], day])
    RESULT.parent.mkdir(parents=True, exist_ok=True)
    RESULT.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument("--write", action="store_true", help="write the seed and the result")
    args = parser.parse_args()
    result = draw()
    for row in result["rows"]:
        numbers = f"{row['rank']:>3} {row['cik']:>8} {row['documents']:>6}"
        print(f"{numbers}  {row['name']}  [{row['reason']}]")
    print(f"{len(result['rows'])} rows; {len(result['skipped'])} candidates were skipped")
    for item in result["skipped"][:40]:
        print("skipped:", json.dumps(item))
    if not args.write:
        return 0
    if len(result["rows"]) != SIZE:
        print(f"the list has {len(result['rows'])} rows, not {SIZE}: nothing was written")
        return 1
    write(result)
    print(f"wrote {SEED.relative_to(REPO)} and {RESULT.relative_to(REPO)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
