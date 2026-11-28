"""Row counts of the recorded SEC files, computed without the code of the pipeline.

The end-to-end tests compare the five SEC tables with the counts of this module. The module
reads the recorded files with the standard library only and states each rule of the week 3
plan (section 3.3) a second time, so the counts cannot inherit a mistake of the pipeline.
"""

from __future__ import annotations

import csv
import io
import json
import os
import zipfile
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

FIXTURES = Path(os.environ.get("GBMI_SEC_FIXTURES_DIR", "/fixtures/sec"))
SEED = Path(os.environ.get("GBMI_WATCHLIST_FILE", "/opt/gbmi/dbt/seeds/watchlist.csv"))
#: The demo stack reads the first five rows of the seed.
WATCHLIST_SIZE = int(os.environ.get("GBMI_WATCHLIST_SIZE", "5"))
DATA = FIXTURES / "data.sec.gov"
ARCHIVES = FIXTURES / "www.sec.gov" / "Archives" / "edgar" / "daily-index"
INDEX_FILE = ARCHIVES / "2026" / "QTR4" / "form.20261125.idx"
ARCHIVE = ARCHIVES / "bulkdata" / "submissions.zip"
TICKER_FILE = FIXTURES / "www.sec.gov" / "files" / "company_tickers_exchange.json"
#: EDGAR works on the clock of New York: a day of EDGAR starts at 06:00.
NEW_YORK = ZoneInfo("America/New_York")


def watchlist() -> list[int]:
    """The CIKs of the first rows of the seed."""
    with SEED.open(encoding="utf-8", newline="") as stream:
        rows = sorted(csv.DictReader(stream), key=lambda row: int(row["rank"]))
    return [int(row["cik"]) for row in rows[:WATCHLIST_SIZE]]


def document_name(cik: int) -> str:
    return f"CIK{cik:010d}.json"


def index_pairs() -> set[tuple[str, int]]:
    """The pairs of accession number and filer of the recorded index file."""
    lines = INDEX_FILE.read_bytes().decode("latin-1").split("\n")
    start = next(number for number, line in enumerate(lines) if line.startswith("-----")) + 1
    pairs = set()
    for line in lines[start:]:
        words = line.split()
        if not words:
            continue
        # The three last words of a row: the CIK, the date and the file name.
        cik, _, file_name = words[-3:]
        pairs.add((file_name.rsplit("/", 1)[-1].removesuffix(".txt"), int(cik)))
    return pairs


def index_rows() -> int:
    """One row for each pair of filing and filer: a repeated pair counts once (F11 c)."""
    return len(index_pairs())


def recent_filings(cik: int) -> list[dict[str, Any]]:
    """The filings which the submissions document of a watchlist company lists, each once."""
    document = json.loads((DATA / "submissions" / document_name(cik)).read_bytes())
    recent = document["filings"]["recent"]
    filings = {}
    for position, accession in enumerate(recent["accessionNumber"]):
        filings[accession] = {name: values[position] for name, values in recent.items()}
    return list(filings.values())


def detail_rows() -> int:
    """One row for each filing which the document of a watchlist company lists."""
    return sum(len(recent_filings(cik)) for cik in watchlist())


def filing_pairs() -> set[tuple[str, int]]:
    """The grain of fct_filing: every pair of the index file and of the watchlist documents."""
    listed = {
        (filing["accessionNumber"], cik) for cik in watchlist() for filing in recent_filings(cik)
    }
    return index_pairs() | listed


def filing_item_rows() -> int:
    """One row for each different item of a filing of the 8-K family of a watchlist company."""
    total = 0
    for cik in watchlist():
        for filing in recent_filings(cik):
            if filing["form"].startswith("8-K"):
                total += len({item.strip() for item in filing["items"].split(",") if item.strip()})
    return total


def universe() -> list[int]:
    """The companies of the ticker file, each once."""
    document = json.loads(TICKER_FILE.read_bytes())
    position = document["fields"].index("cik")
    return sorted({row[position] for row in document["data"]})


def submissions_documents() -> dict[int, Any]:
    """The submissions document of each company of the universe which has one.

    The filings pipeline asks for the documents of the watchlist; every other document of
    the first run comes from the bulk archive.
    """
    found: dict[int, Any] = {}
    watched = set(watchlist())
    with zipfile.ZipFile(io.BytesIO(ARCHIVE.read_bytes())) as archive:
        members = set(archive.namelist())
        for cik in universe():
            name = document_name(cik)
            if cik in watched:
                found[cik] = json.loads((DATA / "submissions" / name).read_bytes())
            elif name in members:
                found[cik] = json.loads(archive.read(name))
    return found


def former_name_rows() -> int:
    """One row for each former name with a first day."""
    total = 0
    for document in submissions_documents().values():
        total += len(
            {(name["name"], name["from"]) for name in document["formerNames"] if name.get("from")}
        )
    return total


def fact_rows() -> int:
    """One row for each fact of the facts document of a watchlist company; an exact repeat
    counts once."""
    total = 0
    for cik in watchlist():
        document = json.loads(
            (DATA / "api" / "xbrl" / "companyfacts" / document_name(cik)).read_bytes()
        )
        facts = set()
        for taxonomy, concepts in document["facts"].items():
            for concept, body in concepts.items():
                for unit, values in body["units"].items():
                    for value in values:
                        facts.add((taxonomy, concept, unit, json.dumps(value, sort_keys=True)))
        total += len(facts)
    return total


def sec_rows() -> dict[str, int]:
    """The rows of each of the five SEC tables after one run of the three pipelines."""
    return {
        "sec_filing_index": index_rows(),
        "sec_filing_detail": detail_rows(),
        "sec_company_snapshot": len(universe()),
        "sec_company_former_name": former_name_rows(),
        "sec_xbrl_fact": fact_rows(),
    }


def acceptance_times(cik: int) -> dict[str, datetime]:
    """The acceptance time of each filing which the submissions document of a company lists."""
    return {
        filing["accessionNumber"]: datetime.fromisoformat(
            filing["acceptanceDateTime"].replace("Z", "+00:00")
        )
        for filing in recent_filings(cik)
        if filing["acceptanceDateTime"]
    }


def public_moment(accepted: datetime | None, filed: date) -> datetime:
    """The moment at which EDGAR made a filing public (week 3 plan, D48 and change C23).

    EDGAR publishes a filing when it accepts the filing, with one exception: a filing which
    arrives after 17:30 in New York receives the filing date of the next business day and is
    published at 06:00 in New York on that date. So the moment is the later of the acceptance
    time and 06:00 in New York on the filing date. Without an acceptance time the moment is
    05:00 UTC of the day after the filing date.
    """
    if accepted is None:
        return datetime.combine(filed + timedelta(days=1), time(5), tzinfo=UTC)
    return max(accepted, datetime.combine(filed, time(6), tzinfo=NEW_YORK))


def reports(cik: int, taxonomy: str, concept: str, unit: str) -> list[dict[str, Any]]:
    """Each report of a concept of a company: the period, the value, and when it became known."""
    document = json.loads(
        (DATA / "api" / "xbrl" / "companyfacts" / document_name(cik)).read_bytes()
    )
    accepted = acceptance_times(cik)
    found = {}
    for value in document["facts"][taxonomy][concept]["units"][unit]:
        known_at = public_moment(accepted.get(value["accn"]), date.fromisoformat(value["filed"]))
        period = (value.get("start", value["end"]), value["end"])
        found[(period, value["accn"])] = {
            "period": period,
            "accession": value["accn"],
            "known_at": known_at,
            "filed": value["filed"],
            "value": Decimal(str(value["val"])),
        }
    return list(found.values())


def known_values(
    cik: int, taxonomy: str, concept: str, unit: str, moment: datetime | None = None
) -> dict[tuple[str, str], Decimal]:
    """For each period: the newest value which was known at the moment (blueprint 7.10).

    Of two reports with the same knowledge time, the higher accession number counts. Without
    a moment: everything which the recorded file holds.
    """
    newest: dict[tuple[str, str], dict[str, Any]] = {}
    for report in reports(cik, taxonomy, concept, unit):
        if moment is not None and report["known_at"] > moment:
            continue
        current = newest.get(report["period"])
        rank = (report["known_at"], report["accession"])
        if current is None or rank > (current["known_at"], current["accession"]):
            newest[report["period"]] = report
    return {period: report["value"] for period, report in newest.items()}


def first_values(
    cik: int, taxonomy: str, concept: str, unit: str
) -> dict[tuple[str, str], Decimal]:
    """For each period: the value of the first report."""
    first: dict[tuple[str, str], dict[str, Any]] = {}
    for report in reports(cik, taxonomy, concept, unit):
        current = first.get(report["period"])
        rank = (report["known_at"], report["accession"])
        if current is None or rank < (current["known_at"], current["accession"]):
            first[report["period"]] = report
    return {period: report["value"] for period, report in first.items()}
