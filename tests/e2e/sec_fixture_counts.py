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
from pathlib import Path
from typing import Any

FIXTURES = Path(os.environ.get("GBMI_SEC_FIXTURES_DIR", "/fixtures/sec"))
SEED = Path(os.environ.get("GBMI_WATCHLIST_FILE", "/opt/gbmi/dbt/seeds/watchlist.csv"))
#: The demo stack reads the first five rows of the seed.
WATCHLIST_SIZE = int(os.environ.get("GBMI_WATCHLIST_SIZE", "5"))
DATA = FIXTURES / "data.sec.gov"
ARCHIVES = FIXTURES / "www.sec.gov" / "Archives" / "edgar" / "daily-index"
INDEX_FILE = ARCHIVES / "2026" / "QTR4" / "form.20261125.idx"
ARCHIVE = ARCHIVES / "bulkdata" / "submissions.zip"
TICKER_FILE = FIXTURES / "www.sec.gov" / "files" / "company_tickers_exchange.json"


def watchlist() -> list[int]:
    """The CIKs of the first rows of the seed."""
    with SEED.open(encoding="utf-8", newline="") as stream:
        rows = sorted(csv.DictReader(stream), key=lambda row: int(row["rank"]))
    return [int(row["cik"]) for row in rows[:WATCHLIST_SIZE]]


def document_name(cik: int) -> str:
    return f"CIK{cik:010d}.json"


def index_rows() -> int:
    """One row for each pair of filing and filer: a repeated pair counts once (F11 c)."""
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
    return len(pairs)


def detail_rows() -> int:
    """One row for each filing which the document of a watchlist company lists."""
    total = 0
    for cik in watchlist():
        document = json.loads((DATA / "submissions" / document_name(cik)).read_bytes())
        total += len(set(document["filings"]["recent"]["accessionNumber"]))
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
