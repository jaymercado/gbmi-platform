"""Compare the gold sheet of phase P0 with the universe of the SEC (week 3 plan, WP-06 step 10).

The labels of ``config/labelling/gold_sheet_v0.csv`` were made with a copy of the SEC ticker list
of 2026-10-22, and no request went to the SEC (open item of phase P0). The script reads the
rows of the sheet which carry a CIK and the newest snapshot of the universe: the CIK is in the
universe or not, and the name of the SEC stands beside the label. The script changes no label
and sends no request.

Usage, in the toolbox of the working stack, after ``gbmi sec reference --bulk``:

    make toolbox CMD="python -m benchmarks.source_profile.compare_gold_sheet --write"
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any

import pyarrow.compute as pc

from gbmi.config import get_settings
from gbmi.lake.catalog import load_lake_catalog
from gbmi.lake.tables import SEC_COMPANY_SNAPSHOT

REPO = Path(__file__).resolve().parents[2]
GOLD_SHEET = REPO / "config" / "labelling" / "gold_sheet_v0.csv"
RESULT = REPO / "benchmarks" / "source_profile" / "results" / "gold-sheet-v0-vs-sec.json"
COLUMNS = ("cik", "name", "primary_ticker", "primary_exchange", "is_inactive")


def compare() -> dict[str, Any]:
    settings = get_settings()
    catalog = load_lake_catalog(settings)
    table = catalog.load_table((settings.silver_namespace, SEC_COMPANY_SNAPSHOT))
    rows = table.scan(selected_fields=("snapshot_date", *COLUMNS)).to_arrow()
    if rows.num_rows == 0:
        raise SystemExit("sec_company_snapshot is empty: run gbmi sec reference --bulk first")
    newest = pc.max(rows["snapshot_date"]).as_py()
    universe = {
        row["cik"]: row for row in rows.filter(pc.equal(rows["snapshot_date"], newest)).to_pylist()
    }
    with GOLD_SHEET.open(encoding="utf-8", newline="") as stream:
        sheet = [row for row in csv.DictReader(stream) if row["cik"].strip()]
    compared = []
    for row in sheet:
        company = universe.get(int(row["cik"]))
        compared.append(
            {
                "org_string": row["org_string"],
                "label": row["label"],
                "cik": int(row["cik"]),
                "in_universe": company is not None,
                "sec_name": company["name"] if company else None,
                "primary_ticker": company["primary_ticker"] if company else None,
                "primary_exchange": company["primary_exchange"] if company else None,
            }
        )
    return {
        "snapshot_date": newest.isoformat(),
        "universe": len(universe),
        "rows_with_a_cik": len(compared),
        "in_universe": sum(1 for row in compared if row["in_universe"]),
        "rows": compared,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument("--write", action="store_true", help="write the result file")
    args = parser.parse_args()
    result = compare()
    if args.write:
        RESULT.parent.mkdir(parents=True, exist_ok=True)
        RESULT.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print("| String of the sheet | Label | CIK | In the universe | Name of the SEC | Ticker |")
    print("|---|---|---|---|---|---|")
    for row in result["rows"]:
        inside = "yes" if row["in_universe"] else "no"
        ticker = f"{row['primary_ticker'] or ''} ({row['primary_exchange'] or 'no exchange'})"
        print(
            f"| {row['org_string']} | `{row['label']}` | {row['cik']} | {inside} | "
            f"{row['sec_name'] or ''} | {ticker if row['in_universe'] else ''} |"
        )
    print(f"{result['in_universe']} of {result['rows_with_a_cik']} CIKs are in the universe")
    return 0


if __name__ == "__main__":
    sys.exit(main())
