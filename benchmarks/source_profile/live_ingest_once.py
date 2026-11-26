"""Two ingest runs against the live GDELT servers, in a throwaway database and a scratch lake.

The check of work package WP-04 (week 2 plan): one run lands the newest slot of each feed, a
second run downloads nothing, and the ledger agrees with the bronze listing. The script touches
neither the database ``gbmi`` nor the lake of the stack: the ledger is a database which the
script creates and drops, and the lake is a directory under ``/lake/_test`` which the script
removes.

Data: The GDELT Project, https://www.gdeltproject.org/

Run on the Compose network:
    make toolbox STACK=gbmi-demo CMD="python benchmarks/source_profile/live_ingest_once.py"
"""

from __future__ import annotations

import json
import shutil
import uuid
from pathlib import Path

from gbmi.core.clock import utc_now
from gbmi.ledger.repository import open_ledger
from gbmi.ledger.scratch import scratch_database
from gbmi.ledger.states import State
from gbmi.logging import configure_logging
from gbmi.sources.gdelt import ingest
from gbmi.sources.gdelt.land import Bronze, HttpFetcher

SCRATCH = Path("/lake/_test")


def main() -> int:
    configure_logging(level="WARNING")
    root = SCRATCH / f"live-ingest-{uuid.uuid4().hex[:12]}"
    root.mkdir(parents=True)
    lake_uri = f"file://{root}"
    fetcher = HttpFetcher()
    try:
        with scratch_database() as dsn, open_ledger(dsn, run_id="live-ingest-once") as ledger:
            bronze = Bronze(lake_uri)
            as_of = utc_now()
            first = ingest.run_ingest(
                as_of=as_of, fetcher=fetcher, ledger=ledger, bronze=bronze, lake_uri=lake_uri
            )
            second = ingest.run_ingest(
                as_of=utc_now(), fetcher=fetcher, ledger=ledger, bronze=bronze, lake_uri=lake_uri
            )
            verified = ledger.find(states=[State.VERIFIED])
            in_ledger = {
                record.key.slot.bronze_key(record.key.feed): record.expected_bytes
                for record in verified
            }
            in_bronze = bronze.sizes("raw")
            report = {
                "as_of": as_of.isoformat(),
                "first_run": first,
                "second_run": second,
                "verified_slots": [
                    {
                        "slot": record.key.label,
                        "bytes": record.expected_bytes,
                        "uploaded_at": record.published_at.isoformat()
                        if record.published_at
                        else None,
                        "seconds_from_upload_to_verified": round(
                            (record.verified_at - record.published_at).total_seconds()
                        )
                        if record.published_at and record.verified_at
                        else None,
                    }
                    for record in verified
                ],
                "ledger_status": ledger.status(as_of.date(), as_of.date()),
                "ledger_agrees_with_bronze": in_ledger == in_bronze,
            }
    finally:
        shutil.rmtree(root, ignore_errors=True)
    print(json.dumps(report, indent=2))
    passed = (
        first["verified"] >= 3
        and first["written"] == first["verified"]
        and second["written"] == 0
        and report["ledger_agrees_with_bronze"]
    )
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
