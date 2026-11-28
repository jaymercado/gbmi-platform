"""The cost of a refresh of a few companies in ``sec_xbrl_fact`` (week 3 plan, change C15).

The daily facts job replaces the rows of the companies with a new filing. The script builds a
made-up facts table of 50 companies with 26,000 facts each (1.3 million rows) in one commit,
once without a partition and once partitioned by ``cik``, and then refreshes 3 companies with
``overwrite_scope`` and a unit of keys. Each step runs in a process of its own, so the peak
memory of the refresh is the memory of the refresh alone.

Usage, in the toolbox of the demo stack:

    make toolbox STACK=gbmi-demo CMD="python -m benchmarks.p2_lake.refresh_facts"

The result is written to benchmarks/p2_lake/results/refresh_facts.json.
"""

from __future__ import annotations

import argparse
import json
import platform
import resource
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa

from gbmi.config import Environment, LakeBackend, Settings
from gbmi.lake.catalog import ensure_silver_namespace, load_lake_catalog
from gbmi.lake.commit import Scope, overwrite_scope
from gbmi.lake.tables import (
    SEC_XBRL_FACT,
    SEC_XBRL_FACT_SCHEMA,
    TABLE_PROPERTIES,
    identity_partition,
)

VARIANTS = {"no_partition": None, "partition_by_cik": "cik"}
RESULTS = Path(__file__).resolve().parent / "results" / "refresh_facts.json"
FIRST_CIK = 1_000_001
PERIODS = 50
INGESTED_AT = datetime(2026, 11, 28, tzinfo=UTC)


def settings(root: Path) -> Settings:
    return Settings(
        env=Environment.LOCAL,
        lake_backend=LakeBackend.LOCAL,
        lake_uri=f"file://{root}",
        catalog_uri=f"sqlite:///{root}/catalog/catalog.db",
    )


def company(cik: int, facts: int, *, version: int) -> pa.Table:
    """The facts of one company: ``facts`` rows over 50 period ends."""
    concepts = max(1, facts // PERIODS)
    columns: dict[str, list[Any]] = {field.name: [] for field in SEC_XBRL_FACT_SCHEMA.fields}
    for number in range(facts):
        concept, period = divmod(number, PERIODS)
        end = date(2014, 3, 31) + timedelta(days=91 * period)
        instant = concept % 2 == 0
        value = Decimal(1_000_000 + cik % 1000 + number + version) / 100
        row = {
            "cik": cik,
            "taxonomy": "us-gaap",
            "concept": f"MadeUpConcept{concept % concepts:04d}",
            "unit": "USD",
            "period_start": end if instant else end - timedelta(days=90),
            "period_end": end,
            "accession_number": f"{cik:010d}-{14 + period // 4:02d}-{period:06d}",
            "is_instant": instant,
            "value": value,
            "value_text": str(value),
            "fiscal_year": 2014 + period // 4,
            "fiscal_period": f"Q{period % 4 + 1}",
            "form": "10-Q",
            "filed_date": end + timedelta(days=40),
            "frame": None,
            "_source_file": f"raw/sec/companyfacts/dt=2026-11-28/CIK{cik:010d}.json",
            "_source_md5": "0" * 32,
            "_ingested_at": INGESTED_AT,
            "_run_id": "benchmark",
            "_pipeline_version": "benchmark",
        }
        for name, cell in row.items():
            columns[name].append(cell)
    return pa.table(columns, schema=SEC_XBRL_FACT_SCHEMA.as_arrow())


def peak_mib() -> float:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # Linux reports kilobytes, macOS reports bytes.
    return round(peak / (1024 * 1024 if platform.system() == "Darwin" else 1024), 1)


def files(table: Any) -> dict[str, int]:
    return {task.file.file_path: task.file.file_size_in_bytes for task in table.scan().plan_files()}


def load(root: Path, variant: str, companies: int, facts: int) -> dict[str, Any]:
    config = settings(root)
    catalog = load_lake_catalog(config)
    namespace = ensure_silver_namespace(catalog, config)
    table = catalog.create_table(
        (namespace, f"{SEC_XBRL_FACT}_{variant}"),
        schema=SEC_XBRL_FACT_SCHEMA,
        partition_spec=identity_partition(SEC_XBRL_FACT_SCHEMA, VARIANTS[variant]),
        properties=TABLE_PROPERTIES,
    )
    ciks = [FIRST_CIK + index for index in range(companies)]
    data = pa.concat_tables(company(cik, facts, version=0) for cik in ciks)
    started = time.monotonic()
    overwrite_scope(table, data, Scope.for_table(), run_id="benchmark")
    seconds = time.monotonic() - started
    held = files(table)
    return {
        "rows": data.num_rows,
        "seconds": round(seconds, 2),
        "data_files": len(held),
        "bytes": sum(held.values()),
        "peak_mib": peak_mib(),
    }


def refresh(root: Path, variant: str, facts: int, refreshed: int) -> dict[str, Any]:
    config = settings(root)
    catalog = load_lake_catalog(config)
    table = catalog.load_table((config.silver_namespace, f"{SEC_XBRL_FACT}_{variant}"))
    before = files(table)
    rows_before = table.scan().count()
    ciks = [FIRST_CIK + 7 * index for index in range(refreshed)]
    data = pa.concat_tables(company(cik, facts + 10, version=1) for cik in ciks)
    after_data = peak_mib()
    started = time.monotonic()
    result = overwrite_scope(table, data, Scope.for_keys("cik", ciks), run_id="benchmark")
    seconds = time.monotonic() - started
    after = files(table)
    return {
        "refreshed_companies": refreshed,
        "new_rows": data.num_rows,
        "rows_before": rows_before,
        "rows_after": table.scan().count(),
        "seconds": round(seconds, 2),
        "peak_mib_before_the_commit": after_data,
        "peak_mib": peak_mib(),
        "operations": list(result.operations),
        "data_files_before": len(before),
        "data_files_after": len(after),
        "data_files_kept": len(set(before) & set(after)),
        "data_files_written": len(set(after) - set(before)),
        "bytes_written": sum(size for path, size in after.items() if path not in before),
    }


def step(arguments: list[str]) -> dict[str, Any]:
    """Run one step in a process of its own and read its result."""
    command = [sys.executable, "-m", "benchmarks.p2_lake.refresh_facts", *arguments]
    finished = subprocess.run(command, capture_output=True, text=True, check=False)  # noqa: S603
    if finished.returncode != 0:
        raise RuntimeError(finished.stderr[-2000:])
    loaded: dict[str, Any] = json.loads(finished.stdout.splitlines()[-1])
    return loaded


def main() -> None:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument("--companies", type=int, default=50)
    parser.add_argument("--facts", type=int, default=26_000)
    parser.add_argument("--refresh", type=int, default=3)
    parser.add_argument("--out", type=Path, default=RESULTS)
    parser.add_argument("--phase", choices=["load", "refresh"])
    parser.add_argument("--variant", choices=sorted(VARIANTS))
    parser.add_argument("--root", type=Path)
    args = parser.parse_args()

    if args.phase == "load":
        print(json.dumps(load(args.root, args.variant, args.companies, args.facts)))
        return
    if args.phase == "refresh":
        print(json.dumps(refresh(args.root, args.variant, args.facts, args.refresh)))
        return

    shared = Path("/lake/_test")
    base = shared if shared.parent.is_dir() else Path(tempfile.gettempdir())
    results: dict[str, Any] = {
        "measured_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "machine": f"{platform.system()} {platform.machine()}",
        "companies": args.companies,
        "facts_per_company": args.facts,
        "variants": {},
    }
    for variant in VARIANTS:
        root = base / f"p2-refresh-facts-{variant}"
        shutil.rmtree(root, ignore_errors=True)
        root.mkdir(parents=True)
        try:
            common = ["--variant", variant, "--root", str(root), "--facts", str(args.facts)]
            loaded = step(["--phase", "load", *common, "--companies", str(args.companies)])
            refreshed = step(["--phase", "refresh", *common, "--refresh", str(args.refresh)])
            results["variants"][variant] = {"load": loaded, "refresh": refreshed}
        finally:
            shutil.rmtree(root, ignore_errors=True)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
