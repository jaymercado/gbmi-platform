"""Benchmark B4, the mart size: the events mart built over the months of the window.

The stretch item of the week 2 plan (section 6; change C8). The script:

1. pins ``lake.gdelt_event`` in ClickHouse to the current metadata file of the silver table;
2. builds the staging view and the mart ``marts.fct_gdelt_event_daily`` with dbt for the
   month partitions which the option ``--months`` names, as the tracer DAG does for one month;
3. reads the size of the mart on disk from ``system.parts``, and compares the events of the
   mart with the rows of the pinned silver table, month by month.

The script writes to ClickHouse only: the pin and the mart partitions of the named months.
The ledger and the lake are not touched.

Data: The GDELT Project, https://www.gdeltproject.org/

Run on the Compose network of the working stack:
    make toolbox CMD="python -m benchmarks.b4_storage.events_mart --months 202610 202611"
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any

from gbmi.config import get_settings
from gbmi.core.clock import utc_now
from gbmi.warehouse.client import connect
from gbmi.warehouse.pin import pin_silver_table_task

RESULTS = Path(__file__).resolve().parent / "results"
MART = ("marts", "fct_gdelt_event_daily")
SELECT = ["path:models/staging/gdelt", "path:models/marts/news"]


def admin() -> Any:
    """A ClickHouse client which may read system.parts."""
    host, port = os.environ["DBT_CH_HOST"], os.environ.get("DBT_CH_PORT", "8123")
    return connect(f"http://default:{os.environ['CLICKHOUSE_ADMIN_PASSWORD']}@{host}:{port}")


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument("--months", type=int, nargs="+", required=True, help="such as 202611")
    parser.add_argument("--out", type=Path, default=None)
    arguments = parser.parse_args()
    months = sorted(arguments.months)
    name = "events-mart-" + "-".join(str(month) for month in months)
    out = arguments.out or RESULTS / f"{name}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    settings = get_settings()

    pinned = pin_silver_table_task("gdelt_event", settings)
    variables = {
        "partitions": months,
        "pipeline_version": settings.pipeline_version,
        "silver_snapshot_id": pinned["snapshot_id"],
    }
    command = ["dbt", "build", "--project-dir", "dbt", "--profiles-dir", "dbt/profiles"]
    command += ["--select", *SELECT, "--vars", json.dumps(variables)]
    started = time.perf_counter()
    build = subprocess.run(command, capture_output=True, text=True, check=False)  # noqa: S603
    seconds = time.perf_counter() - started
    last_lines = [line for line in build.stdout.splitlines() if line.strip()][-4:]

    client = admin()
    month_list = ", ".join(str(month) for month in months)
    parts = client.query(
        "SELECT partition, count() AS parts, sum(rows), sum(bytes_on_disk), "
        "sum(data_compressed_bytes), sum(data_uncompressed_bytes) FROM system.parts "
        f"WHERE database = '{MART[0]}' AND table = '{MART[1]}' AND active "
        "GROUP BY partition ORDER BY partition"
    ).result_rows
    in_mart = dict(
        client.query(
            "SELECT toYYYYMM(date), sum(events) FROM marts.fct_gdelt_event_daily "
            f"WHERE toYYYYMM(date) IN ({month_list}) GROUP BY 1"
        ).result_rows
    )
    in_silver = dict(
        client.query(
            "SELECT toYYYYMM(slot_date), count() FROM lake.gdelt_event "
            f"WHERE toYYYYMM(slot_date) IN ({month_list}) GROUP BY 1"
        ).result_rows
    )
    by_month = {
        str(row[0]): {
            "parts": int(row[1]),
            "rows": int(row[2]),
            "bytes_on_disk": int(row[3]),
            "compressed_bytes": int(row[4]),
            "uncompressed_bytes": int(row[5]),
        }
        for row in parts
    }
    result: dict[str, Any] = {
        "benchmark": "B4",
        "measured_at": utc_now().isoformat(timespec="seconds"),
        "pinned": pinned,
        "months": months,
        "dbt": {
            "exit_code": build.returncode,
            "seconds": round(seconds, 1),
            "last_lines": last_lines,
        },
        "mart": {
            "table": ".".join(MART),
            "partitions": by_month,
            "rows": sum(part["rows"] for part in by_month.values()),
            "bytes_on_disk": sum(part["bytes_on_disk"] for part in by_month.values()),
        },
        "events_in_the_mart": {str(month): int(in_mart.get(month, 0)) for month in months},
        "rows_in_pinned_silver": {str(month): int(in_silver.get(month, 0)) for month in months},
    }
    result["mart_agrees_with_silver"] = (
        result["events_in_the_mart"] == result["rows_in_pinned_silver"]
    )
    out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))
    print(f"the result is in {out}")
    return 0 if build.returncode == 0 and result["mart_agrees_with_silver"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
