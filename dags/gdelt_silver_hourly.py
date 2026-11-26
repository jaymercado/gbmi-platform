"""Pipeline PL-2, incremental part: build the pending units (blueprint 8.4; week 2 plan WP-07).

    find_units -> build_units

Every 15 minutes, three minutes after the ingest. A pending slot is a verified slot without a
claim in the trailing seven days. A date without a day-level version gives one hour unit for
each hour with a pending slot; a finalised date gives one day unit (week 2 plan 3.3). One run
builds 48 units at most. The DAG trusts no upstream event: the task asks the ledger.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from airflow.sdk import Param, dag, task

from common import AS_OF, GBMI_PYTHON, POOL_SILVER_WRITER, SILVER_ARGS, as_of_param, schedule

#: 48 units after a stop: about 20 minutes on the laptop for hours of the three feeds.
BUILD_TIMEOUT = timedelta(minutes=60)


@dag(
    dag_id="gdelt_silver_hourly",
    description="Build the silver units which have verified files waiting.",
    schedule=schedule("11,26,41,56 * * * *"),
    catchup=False,
    max_active_runs=1,
    default_args=SILVER_ARGS,
    params={
        "as_of": as_of_param(),
        "max_units": Param(
            None,
            type=["null", "integer"],
            minimum=1,
            description="Units of one run at most. Empty: 48.",
        ),
    },
    tags=["gdelt", "silver"],
)
def gdelt_silver_hourly() -> None:
    @task.external_python(python=GBMI_PYTHON, expect_airflow=False)
    def find_units(as_of: str, max_units: str) -> dict[str, Any]:
        from gbmi.logging import configure_logging
        from gbmi.transforms.tasks import find_units_task

        configure_logging()
        return find_units_task(as_of, int(max_units) if max_units else None)

    @task.external_python(
        python=GBMI_PYTHON,
        expect_airflow=False,
        pool=POOL_SILVER_WRITER,
        execution_timeout=BUILD_TIMEOUT,
    )
    def build_units(found: dict[str, Any], as_of: str, run_id: str) -> dict[str, Any]:
        from gbmi.logging import configure_logging
        from gbmi.transforms.tasks import build_units_task

        configure_logging(run_id=run_id)
        return build_units_task(found["units"], as_of, run_id)

    found = find_units(as_of=AS_OF, max_units="{{ params.max_units or '' }}")
    build_units(found=found, as_of=AS_OF, run_id="{{ run_id }}")


gdelt_silver_hourly()
