"""Pipeline PL-3: the company universe of the day and the former names (blueprint 8.5).

    reference -> summary

Each day at 09:30 UTC, one hour after the filings pipeline: the documents of the watchlist
are in bronze then, and the bulk archive of the night is more than four hours old (week 3
plan, finding F12). The first run and a Sunday run load the bulk archive; the parameter bulk
asks for the archive on any day. The source comes from the setting GBMI_SEC_SOURCE of the
stack.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from airflow.sdk import CronTriggerTimetable, Param, TriggerRule, dag, task

from common import AS_OF, GBMI_PYTHON, POOL_SEC_API, SEC_ARGS, as_of_param, schedule

#: One download of 1.58 GB with a time limit of 30 minutes, about 8,000 members to copy and
#: to read, and two commits (week 3 plan 3.3).
REFERENCE_TIMEOUT = timedelta(minutes=90)


@dag(
    dag_id="sec_reference_daily",
    description="Build the snapshot of the company universe and the former names.",
    schedule=schedule(CronTriggerTimetable("30 9 * * *", timezone="UTC")),
    catchup=False,
    max_active_runs=1,
    default_args=SEC_ARGS,
    params={
        "bulk": Param(
            False,
            type="boolean",
            description="Load the bulk archive of the submissions documents on any day.",
        ),
        "accept_volume": Param(
            None,
            type=["null", "string"],
            description="The note of an operator who accepts a file outside its volume band.",
        ),
        "as_of": as_of_param(),
    },
    tags=["sec", "reference"],
)
def sec_reference_daily() -> None:
    @task.external_python(
        python=GBMI_PYTHON,
        expect_airflow=False,
        pool=POOL_SEC_API,
        execution_timeout=REFERENCE_TIMEOUT,
    )
    def reference(as_of: str, bulk: str, accept_volume: str, run_id: str) -> dict[str, Any]:
        from gbmi.logging import configure_logging
        from gbmi.sources.sec.tasks import reference_task

        configure_logging(run_id=run_id)
        # A template renders the boolean parameter as the text True or False.
        return reference_task(as_of, bulk == "True", accept_volume or None, run_id)

    @task.external_python(
        python=GBMI_PYTHON, expect_airflow=False, trigger_rule=TriggerRule.ALL_DONE
    )
    def summary(as_of: str, run_id: str) -> dict[str, Any]:
        from gbmi.logging import configure_logging
        from gbmi.sources.sec.tasks import summary_task

        configure_logging(run_id=run_id)
        return summary_task("reference", as_of, run_id)

    built = reference(
        as_of=AS_OF,
        bulk="{{ params.bulk }}",
        accept_volume="{{ params.accept_volume or '' }}",
        run_id="{{ run_id }}",
    )
    built >> summary(as_of=AS_OF, run_id="{{ run_id }}")


sec_reference_daily()
