"""Pipeline PL-4: the daily form index and the filings of the watchlist (blueprint 8.6).

    filings -> summary

Each day at 08:30 UTC, more than five hours after the SEC publishes the index file of the day
before (week 3 plan, finding F10). The task is a thin wrapper around one function of the gbmi
package, which runs in its own environment. A run reads the object ledger and does the work
which is open: a run which starts late does the same work, so the DAG has no start date and
does not catch up.

The source comes from the setting GBMI_SEC_SOURCE of the stack: the DAG has no parameter for
the source. Without the parameters first_day and last_day the run looks at the seven days
before its clock; with them the run is a backfill of the index files.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from airflow.sdk import CronTriggerTimetable, Param, TriggerRule, dag, task

from common import AS_OF, GBMI_PYTHON, POOL_SEC_API, SEC_ARGS, as_of_param, schedule

#: The index files of a backfill of some weeks, and the documents of the watchlist.
FILINGS_TIMEOUT = timedelta(minutes=30)
DAY_PATTERN = r"^\d{4}-\d{2}-\d{2}$"


@dag(
    dag_id="sec_filings",
    description="Load the daily index files of the SEC and the filings of the watchlist.",
    schedule=schedule(CronTriggerTimetable("30 8 * * *", timezone="UTC")),
    catchup=False,
    max_active_runs=1,
    default_args=SEC_ARGS,
    params={
        "first_day": Param(
            None,
            type=["null", "string"],
            pattern=DAY_PATTERN,
            description="First UTC day of a backfill, YYYY-MM-DD. Empty: seven days back.",
        ),
        "last_day": Param(
            None,
            type=["null", "string"],
            pattern=DAY_PATTERN,
            description="Last UTC day of a backfill, YYYY-MM-DD. Empty: the day before the run.",
        ),
        "accept_volume": Param(
            None,
            type=["null", "string"],
            description="The note of an operator who accepts a file outside its volume band.",
        ),
        "as_of": as_of_param(),
    },
    tags=["sec", "filings"],
)
def sec_filings() -> None:
    # The functions below run in /opt/venvs/gbmi, so their imports stay inside them: the
    # Airflow environment has no gbmi. They return built-in types only.

    @task.external_python(
        python=GBMI_PYTHON,
        expect_airflow=False,
        pool=POOL_SEC_API,
        execution_timeout=FILINGS_TIMEOUT,
    )
    def filings(
        as_of: str, first_day: str, last_day: str, accept_volume: str, run_id: str
    ) -> dict[str, Any]:
        from gbmi.logging import configure_logging
        from gbmi.sources.sec.tasks import filings_task

        configure_logging(run_id=run_id)
        return filings_task(
            as_of, first_day or None, last_day or None, accept_volume or None, run_id
        )

    # The summary reads the record of the run and fails when the pipeline did not succeed,
    # so the summary runs after a failed task too.
    @task.external_python(
        python=GBMI_PYTHON, expect_airflow=False, trigger_rule=TriggerRule.ALL_DONE
    )
    def summary(as_of: str, run_id: str) -> dict[str, Any]:
        from gbmi.logging import configure_logging
        from gbmi.sources.sec.tasks import summary_task

        configure_logging(run_id=run_id)
        return summary_task("filings", as_of, run_id)

    loaded = filings(
        as_of=AS_OF,
        first_day="{{ params.first_day or '' }}",
        last_day="{{ params.last_day or '' }}",
        accept_volume="{{ params.accept_volume or '' }}",
        run_id="{{ run_id }}",
    )
    loaded >> summary(as_of=AS_OF, run_id="{{ run_id }}")


sec_filings()
