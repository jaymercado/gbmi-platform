"""Pipeline PL-5: the financial facts of the watchlist companies (blueprint 8.7).

    facts -> summary

Each day at 10:30 UTC, after the filings pipeline has loaded the filings of the watchlist:
the run asks for the facts document of each company with a new report since the last facts
load of the company. A Sunday run, the first run and a run with the parameter full take every
company. The source comes from the setting GBMI_SEC_SOURCE of the stack.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from airflow.sdk import CronTriggerTimetable, Param, TriggerRule, dag, task

from common import AS_OF, GBMI_PYTHON, POOL_SEC_API, SEC_ARGS, as_of_param, schedule

#: Fifty documents of up to 5 MB, one request after the other, and one commit.
FACTS_TIMEOUT = timedelta(minutes=30)


@dag(
    dag_id="sec_xbrl_facts",
    description="Load the facts documents of the watchlist companies with a new report.",
    schedule=schedule(CronTriggerTimetable("30 10 * * *", timezone="UTC")),
    catchup=False,
    max_active_runs=1,
    default_args=SEC_ARGS,
    params={
        "full": Param(
            False,
            type="boolean",
            description="Load the facts of every watchlist company.",
        ),
        "accept_volume": Param(
            None,
            type=["null", "string"],
            description="The note of an operator who accepts a document with fewer facts.",
        ),
        "as_of": as_of_param(),
    },
    tags=["sec", "facts"],
)
def sec_xbrl_facts() -> None:
    @task.external_python(
        python=GBMI_PYTHON,
        expect_airflow=False,
        pool=POOL_SEC_API,
        execution_timeout=FACTS_TIMEOUT,
    )
    def facts(as_of: str, full: str, accept_volume: str, run_id: str) -> dict[str, Any]:
        from gbmi.logging import configure_logging
        from gbmi.sources.sec.tasks import facts_task

        configure_logging(run_id=run_id)
        # A template renders the boolean parameter as the text True or False.
        return facts_task(as_of, full == "True", accept_volume or None, run_id)

    @task.external_python(
        python=GBMI_PYTHON, expect_airflow=False, trigger_rule=TriggerRule.ALL_DONE
    )
    def summary(as_of: str, run_id: str) -> dict[str, Any]:
        from gbmi.logging import configure_logging
        from gbmi.sources.sec.tasks import summary_task

        configure_logging(run_id=run_id)
        return summary_task("facts", as_of, run_id)

    loaded = facts(
        as_of=AS_OF,
        full="{{ params.full }}",
        accept_volume="{{ params.accept_volume or '' }}",
        run_id="{{ run_id }}",
    )
    loaded >> summary(as_of=AS_OF, run_id="{{ run_id }}")


sec_xbrl_facts()
