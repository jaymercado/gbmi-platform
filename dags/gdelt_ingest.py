"""Pipeline PL-1: land the GDELT files (blueprint 8.3; week 2 plan WP-07).

    reconcile -> ingest -> report_overdue

Every 15 minutes, at the minutes 8, 23, 38 and 53: GDELT uploads the files of a slot about ten
minutes before the time in the file name. The reconciliation against the master list does its
work once in each UTC day from 08:00 and runs before the ingest, so the run at 08:08 lands what
the reconciliation found before the daily build at 08:25. Each task is a thin wrapper around
one function of the gbmi package, which runs in its own environment (blueprint 5.5, 8.1).
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from airflow.sdk import Param, TriggerRule, dag, task

from common import AS_OF, DEFAULT_ARGS, GBMI_PYTHON, as_of_param, schedule

#: A run after a stop downloads up to the batch limit; the backfill raises the limit to 1,500.
INGEST_TIMEOUT = timedelta(minutes=60)


@dag(
    dag_id="gdelt_ingest",
    description="Expect, list, download and verify the GDELT files; reconcile once a day.",
    schedule=schedule("8,23,38,53 * * * *"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "source": Param(
            "live",
            enum=["live", "fixture"],
            description="live: the GDELT servers. fixture: the recorded hour under /fixtures.",
        ),
        "batch_limit": Param(
            None,
            type=["null", "integer"],
            minimum=1,
            description="Files of one run at most. Empty: 200. The backfill uses 1,500.",
        ),
        "as_of": as_of_param(),
    },
    tags=["gdelt", "ingest"],
)
def gdelt_ingest() -> None:
    # The functions below run in /opt/venvs/gbmi, so their imports stay inside them: the
    # Airflow environment has no gbmi. They return built-in types only.

    @task.external_python(python=GBMI_PYTHON, expect_airflow=False)
    def reconcile(as_of: str, source: str, run_id: str) -> dict[str, Any]:
        from gbmi.logging import configure_logging
        from gbmi.sources.gdelt.tasks import reconcile_task

        configure_logging(run_id=run_id)
        return reconcile_task(as_of, source, run_id=run_id)

    # The ingest also runs when the reconciliation failed: the files must keep landing.
    @task.external_python(
        python=GBMI_PYTHON,
        expect_airflow=False,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=INGEST_TIMEOUT,
    )
    def ingest(as_of: str, source: str, batch_limit: str, run_id: str) -> dict[str, Any]:
        from gbmi.logging import configure_logging
        from gbmi.sources.gdelt.tasks import ingest_task

        configure_logging(run_id=run_id)
        limit = int(batch_limit) if batch_limit else None
        return ingest_task(as_of, source, batch_limit=limit, run_id=run_id)

    @task.external_python(
        python=GBMI_PYTHON, expect_airflow=False, trigger_rule=TriggerRule.ALL_DONE
    )
    def report_overdue(as_of: str, run_id: str) -> dict[str, Any]:
        from gbmi.logging import configure_logging
        from gbmi.sources.gdelt.tasks import report_overdue_task

        configure_logging(run_id=run_id)
        return report_overdue_task(as_of, run_id=run_id)

    reconciled = reconcile(as_of=AS_OF, source="{{ params.source }}", run_id="{{ run_id }}")
    ingested = ingest(
        as_of=AS_OF,
        source="{{ params.source }}",
        batch_limit="{{ params.batch_limit or '' }}",
        run_id="{{ run_id }}",
    )
    reported = report_overdue(as_of=AS_OF, run_id="{{ run_id }}")

    reconciled >> ingested >> reported


gdelt_ingest()
