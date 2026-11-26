"""Pipeline PL-2, daily part: finalise one UTC day (blueprint 8.4; week 2 plan WP-07).

    build_export -> build_mentions -> build_gkg -> summary

The DAG is a pure function of its logical date: the date of the logical date is the day to
build, which makes a backfill of Airflow meaningful (blueprint 5.5). The run which starts at
08:25 UTC has the logical date of the day before, 08:25, so the run builds the closed day. A
day build replaces the hourly files of the day with one file for each table.

A run which is triggered by hand without a logical date names the day with the parameter
``day``. The DAG has no start date: after an unpause the first scheduled run is the next
08:25 UTC, and no run for a past day appears beside a backfill (week 2 plan 3.5, change C3).
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from airflow.sdk import CronTriggerTimetable, Param, TriggerRule, dag, task

from common import (
    AS_OF,
    GBMI_PYTHON,
    LOGICAL_DATE,
    POOL_SILVER_WRITER,
    SILVER_ARGS,
    as_of_param,
    schedule,
)

#: Three times the expected duration of the largest feed of one day (blueprint 8.1).
BUILD_TIMEOUT = timedelta(minutes=30)
FEEDS = ("export", "mentions", "gkg")


@dag(
    dag_id="gdelt_silver_daily",
    description="Build one whole UTC day of the four silver tables: the day of the logical date.",
    schedule=schedule(
        CronTriggerTimetable("25 8 * * *", timezone="UTC", interval=timedelta(days=1))
    ),
    catchup=False,
    max_active_runs=1,
    default_args=SILVER_ARGS,
    params={
        "day": Param(
            None,
            type=["null", "string"],
            pattern=r"^\d{4}-\d{2}-\d{2}$",
            description="UTC day to build, YYYY-MM-DD. Empty: the date of the logical date.",
        ),
        "as_of": as_of_param(),
        "accept_volume": Param(
            None,
            type=["null", "string"],
            description="The note of an operator who accepts a day outside the volume band.",
        ),
    },
    tags=["gdelt", "silver"],
)
def gdelt_silver_daily() -> None:
    @task.external_python(
        python=GBMI_PYTHON,
        expect_airflow=False,
        pool=POOL_SILVER_WRITER,
        execution_timeout=BUILD_TIMEOUT,
    )
    def build_day(
        feed: str, day: str, logical_date: str, as_of: str, accept_volume: str, run_id: str
    ) -> dict[str, Any]:
        from gbmi.logging import configure_logging
        from gbmi.transforms.tasks import build_day_task

        configure_logging(run_id=run_id)
        return build_day_task(day, feed, as_of, accept_volume, run_id, logical_date)

    # The summary reads the partition versions which the run recorded, and fails when the
    # build of a feed is missing. So the summary runs after a failed build too.
    @task.external_python(
        python=GBMI_PYTHON, expect_airflow=False, trigger_rule=TriggerRule.ALL_DONE
    )
    def summary(day: str, logical_date: str, run_id: str) -> dict[str, Any]:
        from gbmi.logging import configure_logging
        from gbmi.transforms.tasks import day_summary_task

        configure_logging(run_id=run_id)
        return day_summary_task(day, logical_date, run_id)

    day = "{{ params.day or '' }}"
    previous = None
    for feed in FEEDS:
        # One feed after the other. A feed which fails does not stop the feeds after it.
        built = build_day.override(
            task_id=f"build_{feed}",
            trigger_rule=TriggerRule.ALL_SUCCESS if previous is None else TriggerRule.ALL_DONE,
        )(
            feed=feed,
            day=day,
            logical_date=LOGICAL_DATE,
            as_of=AS_OF,
            accept_volume="{{ params.accept_volume or '' }}",
            run_id="{{ run_id }}",
        )
        if previous is not None:
            previous >> built
        previous = built
    previous >> summary(day=day, logical_date=LOGICAL_DATE, run_id="{{ run_id }}")


gdelt_silver_daily()
