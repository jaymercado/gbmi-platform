"""Wiring shared by every DAG file: default arguments, pool names and paths (blueprint 5.5, 8.1).

DAG files contain wiring only; all logic lives in the gbmi package (blueprint 10).
"""

from __future__ import annotations

import os
from datetime import timedelta
from typing import Any

from airflow.sdk import Param

#: Pools with one slot each, created by the airflow-init service.
POOL_SEC_API = "sec_api"
POOL_SILVER_WRITER = "silver_writer"
POOL_HEAVY = "heavy"

#: Interpreter of the gbmi environment, used by @task.external_python.
GBMI_PYTHON = "/opt/venvs/gbmi/bin/python"
#: dbt executable of the dbt environment, used by Cosmos.
DBT_EXECUTABLE = "/opt/venvs/dbt/bin/dbt"
DBT_PROJECT_DIR = "/opt/gbmi/dbt"
DBT_PROFILES_YML = "/opt/gbmi/dbt/profiles/profiles.yml"
DBT_MANIFEST = "/opt/gbmi/dbt/target/manifest.json"

#: Three retries, four attempts in total, with growing delays (blueprint 8.1). The number is
#: configuration: the working stack keeps 3; the demo stack and CI set GBMI_TASK_RETRIES=0 so
#: that a failure shows at once (Makefile).
TASK_RETRIES = int(os.environ.get("GBMI_TASK_RETRIES", "3"))

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "gbmi",
    "retries": TASK_RETRIES,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    # Three times the expected duration of a task on one hour of events (blueprint 8.1).
    "execution_timeout": timedelta(minutes=10),
}
#: The jobs which commit to silver have two retries (blueprint 8.4).
SILVER_ARGS: dict[str, Any] = {**DEFAULT_ARGS, "retries": min(TASK_RETRIES, 2)}
#: An SEC task has one retry, 20 minutes after the failure, without a growing delay. A block
#: by the SEC lasts 15 minutes: the retries of DEFAULT_ARGS would all end before the block
#: ends (week 3 plan, finding F20 and difference D47).
SEC_ARGS: dict[str, Any] = {
    **DEFAULT_ARGS,
    "retries": min(TASK_RETRIES, 1),
    "retry_delay": timedelta(minutes=20),
    "retry_exponential_backoff": False,
}

#: The clock of a run (week 2 plan 3.3): the parameter as_of of a manual run, or the time from
#: which the run may start. Every run has run_after; a run which is triggered by hand without a
#: logical date has no logical date, and a template which names one fails.
AS_OF = "{{ params.as_of or dag_run.run_after.isoformat() }}"
#: The logical date of a run, or an empty text for a run without one.
LOGICAL_DATE = "{{ dag_run.logical_date.isoformat() if dag_run.logical_date else '' }}"


def dbt_target() -> str:
    """The output of the dbt profile which a DAG builds into.

    The setting DBT_TARGET of the stack decides: local on the laptop and in CI, prod on the
    host (dbt/profiles/profiles.yml).
    """
    return os.environ.get("DBT_TARGET", "local")


def as_of_param() -> Param:
    """The optional parameter as_of of a manual run."""
    return Param(
        None,
        type=["null", "string"],
        description="The clock to work with, ISO 8601 with a time zone. Empty: the run's time.",
    )


def schedule[Schedule](timetable: Schedule) -> Schedule | None:
    """The schedule of a DAG, or no schedule on a stack which runs its DAGs by hand only.

    The demo stack sets GBMI_DAG_SCHEDULES=off: its DAGs run on recorded files with a fixed
    clock, and an unpaused DAG must not start a run against the live source by the clock.
    """
    return None if os.environ.get("GBMI_DAG_SCHEDULES", "on") == "off" else timetable
