"""Pipeline PL-7, first version: the SEC marts and the operational marts (blueprint 8.9).

    pin_lake -> dbt (seeds, staging, intermediate models and marts, each with its tests) -> record

Each day at 11:45 UTC, after the three SEC pipelines: the run pins the five silver tables of
the SEC, builds the SEC models and the two operational models through Cosmos, and records the
row counts of the five marts. Every model is rebuilt in full, so a rerun gives the same tables
(blueprint 7.6). The news marts, the watermarks and the hourly schedule join in phase P3.

The dbt tasks leave out the unit tests: a unit test checks the code of a model on made-up rows
and not the data of a run, and runs in make demo (week 3 plan 3.3). A test which reads two
models runs after both, as a task of its own.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from airflow.sdk import CronTriggerTimetable, dag, task
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, InvocationMode, LoadMode, TestBehavior

from common import (
    DBT_EXECUTABLE,
    DBT_MANIFEST,
    DBT_PROFILES_YML,
    DBT_PROJECT_DIR,
    DEFAULT_ARGS,
    GBMI_PYTHON,
    TASK_RETRIES,
    dbt_target,
    schedule,
)

# Values that dbt receives from the pin task, read from XCom when a dbt task runs.
PINNED = "{{ ti.xcom_pull(task_ids='pin_lake')['%s'] }}"
#: The SEC models, the two operational models and the two seeds (week 3 plan 3.3).
SELECTION = [
    "path:seeds",
    "path:models/staging/sec",
    "path:models/staging/ops",
    "path:models/intermediate",
    "path:models/marts/core",
    "path:models/marts/filings",
    "path:models/marts/financials",
    "path:models/ops",
]
#: dbt selectors which no task of the DAG runs.
EXCLUSION = ["resource_type:unit_test"]


@dag(
    dag_id="marts_build",
    description="Pin the five SEC tables, build the SEC marts and the operational marts.",
    schedule=schedule(CronTriggerTimetable("45 11 * * *", timezone="UTC")),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    # The snapshots of the pinned tables reach dbt as a mapping, not as a text.
    render_template_as_native_obj=True,
    tags=["marts", "sec"],
)
def marts_build() -> None:
    @task.external_python(python=GBMI_PYTHON, expect_airflow=False)
    def pin_lake() -> dict[str, Any]:
        from gbmi.logging import configure_logging
        from gbmi.warehouse.marts import pin_sec_tables_task

        configure_logging()
        return pin_sec_tables_task()

    @task.external_python(python=GBMI_PYTHON, expect_airflow=False)
    def record(pinned: Mapping[str, Any], run_id: str) -> dict[str, Any]:
        from gbmi.logging import configure_logging
        from gbmi.warehouse.marts import record_marts_task

        configure_logging(run_id=run_id)
        return record_marts_task(pinned, run_id)

    pinned = pin_lake()

    dbt_models = DbtTaskGroup(
        group_id="dbt",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_DIR,
            manifest_path=DBT_MANIFEST,
            project_name="gbmi",
            install_dbt_deps=False,
        ),
        profile_config=ProfileConfig(
            profile_name="gbmi",
            target_name=dbt_target(),
            profiles_yml_filepath=DBT_PROFILES_YML,
        ),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL,
            # dbt is not installed beside Airflow; it runs as a subprocess of its own executable.
            invocation_mode=InvocationMode.SUBPROCESS,
            dbt_executable_path=DBT_EXECUTABLE,
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            select=SELECTION,
            exclude=EXCLUSION,
            # Each model is followed by its tests; a failed test stops the models after it.
            test_behavior=TestBehavior.AFTER_EACH,
            should_detach_multiple_parents_tests=True,
        ),
        operator_args={
            # Cosmos retries a model twice at most (blueprint 8.9).
            "retries": min(TASK_RETRIES, 2),
            "vars": {
                "silver_snapshot_ids": PINNED % "snapshot_ids",
                "pipeline_version": PINNED % "pipeline_version",
            },
        },
    )

    pinned >> dbt_models >> record(pinned=pinned, run_id="{{ run_id }}")


marts_build()
