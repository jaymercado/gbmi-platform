"""The tracer: one hour of GDELT Events from source to mart (week 1 plan, WP-09).

    land -> build_silver -> pin_lake -> dbt (staging and mart, each with its tests) -> assert_counts

Triggered by hand with two parameters: the UTC hour and the source (the recorded fixture hour
or the live GDELT servers). Every task is a thin wrapper around a function of the gbmi
package, which runs in its own environment (blueprint 5.5, 8.1); dbt runs through Cosmos from
a prebuilt manifest. Rerunning an hour changes nothing: each hop replaces its unit as a whole
(blueprint 5.6).
"""

from __future__ import annotations

from typing import Any

from airflow.sdk import Param, dag, task
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, InvocationMode, LoadMode, TestBehavior

from common import (
    DBT_EXECUTABLE,
    DBT_MANIFEST,
    DBT_PROFILES_YML,
    DBT_PROJECT_DIR,
    DEFAULT_ARGS,
    GBMI_PYTHON,
    POOL_SILVER_WRITER,
)

# Values that dbt receives from the tasks before it, read from XCom when the dbt task runs.
BUILT = "{{ ti.xcom_pull(task_ids='build_silver')['%s'] }}"
PINNED = "{{ ti.xcom_pull(task_ids='pin_lake')['%s'] }}"


@dag(
    dag_id="tracer_gdelt_events",
    description="One hour of GDELT Events: bronze, silver, pinned lake table, mart.",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "hour": Param(
            "2026-11-25T08",
            type="string",
            pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}$",
            description="UTC slot-hour to process, YYYY-MM-DDTHH.",
        ),
        "source": Param(
            "fixture",
            enum=["fixture", "live"],
            description="fixture: the recorded hour under /fixtures. live: the GDELT servers.",
        ),
    },
    # Lists and numbers handed to dbt stay lists and numbers.
    render_template_as_native_obj=True,
    tags=["tracer", "gdelt"],
)
def tracer_gdelt_events() -> None:
    # The functions below run in /opt/venvs/gbmi, so their imports stay inside them: the
    # Airflow environment has no gbmi. They return built-in types only, because the result is
    # unpickled in the Airflow environment.

    @task.external_python(python=GBMI_PYTHON, expect_airflow=False)
    def land(hour: str, source: str) -> dict[str, Any]:
        from gbmi.logging import configure_logging
        from gbmi.sources.gdelt.land import land_export_hour

        configure_logging()
        return land_export_hour(hour, source)

    @task.external_python(python=GBMI_PYTHON, expect_airflow=False, pool=POOL_SILVER_WRITER)
    def build_silver(hour: str, run_id: str) -> dict[str, Any]:
        from gbmi.logging import configure_logging
        from gbmi.transforms.duckdb.gdelt_event import build_event_hour_task

        configure_logging(run_id=run_id)
        return build_event_hour_task(hour, run_id)

    @task.external_python(python=GBMI_PYTHON, expect_airflow=False)
    def pin_lake() -> dict[str, Any]:
        from gbmi.logging import configure_logging
        from gbmi.warehouse.pin import pin_silver_table_task

        configure_logging()
        return pin_silver_table_task("gdelt_event")

    @task.external_python(python=GBMI_PYTHON, expect_airflow=False)
    def assert_counts(hour: str, built: dict[str, Any], pinned: dict[str, Any]) -> dict[str, Any]:
        from gbmi.logging import configure_logging
        from gbmi.quality.reconcile import assert_tracer_counts

        configure_logging()
        return assert_tracer_counts(hour, built["counts"]["committed"], pinned["snapshot_id"])

    landed = land(hour="{{ params.hour }}", source="{{ params.source }}")
    built = build_silver(hour="{{ params.hour }}", run_id="{{ run_id }}")
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
            target_name="local",
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
            select=["path:models/staging/gdelt", "path:models/marts/news"],
            # Each model is followed by its tests; a failed test stops the models after it.
            test_behavior=TestBehavior.AFTER_EACH,
        ),
        operator_args={
            "install_deps": False,
            "vars": {
                "partitions": BUILT % "mart_partitions",
                "pipeline_version": BUILT % "pipeline_version",
                "silver_snapshot_id": PINNED % "snapshot_id",
            },
        },
    )

    checked = assert_counts(hour="{{ params.hour }}", built=built, pinned=pinned)

    landed >> built >> pinned >> dbt_models >> checked


tracer_gdelt_events()
