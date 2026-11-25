"""Spike SK3, part B - Cosmos on Airflow 3.4 (week 1 plan, WP-08; risk R6).

Questions answered by one manual run of this DAG:

B2  Does a DbtTaskGroup render from a prebuilt manifest and run dbt from /opt/venvs/dbt?
B3  Is DAG parsing free of dbt calls?
B4  Do runtime variables reach dbt? (``partitions`` from the trigger configuration)
B5  Does @task.external_python run a gbmi function from /opt/venvs/gbmi and return a result?

This file is removed when the tracer DAG supersedes it; the tracer then stands as the evidence.
"""

from __future__ import annotations

import logging
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
)


@dag(
    dag_id="spike_sk3_cosmos",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retries": 0},
    params={
        "partitions": Param(
            None,
            type=["null", "array"],
            description="Partitions to replace, for example [202611]. Empty on the first run.",
        ),
    },
    # A templated list stays a list, so dbt receives [202611] and not the string "[202611]".
    render_template_as_native_obj=True,
    tags=["spike"],
)
def spike_sk3_cosmos() -> None:
    @task.external_python(python=GBMI_PYTHON, expect_airflow=False)
    def describe_gbmi_environment() -> dict[str, str]:
        """Runs in /opt/venvs/gbmi. Imports stay inside: the Airflow environment has no gbmi."""
        import sys

        import gbmi
        from gbmi.config import get_settings

        settings = get_settings()
        # Built-in types only: the result is unpickled where gbmi is not installed.
        return {
            "gbmi_version": gbmi.__version__,
            "python": sys.executable,
            "lake_backend": settings.lake_backend.value,
            "warehouse_uri": settings.warehouse_uri,
        }

    @task
    def check_result(info: dict[str, Any]) -> None:
        """Runs in the Airflow environment and receives the result through XCom."""
        assert info["python"].startswith("/opt/venvs/gbmi/"), info
        assert info["lake_backend"] == "local", info
        logging.getLogger(__name__).info("gbmi environment: %s", info)

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
            # dbt is not installed beside Airflow, so it runs as a subprocess of its own
            # executable (blueprint 5.5).
            invocation_mode=InvocationMode.SUBPROCESS,
            dbt_executable_path=DBT_EXECUTABLE,
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            select=["path:models/spike"],
            test_behavior=TestBehavior.NONE,
        ),
        operator_args={
            "install_deps": False,
            "vars": {"partitions": "{{ params.partitions }}"},
        },
    )

    check_result(describe_gbmi_environment()) >> dbt_models


spike_sk3_cosmos()
