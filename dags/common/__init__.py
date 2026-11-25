"""Wiring shared by every DAG file: default arguments, pool names and paths (blueprint 5.5, 8.1).

DAG files contain wiring only; all logic lives in the gbmi package (blueprint 10).
"""

from __future__ import annotations

import os
from datetime import timedelta
from typing import Any

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
#: configuration: the local stack sets GBMI_TASK_RETRIES=0 so that a failure shows at once.
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
