"""A small client for the Airflow REST API, used by ``make demo`` and the end-to-end tests.

It does what an operator would do in the UI: unpause a DAG, trigger a run with parameters,
wait for it and read what its tasks returned. Run as a module it triggers the tracer:

    python -m tests.e2e.airflow --hour 2026-11-25T08 --source fixture
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from dataclasses import dataclass
from typing import Any

import httpx

TRACER_DAG = "tracer_gdelt_events"
FIXTURE_HOUR = "2026-11-25T08"
FINAL_STATES = {"success", "failed"}


class DagRunError(RuntimeError):
    """A DAG run did not finish successfully in time."""


@dataclass(frozen=True)
class DagRun:
    dag_id: str
    run_id: str
    state: str
    seconds: float
    task_states: dict[str, str]


class AirflowClient:
    def __init__(
        self, base_url: str | None = None, username: str | None = None, password: str | None = None
    ) -> None:
        self.base_url = (
            base_url or os.environ.get("AIRFLOW_URL", "http://airflow-apiserver:8080")
        ).rstrip("/")
        username = username or os.environ.get("AIRFLOW_ADMIN_USER", "admin")
        password = password or os.environ["AIRFLOW_ADMIN_PASSWORD"]
        self._http = httpx.Client(base_url=self.base_url, timeout=30)
        response = self._http.post("/auth/token", json={"username": username, "password": password})
        response.raise_for_status()
        self._http.headers["Authorization"] = f"Bearer {response.json()['access_token']}"

    def _get(self, path: str, **params: Any) -> Any:
        response = self._http.get(f"/api/v2{path}", params=params)
        response.raise_for_status()
        return response.json()

    def wait_until_listed(self, dag_id: str, timeout: float = 180) -> None:
        """A new DAG file is parsed within the DAG processor's refresh interval."""
        deadline = time.monotonic() + timeout
        while True:
            response = self._http.get(f"/api/v2/dags/{dag_id}")
            if response.status_code == 200 and not response.json().get("has_import_errors"):
                return
            if time.monotonic() > deadline:
                errors = self._get("/importErrors").get("import_errors", [])
                raise DagRunError(f"DAG {dag_id} is not available after {timeout:.0f} s: {errors}")
            time.sleep(3)

    def unpause(self, dag_id: str) -> None:
        """DAGs are paused at creation; a run triggered on a paused DAG stays queued."""
        self._http.patch(f"/api/v2/dags/{dag_id}", json={"is_paused": False}).raise_for_status()

    def trigger(self, dag_id: str, conf: dict[str, Any]) -> str:
        run_id = f"manual__{uuid.uuid4().hex[:12]}"
        response = self._http.post(
            f"/api/v2/dags/{dag_id}/dagRuns",
            json={"dag_run_id": run_id, "logical_date": None, "conf": conf},
        )
        response.raise_for_status()
        return run_id

    def task_states(self, dag_id: str, run_id: str) -> dict[str, str]:
        instances = self._get(f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances")["task_instances"]
        return {instance["task_id"]: instance["state"] or "none" for instance in instances}

    def xcom(self, dag_id: str, run_id: str, task_id: str) -> Any:
        """What a task returned."""
        entry = self._get(
            f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries/return_value",
            deserialize="true",
        )
        return entry["value"]

    def log_tail(self, dag_id: str, run_id: str, task_id: str, lines: int = 40) -> str:
        response = self._http.get(
            f"/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/1",
            headers={"Accept": "text/plain"},
        )
        if response.status_code != 200:
            return f"(no log: HTTP {response.status_code})"
        return "\n".join(response.text.splitlines()[-lines:])

    def run(self, dag_id: str, conf: dict[str, Any], timeout: float = 900) -> DagRun:
        """Trigger a run and wait for it; on failure raise with the failing task's log."""
        self.wait_until_listed(dag_id)
        self.unpause(dag_id)
        run_id = self.trigger(dag_id, conf)
        started = time.monotonic()
        state = "queued"
        while time.monotonic() - started < timeout:
            state = self._get(f"/dags/{dag_id}/dagRuns/{run_id}")["state"]
            if state in FINAL_STATES:
                break
            time.sleep(3)
        seconds = time.monotonic() - started
        states = self.task_states(dag_id, run_id)
        if state != "success":
            failed = [
                task
                for task, task_state in states.items()
                if task_state in ("failed", "up_for_retry")
            ]
            logs = "\n\n".join(
                f"--- {task} ---\n{self.log_tail(dag_id, run_id, task)}" for task in failed
            )
            raise DagRunError(
                f"{dag_id} run {run_id} is {state!r} after {seconds:.0f} s; tasks: {states}\n{logs}"
            )
        return DagRun(dag_id, run_id, state, seconds, states)


def run_tracer(
    client: AirflowClient, hour: str = FIXTURE_HOUR, source: str = "fixture"
) -> dict[str, Any]:
    """Run the tracer for one hour and return what its tasks reported."""
    run = client.run(TRACER_DAG, {"hour": hour, "source": source})
    return {
        "run_id": run.run_id,
        "state": run.state,
        "seconds": round(run.seconds, 1),
        "tasks": run.task_states,
        "land": client.xcom(TRACER_DAG, run.run_id, "land"),
        "build_silver": client.xcom(TRACER_DAG, run.run_id, "build_silver"),
        "pin_lake": client.xcom(TRACER_DAG, run.run_id, "pin_lake"),
        "assert_counts": client.xcom(TRACER_DAG, run.run_id, "assert_counts"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Trigger the tracer DAG and wait for it.")
    parser.add_argument("--hour", default=FIXTURE_HOUR)
    parser.add_argument("--source", default="fixture", choices=["fixture", "live"])
    args = parser.parse_args()
    try:
        result = run_tracer(AirflowClient(), args.hour, args.source)
    except DagRunError as error:
        print(error, file=sys.stderr)
        return 1
    for file in result["land"]["files"]:
        file.pop("md5", None)
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
