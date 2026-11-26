"""Run records: one row in ``ops.pipeline_run`` for each execution of a task entry point.

The row holds the duration, the rows and bytes handled, the outcome and the version of the
code (blueprint 8.1). An entry point wraps its work in ``recorded_run``; a failure is recorded
and raised again.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from psycopg.types.json import Jsonb

from gbmi.config import get_settings
from gbmi.core.clock import utc_now
from gbmi.ledger.db import Connection
from gbmi.logging import get_logger

log = get_logger(__name__)

#: Airflow exports the context of a task to the processes the task starts.
_AIRFLOW_VARIABLES = {
    "dag_id": "AIRFLOW_CTX_DAG_ID",
    "task_id": "AIRFLOW_CTX_TASK_ID",
    "run_id": "AIRFLOW_CTX_DAG_RUN_ID",
    "try_number": "AIRFLOW_CTX_TRY_NUMBER",
}


@dataclass
class Run:
    """What an entry point reports about one execution."""

    row_count: int | None = None
    byte_count: int | None = None
    detail: dict[str, Any] = field(default_factory=dict)


def _try_number() -> int | None:
    value = os.environ.get(_AIRFLOW_VARIABLES["try_number"], "")
    return int(value) if value.isdigit() else None


@contextmanager
def recorded_run(
    connection: Connection,
    entry_point: str,
    *,
    as_of: datetime | None = None,
    run_id: str | None = None,
    pipeline_version: str | None = None,
) -> Iterator[Run]:
    """Record one execution of ``entry_point``.

    The row says success when the block ends, and failed when the block raises.
    """
    started_at = utc_now()
    clock = time.monotonic()
    run = Run()

    def store(status: str, error: str | None) -> None:
        connection.execute(
            "INSERT INTO ops.pipeline_run (entry_point, dag_id, task_id, run_id, try_number, "
            "as_of, started_at, ended_at, duration_ms, status, row_count, byte_count, "
            "pipeline_version, error, detail) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                entry_point,
                os.environ.get(_AIRFLOW_VARIABLES["dag_id"]),
                os.environ.get(_AIRFLOW_VARIABLES["task_id"]),
                run_id or os.environ.get(_AIRFLOW_VARIABLES["run_id"]),
                _try_number(),
                as_of,
                started_at,
                utc_now(),
                round((time.monotonic() - clock) * 1000),
                status,
                run.row_count,
                run.byte_count,
                pipeline_version or get_settings().pipeline_version,
                error,
                Jsonb(run.detail),
            ),
        )

    try:
        yield run
    except BaseException as error:
        try:
            store("failed", f"{type(error).__name__}: {error}"[:2000])
        except Exception:  # the first error is the one to report
            log.warning("run_record_not_written", entry_point=entry_point)
        raise
    store("success", None)
