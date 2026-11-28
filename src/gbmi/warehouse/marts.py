"""The SEC marts of one run of the DAG marts_build (blueprint 8.9; week 3 plan 3.3).

    pin_lake -> dbt (seeds, staging, intermediate models and marts, with their tests) -> record

The first task pins the five silver tables of the SEC. dbt builds the models from the pinned
tables and writes, into each mart, the Iceberg snapshot of the main silver table of the mart
and the version of the pipeline code (blueprint 7.12). The last task reads the two values back
from each mart: a mart which was built from another snapshot than the pinned one, or by another
version of the code, fails the run. The task writes the row counts to ``ops.pipeline_run``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from clickhouse_connect.driver.client import Client

from gbmi.config import Settings, get_settings
from gbmi.lake.tables import (
    SEC_COMPANY_SNAPSHOT,
    SEC_FILING_DETAIL,
    SEC_FILING_INDEX,
    SEC_SCHEMAS,
    SEC_XBRL_FACT,
)
from gbmi.ledger.db import connect as connect_ledger
from gbmi.ops.runs import recorded_run
from gbmi.warehouse.client import connect, quote_identifier
from gbmi.warehouse.pin import pin_silver_tables_task

#: The name of the build in ``ops.pipeline_run``.
ENTRY_POINT = "marts.build"
MARTS_DATABASE = "marts"
#: Each SEC mart with its main silver table: the table whose snapshot the mart records in
#: ``_silver_snapshot_id`` (the same names stand in the models, macro mart_provenance).
SEC_MARTS: dict[str, str] = {
    "dim_company": SEC_COMPANY_SNAPSHOT,
    "fct_filing": SEC_FILING_INDEX,
    "fct_filing_item": SEC_FILING_DETAIL,
    "fct_financial_fact": SEC_XBRL_FACT,
    "fct_financial_metric": SEC_XBRL_FACT,
}


class MartBuildError(Exception):
    """The marts do not come from the pinned tables; the run must not be reported as a success."""


def pin_sec_tables_task(settings: Settings | None = None) -> dict[str, Any]:
    """Task entry point: pin the five silver tables of the SEC, a missing table empty first."""
    return pin_silver_tables_task(list(SEC_SCHEMAS), settings)


def mart_states(client: Client) -> dict[str, dict[str, Any]]:
    """The rows and the provenance of each SEC mart, read from ClickHouse."""
    states: dict[str, dict[str, Any]] = {}
    for mart in SEC_MARTS:
        relation = f"{quote_identifier(MARTS_DATABASE)}.{quote_identifier(mart)}"
        rows, snapshots, versions = client.query(
            "SELECT count(), arraySort(groupUniqArray(_silver_snapshot_id)), "  # noqa: S608
            f"arraySort(groupUniqArray(_pipeline_version)) FROM {relation}"
        ).result_rows[0]
        states[mart] = {
            "rows": int(rows),
            "silver_snapshot_ids": [int(value) for value in snapshots],
            "pipeline_versions": [str(value) for value in versions],
        }
    return states


def build_problems(states: Mapping[str, Mapping[str, Any]], pinned: Mapping[str, Any]) -> list[str]:
    """What is wrong with the marts of a run, in words; an empty list for a sound build.

    ``pinned`` is the result of the pin task. A mart without a row carries no provenance and
    passes: an empty silver table gives an empty mart.
    """
    problems: list[str] = []
    for mart, silver in SEC_MARTS.items():
        state = states[mart]
        expected = int(pinned["snapshot_ids"][silver])
        if state["rows"] and state["silver_snapshot_ids"] != [expected]:
            problems.append(
                f"{mart} was built from the snapshots {state['silver_snapshot_ids']} of "
                f"{silver}, but {expected} was pinned"
            )
        if state["rows"] and state["pipeline_versions"] != [pinned["pipeline_version"]]:
            problems.append(
                f"{mart} carries the pipeline versions {state['pipeline_versions']}, "
                f"but the run is {pinned['pipeline_version']}"
            )
    return problems


def record_marts_task(
    pinned: Mapping[str, Any], run_id: str | None = None, settings: Settings | None = None
) -> dict[str, Any]:
    """Task entry point: check the provenance of the five marts and record the row counts.

    The last task of marts_build. Raises MartBuildError when a mart does not come from the
    pinned tables of the run; the record of the run then says failed.
    """
    settings = settings or get_settings()
    with (
        connect_ledger(autocommit=True) as connection,
        recorded_run(
            connection, ENTRY_POINT, run_id=run_id, pipeline_version=settings.pipeline_version
        ) as run,
    ):
        client = connect(settings=settings)
        try:
            states = mart_states(client)
        finally:
            client.close()
        silver = {name: entry["rows"] for name, entry in pinned["tables"].items()}
        run.row_count = sum(state["rows"] for state in states.values())
        run.detail = {
            "marts": {mart: state["rows"] for mart, state in states.items()},
            "silver": silver,
            "snapshot_ids": dict(pinned["snapshot_ids"]),
        }
        problems = build_problems(states, pinned)
        if problems:
            raise MartBuildError("; ".join(problems))
    return {
        "run_id": run_id,
        "status": "success",
        "pipeline_version": pinned["pipeline_version"],
        "marts": states,
        "silver": silver,
        "created": sorted(name for name, entry in pinned["tables"].items() if entry["created"]),
    }
