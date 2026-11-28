"""The evidence of the first mart build on the live data of the laptop (week 3 plan, WP-08 step 10).

Runs the DAG marts_build once through the API of Airflow and reads the five SEC marts of the
stack. The script writes the numbers which the status report quotes: the rows of each mart, the
ranges of former names by status, the sources of the knowledge time, and the coverage of each
metric. The script sends no request to the SEC: the DAG reads the silver tables of the lake.

The DAG is paused on the working stack. The script unpauses the DAG for the one run and pauses
the DAG again, so the schedule of 11:45 UTC starts no second run.

Usage, in the toolbox of the working stack, after the live load of WP-06:

    make toolbox CMD="python -m benchmarks.p2_sec.marts_evidence"

With --last-run the script starts no run and reads the newest run of the DAG.
"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from clickhouse_connect.driver.client import Client

from gbmi.config import get_settings
from gbmi.warehouse.client import connect
from tests.e2e.airflow import AirflowClient, DagRun

REPO = Path(__file__).resolve().parents[2]
RESULT = REPO / "benchmarks" / "p2_sec" / "results" / "marts-build.json"
DAG = "marts_build"
MARTS = (
    "dim_company",
    "fct_filing",
    "fct_filing_item",
    "fct_financial_fact",
    "fct_financial_metric",
)
META = 1326801


def plain(value: Any) -> Any:
    """A value of ClickHouse as a value of JSON."""
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, list | tuple):
        return [plain(item) for item in value]
    return value


def table(client: Client, statement: str) -> list[dict[str, Any]]:
    result = client.query(statement)
    return [
        {name: plain(value) for name, value in zip(result.column_names, row, strict=True)}
        for row in result.result_rows
    ]


def last_run(airflow: AirflowClient) -> DagRun:
    """The newest run of the DAG, as the client gives a run which it started itself."""
    (newest,) = airflow._get(f"/dags/{DAG}/dagRuns", order_by="-run_after", limit=1)["dag_runs"]
    started = datetime.fromisoformat(newest["start_date"])
    ended = datetime.fromisoformat(newest["end_date"])
    return DagRun(
        DAG,
        newest["dag_run_id"],
        newest["state"],
        (ended - started).total_seconds(),
        airflow.task_states(DAG, newest["dag_run_id"]),
    )


def run_the_dag(*, start: bool) -> dict[str, Any]:
    """One run of marts_build; the DAG is paused again afterwards when it was paused before."""
    airflow = AirflowClient()
    airflow.wait_until_listed(DAG)
    was_paused = bool(airflow.dag(DAG)["is_paused"])
    try:
        run = airflow.run(DAG, {}, timeout=3600) if start else last_run(airflow)
        instances = airflow._get(f"/dags/{DAG}/dagRuns/{run.run_id}/taskInstances", limit=200)[
            "task_instances"
        ]
        record = airflow.xcom(DAG, run.run_id, "record")
        pinned = airflow.xcom(DAG, run.run_id, "pin_lake")
    finally:
        if was_paused:
            airflow._http.patch(f"/api/v2/dags/{DAG}", json={"is_paused": True}).raise_for_status()
    durations = sorted(
        ((instance["duration"] or 0.0, instance["task_id"]) for instance in instances),
        reverse=True,
    )
    return {
        "run_id": run.run_id,
        "state": run.state,
        "seconds": round(run.seconds, 1),
        "tasks": len(instances),
        "task_states": sorted(set(run.task_states.values())),
        "task_seconds_total": round(sum(seconds for seconds, _ in durations), 1),
        "slowest_tasks": [
            {"task": task, "seconds": round(seconds, 1)} for seconds, task in durations[:5]
        ],
        "paused_before": was_paused,
        "paused_after": bool(airflow.dag(DAG)["is_paused"]),
        "pinned": pinned,
        "record": record,
    }


def marts_evidence(client: Client) -> dict[str, Any]:
    return {
        "rows": {
            mart: table(client, f"SELECT count() AS n FROM marts.{mart}")[0]["n"] for mart in MARTS
        },
        "dim_company": {
            "by_source": table(
                client,
                "SELECT history_source, count() AS rows, uniqExact(cik) AS companies, "
                "toString(min(valid_from)) AS first_valid_from FROM marts.dim_company "
                "GROUP BY history_source ORDER BY history_source",
            ),
            "totals": table(
                client,
                "SELECT uniqExact(cik) AS companies, countIf(is_current) AS current_rows, "
                "uniqExactIf(cik, is_watchlist) AS watchlist_companies, "
                "uniqExactIf(cik, history_source = 'former_name') AS companies_with_name_history, "
                "countIf(history_source = 'snapshot' AND sic IS NULL) AS current_rows_without_sic "
                "FROM marts.dim_company",
            )[0],
            "meta": table(
                client,
                "SELECT name, history_source, toString(valid_from) AS valid_from, "
                "toString(valid_to) AS valid_to, is_current, is_watchlist FROM marts.dim_company "
                f"WHERE cik = {META} ORDER BY valid_from",
            ),
        },
        "former_name_ranges": table(
            client,
            "SELECT name_kind, range_status, count() AS ranges, uniqExact(cik) AS companies "
            "FROM intermediate.int_company__former_name_versions "
            "GROUP BY name_kind, range_status ORDER BY name_kind, range_status",
        ),
        "fct_filing": {
            "by_detail_source": table(
                client,
                "SELECT detail_source, count() AS rows, uniqExact(cik) AS filers, "
                "toString(min(filing_date)) AS first_filing_date, "
                "toString(max(filing_date)) AS last_filing_date FROM marts.fct_filing "
                "GROUP BY detail_source ORDER BY detail_source",
            ),
            "totals": table(
                client,
                "SELECT count() AS rows, uniqExact(accession_number, cik) AS pairs, "
                "uniqExact(accession_number) AS filings, uniqExact(cik) AS filers, "
                "uniqExact(index_date) AS index_dates, uniqExact(form) AS forms "
                "FROM marts.fct_filing",
            )[0],
            "top_forms": table(
                client,
                "SELECT form, count() AS rows FROM marts.fct_filing GROUP BY form "
                "ORDER BY rows DESC, form LIMIT 10",
            ),
        },
        "fct_filing_item": {
            "totals": table(
                client,
                "SELECT count() AS rows, uniqExact(accession_number, cik) AS filings, "
                "uniqExact(cik) AS companies, uniqExact(item_code) AS item_codes, "
                "uniqExact(form) AS forms FROM marts.fct_filing_item",
            )[0],
            "top_items": table(
                client,
                "SELECT item_code, count() AS rows FROM marts.fct_filing_item GROUP BY item_code "
                "ORDER BY rows DESC, item_code LIMIT 10",
            ),
            "forms": table(
                client,
                "SELECT form, count() AS rows FROM marts.fct_filing_item GROUP BY form "
                "ORDER BY rows DESC, form",
            ),
            # Filings outside the 8-K family which list items: the model leaves them out.
            "other_forms_with_items": table(
                client,
                "SELECT form, count() AS filings FROM marts.fct_filing "
                "WHERE length(items) > 0 AND NOT startsWith(form, '8-K') GROUP BY form "
                "ORDER BY filings DESC, form LIMIT 10",
            ),
        },
        "fct_financial_fact": {
            "by_known_at_source": table(
                client,
                "SELECT known_at_source, count() AS rows, uniqExact(cik) AS companies, "
                "uniqExact(cik, accession_number) AS filings, "
                "toString(min(known_at)) AS first_known_at, "
                "toString(max(known_at)) AS last_known_at FROM marts.fct_financial_fact "
                "GROUP BY known_at_source ORDER BY known_at_source",
            ),
            "totals": table(
                client,
                "SELECT count() AS rows, uniqExact(cik) AS companies, "
                "uniqExact(taxonomy, concept) AS concepts, uniqExact(unit) AS units, "
                "countIf(is_instant) AS instant_rows FROM marts.fct_financial_fact",
            )[0],
        },
        "metric_facts_by_kind": table(
            client,
            "SELECT flow_or_stock, period_kind, count() AS rows, uniqExact(cik) AS companies, "
            "countIf(period_days = 1) AS one_day_rows "
            "FROM intermediate.int_financials__metric_facts "
            "GROUP BY flow_or_stock, period_kind ORDER BY flow_or_stock, period_kind",
        ),
        "fct_financial_metric": {
            "coverage": table(
                client,
                "SELECT metric, uniqExact(cik) AS companies, count() AS rows, "
                "countIf(is_derived) AS derived_quarters, countIf(is_restated) AS restated_rows, "
                "countIf(change_year_on_year IS NOT NULL) AS rows_with_change, "
                "uniqExact(unit) AS units, uniqExact(taxonomy, concept) AS concepts "
                "FROM marts.fct_financial_metric GROUP BY metric ORDER BY metric",
            ),
            "by_kind": table(
                client,
                "SELECT period_kind, count() AS rows, uniqExact(cik) AS companies "
                "FROM marts.fct_financial_metric GROUP BY period_kind ORDER BY period_kind",
            ),
            "units": table(
                client,
                "SELECT unit, count() AS rows, uniqExact(cik) AS companies "
                "FROM marts.fct_financial_metric GROUP BY unit ORDER BY rows DESC, unit LIMIT 10",
            ),
            "companies_with_two_units": table(
                client,
                "SELECT count() AS companies FROM (SELECT cik FROM marts.fct_financial_metric "
                "WHERE metric != 'employees' GROUP BY cik HAVING uniqExact(unit) > 1)",
            )[0]["companies"],
        },
    }


def main() -> int:
    settings = get_settings()
    result: dict[str, Any] = {
        "what": "One run of marts_build on the live SEC data of the working stack of the laptop.",
        "pipeline_version": settings.pipeline_version,
        "watchlist_size": settings.watchlist_size,
        "run": run_the_dag(start="--last-run" not in sys.argv[1:]),
    }
    client = connect(settings=settings)
    try:
        result.update(marts_evidence(client))
    finally:
        client.close()
    RESULT.parent.mkdir(parents=True, exist_ok=True)
    RESULT.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"written": str(RESULT.relative_to(REPO)), "rows": result["rows"]}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
