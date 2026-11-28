"""The evidence of the live load of WP-06 (week 3 plan, WP-06 step 9).

Reads the request log, the run records, the object ledger, the partition versions and the five
SEC tables of the stack, and writes the numbers which the status report quotes. Sends no
request.

Usage, in the toolbox of the working stack, after the live load:

    make toolbox CMD="python -m benchmarks.p2_sec.live_load_evidence"
"""

from __future__ import annotations

import csv
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import pyarrow.compute as pc

from gbmi.config import get_settings
from gbmi.lake.catalog import load_lake_catalog
from gbmi.lake.tables import SEC_SCHEMAS
from gbmi.ledger.db import connect
from gbmi.ops.requests import max_requests_in_session_window, max_requests_in_window, request_counts

REPO = Path(__file__).resolve().parents[2]
RESULT = REPO / "benchmarks" / "p2_sec" / "results" / "live-load.json"
SEED = REPO / "dbt" / "seeds" / "metric_concept_map.csv"
SOURCE = "sec"
ARCHIVE_PATH = "/Archives/edgar/daily-index/bulkdata/submissions.zip"


def rows(connection: Any, statement: str, *arguments: Any) -> list[dict[str, Any]]:
    cursor = connection.execute(statement, arguments)
    names = [column.name for column in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def ledger_evidence() -> dict[str, Any]:
    with connect(autocommit=True) as connection:
        archive = rows(
            connection,
            "SELECT granted_at, duration_ms, byte_count, http_status, outcome "
            "FROM ops.source_request WHERE source = %s AND path = %s ORDER BY request_id",
            SOURCE,
            ARCHIVE_PATH,
        )
        return {
            "requests": {
                "total": sum(request_counts(connection, source=SOURCE).values()),
                "by_outcome": request_counts(connection, source=SOURCE),
                "max_in_one_second": max_requests_in_window(connection, source=SOURCE),
                "max_in_one_second_of_one_client": max_requests_in_session_window(
                    connection, source=SOURCE
                ),
                "by_run": rows(
                    connection,
                    "SELECT coalesce(run_id, '(none)') AS run_id, count(*) AS requests, "
                    "sum(byte_count) AS bytes, min(granted_at) AS first_request, "
                    "max(granted_at) AS last_request, sum(waited_ms) AS waited_ms "
                    "FROM ops.source_request WHERE source = %s GROUP BY 1 ORDER BY min(request_id)",
                    SOURCE,
                ),
                "archive": archive,
            },
            "circuit": rows(
                connection,
                "SELECT open_count, locked, blocked_until FROM ops.source_circuit "
                "WHERE source = %s",
                SOURCE,
            ),
            "runs": rows(
                connection,
                "SELECT entry_point, run_id, status, started_at, duration_ms, row_count, "
                "byte_count FROM ops.pipeline_run WHERE entry_point LIKE 'sec.%%' "
                "ORDER BY pipeline_run_id",
            ),
            "objects": rows(
                connection,
                "SELECT dataset, state, count(*) AS objects, min(object_date) AS first_date, "
                "max(object_date) AS last_date, sum(byte_count) AS bytes FROM ops.source_object "
                "WHERE source = %s GROUP BY 1, 2 ORDER BY 1, 2",
                SOURCE,
            ),
            "versions": rows(
                connection,
                "SELECT table_name, unit_kind, coalesce(unit_label, slot_date::text) AS unit, "
                "committed_rows, duplicate_rows, quarantined_rows, file_count, duration_ms, run_id "
                "FROM ops.silver_partition WHERE source = %s ORDER BY partition_version_id",
                SOURCE,
            ),
            "failed_checks": rows(
                connection,
                "SELECT gate, check_name, unit, status FROM ops.dq_result WHERE source = %s "
                "AND status IN ('failed', 'accepted') ORDER BY created_at",
                SOURCE,
            ),
        }


def lake_evidence() -> dict[str, Any]:
    settings = get_settings()
    catalog = load_lake_catalog(settings)

    def table(name: str) -> Any:
        return catalog.load_table((settings.silver_namespace, name))

    sizes = {}
    for name in SEC_SCHEMAS:
        files = [task.file for task in table(name).scan().plan_files()]
        sizes[name] = {
            "rows": sum(file.record_count for file in files),
            "data_files": len(files),
            "bytes": sum(file.file_size_in_bytes for file in files),
        }
    index = (
        table("sec_filing_index")
        .scan(selected_fields=("index_date", "accession_number", "cik", "filing_date", "form"))
        .to_arrow()
    )
    pairs = index.group_by(["accession_number", "cik"]).aggregate(
        [("index_date", "count_distinct")]
    )
    in_two = int(pc.sum(pc.greater(pairs["index_date_count_distinct"], 1)).as_py() or 0)
    earlier = int(pc.sum(pc.less(index["filing_date"], index["index_date"])).as_py() or 0)
    snapshot = table("sec_company_snapshot").scan().to_arrow()
    facts = table("sec_xbrl_fact").scan(selected_fields=("cik", "taxonomy", "concept")).to_arrow()
    concepts = facts.group_by(["cik", "taxonomy", "concept"]).aggregate([]).to_pylist()
    have: dict[tuple[str, str], set[int]] = {}
    for row in concepts:
        have.setdefault((row["taxonomy"], row["concept"]), set()).add(row["cik"])
    coverage: dict[str, set[int]] = {}
    with SEED.open(encoding="utf-8", newline="") as stream:
        for row in csv.DictReader(stream):
            coverage.setdefault(row["metric"], set()).update(
                have.get((row["taxonomy"], row["concept"]), set())
            )
    companies = len(set(facts["cik"].to_pylist()))
    detail = table("sec_filing_detail").scan(selected_fields=("cik", "form")).to_arrow()
    return {
        "tables": sizes,
        "index": {
            "files": len(set(index["index_date"].to_pylist())),
            "rows": index.num_rows,
            "pairs_in_two_index_files": in_two,
            "rows_with_an_earlier_date": earlier,
            "forms": len(set(index["form"].to_pylist())),
        },
        "snapshot": {
            "dates": sorted({day.isoformat() for day in snapshot["snapshot_date"].to_pylist()}),
            "companies": snapshot.num_rows,
            "with_a_sic_code": snapshot.num_rows - snapshot["sic"].null_count,
            "with_a_fiscal_year_end": snapshot.num_rows - snapshot["fiscal_year_end"].null_count,
            "with_a_state": snapshot.num_rows - snapshot["state_of_incorporation"].null_count,
            "without_an_exchange": snapshot["primary_exchange"].null_count,
            "stale": int(pc.sum(snapshot["is_stale"]).as_py() or 0),
            "inactive": int(pc.sum(snapshot["is_inactive"]).as_py() or 0),
            "distinct_attr_hash": len(set(snapshot["attr_hash"].to_pylist())),
        },
        "detail": {
            "companies": len(set(detail["cik"].to_pylist())),
            "rows": detail.num_rows,
            "largest_forms": Counter(detail["form"].to_pylist()).most_common(8),
        },
        "facts": {
            "companies": companies,
            "rows": facts.num_rows,
            "taxonomies": dict(Counter(row["taxonomy"] for row in concepts).most_common()),
            "companies_with_each_metric": {
                metric: len(ciks) for metric, ciks in sorted(coverage.items())
            },
        },
    }


def main() -> int:
    result = {"ledger": ledger_evidence(), "lake": lake_evidence()}
    RESULT.parent.mkdir(parents=True, exist_ok=True)
    RESULT.write_text(json.dumps(result, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
