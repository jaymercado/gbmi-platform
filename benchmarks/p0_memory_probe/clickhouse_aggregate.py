"""Aggregate the probe table in ClickHouse while a silver build runs (week 1 plan, WP-10).

The table is pinned as ``lake.probe_gkg_day`` and three aggregations are run in a loop until
the stop file appears: a count, the most frequent organisations (the shape of the future
organisation-mention mart) and the most frequent themes. Peak memory per query is read from
``system.query_log`` afterwards.

Run on the Compose network:
    make toolbox CMD="python benchmarks/p0_memory_probe/clickhouse_aggregate.py \\
        --stop-file /lake/_probe/stop"
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from gbmi.config import get_settings
from gbmi.lake.catalog import load_lake_catalog
from gbmi.warehouse.client import connect
from gbmi.warehouse.pin import pin_table

TABLE = "probe_gkg_day"

QUERIES = {
    "count": f"SELECT count(), uniqExact(gkg_record_id) FROM lake.{TABLE} /* gbmi-probe */",
    "organisations": f"""
        SELECT organisation, count() AS documents
        FROM (
            SELECT arrayJoin(arrayDistinct(arrayMap(
                entry -> splitByChar(',', entry)[1],
                splitByChar(';', ifNull(v2_organizations, ''))
            ))) AS organisation
            FROM lake.{TABLE}
        )
        WHERE organisation != ''
        GROUP BY organisation ORDER BY documents DESC LIMIT 20 /* gbmi-probe */
    """,
    "themes": f"""
        SELECT theme, count() AS documents
        FROM (
            SELECT arrayJoin(arrayDistinct(splitByChar(';', ifNull(themes, '')))) AS theme
            FROM lake.{TABLE}
        )
        WHERE theme != ''
        GROUP BY theme ORDER BY documents DESC LIMIT 20 /* gbmi-probe */
    """,
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--stop-file", type=Path, required=True)
    parser.add_argument("--max-seconds", type=float, default=900)
    args = parser.parse_args()

    settings = get_settings()
    client = connect(settings=settings)
    table = load_lake_catalog(settings).load_table((settings.silver_namespace, TABLE))
    pin_table(client, table, name=TABLE)

    started = time.time()
    rounds = 0
    slowest: dict[str, float] = dict.fromkeys(QUERIES, 0.0)
    first: dict[str, object] = {}
    while not args.stop_file.exists() and time.time() - started < args.max_seconds:
        for name, query in QUERIES.items():
            tick = time.perf_counter()
            rows = client.query(query).result_rows
            slowest[name] = max(slowest[name], time.perf_counter() - tick)
            first.setdefault(name, rows[:3])
        rounds += 1

    host, port = os.environ["DBT_CH_HOST"], os.environ.get("DBT_CH_PORT", "8123")
    admin = connect(f"http://default:{os.environ['CLICKHOUSE_ADMIN_PASSWORD']}@{host}:{port}")
    admin.command("SYSTEM FLUSH LOGS")
    # No bound parameters here: the LIKE patterns contain per cent signs.
    peak, total_rows, finished = admin.query(
        "SELECT max(memory_usage), sum(read_rows), count() FROM system.query_log "
        "WHERE type = 'QueryFinish' AND query LIKE '%gbmi-probe%' "
        f"AND query NOT LIKE '%query_log%' AND event_time >= toDateTime({int(started)})"
    ).result_rows[0]
    client.command(f"DROP TABLE IF EXISTS lake.{TABLE}")
    print(
        json.dumps(
            {
                "rounds": rounds,
                "queries_finished": int(finished),
                "seconds": round(time.time() - started, 1),
                "slowest_query_seconds": {name: round(value, 2) for name, value in slowest.items()},
                "peak_query_memory_mib": round(int(peak) / 1024**2, 1),
                "rows_read": int(total_rows),
                "sample": {name: [list(row) for row in rows] for name, rows in first.items()},  # type: ignore[union-attr]
            },
            indent=2,
            default=str,
        )
    )


if __name__ == "__main__":
    main()
