"""Load for benchmark B2: ClickHouse aggregates the GKG silver tables until a stop file appears.

The measurement "a GKG day build while ClickHouse aggregates" (week 2 plan, WP-10) needs
ClickHouse at work. The script pins the two GKG silver tables under scratch names
(``lake.b2_gkg_doc``, ``lake.b2_gkg_org``) at the current metadata files and runs three
aggregations in a loop: a count of documents, the organisation strings with the most
documents, and the themes with the most documents. The peak memory of the queries is read
from ``system.query_log`` afterwards, and the two scratch names are dropped.

The script of phase P0 (``benchmarks/p0_memory_probe/clickhouse_aggregate.py``) ran the same
three aggregations on a scratch table of one day, which the probe of phase P0 removed. The
script here reads the real silver tables of the stack and writes nothing to the lake.

Data: The GDELT Project, https://www.gdeltproject.org/

Run on the Compose network of the working stack:
    make toolbox CMD="python -m benchmarks.b2_transform.clickhouse_load \
        --stop-file /lake/_test/b2-stop"
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from gbmi.config import get_settings
from gbmi.lake.catalog import load_lake_catalog
from gbmi.lake.tables import GDELT_GKG_DOC, GDELT_GKG_ORG
from gbmi.warehouse.client import connect
from gbmi.warehouse.pin import pin_table

DOC = "b2_gkg_doc"
ORG = "b2_gkg_org"
MARK = "gbmi-b2-load"

QUERIES = {
    "count": f"SELECT count(), uniqExact(gkg_record_id) FROM lake.{DOC} /* {MARK} */",
    "organisations": f"""
        SELECT org_string_id, any(org_string_norm) AS organisation, count() AS documents
        FROM lake.{ORG}
        GROUP BY org_string_id ORDER BY documents DESC LIMIT 20 /* {MARK} */
    """,
    "themes": f"""
        SELECT theme, count() AS documents
        FROM (SELECT arrayJoin(themes) AS theme FROM lake.{DOC})
        GROUP BY theme ORDER BY documents DESC LIMIT 20 /* {MARK} */
    """,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument("--stop-file", type=Path, required=True)
    parser.add_argument("--max-seconds", type=float, default=900)
    arguments = parser.parse_args()

    settings = get_settings()
    client = connect(settings=settings)
    catalog = load_lake_catalog(settings)
    pinned = {}
    for scratch_name, table_name in ((DOC, GDELT_GKG_DOC), (ORG, GDELT_GKG_ORG)):
        table = catalog.load_table((settings.silver_namespace, table_name))
        pinned[scratch_name] = pin_table(client, table, name=scratch_name).snapshot_id

    started = time.time()
    rounds = 0
    slowest: dict[str, float] = dict.fromkeys(QUERIES, 0.0)
    first: dict[str, list[list[object]]] = {}
    try:
        while not arguments.stop_file.exists() and time.time() - started < arguments.max_seconds:
            for name, query in QUERIES.items():
                tick = time.perf_counter()
                rows = client.query(query).result_rows
                slowest[name] = max(slowest[name], time.perf_counter() - tick)
                first.setdefault(name, [list(row) for row in rows[:3]])
            rounds += 1
        host, port = os.environ["DBT_CH_HOST"], os.environ.get("DBT_CH_PORT", "8123")
        admin = connect(f"http://default:{os.environ['CLICKHOUSE_ADMIN_PASSWORD']}@{host}:{port}")
        admin.command("SYSTEM FLUSH LOGS")
        # No bound parameters here: the LIKE patterns contain per cent signs.
        peak, read_rows, finished = admin.query(
            "SELECT max(memory_usage), sum(read_rows), count() FROM system.query_log "
            f"WHERE type = 'QueryFinish' AND query LIKE '%{MARK}%' "
            f"AND query NOT LIKE '%query_log%' AND event_time >= toDateTime({int(started)})"
        ).result_rows[0]
    finally:
        for scratch_name in (DOC, ORG):
            client.command(f"DROP TABLE IF EXISTS lake.{scratch_name}")
    print(
        json.dumps(
            {
                "pinned_snapshots": pinned,
                "rounds": rounds,
                "queries_finished": int(finished),
                "seconds": round(time.time() - started, 1),
                "slowest_query_seconds": {name: round(value, 2) for name, value in slowest.items()},
                "peak_query_memory_mib": round(int(peak) / 2**20, 1),
                "rows_read": int(read_rows),
                "sample": first,
            },
            indent=2,
            default=str,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
