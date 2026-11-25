#!/bin/bash
# The GKG memory probe of week 1 (WP-10): one closed day of GKG built in the scheduler
# container, first alone and then while ClickHouse aggregates the same table.
#
#   bash benchmarks/p0_memory_probe/run_probe.sh 2026-11-24
#
# Needs the core stack up with limits in force (make up). Results are written as JSON to
# benchmarks/p0_memory_probe/results/. Run from the repository root.
set -euo pipefail

DAY="${1:?usage: run_probe.sh YYYY-MM-DD (a closed UTC day)}"
RESULTS=benchmarks/p0_memory_probe/results
COMPOSE=(docker compose --project-directory . --env-file .env -f compose/compose.yaml)
STATS=(uv run python benchmarks/p0_memory_probe/docker_stats.py)
TOOLBOX=("${COMPOSE[@]}" --profile tools run --rm -T toolbox)
BUILD=("${COMPOSE[@]}" exec -T -e "PROBE_DAY=$DAY" airflow-scheduler /opt/venvs/gbmi/bin/python -)
mkdir -p "$RESULTS" data/lake/_probe
rm -f data/lake/_probe/stop

echo "== 1. land the GKG day $DAY (96 slots, about 0.43 GB)"
"${TOOLBOX[@]}" python benchmarks/p0_memory_probe/land_gkg_day.py --day "$DAY" | tee "$RESULTS/gkg-land.json"

echo "== 2. restart the scheduler and ClickHouse, so that their cgroup peaks belong to the probe"
"${COMPOSE[@]}" restart airflow-scheduler clickhouse >/dev/null
"${COMPOSE[@]}" up --detach --wait >/dev/null
sleep 20

echo "== 3. build the day alone"
"${STATS[@]}" watch --label gkg-build-alone --interval 2 --out "$RESULTS/gkg-build-alone-stats.json" -- \
  bash -c '"$@" < benchmarks/p0_memory_probe/gkg_day_probe.py > "$0"' "$RESULTS/gkg-build-alone.json" "${BUILD[@]}"
cat "$RESULTS/gkg-build-alone.json"

echo "== 4. build the day again while ClickHouse aggregates the first version"
"${TOOLBOX[@]}" python benchmarks/p0_memory_probe/clickhouse_aggregate.py --stop-file /lake/_probe/stop \
  > "$RESULTS/gkg-clickhouse-aggregate.json" &
aggregate=$!
sleep 5
"${STATS[@]}" watch --label gkg-build-with-aggregation --interval 2 --out "$RESULTS/gkg-build-with-aggregation-stats.json" -- \
  bash -c '"$@" < benchmarks/p0_memory_probe/gkg_day_probe.py > "$0"' "$RESULTS/gkg-build-with-aggregation.json" "${BUILD[@]}"
touch data/lake/_probe/stop
wait "$aggregate"
cat "$RESULTS/gkg-build-with-aggregation.json" "$RESULTS/gkg-clickhouse-aggregate.json"

echo "== 5. clean up the scratch table"
"${TOOLBOX[@]}" python - <<'PY'
from gbmi.config import get_settings
from gbmi.lake.catalog import load_lake_catalog

settings = get_settings()
load_lake_catalog(settings).purge_table((settings.silver_namespace, "probe_gkg_day"))
print("scratch table purged")
PY
rm -rf data/lake/_probe
echo "probe finished; the GKG files stay in bronze (data/lake/raw/gdelt/v2/gkg) until make reset"
