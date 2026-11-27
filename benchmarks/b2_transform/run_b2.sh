#!/bin/bash
# Benchmark B2 on the working stack (week 2 plan, WP-10): rebuilds of one real day.
#
#   bash benchmarks/b2_transform/run_b2.sh 2026-11-04
#
# Needs the working stack (make up) with the three GDELT DAGs paused. Run from the repository
# root. The script stops and starts the working stack once, in step 1; the volumes and the lake
# stay. Each rebuild adds one partition version with an equal checksum (week 2 plan, 3.3).
# The results are written to benchmarks/b2_transform/results/.
set -euo pipefail

DAY="${1:?usage: run_b2.sh YYYY-MM-DD (a day of the working stack)}"
RESULTS=benchmarks/b2_transform/results
SCRATCH=data/lake/_test
SCHEDULER=(docker exec -i gbmi-airflow-scheduler-1 /opt/venvs/gbmi/bin/python -)
STATS=(uv run python benchmarks/p0_memory_probe/docker_stats.py)
mkdir -p "$RESULTS" "$SCRATCH"

unpaused() {
  docker exec gbmi-postgres-1 psql -U postgres -d airflow -Atc \
    "SELECT count(*) FROM dag WHERE dag_id LIKE 'gdelt%' AND NOT is_paused"
}
if [ "$(unpaused)" != "0" ]; then
  echo "a GDELT DAG is not paused: pause gdelt_ingest, gdelt_silver_hourly and gdelt_silver_daily first"
  exit 1
fi

echo "== 1. the first run of each feed after a start of the stack"
make down >/dev/null
make up >/dev/null
make -s toolbox CMD="python -m benchmarks.b2_transform.rebuild_day --day $DAY --runs 1 --label first-after-start"

echo "== 2. three more runs of each feed"
make -s toolbox CMD="python -m benchmarks.b2_transform.rebuild_day --day $DAY --runs 3 --label warm"

# Steps 3 and 4 run the build where a task process runs: inside the scheduler container, under
# the memory limit of the container. The folder benchmarks is not mounted there, so the script
# comes from standard input and the result goes through the scratch area of the lake.
echo "== 3. the GKG day inside the scheduler container, alone"
"${STATS[@]}" watch --label gkg-day-alone --interval 1 --out "$RESULTS/stats-$DAY-scheduler-alone.json" -- \
  bash -c '"$@" < benchmarks/b2_transform/rebuild_day.py' _ "${SCHEDULER[@]}" \
  --day "$DAY" --feed gkg --runs 3 --label scheduler-alone --out /lake/_test/b2-scheduler-alone.json
cp "$SCRATCH/b2-scheduler-alone.json" "$RESULTS/rebuild-$DAY-scheduler-alone.json"

echo "== 4. the same while ClickHouse aggregates the GKG tables and a download of the day runs"
rm -f "$SCRATCH/b2-stop" "$SCRATCH"/b2-download-*.json "$RESULTS"/download-load-"$DAY"-*.json
make -s toolbox CMD="python -m benchmarks.b2_transform.clickhouse_load --stop-file /lake/_test/b2-stop" \
  > "$RESULTS/clickhouse-load-$DAY.json" 2>/dev/null &
aggregation=$!
(
  round=0
  while [ ! -e "$SCRATCH/b2-stop" ]; do
    round=$((round + 1))
    make -s toolbox CMD="python -m benchmarks.b1_ingest.throughput --day $DAY --from-hour 0 --hours 24 --parallel 4 --runs 1 --out /lake/_test/b2-download-$round.json" \
      >/dev/null 2>&1 || true
  done
) &
download=$!
sleep 10
"${STATS[@]}" watch --label gkg-day-under-load --interval 1 --out "$RESULTS/stats-$DAY-scheduler-under-load.json" -- \
  bash -c '"$@" < benchmarks/b2_transform/rebuild_day.py' _ "${SCHEDULER[@]}" \
  --day "$DAY" --feed gkg --runs 3 --label scheduler-under-load --out /lake/_test/b2-scheduler-under-load.json
touch "$SCRATCH/b2-stop"
wait "$aggregation" "$download" || true
cp "$SCRATCH/b2-scheduler-under-load.json" "$RESULTS/rebuild-$DAY-scheduler-under-load.json"
for file in "$SCRATCH"/b2-download-*.json; do
  [ -e "$file" ] && cp "$file" "$RESULTS/download-load-$DAY-$(basename "$file" .json | sed 's/b2-download-//').json"
done
rm -f "$SCRATCH/b2-stop" "$SCRATCH"/b2-download-*.json "$SCRATCH"/b2-scheduler-*.json

echo "== done; the results are in $RESULTS"
ls -1 "$RESULTS"
