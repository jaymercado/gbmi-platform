#!/bin/bash
# Smoke checks for the running stack (week 1 plan, WP-05 step 6). Run with: make smoke
#
# Every check prints PASS or FAIL; the script exits non-zero if any check failed.
#
# Two modes. The default checks a stack of the laptop or of CI. GBMI_SMOKE_MODE=host checks
# the stack of the AWS host after a deployment (compose/host/deploy.sh; week 3 plan, WP-10
# step 5): both Compose files, the environment file of the host, and in place of the check of
# the lake folder the checks of S3, Glue and the lake role, with one live hour of the tracer DAG
# at the end.
set -uo pipefail

MODE="${GBMI_SMOKE_MODE:-laptop}"
if [ "$MODE" = "host" ]; then
  STACK="gbmi"
  ENV_FILE="${GBMI_ENV_FILE:-/etc/gbmi/env}"
  COMPOSE=(docker compose --project-name "$STACK" --project-directory . --env-file "$ENV_FILE" -f compose/compose.yaml -f compose/compose.prod.yaml)
else
  # The Makefile exports STACK, the Compose project of the stack to check (week 2 plan 3.3).
  # Run by hand, the script checks the working stack.
  STACK="${STACK:-gbmi}"
  ENV_FILE=".env"
  COMPOSE=(docker compose --project-name "$STACK" --project-directory . --env-file "$ENV_FILE" -f compose/compose.yaml)
fi
echo "smoke: stack $STACK, mode $MODE"
failures=0

check() {
  local name="$1"
  shift
  local output
  if output="$("$@" 2>&1)"; then
    printf 'PASS  %s\n' "$name"
  else
    printf 'FAIL  %s\n' "$name"
    printf '%s\n' "$output" | sed 's/^/      /' | tail -15
    failures=$((failures + 1))
  fi
}

# Passwords come from the env file, the same file Compose reads. The file is parsed, not
# sourced: Compose accepts values that a shell would not. The file of the host holds each value
# in single quotes (compose/host/render_env.py).
env_value() {
  local value
  value="$(grep -E "^$1=" "$ENV_FILE" | head -1 | cut -d= -f2-)"
  case "$value" in
    \'*\') value="${value#\'}"; value="${value%\'}" ;;
  esac
  printf '%s' "$value"
}
CLICKHOUSE_ADMIN_PASSWORD="$(env_value CLICKHOUSE_ADMIN_PASSWORD)"
CH_DBT_PASSWORD="$(env_value CH_DBT_PASSWORD)"
CH_BI_PASSWORD="$(env_value CH_BI_PASSWORD)"
CH_GBMI_PASSWORD="$(env_value CH_GBMI_PASSWORD)"
GBMI_DB_PASSWORD="$(env_value GBMI_DB_PASSWORD)"

ch() {
  "${COMPOSE[@]}" exec -T clickhouse clickhouse-client \
    --user default --password "$CLICKHOUSE_ADMIN_PASSWORD" --query "$1"
}

services_healthy() {
  local status service bad=0
  for service in postgres clickhouse airflow-apiserver airflow-scheduler airflow-dag-processor airflow-triggerer; do
    status="$("${COMPOSE[@]}" ps --format '{{.Health}}' "$service" 2>/dev/null)"
    if [ "$status" != "healthy" ]; then
      echo "$service: ${status:-not running}"
      bad=1
    fi
  done
  return "$bad"
}

one_shots_succeeded() {
  local service code bad=0
  for service in clickhouse-init ops-migrate postgres-roles airflow-init dbt-manifest; do
    code="$("${COMPOSE[@]}" ps --all --format '{{.ExitCode}}' "$service" 2>/dev/null)"
    if [ "$code" != "0" ]; then
      echo "$service: exit code ${code:-unknown}"
      bad=1
    fi
  done
  return "$bad"
}

no_restarts() {
  local bad=0 id name count
  for id in $("${COMPOSE[@]}" ps --quiet); do
    name="$(docker inspect --format '{{.Name}}' "$id")"
    count="$(docker inspect --format '{{.RestartCount}}' "$id")"
    if [ "$count" != "0" ]; then
      echo "$name restarted $count times"
      bad=1
    fi
  done
  return "$bad"
}

no_anonymous_volumes() {
  # An image that declares a volume gets an anonymous one unless the service mounts something
  # there, and `make down` would leave it behind.
  local id name bad=0
  for id in $("${COMPOSE[@]}" ps --all --quiet); do
    for name in $(docker inspect --format '{{range .Mounts}}{{if eq .Type "volume"}}{{.Name}} {{end}}{{end}}' "$id"); do
      if printf '%s' "$name" | grep -qE '^[0-9a-f]{64}$'; then
        echo "$(docker inspect --format '{{.Name}}' "$id") uses the anonymous volume ${name:0:12}"
        bad=1
      fi
    done
  done
  return "$bad"
}

no_import_errors() {
  local output
  output="$("${COMPOSE[@]}" exec -T airflow-scheduler airflow dags list-import-errors --output json 2>/dev/null)"
  [ "$(printf '%s' "$output" | tr -d '[:space:]')" = "[]" ] || { echo "$output"; return 1; }
}

pools_exist() {
  local output pool bad=0
  output="$("${COMPOSE[@]}" exec -T airflow-scheduler airflow pools list --output json 2>/dev/null)"
  for pool in sec_api silver_writer heavy; do
    if ! printf '%s' "$output" | tr -d ' \n' | grep -q "\"pool\":\"$pool\",\"slots\":\"\?1\"\?"; then
      echo "pool $pool with one slot not found in: $output"
      bad=1
    fi
  done
  return "$bad"
}

clickhouse_version() {
  local version
  version="$(ch 'SELECT version()')" || return 1
  case "$version" in
    26.8.*) echo "$version" ;;
    *) echo "expected 26.8.x, got $version"; return 1 ;;
  esac
}

clickhouse_objects() {
  local databases roles name bad=0
  databases="$(ch 'SHOW DATABASES')" || return 1
  roles="$(ch 'SHOW ROLES')" || return 1
  for name in lake staging intermediate marts ops seeds; do
    printf '%s\n' "$databases" | grep -qx "$name" || { echo "database $name missing"; bad=1; }
  done
  for name in dbt_rw bi_ro pipeline_rw; do
    printf '%s\n' "$roles" | grep -qx "$name" || { echo "role $name missing"; bad=1; }
  done
  return "$bad"
}

clickhouse_users_can_log_in() {
  "${COMPOSE[@]}" exec -T clickhouse clickhouse-client --user dbt --password "$CH_DBT_PASSWORD" --query 'SELECT 1' >/dev/null &&
    "${COMPOSE[@]}" exec -T clickhouse clickhouse-client --user bi --password "$CH_BI_PASSWORD" --query 'SELECT 1' >/dev/null &&
    "${COMPOSE[@]}" exec -T clickhouse clickhouse-client --user gbmi --password "$CH_GBMI_PASSWORD" --query 'SELECT 1' >/dev/null
}

postgres_databases() {
  local output name bad=0
  output="$("${COMPOSE[@]}" exec -T postgres psql -U postgres -At -c 'SELECT datname FROM pg_database')" || return 1
  for name in airflow gbmi metabase; do
    printf '%s\n' "$output" | grep -qx "$name" || { echo "database $name missing"; bad=1; }
  done
  return "$bad"
}

migrations_applied() {
  # The newest migration file of the repository is applied in the database gbmi (week 2 plan,
  # WP-02): the service ops-migrate ran on this start, with the files of this checkout.
  local newest applied
  newest="$(find db/postgres/migrations -name '[0-9][0-9][0-9][0-9]_*.sql' -exec basename {} \; | sort | tail -1 | cut -c1-4)"
  [ -n "$newest" ] || { echo "no migration file under db/postgres/migrations"; return 1; }
  applied="$("${COMPOSE[@]}" exec -T postgres psql -U postgres -d gbmi -At -c 'SELECT max(version) FROM ops.schema_migration')" || { echo "$applied"; return 1; }
  [ "$((10#$newest))" = "$applied" ] || { echo "the newest file is $newest, the database holds ${applied:-nothing}"; return 1; }
}

clickhouse_reads_postgres() {
  # dbt will read the operational tables through postgresql() (blueprint 7.1). The gbmi
  # database has no tables before phase P1, so this reads a system view over the same path:
  # the network and the gbmi role's password. The password travels as a query parameter, so
  # that a failure cannot print it with the query text.
  local tables
  tables="$("${COMPOSE[@]}" exec -T clickhouse clickhouse-client \
    --user default --password "$CLICKHOUSE_ADMIN_PASSWORD" --param_pg_password "$GBMI_DB_PASSWORD" \
    --query "SELECT count() FROM postgresql('postgres:5432', 'gbmi', 'pg_tables', 'gbmi', {pg_password:String}, 'pg_catalog')")" || {
    echo "$tables"
    return 1
  }
  [ "$tables" -gt 0 ] 2>/dev/null || { echo "expected a count of PostgreSQL system tables, got '$tables'"; return 1; }
}

dbt_reads_the_ledger() {
  # The dbt user reads the schema ops through the named collection pg_ops, which holds the
  # connection of the read-only role gbmi_ro: no query and no dbt model carries a password
  # (week 2 plan, WP-08).
  local files
  files="$("${COMPOSE[@]}" exec -T clickhouse clickhouse-client \
    --user dbt --password "$CH_DBT_PASSWORD" \
    --query "SELECT count() FROM postgresql(pg_ops, schema = 'ops', table = 'schema_migration')")" || {
    echo "$files"
    return 1
  }
  [ "$files" -gt 0 ] 2>/dev/null || { echo "expected the number of applied migration files, got '$files'"; return 1; }
}

lake_is_shared() {
  # Airflow writes, ClickHouse reads the same path, and ClickHouse cannot write.
  local token="smoke-$$-$RANDOM"
  "${COMPOSE[@]}" exec -T airflow-scheduler bash -c "mkdir -p /lake/.smoke && echo $token > /lake/.smoke/probe" || return 1
  local seen
  seen="$("${COMPOSE[@]}" exec -T --user clickhouse clickhouse cat /lake/.smoke/probe)" || return 1
  [ "$seen" = "$token" ] || { echo "ClickHouse read '$seen', expected '$token'"; return 1; }
  if "${COMPOSE[@]}" exec -T --user clickhouse clickhouse touch /lake/.smoke/from-clickhouse 2>/dev/null; then
    echo "ClickHouse was able to write to /lake; the mount must be read-only"
    return 1
  fi
  "${COMPOSE[@]}" exec -T airflow-scheduler rm -rf /lake/.smoke
}

environments_are_isolated() {
  # Airflow must not see gbmi or dbt, and each environment must have its own interpreter.
  "${COMPOSE[@]}" exec -T airflow-scheduler bash -c '
    set -e
    ! python -c "import gbmi" 2>/dev/null
    ! python -c "import dbt" 2>/dev/null
    python -c "import cosmos"
    /opt/venvs/gbmi/bin/python -c "import gbmi, duckdb, pyiceberg"
    ! /opt/venvs/gbmi/bin/python -c "import airflow" 2>/dev/null
    /opt/venvs/dbt/bin/dbt --version >/dev/null
  '
}

# --- the checks of the AWS host (week 3 plan, WP-10 step 5) -----------------------------------

task_retries_are_three() {
  local retries
  retries="$("${COMPOSE[@]}" exec -T airflow-scheduler printenv GBMI_TASK_RETRIES)" || return 1
  [ "$retries" = "3" ] || { echo "GBMI_TASK_RETRIES is '$retries', expected 3"; return 1; }
}

pipeline_version_is_the_release() {
  # The image carries the commit: rows, run records and log lines name the deployed commit
  # (week 3 plan, exit-gate row 13).
  local version
  version="$("${COMPOSE[@]}" exec -T airflow-scheduler printenv GBMI_PIPELINE_VERSION)" || return 1
  [ "$version" = "${GBMI_RELEASE:-}" ] || { echo "the scheduler runs '$version', the release is '${GBMI_RELEASE:-}'"; return 1; }
}

lake_backend_answers() {
  # Glue answers; one object is written, read and deleted below tmp/smoke/ of the ops bucket;
  # ClickHouse reads one pinned table through the lake role; the pipeline version is the commit.
  "${COMPOSE[@]}" --profile tools run --rm -T toolbox gbmi lake check \
    --scratch "s3://$(env_value GBMI_OPS_BUCKET)/tmp/smoke" --release "${GBMI_RELEASE:-}"
}

clickhouse_write_is_refused() {
  # $1: the arguments after the address in s3(...). ClickHouse must not be able to write to
  # the lake: without credentials ClickHouse itself refuses (a query never uses the role of
  # the host), and with the lake role S3 refuses (the role can only read).
  local output
  if output="$(ch "INSERT INTO FUNCTION s3('s3://$(env_value GBMI_LAKE_BUCKET)/warehouse/_smoke/refused.csv', 'CSV', 'x UInt8'$1) VALUES (1)" 2>&1)"; then
    echo "ClickHouse wrote an object to the lake"
    return 1
  fi
  printf '%s' "$output" | grep -qiE "ACCESS_DENIED|AccessDenied|Access Denied" || {
    echo "the write failed, but not with a refusal:"
    printf '%s\n' "$output" | head -c 400
    return 1
  }
}

clickhouse_cannot_write_without_credentials() {
  clickhouse_write_is_refused ""
}

clickhouse_cannot_write_with_the_lake_role() {
  clickhouse_write_is_refused ", extra_credentials(role_arn = '$(env_value GBMI_CH_LAKE_ROLE_ARN)')"
}

tracer_builds_one_live_hour() {
  # The tracer DAG on one closed hour of the live GDELT files (week 3 plan, WP-12 step 5): the
  # files land below raw/ on S3, the hour is committed to the four silver tables through
  # Glue, ClickHouse pins gdelt_event through the lake role, dbt builds the mart with the
  # target of the host, and the last task compares the counts of the layers. The hour which
  # ended at least 30 minutes ago: its four files are published.
  local hour run_id state attempt
  hour="$(date -u -d '90 minutes ago' +%Y-%m-%dT%H)" || { echo "the host mode needs GNU date"; return 1; }
  run_id="smoke__$(date -u +%Y%m%dT%H%M%S)"
  "${COMPOSE[@]}" exec -T airflow-scheduler airflow dags unpause tracer_gdelt_events > /dev/null 2>&1 || return 1
  "${COMPOSE[@]}" exec -T airflow-scheduler airflow dags trigger tracer_gdelt_events \
    --run-id "$run_id" --conf "{\"hour\": \"$hour\", \"source\": \"live\"}" > /dev/null 2>&1 || {
    echo "the run $run_id could not be triggered"
    return 1
  }
  # Up to 30 minutes: an hour of the three feeds is about 20 files.
  for attempt in $(seq 1 180); do
    state="$("${COMPOSE[@]}" exec -T airflow-scheduler airflow dags state tracer_gdelt_events "$run_id" 2> /dev/null | tail -1)"
    case "$state" in
      success*) echo "the hour $hour is built (run $run_id)"; return 0 ;;
      failed*) echo "the run $run_id for the hour $hour failed"; return 1 ;;
    esac
    sleep 10
  done
  echo "the run $run_id for the hour $hour did not end in 30 minutes (state: $state)"
  return 1
}

check "long-running services are healthy" services_healthy
check "the one-shot services finished with exit code 0 (init, migrations, roles, manifest)" one_shots_succeeded
check "no container has restarted" no_restarts
check "no service uses an anonymous volume" no_anonymous_volumes
check "no DAG import errors" no_import_errors
check "pools sec_api, silver_writer and heavy exist with one slot" pools_exist
check "ClickHouse reports version 26.8" clickhouse_version
check "ClickHouse databases and roles exist" clickhouse_objects
check "ClickHouse users dbt, bi and gbmi can log in" clickhouse_users_can_log_in
check "PostgreSQL databases airflow, gbmi and metabase exist" postgres_databases
check "the newest migration file is applied in the schema ops" migrations_applied
check "ClickHouse reads PostgreSQL through postgresql()" clickhouse_reads_postgres
check "the dbt user reads the ledger through the named collection pg_ops" dbt_reads_the_ledger
check "Airflow, gbmi and dbt environments are isolated" environments_are_isolated
if [ "$MODE" = "host" ]; then
  check "a failed task is tried again three times" task_retries_are_three
  check "the pipeline version is the commit of the release" pipeline_version_is_the_release
  check "Glue, S3 and the pinned read through the lake role answer (gbmi lake check)" lake_backend_answers
  check "ClickHouse cannot write to the lake without credentials" clickhouse_cannot_write_without_credentials
  check "ClickHouse cannot write to the lake with the lake role" clickhouse_cannot_write_with_the_lake_role
  check "the tracer DAG lands and builds one live hour on S3 and Glue" tracer_builds_one_live_hour
else
  check "Airflow writes /lake, ClickHouse reads it read-only" lake_is_shared
fi

if [ "$failures" -gt 0 ]; then
  echo "smoke: $failures check(s) failed"
  exit 1
fi
echo "smoke: all checks passed"
