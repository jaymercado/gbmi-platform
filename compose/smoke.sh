#!/bin/bash
# Smoke checks for the running stack (week 1 plan, WP-05 step 6). Run with: make smoke
#
# Every check prints PASS or FAIL; the script exits non-zero if any check failed.
set -uo pipefail

COMPOSE=(docker compose --project-directory . --env-file .env -f compose/compose.yaml)
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

# Passwords come from .env, the same file Compose reads. The file is parsed, not sourced:
# Compose accepts values that a shell would not.
env_value() {
  grep -E "^$1=" .env | head -1 | cut -d= -f2-
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
  for service in clickhouse-init airflow-init dbt-manifest; do
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

check "long-running services are healthy" services_healthy
check "clickhouse-init, airflow-init and dbt-manifest finished with exit code 0" one_shots_succeeded
check "no container has restarted" no_restarts
check "no service uses an anonymous volume" no_anonymous_volumes
check "no DAG import errors" no_import_errors
check "pools sec_api, silver_writer and heavy exist with one slot" pools_exist
check "ClickHouse reports version 26.8" clickhouse_version
check "ClickHouse databases and roles exist" clickhouse_objects
check "ClickHouse users dbt, bi and gbmi can log in" clickhouse_users_can_log_in
check "PostgreSQL databases airflow, gbmi and metabase exist" postgres_databases
check "ClickHouse reads PostgreSQL through postgresql()" clickhouse_reads_postgres
check "Airflow writes /lake, ClickHouse reads it read-only" lake_is_shared
check "Airflow, gbmi and dbt environments are isolated" environments_are_isolated

if [ "$failures" -gt 0 ]; then
  echo "smoke: $failures check(s) failed"
  exit 1
fi
echo "smoke: all checks passed"
