#!/usr/bin/env bash
# Checks one built image of the stage "runtime" (week 3 plan, WP-10 step 3). The image has to
# work without any mount: the AWS host mounts no source folder.
#
#   docker/airflow/check_runtime.sh <image> <commit> [<address of the repository>]
#
# Used by "make images" and by the workflow "images".
set -euo pipefail

image="${1:?the image, for example gbmi-airflow:runtime}"
commit="${2:?the commit which the image was built from}"
source="${3:-}"

run() {
  docker run --rm --network none --entrypoint "$1" "$image" "${@:2}"
}

fail() {
  echo "check_runtime: $1" >&2
  exit 1
}

echo "== the gbmi package answers"
run /opt/venvs/gbmi/bin/gbmi version

echo "== the pipeline version is the commit"
version="$(run printenv GBMI_PIPELINE_VERSION)"
[ "$version" = "$commit" ] || fail "GBMI_PIPELINE_VERSION is '$version', expected '$commit'"

echo "== the labels name the commit and the repository"
revision="$(docker inspect --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' "$image")"
[ "$revision" = "$commit" ] || fail "the label revision is '$revision', expected '$commit'"
if [ -n "$source" ]; then
  labelled="$(docker inspect --format '{{ index .Config.Labels "org.opencontainers.image.source" }}' "$image")"
  [ "$labelled" = "$source" ] || fail "the label source is '$labelled', expected '$source'"
fi

echo "== the Amazon provider of Airflow is installed (task logs on S3)"
# sed reads its whole input: a command which stops early would break the pipe of the writer.
run bash -c "pip show apache-airflow-providers-amazon | sed -n '1,2p'"

echo "== dbt and the manifest of the dbt project"
run /opt/venvs/dbt/bin/dbt --version | sed -n '1,3p'
run test -s /opt/gbmi/dbt/target/manifest.json || fail "the image holds no dbt manifest"

echo "== the DAG files, the migration files and the seed of the watchlist"
run bash -c 'ls /opt/gbmi/dags/*.py | wc -l; ls /opt/gbmi/db/postgres/migrations/*.sql | wc -l; test -s /opt/gbmi/dbt/seeds/watchlist.csv'

echo "check_runtime: passed for $image"
