# Shared by the scripts of the AWS host (week 3 plan 3.3). Sourced, not run.
#
# The folders of the host:
#
#   $GBMI_HOME/releases/<commit>/   the files of one commit of main: Compose files, host
#                                   scripts, the configuration and initialisation files of
#                                   PostgreSQL and ClickHouse
#   $GBMI_HOME/DEPLOYED             the commit which the host runs: written only after a passed
#                                   smoke test, and read by the unit which starts the stack at boot
#   $GBMI_HOME/PREVIOUS             the commit which the host ran before the deployed one: its
#                                   release folder stays on the disk for a way back
#   $GBMI_ENV_FILE                  the environment file of Compose, from Parameter Store
#
# No script of the host prints a value of the environment file, and none runs with command
# tracing: the output of a deployment is stored by Systems Manager.

GBMI_HOME="${GBMI_HOME:-/opt/gbmi}"
GBMI_ENV_FILE="${GBMI_ENV_FILE:-/etc/gbmi/env}"
GBMI_PARAMETER_PREFIX="${GBMI_PARAMETER_PREFIX:-/gbmi/prod/}"
# The lock of the fixed deployment script of the host. The start at boot takes the same lock:
# a deployment and a start both write the environment file and call Compose.
GBMI_DEPLOY_LOCK="${GBMI_DEPLOY_LOCK:-/run/gbmi-deploy.lock}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

say() {
  printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*"
}

die() {
  printf 'FAILED: %s\n' "$*" >&2
  exit 1
}

require_commit() {
  case "$1" in
    *[!0-9a-f]* | "") die "a commit is 40 hexadecimal characters" ;;
  esac
  [ "${#1}" -eq 40 ] || die "a commit is 40 hexadecimal characters"
}

release_dir() {
  printf '%s/releases/%s' "$GBMI_HOME" "$1"
}

# Compose for one release: both files, the project name of the host and its environment file.
compose() {
  local release="$1"
  shift
  docker compose --project-name gbmi --project-directory "$(release_dir "$release")" \
    --env-file "$GBMI_ENV_FILE" \
    -f "$(release_dir "$release")/compose/compose.yaml" \
    -f "$(release_dir "$release")/compose/compose.prod.yaml" "$@"
}
