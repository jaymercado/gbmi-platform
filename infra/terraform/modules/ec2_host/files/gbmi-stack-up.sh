#!/usr/bin/env bash
# Starts the release which /opt/gbmi/DEPLOYED names (week 3 plan 3.3). The unit
# gbmi-stack.service calls the script at boot. The steps of a start live in the release:
# compose/host/stack-up writes the environment file again and brings the stack up.
set -euo pipefail

GBMI_HOME="${GBMI_HOME:-/opt/gbmi}"

release="$(tr -d '[:space:]' < "$GBMI_HOME/DEPLOYED")"
case "$release" in
  *[!0-9a-f]* | "") echo "FAILED: $GBMI_HOME/DEPLOYED holds no commit" >&2; exit 1 ;;
esac
exec "$GBMI_HOME/releases/$release/compose/host/stack-up" "$release"
