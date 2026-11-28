#!/usr/bin/env bash
# Stand-in for compose/smoke.sh in the commits of the rehearsal (tools/host_rehearsal/run.sh):
# no stack runs in the container. Records the call; a marker file makes the test fail.
echo "smoke mode=${GBMI_SMOKE_MODE:-} release=${GBMI_RELEASE:-} folder=$PWD" \
  >> /var/log/rehearsal-smoke.log
[ ! -e /run/rehearsal/smoke-fails ]
