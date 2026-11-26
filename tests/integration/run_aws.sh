#!/usr/bin/env bash
# Runs the Glue/S3 halves of the integration tests (spike questions SK1 Q8 and SK2 Q6), one
# test at a time.
#
# Credentials from `aws login` last 15 minutes and cannot be renewed inside a container, and
# the AWS halves together take longer than that. Each test therefore starts with credentials
# that stay valid for at least GBMI_AWS_MIN_SECONDS more seconds (week 1 plan, D13). The default
# is above the slowest test, which took 471 seconds from a laptop on 2026-11-26.
#
#   GBMI_TEST_AWS_LAKE_BUCKET=<lake bucket> make integration-aws
#
# GBMI_AWS_ONLY=<pattern> limits the run to the test ids that match the grep pattern.
#
# Needs the Compose stack (make up) and a signed-in profile (GBMI_AWS_PROFILE, default gbmi).
set -euo pipefail

profile="${GBMI_AWS_PROFILE:-gbmi}"
min_seconds="${GBMI_AWS_MIN_SECONDS:-540}"
only="${GBMI_AWS_ONLY:-.}"
: "${GBMI_TEST_AWS_LAKE_BUCKET:?set GBMI_TEST_AWS_LAKE_BUCKET to the name of the lake bucket}"
export GBMI_TEST_AWS=1
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

field() {
  python3 -c 'import json, sys; print(json.load(sys.stdin).get(sys.argv[1]) or "")' "$1"
}

seconds_left() {
  python3 -c 'import datetime, json, sys
expiration = json.load(sys.stdin).get("Expiration")
if expiration is None:
    print(86400)
else:
    left = datetime.datetime.fromisoformat(expiration) - datetime.datetime.now(datetime.timezone.utc)
    print(int(left.total_seconds()))'
}

# Export credentials that stay valid long enough for one test. The AWS CLI hands out new ones
# only after the current ones have expired, so a short remainder is waited out.
fresh_credentials() {
  local json left
  while :; do
    json="$(aws configure export-credentials --profile "$profile" --format process)"
    left="$(seconds_left <<<"$json")"
    if [ "$left" -ge "$min_seconds" ]; then
      break
    fi
    echo "credentials expire in ${left}s; waiting for the next ones" >&2
    sleep $((left > 0 ? left + 3 : 3))
  done
  AWS_ACCESS_KEY_ID="$(field AccessKeyId <<<"$json")"
  AWS_SECRET_ACCESS_KEY="$(field SecretAccessKey <<<"$json")"
  AWS_SESSION_TOKEN="$(field SessionToken <<<"$json")"
  export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
}

list="$(mktemp)"
output="$(mktemp)"
trap 'rm -f "$list" "$output"' EXIT
make -s toolbox CMD='pytest tests/integration -m "integration and aws" --collect-only -q -p no:cacheprovider' \
  </dev/null | grep '::' | grep -e "$only" >"$list" || true
total="$(wc -l <"$list" | tr -d ' ')"
if [ "$total" -eq 0 ]; then
  echo "no AWS test was collected" >&2
  exit 1
fi

passed=0
skipped=0
failed=()
number=0
while IFS= read -r test_id <&3; do
  number=$((number + 1))
  fresh_credentials
  echo "== [$number/$total] $test_id"
  started=$SECONDS
  if make -s toolbox CMD="pytest '$test_id' -p no:cacheprovider -q -rs" </dev/null 2>&1 | tee "$output"; then
    # pytest also exits with 0 for a test that skipped itself, so read its summary line.
    if grep -Eq '^[0-9]+ passed' "$output"; then
      passed=$((passed + 1))
      echo "== passed in $((SECONDS - started))s"
    else
      skipped=$((skipped + 1))
      echo "== skipped"
    fi
  else
    failed+=("$test_id")
    echo "== FAILED after $((SECONDS - started))s"
  fi
done 3<"$list"

echo
echo "AWS halves: $passed passed, $skipped skipped, ${#failed[@]} failed, of $total"
if [ "${#failed[@]}" -gt 0 ]; then
  printf 'failed: %s\n' "${failed[@]}"
  exit 1
fi
