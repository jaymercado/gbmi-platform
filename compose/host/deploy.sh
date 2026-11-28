#!/usr/bin/env bash
# Deploys the release of one commit on the AWS host (blueprint 9.7; week 3 plan 3.3).
#
#   <release folder>/compose/host/deploy.sh <commit>
#
# Started by the fixed script which the user data of the host installed. The fixed script has
# checked that the commit is part of main and has an image in the registry, and has unpacked
# the commit into its release folder. The steps of a deployment live here, in the release,
# so a pull request can change them (change C16 b).
#
#   1. write the environment file from Parameter Store
#   2. pull the images of the release
#   3. start the stack: the one-shot services run the migrations and the initialisation
#   4. the smoke test of the host, which lands and builds one live hour with the tracer DAG
#   5. only after a passed smoke test: write DEPLOYED, which the start at boot reads
#
# A deployment which fails leaves DEPLOYED as it was: the next start at boot brings the
# release before back. The runbook docs/runbooks/deploy-and-roll-back.md has the steps to
# bring the release before back at once.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=compose/host/common.sh
. "$here/common.sh"

commit="${1:-}"
require_commit "$commit"
release="$(release_dir "$commit")"
# The release which runs until now stays on the disk for a way back.
previous=""
if [ -s "$GBMI_HOME/DEPLOYED" ]; then
  previous="$(tr -d '[:space:]' < "$GBMI_HOME/DEPLOYED")"
fi
[ "$here" = "$release/compose/host" ] || die "deploy.sh has to run from the release folder of its commit"

say "1/5 render the environment file"
"$here/render_env.sh" "$commit"

say "2/5 pull the images"
compose "$commit" pull --quiet

say "3/5 start the stack"
compose "$commit" up --detach --wait --remove-orphans

say "4/5 smoke test"
(cd "$release" && GBMI_SMOKE_MODE=host GBMI_RELEASE="$commit" bash compose/smoke.sh)

say "5/5 record the deployed release"
printf '%s\n' "$commit" > "$GBMI_HOME/DEPLOYED.new"
mv "$GBMI_HOME/DEPLOYED.new" "$GBMI_HOME/DEPLOYED"

# Two releases stay on the disk: the new one and the one before. Older folders and the
# images which no container uses are removed.
for folder in "$GBMI_HOME"/releases/*/; do
  name="$(basename "$folder")"
  if [ "$name" != "$commit" ] && [ "$name" != "$previous" ]; then
    rm -rf "$folder"
  fi
done
docker image prune --all --force > /dev/null 2>&1 || true

say "DEPLOYED $commit"
