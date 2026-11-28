#!/usr/bin/env bash
# The fixed deployment script of the host (blueprint 9.7; week 3 plan 3.3, change C16 b).
#
#   /usr/local/sbin/gbmi-deploy <commit>
#
# The user data installs the script once, outside any checkout. The command document
# gbmi-prod-deploy calls it with one argument: the 40 characters of a commit. The role of the
# deployment workflow can send that document and nothing else, so the script decides what the
# workflow can do on the host:
#
#   1. the argument is a commit, and nothing else runs a deployment;
#   2. the commit is part of the branch main of the repository;
#   3. the registry holds the image of the commit;
#   4. the files of the commit are unpacked into the release folder;
#   5. compose/host/deploy.sh of the release runs the deployment.
#
# The script is small on purpose: a later change of the user data does not reach a running
# host, so the steps of a deployment live in the release, where a pull request changes them.
# The script prints the steps and never a value of a secret.
set -euo pipefail

GBMI_HOME="${GBMI_HOME:-/opt/gbmi}"
GBMI_HOST_CONF="${GBMI_HOST_CONF:-/etc/gbmi/host.conf}"
GBMI_DEPLOY_LOG="${GBMI_DEPLOY_LOG:-/var/log/gbmi-deploy.log}"
GBMI_DEPLOY_LOCK="${GBMI_DEPLOY_LOCK:-/run/gbmi-deploy.lock}"

die() {
  printf 'FAILED: %s\n' "$*" >&2
  exit 1
}

commit="${1:-}"
case "$commit" in
  *[!0-9a-f]* | "") die "the argument has to be the 40 hexadecimal characters of a commit" ;;
esac
[ "${#commit}" -eq 40 ] || die "the argument has to be the 40 hexadecimal characters of a commit"

# GBMI_REPOSITORY (owner/name) and GBMI_IMAGE (registry/owner/name), written by the user data.
[ -r "$GBMI_HOST_CONF" ] || die "the configuration of the host is missing"
# shellcheck disable=SC1090
. "$GBMI_HOST_CONF"
: "${GBMI_REPOSITORY:?}" "${GBMI_IMAGE:?}"

# One lock for the deployments and for the start at boot (compose/host/stack-up of a release):
# both write the environment file and call Compose.
exec 9> "$GBMI_DEPLOY_LOCK"
flock --nonblock 9 || die "another deployment or the start at boot is running"

exec > >(tee -a "$GBMI_DEPLOY_LOG") 2>&1
printf '=== deployment of %s, started %s\n' "$commit" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"

# The first clone is written beside its final name: a clone which is cut halfway leaves no
# broken folder under the name which every later deployment uses.
mirror="$GBMI_HOME/repo.git"
if [ ! -d "$mirror" ]; then
  rm -rf "$mirror.new"
  git clone --quiet --bare "https://github.com/$GBMI_REPOSITORY.git" "$mirror.new"
  mv "$mirror.new" "$mirror"
fi
git -C "$mirror" fetch --quiet origin "+refs/heads/main:refs/heads/main"
if ! git -C "$mirror" cat-file -e "$commit^{commit}" 2> /dev/null \
  || ! git -C "$mirror" merge-base --is-ancestor "$commit" refs/heads/main; then
  die "the commit is not part of the branch main"
fi

# The answer of Docker is shown after a failure: a missing image and a registry which does
# not answer need different repairs.
if ! answer="$(docker manifest inspect "$GBMI_IMAGE:$commit" 2>&1 > /dev/null)"; then
  printf '%s\n' "$answer" >&2
  die "the registry holds no image of the commit, or the registry did not answer"
fi

release="$GBMI_HOME/releases/$commit"
if [ ! -d "$release" ]; then
  rm -rf "$release.new"
  mkdir -p "$release.new"
  git -C "$mirror" archive "$commit" | tar -x -C "$release.new"
  mv "$release.new" "$release"
fi

exec "$release/compose/host/deploy.sh" "$commit"
