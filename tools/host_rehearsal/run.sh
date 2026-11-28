#!/usr/bin/env bash
# make host-rehearsal: the first boot and the deployments of the AWS host, on the laptop
# (week 3 plan, change C16 b).
#
# An instance ignores a later change of its user data, so a defect of the user data costs a
# replacement of the instance. The rehearsal runs the user data and the host scripts in a
# container of Amazon Linux 2023 for arm64, before any apply:
#
#   1. the user data as Terraform renders it: the real packages, the real file of the Compose
#      plugin with its checksum, the real validation of the CloudWatch agent configuration;
#   2. deployments with the fixed script and the scripts of the release: three commits, and
#      each refusal (a wrong argument, a commit outside main, a missing image, a failed smoke
#      test, a second deployment at the same time);
#   3. the start at boot.
#
# Stand-ins replace what a container cannot do: systemctl and swapon (no systemd as the first
# process, no swap), docker and aws during the deployments (no Docker daemon, no AWS access),
# GitHub (a local copy of the commit under test) and the smoke test of the release. A
# container has no metadata service, so the control script of the CloudWatch agent runs in
# the mode onPremise instead of ec2: the one changed word of the user data.
#
# Not covered: cloud-init, systemd, swap, the roles of AWS and the real Compose stack. The
# first deployment on the host tests those.
#
# Needs Docker on an arm64 machine, Terraform and Git. No AWS access and no sudo.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
module="$repo/infra/terraform/modules/ec2_host"
image="${GBMI_REHEARSAL_IMAGE:-public.ecr.aws/amazonlinux/amazonlinux:2023}"
container="gbmi-host-rehearsal-$$"
work="$(mktemp -d)"

cleanup() {
  docker rm --force "$container" > /dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT

if [ "$(docker info --format '{{.Architecture}}')" != "aarch64" ]; then
  echo "The rehearsal needs Docker on an arm64 machine: the host is an arm64 instance."
  exit 1
fi

# The default of a variable of the module ec2_host.
default_of() {
  sed -n "/^variable \"$1\"/,/^}/s/^ *default *= *\"\(.*\)\"$/\1/p" "$module/variables.tf"
}

stage="$work/rehearsal"
mkdir -p "$stage" "$work/empty"
cp -R "$here/inside.sh" "$here/bin" "$stage/"
cp -R "$module/files" "$stage/module"
cp -R "$repo/compose/host" "$stage/host"

# 1. The user data as Terraform renders it. The repository and the image are stand-ins: the
#    container fetches the commits from a local copy.
compose_version="$(default_of compose_version)"
compose_sha256="$(default_of compose_sha256)"
[ -n "$compose_version" ] && [ -n "$compose_sha256" ] || {
  echo "The defaults compose_version and compose_sha256 of the module ec2_host were not found."
  exit 1
}
files="$module/files"
template="templatefile(\"$files/user_data.sh.tftpl\", { repository = \"rehearsal-owner/gbmi-platform\", image = \"ghcr.io/rehearsal-owner/gbmi-airflow\", compose_version = \"$compose_version\", compose_sha256 = \"$compose_sha256\", deploy_script_b64 = filebase64(\"$files/gbmi-deploy.sh\"), stack_up_b64 = filebase64(\"$files/gbmi-stack-up.sh\"), stack_unit_b64 = filebase64(\"$files/gbmi-stack.service\"), agent_config_b64 = base64encode(replace(file(\"$files/cloudwatch-agent.json\"), \"__HOST_LOG_GROUP__\", \"/gbmi/prod/host\")) })"
echo "base64encode($template)" | terraform -chdir="$work/empty" console \
  | tr -d '"\n' | base64 --decode > "$stage/user_data.sh"
# The instance receives the script compressed; EC2 accepts 16,384 bytes.
sent="$(echo "length(base64gzip($template))" | terraform -chdir="$work/empty" console)"
printf '%s\n' "$compose_version" > "$stage/compose_version"
printf '%s\n' "$(wc -c < "$stage/user_data.sh" | tr -d ' ')" "$((sent * 3 / 4))" > "$stage/user_data_sizes"

# 2. The commits under test: the working folder with its host scripts as they are now, and a
#    stand-in for the smoke test. Three commits on main, and one commit outside main.
git clone --quiet "$repo" "$work/tree"
git -C "$work/tree" checkout --quiet -B main "$(git -C "$repo" rev-parse HEAD)"
cp -R "$repo/compose/host/." "$work/tree/compose/host/"
cp "$here/smoke.sh" "$work/tree/compose/smoke.sh"
commit() {
  git -C "$work/tree" add --all
  git -C "$work/tree" -c user.name=rehearsal -c user.email=rehearsal@example.invalid \
    commit --quiet --no-verify --allow-empty --message "$1"
  git -C "$work/tree" rev-parse HEAD
}
first="$(commit "rehearsal: the first release")"
echo 2 > "$work/tree/REHEARSAL"
second="$(commit "rehearsal: the second release")"
echo 3 > "$work/tree/REHEARSAL"
third="$(commit "rehearsal: the third release")"
git -C "$work/tree" checkout --quiet -b other "$first"
echo outside > "$work/tree/REHEARSAL"
outside="$(commit "rehearsal: a commit outside main")"
git init --quiet --bare "$stage/origin.git"
git -C "$work/tree" push --quiet "$stage/origin.git" main other
printf '%s\n' "$first" "$second" "$third" "$outside" > "$stage/commits"

# 3. The container. The files are copied in, so no folder of the laptop is mounted.
docker create --name "$container" --platform linux/arm64 "$image" bash /rehearsal/inside.sh \
  > /dev/null
docker cp "$stage" "$container:/rehearsal"
docker start --attach "$container"
