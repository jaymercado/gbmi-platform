#!/usr/bin/env bash
# The part of the rehearsal which runs inside the container (see run.sh): the user data, the
# deployments and the start at boot, with checks after each step. Prints one line for each
# check and ends with the exit code 1 when a check failed.
set -uo pipefail

R=/rehearsal
checks=0
failures=0

ok() {
  checks=$((checks + 1))
  printf '  ok      %s\n' "$1"
}

bad() {
  checks=$((checks + 1))
  failures=$((failures + 1))
  printf '  FAILED  %s\n' "$1"
}

# check <text> <command with arguments>
check() {
  local text="$1"
  shift
  if "$@" > /dev/null 2>&1; then ok "$text"; else bad "$text"; fi
}

section() {
  printf '\n== %s\n' "$1"
}

show() {
  sed 's/^/    | /' "$1" | tail -n 40
}

has() {
  grep --quiet --fixed-strings -- "$2" "$1"
}

lacks() {
  ! grep --quiet --fixed-strings -- "$2" "$1"
}

same() {
  [ "$(sha256sum < "$1")" = "$(sha256sum < "$2")" ]
}

mode() {
  [ "$(stat --format '%a %U' "$1")" = "$2" ]
}

chmod 755 $R/bin/boot/* $R/bin/deploy/*
mkdir -p /run/rehearsal
mapfile -t commits < $R/commits
first="${commits[0]}"
second="${commits[1]}"
third="${commits[2]}"
outside="${commits[3]}"
compose_version="$(cat $R/compose_version)"
mapfile -t sizes < $R/user_data_sizes

section "The container"
# shellcheck disable=SC1091
. /etc/os-release
printf '  %s %s, %s, %s, %s\n' "$NAME" "$VERSION" "$(uname -m)" \
  "$(bash --version | head -n 1 | cut -d '(' -f 1)" "$(python3 --version)"

# --- 1. the user data ---------------------------------------------------------------------------
section "1. The user data of the first boot"
printf '  the script has %s bytes; the instance receives about %s bytes after compression\n' \
  "${sizes[0]}" "${sizes[1]}"
check "the compressed user data fits into the 16,384 bytes of EC2" test "${sizes[1]}" -lt 16384
check "the syntax of the user data is valid" bash -n $R/user_data.sh

# A container has no metadata service: the control script of the CloudWatch agent runs in the
# mode onPremise. Nothing else of the user data changes.
sed 's/ -a fetch-config -m ec2 -s / -a fetch-config -m onPremise -s /' $R/user_data.sh \
  > /root/user_data.sh
one_word_changed() {
  ! same $R/user_data.sh /root/user_data.sh \
    && [ "$(sed 's/ -m onPremise -s / -m ec2 -s /' /root/user_data.sh | sha256sum)" = "$(sha256sum < $R/user_data.sh)" ]
}
check "the rehearsal changes one word of the user data: the mode of the CloudWatch agent" \
  one_word_changed

started=$SECONDS
AWS_REGION=us-east-1 PATH="$R/bin/boot:$PATH" bash /root/user_data.sh > /root/user_data.out 2>&1
code=$?
sleep 1
printf '  the user data ran for %s seconds\n' "$((SECONDS - started))"
if [ "$code" -eq 0 ]; then
  ok "the user data ends with the exit code 0"
else
  bad "the user data ends with the exit code 0 (the exit code was $code)"
  show /root/user_data.out
fi

boot_log=/var/log/gbmi-user-data.log
check "the log of the first boot has the start line" has $boot_log "gbmi user data: start"
check "the log of the first boot has the done line" has $boot_log "gbmi user data: done"
for name in docker git tar flock aws python3.12; do
  check "the command $name is installed" command -v "$name"
done

plugin=/usr/local/lib/docker/cli-plugins/docker-compose
plugin_version() {
  [ "v$(/usr/bin/docker compose version --short)" = "$compose_version" ]
}
check "the Compose plugin has the mode 755" mode $plugin "755 root"
check "the real docker command finds the plugin, and the plugin is the release $compose_version" \
  plugin_version

swap_file() {
  [ "$(stat --format '%s %a' /swapfile)" = "2147483648 600" ] \
    && grep --quiet '^/swapfile none swap sw 0 0$' /etc/fstab
}
check "the swap file has 2 GiB and the mode 600, and stands in /etc/fstab" swap_file
check "swapon was called for the swap file" has /var/log/rehearsal-swapon.log "swapon /swapfile"

host_conf() {
  [ "$(cat /etc/gbmi/host.conf)" = "GBMI_REPOSITORY=rehearsal-owner/gbmi-platform
GBMI_IMAGE=ghcr.io/rehearsal-owner/gbmi-airflow" ]
}
check "only root can enter /etc/gbmi" mode /etc/gbmi "700 root"
check "host.conf names the repository and the image" host_conf
check "the fixed deployment script is the file of the repository" \
  same /usr/local/sbin/gbmi-deploy $R/module/gbmi-deploy.sh
check "the fixed deployment script has the mode 755" mode /usr/local/sbin/gbmi-deploy "755 root"
check "the start script is the file of the repository" \
  same /usr/local/sbin/gbmi-stack-up $R/module/gbmi-stack-up.sh
check "the start script has the mode 755" mode /usr/local/sbin/gbmi-stack-up "755 root"
check "the unit is the file of the repository" \
  same /etc/systemd/system/gbmi-stack.service $R/module/gbmi-stack.service
systemd-analyze verify /etc/systemd/system/gbmi-stack.service > /root/unit.out 2>&1
unit_code=$?
unit_accepted() {
  [ "$unit_code" -eq 0 ] && lacks /root/unit.out "gbmi-stack.service"
}
if unit_accepted; then
  ok "systemd accepts the unit (systemd-analyze verify)"
else
  bad "systemd accepts the unit (systemd-analyze verify)"
  show /root/unit.out
fi

agent_groups() {
  [ "$(grep --count '"log_group_name": "/gbmi/prod/host"' /etc/gbmi/cloudwatch-agent.json)" -eq 2 ] \
    && lacks /etc/gbmi/cloudwatch-agent.json "__HOST_LOG_GROUP__"
}
check "the configuration of the CloudWatch agent names the log group of the host twice" \
  agent_groups
check "the CloudWatch agent accepts the configuration: first phase (the JSON schema)" \
  has /root/user_data.out "Configuration validation first phase succeeded"
check "the CloudWatch agent accepts the configuration: second phase (the agent itself)" \
  has /root/user_data.out "Configuration validation second phase succeeded"
for call in "systemctl daemon-reload" "systemctl enable --now docker" \
  "systemctl enable gbmi-stack.service" "systemctl restart amazon-cloudwatch-agent.service"; do
  check "called: $call" has /var/log/rehearsal-systemctl.log "$call"
done

# --- 2. deployments ----------------------------------------------------------------------------
section "2. Deployments with the fixed script and the scripts of the release"
cp -R $R/origin.git /srv/origin.git
git config --system url./srv/origin.git.insteadOf \
  https://github.com/rehearsal-owner/gbmi-platform.git
# One parameter for each name which compose/host/render_env.py requires, each with a value
# which holds a quote, a dollar sign and a hash; the declared identity; the source "off".
python3 - << 'PY'
import json
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, "/rehearsal/host")
import render_env

parameters = [
    {"Name": f"/gbmi/prod/{name}", "Value": f"value of {name}: it's $HOME #1"}
    for name in render_env.REQUIRED
]
parameters.append({"Name": "/gbmi/prod/GBMI_SEC_SOURCE", "Value": "off"})
parameters.append(
    {"Name": "/gbmi/prod/GBMI_SEC_USER_AGENT", "Value": "value of the identity contact@example.org"}
)
with open("/run/rehearsal/parameters.json", "w", encoding="utf-8") as stream:
    json.dump({"Parameters": parameters}, stream)
PY

# Run Command gives a script a short PATH and no HOME; so does the rehearsal.
deploy_path="$R/bin/deploy:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
deploy() {
  env -i PATH="$deploy_path" /usr/local/sbin/gbmi-deploy "$@" > /root/deploy.out 2>&1
}
deployed() {
  cat /opt/gbmi/DEPLOYED 2> /dev/null
}
previous() {
  cat /opt/gbmi/PREVIOUS 2> /dev/null
}
passed() {
  if [ "$2" -eq 0 ]; then
    ok "$1"
  else
    bad "$1 (the exit code was $2)"
    show /root/deploy.out
  fi
}
refused() {
  if [ "$2" -ne 0 ] && has /root/deploy.out "FAILED: $3"; then
    ok "$1"
  else
    bad "$1 (the exit code was $2)"
    show /root/deploy.out
  fi
}
compose_of() {
  printf 'compose --project-name gbmi --project-directory /opt/gbmi/releases/%s --env-file /etc/gbmi/env -f /opt/gbmi/releases/%s/compose/compose.yaml -f /opt/gbmi/releases/%s/compose/compose.prod.yaml' "$1" "$1" "$1"
}

number=0
for argument in "" "${first:0:39}" "${first}0" "ABCDEF0123456789ABCDEF0123456789ABCDEF01" \
  "main" "$first; touch /run/rehearsal/injected"; do
  number=$((number + 1))
  deploy "$argument"
  refused "wrong argument $number of 6 is refused" $? "the argument has to be the 40 hexadecimal"
done
nothing_happened() {
  [ ! -e /opt/gbmi/repo.git ] && [ ! -e /var/log/rehearsal-docker.log ] \
    && [ ! -e /run/rehearsal/injected ] && [ ! -e /var/log/gbmi-deploy.log ]
}
check "after the six refusals: no clone, no docker call, no log line, no injected command" \
  nothing_happened

deploy "$first"
passed "the first deployment ends with the exit code 0" $?
check "the release folder holds the files of the commit" \
  test -x "/opt/gbmi/releases/$first/compose/host/deploy.sh"
check "the clone names the address of GitHub as its origin" \
  test "$(git -C /opt/gbmi/repo.git config --get remote.origin.url)" = "https://github.com/rehearsal-owner/gbmi-platform.git"
check "the environment file belongs to root and has the mode 600" mode /etc/gbmi/env "600 root"
derived_values() {
  has /etc/gbmi/env "GBMI_RELEASE='$first'" \
    && has /etc/gbmi/env "GBMI_LAKE_BUCKET='gbmi-prod-lake-123456789012'" \
    && has /etc/gbmi/env "GBMI_OPS_BUCKET='gbmi-prod-ops-123456789012'" \
    && has /etc/gbmi/env "GBMI_CH_LAKE_ROLE_ARN='arn:aws:iam::123456789012:role/gbmi-clickhouse-lake-ro'" \
    && has /etc/gbmi/env "GBMI_SEC_SOURCE='off'"
}
check "the environment file names the release, the buckets, the lake role and the source off" \
  derived_values
no_value_shown() {
  lacks /root/deploy.out "value of" && lacks /var/log/gbmi-deploy.log "value of"
}
check "no value of a parameter stands in the output or in the log of the deployment" \
  no_value_shown
check "the parameters were read below /gbmi/prod/, with decryption" has /var/log/rehearsal-aws.log \
  "aws ssm get-parameters-by-path --path /gbmi/prod/ --recursive --with-decryption --output json"
expected="docker manifest inspect ghcr.io/rehearsal-owner/gbmi-airflow:$first
docker $(compose_of "$first") pull --quiet
docker $(compose_of "$first") up --detach --wait --remove-orphans
docker image prune --all --force"
check "docker was called four times, in the order: image check, pull, up, prune" \
  test "$(cat /var/log/rehearsal-docker.log)" = "$expected"
check "the smoke test ran in the host mode, in the release folder" has /var/log/rehearsal-smoke.log \
  "smoke mode=host release=$first folder=/opt/gbmi/releases/$first"
check "DEPLOYED names the first commit" test "$(deployed)" = "$first"
check "the first deployment names no release before" test ! -e /opt/gbmi/PREVIOUS
check "the log of the deployments has the start line" \
  has /var/log/gbmi-deploy.log "=== deployment of $first, started"
check "the log of the deployments has the end line" has /var/log/gbmi-deploy.log "DEPLOYED $first"
check "the output of the command ends with the end line" \
  test "$(tail -n 1 /root/deploy.out | cut -d ' ' -f 2-)" = "DEPLOYED $first"

# The real Compose plugin reads the environment file which the release wrote.
# shellcheck disable=SC2046
/usr/bin/docker $(compose_of "$first") config --format json > /root/config.json 2> /root/config.err
if [ -s /root/config.json ]; then
  ok "the real Compose plugin renders the two Compose files with the environment file"
else
  bad "the real Compose plugin renders the two Compose files with the environment file"
  show /root/config.err
fi
check "a value with a quote, a dollar sign and a hash reaches the service unchanged" python3 -c '
import json
with open("/root/config.json", encoding="utf-8") as stream:
    services = json.load(stream)["services"]
value = services["postgres"]["environment"]["POSTGRES_PASSWORD"].replace("$$", "$")
assert value == "value of POSTGRES_PASSWORD: it'"'"'s $HOME #1"
'

deploy "$outside"
refused "a commit outside main is refused" $? "the commit is not part of the branch main"
deploy "0000000000000000000000000000000000000001"
refused "a commit which the repository does not hold is refused" $? \
  "the commit is not part of the branch main"
check "no release folder exists for the commit outside main" \
  test ! -e "/opt/gbmi/releases/$outside"

touch /run/rehearsal/no-image
deploy "$second"
refused "a commit without an image in the registry is refused" $? \
  "the registry holds no image of the commit"
rm /run/rehearsal/no-image
check "no release folder exists for the commit without an image" \
  test ! -e "/opt/gbmi/releases/$second"

touch /run/rehearsal/smoke-fails
deploy "$second"
code=$?
rm /run/rehearsal/smoke-fails
if [ "$code" -ne 0 ]; then
  ok "a deployment with a failed smoke test ends with an exit code above 0"
else
  bad "a deployment with a failed smoke test ends with an exit code above 0"
fi
check "after the failed smoke test DEPLOYED still names the first commit" \
  test "$(deployed)" = "$first"

(
  flock 8
  sleep 3
) 8> /run/gbmi-deploy.lock &
sleep 1
deploy "$second"
refused "a second deployment at the same time is refused" $? "another deployment is running"
wait

deploy "$second"
passed "the second deployment ends with the exit code 0" $?
two_releases() {
  [ "$(deployed)" = "$second" ] && [ "$(previous)" = "$first" ] \
    && [ -d "/opt/gbmi/releases/$first" ] && [ -d "/opt/gbmi/releases/$second" ]
}
check "DEPLOYED names the second commit, PREVIOUS the first; both release folders exist" \
  two_releases

deploy "$third"
passed "the third deployment ends with the exit code 0" $?
oldest_removed() {
  [ "$(deployed)" = "$third" ] && [ "$(previous)" = "$second" ] \
    && [ ! -e "/opt/gbmi/releases/$first" ] \
    && [ -d "/opt/gbmi/releases/$second" ] && [ -d "/opt/gbmi/releases/$third" ]
}
check "DEPLOYED names the third commit, PREVIOUS the second; the first folder is removed" \
  oldest_removed

deploy "$third"
passed "a deployment of the commit which already runs ends with the exit code 0" $?
check "the repeated deployment keeps PREVIOUS and the folder of the release before" \
  oldest_removed

deploy "$second"
passed "going back to the release before is a deployment which ends with the exit code 0" $?
went_back() {
  [ "$(deployed)" = "$second" ] && [ "$(previous)" = "$third" ] \
    && [ -d "/opt/gbmi/releases/$second" ] && [ -d "/opt/gbmi/releases/$third" ]
}
check "after going back DEPLOYED names the second commit and PREVIOUS the third" went_back
deploy "$third"
passed "the third commit is deployed again" $?

# --- 3. the start at boot -----------------------------------------------------------------------
section "3. The start at boot (the command of the unit gbmi-stack.service)"
rm /var/log/rehearsal-docker.log
inode="$(stat --format %i /etc/gbmi/env)"
env -i PATH="$deploy_path" /usr/local/sbin/gbmi-stack-up > /root/deploy.out 2>&1
passed "the start script ends with the exit code 0" $?
check "the environment file was written again" test "$(stat --format %i /etc/gbmi/env)" != "$inode"
check "the environment file still belongs to root with the mode 600" mode /etc/gbmi/env "600 root"
check "docker was called once: up for the deployed release, without a pull of its own" \
  test "$(cat /var/log/rehearsal-docker.log)" = "docker $(compose_of "$third") up --detach --wait --remove-orphans --quiet-pull"

mv /opt/gbmi/DEPLOYED /opt/gbmi/DEPLOYED.away
rm /var/log/rehearsal-docker.log
env -i PATH="$deploy_path" /usr/local/sbin/gbmi-stack-up > /root/deploy.out 2>&1
code=$?
no_start() {
  [ "$code" -ne 0 ] && [ ! -e /var/log/rehearsal-docker.log ]
}
check "without DEPLOYED the start script fails and calls docker not once" no_start

section "Result"
if [ "$failures" -eq 0 ]; then
  printf '  all %s checks passed\n' "$checks"
else
  printf '  %s of %s checks FAILED\n' "$failures" "$checks"
fi
[ "$failures" -eq 0 ]
