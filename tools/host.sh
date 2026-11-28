#!/usr/bin/env bash
# The AWS host from the laptop (week 3 plan, WP-12): make host-status, host-shell, host-forward.
#
#   tools/host.sh status             the instance, the agent, the deployed release, the services
#   tools/host.sh shell              a shell on the host through Session Manager
#   tools/host.sh forward [port]     forward a port of the host to 127.0.0.1 of the laptop:
#                                    8080 is the API server of Airflow (the default), 3000 Metabase
#   tools/host.sh deploy-log         the status and the output of the newest deployment
#
# The host has no inbound rule and no key pair: Session Manager is the only way in (exit-gate
# row 4). The commands use the profile gbmi of "aws login" (infra/terraform/README.md). The
# commands shell and forward need the Session Manager plugin of the AWS CLI.
set -euo pipefail

export AWS_PROFILE="${AWS_PROFILE:-gbmi}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

instance() {
  local id
  id="$(aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=gbmi-prod-host" \
              "Name=instance-state-name,Values=pending,running,stopping,stopped" \
    --query 'Reservations[].Instances[].InstanceId' --output text)"
  if [ -z "$id" ] || [ "$(printf '%s' "$id" | wc -w | tr -d ' ')" != "1" ]; then
    echo "expected one instance with the tag Name=gbmi-prod-host, found: ${id:-none}" >&2
    exit 1
  fi
  printf '%s' "$id"
}

on_host() {
  # Runs fixed commands on the host and prints their output. Nothing here reads a secret.
  local id="$1" command status attempt
  command="$(aws ssm send-command --document-name AWS-RunShellScript --instance-ids "$id" \
    --parameters commands="$2" --query Command.CommandId --output text)"
  status="Pending"
  for attempt in $(seq 1 30); do
    status="$(aws ssm get-command-invocation --command-id "$command" --instance-id "$id" \
      --query Status --output text 2> /dev/null || echo Pending)"
    case "$status" in
      Pending | InProgress | Delayed) sleep 2 ;;
      *) break ;;
    esac
  done
  aws ssm get-command-invocation --command-id "$command" --instance-id "$id" \
    --query StandardOutputContent --output text
  [ "$status" = "Success" ] || echo "the command on the host ended with: $status"
}

case "${1:-}" in
  status)
    id="$(instance)"
    aws ec2 describe-instances --instance-ids "$id" --output table \
      --query 'Reservations[0].Instances[0].{State:State.Name,Type:InstanceType,Market:InstanceLifecycle,Zone:Placement.AvailabilityZone,Launched:LaunchTime}'
    state="$(aws ec2 describe-instances --instance-ids "$id" \
      --query 'Reservations[0].Instances[0].State.Name' --output text)"
    if [ "$state" != "running" ]; then
      echo "The host is $state: the schedule stops the host at 24:00 UTC and starts it at 08:00 UTC."
      exit 0
    fi
    aws ssm describe-instance-information --filters "Key=InstanceIds,Values=$id" --output table \
      --query 'InstanceInformationList[0].{Agent:PingStatus,LastPing:LastPingDateTime,AgentVersion:AgentVersion}'
    on_host "$id" '["echo deployed: $(cat /opt/gbmi/DEPLOYED 2>/dev/null || echo none)","docker ps --format \"{{.Names}} {{.Status}}\" | sort","uptime","df -h / | tail -1","free -m | sed -n 2p"]'
    ;;
  shell)
    aws ssm start-session --target "$(instance)"
    ;;
  forward)
    port="${2:-8080}"
    local_port="${LOCAL_PORT:-1$port}"
    echo "Forwarding the port $port of the host to http://127.0.0.1:$local_port (Ctrl-C ends the session)."
    aws ssm start-session --target "$(instance)" \
      --document-name AWS-StartPortForwardingSession \
      --parameters "{\"portNumber\":[\"$port\"],\"localPortNumber\":[\"$local_port\"]}"
    ;;
  deploy-log)
    # Systems Manager keeps the first 24,000 characters of the output of a command. The host
    # scripts print the steps of a deployment and no value of a secret.
    id="$(instance)"
    command="$(aws ssm list-commands --filters "key=DocumentName,value=gbmi-prod-deploy" \
      --max-items 1 --query 'Commands[0].CommandId' --output text | head -1)"
    if [ -z "$command" ] || [ "$command" = "None" ]; then
      echo "no deployment was sent to the host yet"
      exit 0
    fi
    aws ssm get-command-invocation --command-id "$command" --instance-id "$id" --output table \
      --query '{Status:Status,Started:ExecutionStartDateTime,Ended:ExecutionEndDateTime,ExitCode:ResponseCode}'
    aws ssm get-command-invocation --command-id "$command" --instance-id "$id" \
      --query StandardOutputContent --output text
    aws ssm get-command-invocation --command-id "$command" --instance-id "$id" \
      --query StandardErrorContent --output text
    ;;
  *)
    echo "usage: tools/host.sh status | shell | forward [port] | deploy-log" >&2
    exit 2
    ;;
esac
