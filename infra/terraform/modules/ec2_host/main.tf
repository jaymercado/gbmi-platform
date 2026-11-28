# The host of the platform (blueprint 9.2; week 3 plan 3.3): one arm64 instance which runs the
# Compose stack, the two schedules which stop it at night, and the one command which the
# deployment workflow may send.

# The newest Amazon Linux 2023 image for arm64. A newer image does not replace the instance
# (lifecycle below): the operational database of the host has no backup before phase P4.
data "aws_ssm_parameter" "al2023_arm64" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
}

locals {
  owner = lower(split("/", var.github_repository)[0])

  user_data = templatefile("${path.module}/files/user_data.sh.tftpl", {
    repository        = var.github_repository
    image             = "ghcr.io/${local.owner}/gbmi-airflow"
    compose_version   = var.compose_version
    compose_sha256    = var.compose_sha256
    deploy_script_b64 = filebase64("${path.module}/files/gbmi-deploy.sh")
    stack_up_b64      = filebase64("${path.module}/files/gbmi-stack-up.sh")
    stack_unit_b64    = filebase64("${path.module}/files/gbmi-stack.service")
    agent_config_b64 = base64encode(replace(
      file("${path.module}/files/cloudwatch-agent.json"), "__HOST_LOG_GROUP__", var.host_log_group_name
    ))
  })
}

resource "aws_instance" "host" {
  ami                         = data.aws_ssm_parameter.al2023_arm64.insecure_value
  instance_type               = var.instance_type
  subnet_id                   = var.subnet_id
  vpc_security_group_ids      = [var.security_group_id]
  iam_instance_profile        = var.instance_profile_name
  associate_public_ip_address = true
  ebs_optimized               = true
  # Detailed monitoring costs extra; the alarms use periods of five minutes.
  monitoring = false
  # No key pair: Session Manager is the only way in.
  user_data_base64            = base64gzip(local.user_data)
  user_data_replace_on_change = false

  # Version 2 of the metadata service only. The hop limit is 2 because the containers reach
  # the credentials of the host role through the Docker bridge: the pipelines and ClickHouse.
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
    instance_metadata_tags      = "disabled"
  }

  # The disk holds the operational database, which has no backup before phase P4. The disk
  # outlives the instance: after a termination by mistake (a cancelled spot request, a click
  # in the console) a new instance can take the disk over. A host which is removed on purpose
  # leaves the disk behind; a person deletes the disk when its data is no longer needed.
  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.root_volume_gb
    encrypted             = true
    delete_on_termination = false

    tags = {
      Name = "${var.host_name}-root"
    }
  }

  # A burstable instance in unlimited mode never slows down; an alarm reports any charge for
  # surplus credits (week 3 plan, difference D45).
  credit_specification {
    cpu_credits = "unlimited"
  }

  # A persistent spot request: on an interruption AWS stops the instance and keeps its disk,
  # and starts the instance again when capacity returns. The market is fixed when the
  # instance is created (variable market): the block forces a new resource when it is added,
  # and Terraform sees no difference when it is removed.
  dynamic "instance_market_options" {
    for_each = var.market == "spot" ? [1] : []

    content {
      market_type = "spot"

      spot_options {
        spot_instance_type             = "persistent"
        instance_interruption_behavior = "stop"
      }
    }
  }

  tags = {
    Name = var.host_name
  }

  lifecycle {
    # Nothing destroys or replaces the host by accident: Terraform refuses every plan which
    # would. To replace or remove the host on purpose, remove the line first.
    prevent_destroy = true

    # A new image or a changed user data script must not replace a running host.
    #
    # The public address: a stopped instance has none, and the AWS provider then reads
    # associate_public_ip_address as false (provider 6.74.0, ec2_instance.go: the value is
    # "the primary network interface has an association"). The attribute forces a new
    # resource, and the host stops every night: without the entry, a plan at night would
    # replace the instance.
    ignore_changes = [ami, user_data, user_data_base64, associate_public_ip_address]
  }
}

# --- the daily stop and start (week 3 plan, difference D45) ----------------------------------

resource "aws_scheduler_schedule_group" "host" {
  name = "${var.name_prefix}-host"
}

# A start fails when no spot capacity is free, and AWS does not start a spot instance which
# the schedule stopped: only a new start call does. The schedule therefore tries a failed
# start again for 12 hours, until 20:00 UTC. A failed stop is tried again for one hour. An
# alarm of the module monitoring reports each failed call.
locals {
  schedules = {
    start = {
      expression      = "cron(0 8 * * ? *)"
      action          = "startInstances"
      what            = "Starts"
      attempts        = 185
      max_age_seconds = 43200
    }
    stop = {
      expression      = "cron(0 0 * * ? *)"
      action          = "stopInstances"
      what            = "Stops"
      attempts        = 3
      max_age_seconds = 3600
    }
  }
}

resource "aws_scheduler_schedule" "host" {
  for_each = local.schedules

  name        = "${var.name_prefix}-host-${each.key}"
  group_name  = aws_scheduler_schedule_group.host.name
  description = "${each.value.what} the host of the platform each day (UTC)."
  state       = var.schedule_enabled ? "ENABLED" : "DISABLED"

  schedule_expression          = each.value.expression
  schedule_expression_timezone = "UTC"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = "arn:aws:scheduler:::aws-sdk:ec2:${each.value.action}"
    role_arn = var.scheduler_role_arn
    input    = jsonencode({ InstanceIds = [aws_instance.host.id] })

    retry_policy {
      maximum_retry_attempts       = each.value.attempts
      maximum_event_age_in_seconds = each.value.max_age_seconds
    }
  }
}

# --- the one command of the deployment workflow (week 3 plan, difference D41) ----------------
# The role gbmi-ci-deploy may send this document and no other. The document takes one
# parameter, which can only be 40 hexadecimal characters, and calls the fixed script which the
# user data installed outside any checkout.

resource "aws_ssm_document" "deploy" {
  name            = "${var.name_prefix}-deploy"
  document_type   = "Command"
  document_format = "YAML"
  target_type     = "/AWS::EC2::Instance"

  content = yamlencode({
    schemaVersion = "2.2"
    description   = "Deploy one commit of the branch main to the host of the GBMI platform."
    parameters = {
      Commit = {
        type           = "String"
        description    = "The 40 hexadecimal characters of a commit of the branch main."
        allowedPattern = "^[0-9a-f]{40}$"
      }
    }
    mainSteps = [
      {
        action = "aws:runShellScript"
        name   = "deploy"
        inputs = {
          timeoutSeconds = "3600"
          runCommand     = ["/usr/local/sbin/gbmi-deploy '{{ Commit }}'"]
        }
      }
    ]
  })
}
