# Monitoring of the host (blueprint 5.10 and 9.2; week 3 plan 3.3): two log groups, one topic
# for alerts and six alarms. The host stops each night, so missing data counts as healthy:
# no alarm of the instance can report a host which does not start in the morning. The sixth
# alarm watches the schedule itself and reports a start or a stop which failed. The heartbeat
# of phase P4 closes the rest of the gap; make host-status is the check of each working day.

data "aws_caller_identity" "current" {}

# --- log groups -------------------------------------------------------------------------------
# containers: the output of every container (Docker log driver awslogs).
# host:       the log of the first boot and the log of the deployments (CloudWatch agent).

resource "aws_cloudwatch_log_group" "this" {
  for_each = toset(["containers", "host"])

  name              = "${var.log_group_prefix}/${each.key}"
  retention_in_days = var.log_retention_days
}

# --- the topic for alerts ---------------------------------------------------------------------

resource "aws_sns_topic" "alerts" {
  name = "${var.name_prefix}-alerts"
}

data "aws_iam_policy_document" "alerts" {
  statement {
    sid       = "CloudWatchAlarmsOfTheAccount"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]

    principals {
      type        = "Service"
      identifiers = ["cloudwatch.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_sns_topic_policy" "alerts" {
  arn    = aws_sns_topic.alerts.arn
  policy = data.aws_iam_policy_document.alerts.json
}

# AWS sends one email with a link; the subscription delivers nothing before the link is opened.
resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# --- alarms -----------------------------------------------------------------------------------

locals {
  alarms = {
    status-check-failed = {
      description = "A status check of the host failed for 10 minutes."
      namespace   = "AWS/EC2"
      metric      = "StatusCheckFailed"
      statistic   = "Maximum"
      comparison  = "GreaterThanOrEqualToThreshold"
      threshold   = 1
      periods     = 2
      dimensions  = { InstanceId = var.instance_id }
    }
    cpu-high = {
      description = "The CPU of the host was above 90 percent for 30 minutes."
      namespace   = "AWS/EC2"
      metric      = "CPUUtilization"
      statistic   = "Average"
      comparison  = "GreaterThanThreshold"
      threshold   = 90
      periods     = 6
      dimensions  = { InstanceId = var.instance_id }
    }
    memory-high = {
      description = "The memory of the host was above 90 percent for 15 minutes."
      namespace   = "CWAgent"
      metric      = "mem_used_percent"
      statistic   = "Average"
      comparison  = "GreaterThanThreshold"
      threshold   = 90
      periods     = 3
      dimensions  = { InstanceId = var.instance_id }
    }
    disk-high = {
      description = "The root disk of the host is more than 80 percent full."
      namespace   = "CWAgent"
      metric      = "disk_used_percent"
      statistic   = "Maximum"
      comparison  = "GreaterThanThreshold"
      threshold   = 80
      periods     = 1
      dimensions  = { InstanceId = var.instance_id, path = "/" }
    }
    # AWS charges spent surplus credits when an instance stops, and the host stops every
    # night: a stopped instance sends no data point for the charge itself. The alarm
    # therefore watches the balance which the next stop would charge. 150 credits are 2.5
    # vCPU hours; the start of the stack in the morning stays far below.
    surplus-credits = {
      description = "The host has spent more than 150 surplus CPU credits. AWS charges the balance when the host stops."
      namespace   = "AWS/EC2"
      metric      = "CPUSurplusCreditBalance"
      statistic   = "Maximum"
      comparison  = "GreaterThanThreshold"
      threshold   = 150
      periods     = 1
      dimensions  = { InstanceId = var.instance_id }
    }
    # EventBridge Scheduler counts each call of a target which ends with an error. A start
    # which fails for lack of spot capacity is tried again for 12 hours; the alarm tells a
    # person at the first failure.
    schedule-failed = {
      description = "A start or a stop of the host by the schedule failed."
      namespace   = "AWS/Scheduler"
      metric      = "TargetErrorCount"
      statistic   = "Sum"
      comparison  = "GreaterThanOrEqualToThreshold"
      threshold   = 1
      periods     = 1
      dimensions  = { ScheduleGroup = var.schedule_group_name }
    }
  }
}

resource "aws_cloudwatch_metric_alarm" "host" {
  for_each = local.alarms

  alarm_name          = "${var.name_prefix}-host-${each.key}"
  alarm_description   = each.value.description
  namespace           = each.value.namespace
  metric_name         = each.value.metric
  dimensions          = each.value.dimensions
  statistic           = each.value.statistic
  comparison_operator = each.value.comparison
  threshold           = each.value.threshold
  period              = 300
  evaluation_periods  = each.value.periods
  # The host stops each night: no data is no alarm.
  treat_missing_data = "notBreaching"
  alarm_actions      = [aws_sns_topic.alerts.arn]
  ok_actions         = [aws_sns_topic.alerts.arn]
}
