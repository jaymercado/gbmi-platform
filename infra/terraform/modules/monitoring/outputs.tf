output "log_group_arns" {
  description = "ARNs of the two log groups of the host."
  value       = [for group in aws_cloudwatch_log_group.this : group.arn]
}

output "container_log_group_name" {
  description = "Log group of the container output: GBMI_LOG_GROUP on the host."
  value       = aws_cloudwatch_log_group.this["containers"].name
}

output "host_log_group_name" {
  description = "Log group of the first boot and of the deployments."
  value       = aws_cloudwatch_log_group.this["host"].name
}

output "alerts_topic_arn" {
  description = "Topic which receives the alarms."
  value       = aws_sns_topic.alerts.arn
}

output "alarm_names" {
  description = "Names of the five alarms of the host."
  value       = [for alarm in aws_cloudwatch_metric_alarm.host : alarm.alarm_name]
}
