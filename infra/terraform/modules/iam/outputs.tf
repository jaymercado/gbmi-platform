output "host_role_name" {
  description = "Name of the role of the host."
  value       = aws_iam_role.host.name
}

output "host_role_arn" {
  description = "ARN of the role of the host."
  value       = aws_iam_role.host.arn
}

output "host_instance_profile_name" {
  description = "Instance profile which gives the host its role."
  value       = aws_iam_instance_profile.host.name
}

output "clickhouse_lake_role_arn" {
  description = "Role through which ClickHouse reads the lake: GBMI_CH_LAKE_ROLE_ARN on the host."
  value       = aws_iam_role.clickhouse_lake.arn
}

output "host_scheduler_role_arn" {
  description = "Role with which EventBridge Scheduler starts and stops the host."
  value       = aws_iam_role.host_scheduler.arn
}
