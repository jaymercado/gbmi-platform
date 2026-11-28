output "lake_bucket" {
  description = "Lake bucket: GBMI_LAKE_URI is s3://<this value>."
  value       = module.lake_s3.lake_bucket
}

output "ops_bucket" {
  description = "Ops bucket: backups, logs, artifacts and Athena results."
  value       = module.lake_s3.ops_bucket
}

output "silver_database" {
  description = "Glue database of the silver tables."
  value       = module.glue_catalog.database_name
}

output "silver_location" {
  description = "Location under which silver tables are created."
  value       = module.glue_catalog.location_uri
}

output "monthly_budget_usd" {
  description = "Monthly cost limit currently in force."
  value       = module.budgets.limit_usd
}

output "host_instance_id" {
  description = "Id of the host instance."
  value       = module.ec2_host.instance_id
}

output "host_role_arn" {
  description = "Role of the host."
  value       = module.iam.host_role_arn
}

output "clickhouse_lake_role_arn" {
  description = "Role through which ClickHouse reads the lake."
  value       = module.iam.clickhouse_lake_role_arn
}

output "deploy_document" {
  description = "Command document which the deployment workflow sends to the host."
  value       = module.ec2_host.deploy_document_name
}

output "container_log_group" {
  description = "Log group of the container output."
  value       = module.monitoring.container_log_group_name
}

output "host_log_group" {
  description = "Log group of the first boot and of the deployments."
  value       = module.monitoring.host_log_group_name
}

output "alerts_topic_arn" {
  description = "Topic which receives the alarms of the host."
  value       = module.monitoring.alerts_topic_arn
}
