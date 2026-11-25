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
