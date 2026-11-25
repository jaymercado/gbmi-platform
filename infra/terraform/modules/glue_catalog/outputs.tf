output "database_name" {
  description = "Name of the Glue database."
  value       = aws_glue_catalog_database.silver.name
}

output "location_uri" {
  description = "Location under which the database's tables are created."
  value       = aws_glue_catalog_database.silver.location_uri
}
