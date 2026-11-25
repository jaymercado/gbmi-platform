variable "database_name" {
  description = "Glue database that holds the silver Iceberg tables, for example gbmi_prod_silver."
  type        = string
}

variable "warehouse_uri" {
  description = "S3 URI of the Iceberg warehouse, without a trailing slash, for example s3://bucket/warehouse."
  type        = string

  validation {
    condition     = can(regex("^s3://[^/]+(/.*[^/])?$", var.warehouse_uri))
    error_message = "warehouse_uri must be an s3:// URI without a trailing slash."
  }
}
