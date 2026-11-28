variable "lake_bucket_arn" {
  description = "ARN of the lake bucket: raw/, quarantine/ and warehouse/."
  type        = string
}

variable "ops_bucket_arn" {
  description = "ARN of the ops bucket: airflow-logs/, artifacts/, backups/ and tmp/."
  type        = string
}

variable "glue_database_name" {
  description = "Glue database of the silver tables."
  type        = string
}

variable "parameter_prefix" {
  description = "Path of the Parameter Store entries of the host, without a slash at the end."
  type        = string
  default     = "/gbmi/prod"
}

variable "log_group_arns" {
  description = "ARNs of the log groups to which the host and its containers write."
  type        = list(string)
}

variable "host_name" {
  description = "Value of the tag Name of the host instance; the scheduler role may start and stop that instance only."
  type        = string
}
