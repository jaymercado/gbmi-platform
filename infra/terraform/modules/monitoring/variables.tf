variable "name_prefix" {
  description = "Prefix of the resource names, for example gbmi-prod."
  type        = string
}

variable "alert_email" {
  description = "Address which receives the alarms. Supplied as TF_VAR_alert_email, never committed."
  type        = string
  sensitive   = true
}

variable "instance_id" {
  description = "Id of the host instance which the alarms watch."
  type        = string
}

variable "log_group_prefix" {
  description = "Path of the two log groups of the host."
  type        = string
  default     = "/gbmi/prod"
}

variable "log_retention_days" {
  description = "Days which CloudWatch keeps a log line (blueprint 7.9)."
  type        = number
  default     = 30
}
