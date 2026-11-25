variable "aws_region" {
  description = "Region of every regional resource."
  type        = string
  default     = "us-east-1"
}

variable "alert_email" {
  description = "Address that receives budget notifications. Supplied as TF_VAR_alert_email, never committed."
  type        = string
  sensitive   = true
}

variable "monthly_budget_usd" {
  description = "Monthly cost limit in US dollars (blueprint 9.2). The alert test lowers it for a day through the git-ignored budget-test.auto.tfvars."
  type        = number
  default     = 30
}
