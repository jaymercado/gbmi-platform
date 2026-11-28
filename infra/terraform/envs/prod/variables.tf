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

variable "github_repository" {
  description = "Repository whose commits of main the host deploys, as owner/name. Supplied as TF_VAR_github_repository, as for the bootstrap configuration."
  type        = string
}

variable "host_market" {
  description = "spot: a persistent spot request (about a third of the on-demand price). on_demand: the fallback when no spot capacity exists."
  type        = string
  default     = "spot"
}

variable "host_schedule_enabled" {
  description = "true: the host stops at 24:00 UTC and starts at 08:00 UTC each day (week 3 plan, difference D45)."
  type        = bool
  default     = true
}

variable "host_instance_type" {
  description = "Instance type of the host (blueprint 9.2)."
  type        = string
  default     = "t4g.large"
}

variable "host_zone" {
  description = "Availability zone of the subnet and of the host. The status report records why the zone was chosen."
  type        = string
  default     = "us-east-1a"
}
