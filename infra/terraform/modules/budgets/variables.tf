variable "name" {
  description = "Name of the budget."
  type        = string
}

variable "monthly_budget_usd" {
  description = "Monthly cost limit in US dollars."
  type        = number

  validation {
    condition     = var.monthly_budget_usd > 0
    error_message = "monthly_budget_usd must be greater than zero."
  }
}

variable "alert_email" {
  description = "Address that receives the budget notifications."
  type        = string
  sensitive   = true
}

variable "thresholds_percent" {
  description = "Percentages of the limit at which a notification is sent, on actual spend."
  type        = list(number)
  default     = [50, 80, 100]
}
