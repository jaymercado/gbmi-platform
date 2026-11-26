output "budget_name" {
  description = "Name of the budget."
  value       = aws_budgets_budget.monthly.name
}

output "limit_usd" {
  description = "Monthly limit in US dollars."
  # Read from the variable, not from the resource. AWS returns the amount as "30.0", so an
  # output taken from the resource differs from the stored one on every plan after an apply.
  value = var.monthly_budget_usd
}
