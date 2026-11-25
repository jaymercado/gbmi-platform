output "budget_name" {
  description = "Name of the budget."
  value       = aws_budgets_budget.monthly.name
}

output "limit_usd" {
  description = "Monthly limit in US dollars."
  value       = aws_budgets_budget.monthly.limit_amount
}
