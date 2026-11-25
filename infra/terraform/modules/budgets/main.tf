# One monthly cost budget with email notifications on actual spend (blueprint 9.2, 12.6).
# The action that stops the host at the last threshold is added in phase P4.
#
# Credits and refunds are left out of the budgeted amount. By default AWS nets them off, so on
# an account with credits the spend would read as zero and no alert would ever be sent.
#
# AWS only notifies a recipient who has confirmed the address: after the first apply, open the
# verification email and follow its link.

resource "aws_budgets_budget" "monthly" {
  name         = var.name
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  cost_types {
    include_credit = false
    include_refund = false
  }

  dynamic "notification" {
    for_each = toset(var.thresholds_percent)

    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value
      threshold_type             = "PERCENTAGE"
      notification_type          = "ACTUAL"
      subscriber_email_addresses = [var.alert_email]
    }
  }
}
