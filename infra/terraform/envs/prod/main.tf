# Production composition for week 1 (blueprint 9.2): the two buckets, the Glue database and
# the budget. Network, host, IAM roles for the host, Athena, EMR Serverless and monitoring are
# added by the phases that need them.

data "aws_caller_identity" "current" {}

locals {
  name_prefix = "gbmi-prod"
}

module "lake_s3" {
  source = "../../modules/lake_s3"

  name_prefix = local.name_prefix
  account_id  = data.aws_caller_identity.current.account_id
}

module "glue_catalog" {
  source = "../../modules/glue_catalog"

  database_name = "gbmi_prod_silver"
  warehouse_uri = "s3://${module.lake_s3.lake_bucket}/warehouse"
}

module "budgets" {
  source = "../../modules/budgets"

  name               = "${local.name_prefix}-monthly"
  monthly_budget_usd = var.monthly_budget_usd
  alert_email        = var.alert_email
}
