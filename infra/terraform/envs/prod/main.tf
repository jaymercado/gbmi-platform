# Production composition (blueprint 9.2). Week 1: the two buckets, the Glue database and the
# budget. Week 3: the network, the roles, the host and its monitoring. Athena and EMR
# Serverless are added by the phases that need them.

data "aws_caller_identity" "current" {}

locals {
  name_prefix = "gbmi-prod"
  # The tag Name of the instance: the deployment workflow and the scheduler role find the
  # host by the tag.
  host_name = "${local.name_prefix}-host"
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

module "network" {
  source = "../../modules/network"

  name_prefix       = local.name_prefix
  availability_zone = var.host_zone
}

module "monitoring" {
  source = "../../modules/monitoring"

  name_prefix = local.name_prefix
  alert_email = var.alert_email
  instance_id = module.ec2_host.instance_id
}

module "iam" {
  source = "../../modules/iam"

  lake_bucket_arn    = module.lake_s3.lake_bucket_arn
  ops_bucket_arn     = module.lake_s3.ops_bucket_arn
  glue_database_name = module.glue_catalog.database_name
  log_group_arns     = module.monitoring.log_group_arns
  host_name          = local.host_name
}

module "ec2_host" {
  source = "../../modules/ec2_host"

  name_prefix           = local.name_prefix
  host_name             = local.host_name
  instance_type         = var.host_instance_type
  market                = var.host_market
  subnet_id             = module.network.public_subnet_id
  security_group_id     = module.network.host_security_group_id
  instance_profile_name = module.iam.host_instance_profile_name
  scheduler_role_arn    = module.iam.host_scheduler_role_arn
  schedule_enabled      = var.host_schedule_enabled
  github_repository     = var.github_repository
  host_log_group_name   = module.monitoring.host_log_group_name
}
