# GitHub Actions assumes roles through OIDC; there are no long-lived AWS keys (blueprint 5.9).
#
#   gbmi-ci-plan    pull requests: read-only access for `terraform plan`
#   gbmi-ci-deploy  jobs in the protected environment: send one command document to the host
#
# No role of CI can apply Terraform: a person applies on the laptop (week 3 plan, D40).
#
# Repositories created after 15 July 2026 issue an immutable subject that carries the owner
# and repository ids:  repo:OWNER@OWNER-ID/REPO@REPO-ID:<context>

locals {
  github_issuer  = "token.actions.githubusercontent.com"
  github_owner   = split("/", var.github_repository)[0]
  github_repo    = split("/", var.github_repository)[1]
  github_subject = "repo:${local.github_owner}@${var.github_owner_id}/${local.github_repo}@${var.github_repository_id}"

  oidc_provider_arn = (
    var.create_github_oidc_provider
    ? aws_iam_openid_connect_provider.github[0].arn
    : data.aws_iam_openid_connect_provider.github[0].arn
  )
}

resource "aws_iam_openid_connect_provider" "github" {
  count = var.create_github_oidc_provider ? 1 : 0

  url            = "https://${local.github_issuer}"
  client_id_list = ["sts.amazonaws.com"]
}

data "aws_iam_openid_connect_provider" "github" {
  count = var.create_github_oidc_provider ? 0 : 1

  url = "https://${local.github_issuer}"
}

data "aws_iam_policy_document" "assume_plan" {
  statement {
    sid     = "GitHubPullRequests"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [local.oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.github_issuer}:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.github_issuer}:sub"
      values   = ["${local.github_subject}:pull_request"]
    }
  }
}

data "aws_iam_policy_document" "assume_deploy" {
  statement {
    sid     = "GitHubProtectedEnvironment"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [local.oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.github_issuer}:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.github_issuer}:sub"
      values   = ["${local.github_subject}:environment:${var.github_environment}"]
    }
  }
}

# Reading the state. The job plan takes no lock (tools/ci_plan.py runs the plan with
# -lock=false): a job which GitHub cancels halfway would leave the lock file behind, and the
# next apply on the laptop would stop. A plan reads one whole state file and writes nothing,
# so the role has no write access to the state bucket.
data "aws_iam_policy_document" "state_read" {
  statement {
    sid       = "ListStateBucket"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.state.arn]
  }

  statement {
    sid       = "ReadState"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.state.arn}/*"]
  }
}

# What `terraform plan` has to read, scoped to the names of the project wherever the service
# allows a resource. Week 1: buckets, the Glue database, the budget and the identities.
# Week 3: the network, the host, its command document, the log groups, the alarms, the topic
# and the schedules. The role never reads a parameter below /gbmi/prod/: the secrets of the
# host are no part of Terraform (week 3 plan, D43).
data "aws_region" "current" {}

locals {
  region   = data.aws_region.current.region
  iam_arn  = "arn:aws:iam::${local.account_id}"
  glue_arn = "arn:aws:glue:${local.region}:${local.account_id}"
}

data "aws_iam_policy_document" "plan_read_only" {
  #checkov:skip=CKV_AWS_356:The Describe actions of EC2 and the listing of log groups accept no resource. Every other statement names resources of the account.
  statement {
    sid = "ReadProjectBuckets"
    actions = [
      "s3:GetAccelerateConfiguration",
      "s3:GetBucket*",
      "s3:GetEncryptionConfiguration",
      "s3:GetLifecycleConfiguration",
      "s3:GetReplicationConfiguration",
      "s3:ListBucket",
    ]
    resources = ["arn:aws:s3:::gbmi-*"]
  }

  statement {
    sid     = "ReadGlueCatalog"
    actions = ["glue:GetDatabase", "glue:GetDatabases", "glue:GetTags"]
    resources = [
      "${local.glue_arn}:catalog",
      "${local.glue_arn}:database/gbmi_*",
    ]
  }

  statement {
    sid       = "ReadBudgets"
    actions   = ["budgets:ViewBudget", "budgets:ListTagsForResource"]
    resources = ["arn:aws:budgets::${local.account_id}:budget/gbmi-*"]
  }

  statement {
    sid = "ReadIdentity"
    actions = [
      "iam:GetOpenIDConnectProvider",
      "iam:GetPolicy",
      "iam:GetPolicyVersion",
      "iam:GetRole",
      "iam:GetRolePolicy",
      "iam:ListAttachedRolePolicies",
      "iam:ListRolePolicies",
    ]
    resources = [
      "${local.iam_arn}:role/gbmi-*",
      "${local.iam_arn}:policy/gbmi-*",
      "${local.iam_arn}:oidc-provider/${local.github_issuer}",
    ]
  }

  statement {
    sid       = "ReadInstanceProfiles"
    actions   = ["iam:GetInstanceProfile"]
    resources = ["${local.iam_arn}:instance-profile/gbmi-*"]
  }

  # The Describe actions of EC2 take no resource: the network, the instance, its disk, its
  # spot request and the endpoint.
  statement {
    sid       = "ReadNetworkAndHost"
    actions   = ["ec2:Describe*"]
    resources = ["*"]
  }

  statement {
    sid = "ReadCommandDocument"
    actions = [
      "ssm:DescribeDocument",
      "ssm:DescribeDocumentPermission",
      "ssm:GetDocument",
      "ssm:ListTagsForResource",
    ]
    resources = ["arn:aws:ssm:${local.region}:${local.account_id}:document/gbmi-*"]
  }

  # The public parameter of AWS which names the newest Amazon Linux image; no parameter of
  # the account.
  statement {
    sid       = "ReadPublicImageParameter"
    actions   = ["ssm:GetParameter"]
    resources = ["arn:aws:ssm:${local.region}::parameter/aws/service/ami-amazon-linux-latest/*"]
  }

  # Listing log groups is one request for the account, whatever name the request asks for:
  # the statement cannot name the groups of the project. The tags are read for the groups of
  # the project only.
  statement {
    sid       = "ListLogGroups"
    actions   = ["logs:DescribeLogGroups"]
    resources = ["*"]
  }

  statement {
    sid       = "ReadLogGroupTags"
    actions   = ["logs:ListTagsForResource"]
    resources = ["arn:aws:logs:${local.region}:${local.account_id}:log-group:/gbmi/*"]
  }

  # Reading alarms: every alarm of the account in the region. CloudWatch can check a request
  # for alarms against the whole list, so a statement for the names gbmi-* alone could refuse
  # the read of the provider.
  statement {
    sid       = "ReadAlarms"
    actions   = ["cloudwatch:DescribeAlarms", "cloudwatch:ListTagsForResource"]
    resources = ["arn:aws:cloudwatch:${local.region}:${local.account_id}:alarm:*"]
  }

  statement {
    sid = "ReadAlertTopic"
    actions = [
      "sns:GetSubscriptionAttributes",
      "sns:GetTopicAttributes",
      "sns:ListTagsForResource",
    ]
    resources = ["arn:aws:sns:${local.region}:${local.account_id}:gbmi-*"]
  }

  statement {
    sid = "ReadSchedules"
    actions = [
      "scheduler:GetSchedule",
      "scheduler:GetScheduleGroup",
      "scheduler:ListTagsForResource",
    ]
    resources = [
      "arn:aws:scheduler:${local.region}:${local.account_id}:schedule/gbmi-*/*",
      "arn:aws:scheduler:${local.region}:${local.account_id}:schedule-group/gbmi-*",
    ]
  }
}

# The deployment workflow can do one thing on the host: send the command document
# gbmi-prod-deploy, whose one parameter is a commit, to an instance of the project. The script
# behind the document decides what happens (week 3 plan, D41). The role reads the result of a
# command and finds the instance; the two read actions take no resource.
data "aws_iam_policy_document" "deploy_send_command" {
  statement {
    sid       = "SendTheDeployDocument"
    actions   = ["ssm:SendCommand"]
    resources = ["arn:aws:ssm:${local.region}:${local.account_id}:document/gbmi-prod-deploy"]
  }

  statement {
    sid       = "ToInstancesOfTheProject"
    actions   = ["ssm:SendCommand"]
    resources = ["arn:aws:ec2:${local.region}:${local.account_id}:instance/*"]

    condition {
      test     = "StringEquals"
      variable = "ssm:resourceTag/project"
      values   = ["gbmi"]
    }
  }

  statement {
    sid       = "ReadTheResultAndFindTheHost"
    actions   = ["ssm:GetCommandInvocation", "ec2:DescribeInstances"]
    resources = ["*"]
  }
}

resource "aws_iam_role" "ci_plan" {
  name                 = "gbmi-ci-plan"
  description          = "GitHub Actions on pull requests: terraform plan (read-only)."
  assume_role_policy   = data.aws_iam_policy_document.assume_plan.json
  max_session_duration = 3600
}

resource "aws_iam_role_policy" "ci_plan_state" {
  name   = "state-read"
  role   = aws_iam_role.ci_plan.id
  policy = data.aws_iam_policy_document.state_read.json
}

resource "aws_iam_role_policy" "ci_plan_read_only" {
  name   = "plan-read-only"
  role   = aws_iam_role.ci_plan.id
  policy = data.aws_iam_policy_document.plan_read_only.json
}

resource "aws_iam_role" "ci_deploy" {
  name                 = "gbmi-ci-deploy"
  description          = "GitHub Actions in the protected environment: send the deployment command to the host."
  assume_role_policy   = data.aws_iam_policy_document.assume_deploy.json
  max_session_duration = 3600
}

resource "aws_iam_role_policy" "ci_deploy_send_command" {
  name   = "send-deploy-command"
  role   = aws_iam_role.ci_deploy.id
  policy = data.aws_iam_policy_document.deploy_send_command.json
}
