# GitHub Actions assumes roles through OIDC; there are no long-lived AWS keys (blueprint 5.9).
#
#   gbmi-ci-plan    pull requests: read-only access for `terraform plan`, plus the state lock
#   gbmi-ci-deploy  jobs in the protected environment: state access only until phase P2
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

# Reading the state, and taking and releasing its lock file (the S3 backend's native locking
# needs get, put and delete on <key>.tflock).
data "aws_iam_policy_document" "state_read_and_lock" {
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

  statement {
    sid       = "LockState"
    actions   = ["s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.state.arn}/*.tflock"]
  }
}

data "aws_iam_policy_document" "state_write" {
  source_policy_documents = [data.aws_iam_policy_document.state_read_and_lock.json]

  statement {
    sid       = "WriteState"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.state.arn}/*"]
  }
}

# What `terraform plan` has to read for the resources that exist in week 1, scoped to this
# project's names. It grows with the infrastructure (network, host and their roles arrive in
# phase P2).
data "aws_region" "current" {}

locals {
  iam_arn  = "arn:aws:iam::${local.account_id}"
  glue_arn = "arn:aws:glue:${data.aws_region.current.region}:${local.account_id}"
}

data "aws_iam_policy_document" "plan_read_only" {
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
}

resource "aws_iam_role" "ci_plan" {
  name                 = "gbmi-ci-plan"
  description          = "GitHub Actions on pull requests: terraform plan (read-only)."
  assume_role_policy   = data.aws_iam_policy_document.assume_plan.json
  max_session_duration = 3600
}

resource "aws_iam_role_policy" "ci_plan_state" {
  name   = "state-read-and-lock"
  role   = aws_iam_role.ci_plan.id
  policy = data.aws_iam_policy_document.state_read_and_lock.json
}

resource "aws_iam_role_policy" "ci_plan_read_only" {
  name   = "plan-read-only"
  role   = aws_iam_role.ci_plan.id
  policy = data.aws_iam_policy_document.plan_read_only.json
}

resource "aws_iam_role" "ci_deploy" {
  name                 = "gbmi-ci-deploy"
  description          = "GitHub Actions in the protected environment: terraform apply and deployment (from phase P2)."
  assume_role_policy   = data.aws_iam_policy_document.assume_deploy.json
  max_session_duration = 3600
}

resource "aws_iam_role_policy" "ci_deploy_state" {
  name   = "state-read-write"
  role   = aws_iam_role.ci_deploy.id
  policy = data.aws_iam_policy_document.state_write.json
}
