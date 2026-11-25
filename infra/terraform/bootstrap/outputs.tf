output "state_bucket" {
  description = "Bucket that holds the Terraform state of every configuration. Goes into backend.hcl."
  value       = aws_s3_bucket.state.bucket
}

output "ci_plan_role_arn" {
  description = "Role assumed by pull-request workflows."
  value       = aws_iam_role.ci_plan.arn
}

output "ci_deploy_role_arn" {
  description = "Role assumed by jobs in the protected GitHub environment."
  value       = aws_iam_role.ci_deploy.arn
}

output "github_oidc_subject_prefix" {
  description = "Prefix of the OIDC subject the roles trust. Compare it with a token's sub claim if a role cannot be assumed."
  value       = local.github_subject
}
