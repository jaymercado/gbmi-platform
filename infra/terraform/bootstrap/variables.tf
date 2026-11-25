variable "aws_region" {
  description = "Region of the state bucket and of every regional resource."
  type        = string
  default     = "us-east-1"
}

variable "github_repository" {
  description = "Repository that may assume the CI roles, as owner/name. Supplied as TF_VAR_github_repository."
  type        = string

  validation {
    condition     = can(regex("^[^/]+/[^/]+$", var.github_repository))
    error_message = "github_repository must have the form owner/name."
  }
}

variable "github_owner_id" {
  description = "Numeric id of the repository owner (gh api repos/OWNER/REPO --jq .owner.id). Repositories created after 15 July 2026 put it in the OIDC subject."
  type        = number
}

variable "github_repository_id" {
  description = "Numeric id of the repository (gh api repos/OWNER/REPO --jq .id). Part of the OIDC subject, like the owner id."
  type        = number
}

variable "github_environment" {
  description = "GitHub environment whose jobs may assume the deploy role."
  type        = string
  default     = "prod"
}

variable "create_github_oidc_provider" {
  description = "An AWS account can hold one OIDC provider per issuer URL. Set to false when the account already has the GitHub one; it is then looked up, not created."
  type        = bool
  default     = true
}
