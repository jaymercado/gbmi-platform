variable "name_prefix" {
  description = "Prefix of both bucket names, for example gbmi-prod."
  type        = string
}

variable "account_id" {
  description = "AWS account id, appended to the bucket names to make them globally unique."
  type        = string
}

variable "gkg_glacier_transition_enabled" {
  description = "Move raw GKG files to Glacier Instant Retrieval after 30 days. Stays off until the bulk backfill and its benchmarks are done (phase P5, blueprint 7.9)."
  type        = bool
  default     = false
}
