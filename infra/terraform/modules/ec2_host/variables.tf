variable "name_prefix" {
  description = "Prefix of the resource names, for example gbmi-prod."
  type        = string
}

variable "host_name" {
  description = "Value of the tag Name of the instance. The workflow deploy and the scheduler role find the host by the tag."
  type        = string
}

variable "instance_type" {
  description = "Instance type of the host: an arm64 type with 8 GB (blueprint 9.2)."
  type        = string
  default     = "t4g.large"
}

variable "market" {
  description = "spot: a persistent spot request which stops the instance on an interruption. on_demand: the fallback when no spot capacity exists."
  type        = string
  default     = "spot"

  validation {
    condition     = contains(["spot", "on_demand"], var.market)
    error_message = "market must be spot or on_demand."
  }
}

variable "subnet_id" {
  description = "Public subnet of the host."
  type        = string
}

variable "security_group_id" {
  description = "Security group of the host: no inbound rule."
  type        = string
}

variable "instance_profile_name" {
  description = "Instance profile which gives the host its role."
  type        = string
}

variable "scheduler_role_arn" {
  description = "Role with which EventBridge Scheduler starts and stops the host."
  type        = string
}

variable "schedule_enabled" {
  description = "true: the host stops at 24:00 UTC and starts at 08:00 UTC each day. false: the two schedules exist and do nothing."
  type        = bool
  default     = true
}

variable "github_repository" {
  description = "Repository whose commits of main the host deploys, as owner/name."
  type        = string

  validation {
    condition     = can(regex("^[^/]+/[^/]+$", var.github_repository))
    error_message = "github_repository must have the form owner/name."
  }
}

variable "host_log_group_name" {
  description = "Log group which receives the two logs of the host: the first boot and the deployments."
  type        = string
}

variable "root_volume_gb" {
  description = "Size of the encrypted gp3 root volume in GiB."
  type        = number
  default     = 50
}

variable "compose_version" {
  description = "Release of the Docker Compose plugin which the user data installs: the release with which the overlay compose.prod.yaml was tested."
  type        = string
  default     = "v2.36.2"
}

variable "compose_sha256" {
  description = "SHA-256 of the file docker-compose-linux-aarch64 of the release, from the checksum file of the release."
  type        = string
  default     = "d1148609319706a57b755ff0f61d604a63a8cf57adb24c17535baa766ff14b4f"
}
