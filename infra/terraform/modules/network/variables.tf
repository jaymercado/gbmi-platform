variable "name_prefix" {
  description = "Prefix of every resource name, for example gbmi-prod."
  type        = string
}

variable "vpc_cidr" {
  description = "Address range of the VPC."
  type        = string
  default     = "10.42.0.0/16"
}

variable "public_subnet_cidr" {
  description = "Address range of the one public subnet."
  type        = string
  default     = "10.42.1.0/24"
}

variable "availability_zone" {
  description = "Availability zone of the subnet and so of the host."
  type        = string
}
