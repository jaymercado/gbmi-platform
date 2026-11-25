terraform {
  required_version = "~> 1.17.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.74"
    }
  }

  # State lives in the bucket this configuration creates, so the very first apply runs with
  # local state and is then migrated (see ../README.md). Bucket, key and region come from the
  # git-ignored backend.hcl; locking uses S3's native lock file, with no DynamoDB table.
  backend "s3" {
    use_lockfile = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      project = "gbmi"
      env     = "prod"
      stack   = "bootstrap"
    }
  }
}
