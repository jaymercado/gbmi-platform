terraform {
  required_version = "~> 1.17.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.74"
    }
  }

  # Bucket, key and region come from the git-ignored backend.hcl (see backend.hcl.example).
  # Locking uses S3's native lock file; there is no DynamoDB table (blueprint 9.4).
  backend "s3" {
    use_lockfile = true
  }
}

provider "aws" {
  region = var.aws_region

  # Every resource carries these tags for cost allocation (blueprint 9.4).
  default_tags {
    tags = {
      project = "gbmi"
      env     = "prod"
    }
  }
}
