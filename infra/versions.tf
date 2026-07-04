#===============================================================================
# Terraform & Provider Versions
#===============================================================================

terraform {
  required_version = ">= 1.5"

  backend "s3" {
    bucket = "lakehouse-workspace-331504768406"
    key    = "terraform/state/streaming-glue-job"
    region = "us-east-1"
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}
