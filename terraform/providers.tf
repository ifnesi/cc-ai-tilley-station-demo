terraform {
  required_version = ">= 1.5"
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "2.86.0"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.5"
    }
  }
}

# Auth via environment variables (see the repo-root .env / README):
#   CONFLUENT_CLOUD_API_KEY, CONFLUENT_CLOUD_API_SECRET
# The Bedrock connection's AWS keys come from TF_VAR_aws_access_key /
# TF_VAR_aws_secret_key (also in .env). No hashicorp/aws provider is needed —
# the connection is a native Confluent resource.
provider "confluent" {}
