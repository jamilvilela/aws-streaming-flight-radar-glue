#===============================================================================
# Glue Streaming Mini-Batch Module — Terraform
#===============================================================================
#
# This module provisions the AWS resources for the Glue Streaming Mini-Batch
# CDC pipeline. Resources are organized by service in dedicated files:
#
#   kms.tf        — KMS key + alias (encryption)
#   glue.tf       — Glue security config, VPC connection, jobs, workflow, triggers
#   iam.tf        — IAM role + policy (Lambda → Glue)
#   lambda.tf     — Lambda function + permission (starts full-load job)
#   cloudwatch.tf — EventBridge rule + target (full load completion poll)
#   dynamodb.tf   — Workflow lock table (starts the workflow exactly once)
#   s3.tf         — Artifact upload (main.py, helpers.zip, config.json)
#   data.tf       — Data sources (account, IAM role, VPC, subnets, SG)
#   locals.tf     — Computed values (buckets, spark_conf, tags)
#   variables.tf  — Input variables
#   outputs.tf    — Module outputs
#   versions.tf   — Provider versions
#
# No resources are declared here — see the service files above.
