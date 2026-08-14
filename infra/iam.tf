#===============================================================================
# IAM — Roles and Policies
#===============================================================================

# ── Glue Job Role — Catalog & Connections ─────────────────────────────────────
# The existing Glue role (role-datalake-analytics) runs the jobs, which use a
# VPC NETWORK connection. Glue must be able to read connection metadata from
# the Data Catalog (glue:GetConnection/GetConnections).

resource "aws_iam_role_policy" "glue_catalog_connections" {
  name = "glue-catalog-connections"
  role = data.aws_iam_role.datalake_analytics.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:GetConnection",
          "glue:GetConnections",
        ]
        Resource = [
          "arn:aws:glue:${var.region}:${local.account_id}:catalog",
          "arn:aws:glue:${var.region}:${local.account_id}:connection/*",
        ]
      },
    ]
  })
}

# ── Glue Job Role — Data Catalog tables ───────────────────────────────────────
# Spark reads/writes Delta tables resolved through the Glue Catalog
# (DeltaTable.forName + enableHiveSupport), so the role needs read access
# to databases, tables, partitions and UDFs in the catalog.

resource "aws_iam_role_policy" "glue_catalog_tables" {
  name = "glue-catalog-tables"
  role = data.aws_iam_role.datalake_analytics.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:GetUserDefinedFunction",
          "glue:GetDataCatalogEncryptionSettings",
        ]
        Resource = [
          "arn:aws:glue:${var.region}:${local.account_id}:catalog",
          "arn:aws:glue:${var.region}:${local.account_id}:database/*",
          "arn:aws:glue:${var.region}:${local.account_id}:table/*/*",
          "arn:aws:glue:${var.region}:${local.account_id}:userDefinedFunction/*/*/*",
        ]
      },
    ]
  })
}

# ── Glue Job Role — S3, KMS and CloudWatch Logs ──────────────────────────────
# The Glue jobs read full-load/CDC parquet from landing, write Delta to raw,
# and read scripts/config/dependencies from workspace. Writes use SSE-KMS
# (glue KMS key) and job logs go to CloudWatch.

resource "aws_iam_role_policy" "glue_data_access" {
  name = "glue-data-access"
  role = data.aws_iam_role.datalake_analytics.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:ListBucketMultipartUploads",
          "s3:ListMultipartUploadParts",
          "s3:AbortMultipartUpload",
        ]
        Resource = [
          "arn:aws:s3:::${local.buckets.landing}",
          "arn:aws:s3:::${local.buckets.landing}/*",
          "arn:aws:s3:::${local.buckets.raw}",
          "arn:aws:s3:::${local.buckets.raw}/*",
          "arn:aws:s3:::${local.buckets.workspace}",
          "arn:aws:s3:::${local.buckets.workspace}/*",
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:Encrypt",
          "kms:GenerateDataKey",
          "kms:ReEncryptFrom",
          "kms:ReEncryptTo",
          "kms:DescribeKey",
        ]
        Resource = [
          aws_kms_key.glue.arn,
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = [
          "arn:aws:logs:${var.region}:${local.account_id}:log-group:/aws-glue/jobs/*:*",
        ]
      },
    ]
  })
}

# ── Lambda IAM Role ──────────────────────────────────────────────────────────
# Role for the Lambda function that starts the Glue full-load batch job.

resource "aws_iam_role" "lambda_glue_starter" {
  name = "role-lambda-start-${var.full_load_job_name}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = merge(local.common_tags, {
    Name = "role-lambda-start-${var.full_load_job_name}"
  })
}

resource "aws_iam_role_policy" "lambda_glue_starter" {
  name = "lambda-start-glue-policy"
  role = aws_iam_role.lambda_glue_starter.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartWorkflowRun",
        ]
        Resource = [
          aws_glue_workflow.dms_full_load.arn,
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = [
          "arn:aws:logs:${var.region}:${local.account_id}:log-group:/aws/lambda/${var.full_load_job_name}-starter:*",
        ]
      },
    ]
  })
}