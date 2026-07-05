#===============================================================================
# Main Resources — Glue Streaming Mini-Batch DMS Module
#===============================================================================

# ── KMS Key for Glue encryption ──────────────────────────────────────────────

resource "aws_kms_key" "glue" {
  description             = "KMS key for Glue Streaming Mini-Batch DMS job encryption"
  deletion_window_in_days = 30
  enable_key_rotation     = true

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "EnableIAMAdminAccess"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${local.account_id}:root"
        }
        Action   = "kms:*"
        Resource = "*"
      },
      {
        Sid    = "AllowGlueServiceAccess"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = "*"
      }
    ]
  })

  tags = merge(local.common_tags, {
    Name = local.kms_key_alias
  })
}

resource "aws_kms_alias" "glue" {
  name          = local.kms_key_alias
  target_key_id = aws_kms_key.glue.key_id
}

# ── Glue Security Configuration ──────────────────────────────────────────────

resource "aws_glue_security_configuration" "glue" {
  name = "glue-streaming-minibatch-dms-security-config"

  encryption_configuration {
    cloudwatch_encryption {
      cloudwatch_encryption_mode = "SSE-KMS"
      kms_key_arn               = aws_kms_key.glue.arn
    }

    job_bookmarks_encryption {
      job_bookmarks_encryption_mode = "CSE-KMS"
      kms_key_arn                  = aws_kms_key.glue.arn
    }

    s3_encryption {
      s3_encryption_mode = "SSE-KMS"
      kms_key_arn        = aws_kms_key.glue.arn
    }
  }
}

# ── Glue Connection (VPC) ────────────────────────────────────────────────────

resource "aws_glue_connection" "vpc" {
  name = "glue-streaming-minibatch-dms-vpc"

  connection_properties = {
    CONNECTION_TYPE = "NETWORK"
  }

  physical_connection_requirements {
    availability_zone      = data.aws_subnet.all[keys(data.aws_subnet.all)[0]].availability_zone
    # Prefer private subnets; fall back to any subnet if none are private
    subnet_id              = length(data.aws_subnets.private.ids) > 0 ? data.aws_subnets.private.ids[0] : data.aws_subnets.all.ids[0]
    security_group_id_list = [data.aws_security_group.default.id]
  }

  tags = merge(local.common_tags, {
    Name = "glue-streaming-minibatch-dms-vpc"
  })
}

# ── Glue Job — Streaming Mini-Batch DMS ──────────────────────────────────────

resource "aws_glue_job" "streaming_minibatch_dms" {
  name              = var.glue_job_name
  role_arn          = data.aws_iam_role.datalake_analytics.arn
  glue_version      = "5.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = var.glue_job_timeout

  command {
    script_location = local.script_location
    python_version  = "3.10"
  }

  default_arguments = {
    # Job bookmarks & logging
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--continuous-log-logGroup"          = "/aws-glue/jobs/${var.glue_job_name}"
    "--enable-auto-scaling"              = "true"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"

    # Job configuration
    "--origins_s3_path"       = local.origins_s3_path
    "--target_s3_path"        = local.target_s3_path
    "--extra-py-files"        = local.extra_py_files

    # Spark configs passed via --conf (parsed dynamically by main.py)
    "--conf"                  = local.spark_conf

    # Security configuration
    "--encryption-type"        = "sse-s3-kms"
    "--security-configuration" = aws_glue_security_configuration.glue.name
  }

  # Associate Glue connection if provided
  connections = length(var.glue_connections) > 0 ? var.glue_connections : [aws_glue_connection.vpc.name]

  tags = merge(local.common_tags, {
    Name = var.glue_job_name
  })
}

# ── Archive (helpers.zip) ──────────────────────────────────────────────────
# Creates a zip of the dependencies directory using hashicorp/archive provider.

data "archive_file" "helpers" {
  type        = "zip"
  source_dir  = "${path.module}/../app/src/dependencies/"
  output_path = "${path.module}/../.terraform/helpers.zip"
  excludes    = ["__pycache__", "*.pyc"]
}

# ── S3 Artifact Upload ─────────────────────────────────────────────────────
# Uploads scripts, dependencies, and configs to the workspace bucket
# using declarative aws_s3_object resources.

resource "aws_s3_object" "main_py" {
  bucket       = local.buckets.workspace
  key          = "scripts/glue-streaming-minibatch-dms/main.py"
  source       = "${path.module}/../app/src/main.py"
  source_hash  = filemd5("${path.module}/../app/src/main.py")
  tags         = local.common_tags
}

resource "aws_s3_object" "helpers_zip" {
  bucket       = local.buckets.workspace
  key          = "dependencies/helpers.zip"
  source       = data.archive_file.helpers.output_path
  source_hash  = data.archive_file.helpers.output_md5
  tags         = local.common_tags
}

resource "aws_s3_object" "origins_json" {
  bucket       = local.buckets.workspace
  key          = "config/origins.json"
  content      = replace(file("${path.module}/../app/src/dependencies/config/origins.json"), "{account_id}", local.account_id)
  content_type = "application/json"
  tags         = local.common_tags
}

resource "aws_s3_object" "target_json" {
  bucket       = local.buckets.workspace
  key          = "config/target.json"
  content      = replace(file("${path.module}/../app/src/dependencies/config/target.json"), "{account_id}", local.account_id)
  content_type = "application/json"
  tags         = local.common_tags
}
