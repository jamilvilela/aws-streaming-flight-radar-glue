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
    "--config_s3_path"        = local.config_s3_path
    "--source"                = "flights"
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

# ── S3 Artifact Upload (via Terraform) ──────────────────────────────────────
# Uploads Python scripts and JSON configs to the workspace bucket on apply.
# Uses null_resource with local-exec so files are synced without managing
# individual aws_s3_object resources.

resource "null_resource" "upload_artifacts" {
  triggers = {
    main_py_hash       = filesha1("${path.module}/../src/main.py")
    processor_py_hash  = filesha1("${path.module}/../src/processor.py")
    config_py_hash     = filesha1("${path.module}/../src/config.py")
    reader_py_hash     = filesha1("${path.module}/../src/reader.py")
    quality_py_hash    = filesha1("${path.module}/../src/data_quality.py")
    writer_py_hash     = filesha1("${path.module}/../src/writer.py")
    etl_control_hash   = filesha1("${path.module}/../src/etl_control.py")
    quality_metrics_hash = filesha1("${path.module}/../src/quality_metrics.py")
    origins_hash       = filesha1("${path.module}/../config/origins.json")
    target_hash        = filesha1("${path.module}/../config/target.json")
  }

  provisioner "local-exec" {
    command = <<EOT
      # Sync Python scripts
      aws s3 sync ${path.module}/../src/ s3://${local.buckets.workspace}/scripts/glue-streaming-minibatch-dms/ \
        --exclude "*.pyc" --exclude "__pycache__/*"

      # Upload origins.json with account_id resolved
      sed "s/{account_id}/${local.account_id}/g" ${path.module}/../config/origins.json > /tmp/origins-resolved.json
      aws s3 cp /tmp/origins-resolved.json s3://${local.buckets.workspace}/config/origins.json
      rm -f /tmp/origins-resolved.json

      # Upload target.json with account_id resolved
      sed "s/{account_id}/${local.account_id}/g" ${path.module}/../config/target.json > /tmp/target-resolved.json
      aws s3 cp /tmp/target-resolved.json s3://${local.buckets.workspace}/config/target.json
      rm -f /tmp/target-resolved.json

      echo "Artifacts uploaded to s3://${local.buckets.workspace}/"
    EOT
  }

  depends_on = [aws_glue_job.streaming_minibatch_dms]
}
