#===============================================================================
# Glue — Security Configuration, Connection, Jobs and Triggers
#===============================================================================

# Glue Security Configuration
# Configures encryption for CloudWatch logs (SSE-KMS), job bookmarks (CSE-KMS),
# and S3 data (SSE-KMS) using the Glue KMS key.
resource "aws_glue_security_configuration" "glue" {
  name = "${var.glue_job_name}-security-config"

  encryption_configuration {
    cloudwatch_encryption {
      cloudwatch_encryption_mode = "SSE-KMS"
      kms_key_arn                = aws_kms_key.glue.arn
    }

    job_bookmarks_encryption {
      job_bookmarks_encryption_mode = "CSE-KMS"
      kms_key_arn                   = aws_kms_key.glue.arn
    }

    s3_encryption {
      s3_encryption_mode = "SSE-KMS"
      kms_key_arn        = aws_kms_key.glue.arn
    }
  }
}

# Glue VPC Connection (NETWORK type)
# Provides VPC access for Glue jobs. Uses a single private subnet and default
# security group. AZ is derived from the subnet to avoid mismatch errors.
resource "aws_glue_connection" "vpc" {
  name            = "${var.glue_job_name}-vpc"
  connection_type = "NETWORK"

  physical_connection_requirements {
    availability_zone      = data.aws_subnet.glue.availability_zone
    subnet_id              = data.aws_subnet.glue.id
    security_group_id_list = [data.aws_security_group.default.id]
  }

  tags = merge(local.common_tags, {
    Name = "${var.glue_job_name}-vpc"
  })
}

# Glue Job — Full-Load Batch
# Processes all tables sequentially (--mode=batch). Uses Glue 5.0, Python 3.9,
# FLEX execution class for cost optimization. Delta Lake support via --datalake-formats.
resource "aws_glue_job" "full_load_batch" {
  name              = local.glue_full_load_job_name
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "5.0"
  worker_type       = var.full_load_worker_type
  number_of_workers = var.full_load_number_of_workers
  timeout           = var.glue_job_timeout
  execution_class   = "FLEX"

  security_configuration = aws_glue_security_configuration.glue.name

  command {
    script_location = local.script_location
    python_version  = "3.9"
  }

  default_arguments = {
    # Job bookmarks & logging
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--continuous-log-logGroup"          = "/aws-glue/jobs/${local.glue_full_load_job_name}"
    "--enable-auto-scaling"              = "true"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-observability-metrics"     = "true"

    # Job configuration
    "--config_s3_path"        = local.config_s3_path
    "--extra-py-files"        = local.extra_py_files
    "--conf"                  = local.spark_conf_argument
    "--mode"                  = "batch"
    "--generate-test-rejects" = "true"

    # Delta Lake support
    "--datalake-formats" = "delta"

    # Spark UI
    "--enable-spark-ui"       = "true"
    "--spark-event-logs-path" = "s3://${local.buckets.workspace}/spark-logs/${local.glue_full_load_job_name}/"

    # Security configuration
    "--encryption-type" = "sse-s3-kms"
  }

  # Associate Glue connection
  connections = length(var.glue_connections) > 0 ? var.glue_connections : [aws_glue_connection.vpc.name]

  tags = merge(local.common_tags, {
    Name = local.glue_full_load_job_name
  })
}

# Glue Job — Streaming CDC
# Runs continuous streaming with one concurrent query per table (--mode=streaming).
# Uses Glue 5.0, Python 3.9, FLEX execution class. Delta Lake support enabled.
resource "aws_glue_job" "streaming_minibatch" {
  name              = local.glue_streaming_job_name
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "5.0"
  worker_type       = var.streaming_worker_type
  number_of_workers = var.streaming_number_of_workers
  timeout           = var.glue_job_timeout
  execution_class   = "FLEX"

  security_configuration = aws_glue_security_configuration.glue.name

  command {
    script_location = local.script_location
    python_version  = "3.9"
  }

  default_arguments = {
    # Job bookmarks & logging
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--continuous-log-logGroup"          = "/aws-glue/jobs/${local.glue_streaming_job_name}"
    "--enable-auto-scaling"              = "true"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-observability-metrics"     = "true"

    # Job configuration
    "--config_s3_path"        = local.config_s3_path
    "--extra-py-files"        = local.extra_py_files
    "--conf"                  = local.spark_conf_argument
    "--mode"                  = "streaming"
    "--generate-test-rejects" = "true"

    # Delta Lake support
    "--datalake-formats" = "delta"

    # Spark UI
    "--enable-spark-ui"       = "true"
    "--spark-event-logs-path" = "s3://${local.buckets.workspace}/spark-logs/${local.glue_streaming_job_name}/"

    # Security configuration
    "--encryption-type" = "sse-s3-kms"
  }

  # Associate Glue connection
  connections = length(var.glue_connections) > 0 ? var.glue_connections : [aws_glue_connection.vpc.name]

  tags = merge(local.common_tags, {
    Name = local.glue_streaming_job_name
  })
}

# Glue Workflow — Full Load → Streaming (native sequencing)
# Lambda starts the workflow via StartWorkflowRun. The workflow uses an
# ON_DEMAND trigger to start the batch job and a CONDITIONAL trigger
# (native Glue) to start streaming only after batch SUCCEEDED.
# This avoids polling in Lambda and prevents concurrent job execution.
resource "aws_glue_workflow" "dms_full_load" {
  name = "${local.glue_full_load_job_name}-workflow"

  tags = merge(local.common_tags, {
    Name = "${local.glue_full_load_job_name}-workflow"
  })
}

# On-demand Trigger — Start Full-Load Batch
# Fires when the workflow run starts (via StartWorkflowRun).
resource "aws_glue_trigger" "start_full_load" {
  name          = "${local.glue_full_load_job_name}-start-full-load"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.dms_full_load.name

  actions {
    job_name = aws_glue_job.full_load_batch.name
  }

  tags = merge(local.common_tags, {
    Name = "${local.glue_full_load_job_name}-start-full-load"
  })
}

# Conditional Trigger — Start Streaming After Full Load
# Fires when the full-load batch job succeeds within the workflow.
resource "aws_glue_trigger" "start_streaming_after_full_load" {
  name          = "${local.glue_full_load_job_name}-start-streaming"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.dms_full_load.name

  actions {
    job_name = aws_glue_job.streaming_minibatch.name
    arguments = {
      "--mode" = "streaming"
    }
  }

  predicate {
    conditions {
      job_name = aws_glue_job.full_load_batch.name
      state    = "SUCCEEDED"
    }
  }

  tags = merge(local.common_tags, {
    Name = "${local.glue_full_load_job_name}-start-streaming"
  })
}