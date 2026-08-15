#===============================================================================
# Glue — Security Configuration, Connection, Jobs and Trigger
#===============================================================================

# ── Glue Security Configuration ──────────────────────────────────────────────

resource "aws_glue_security_configuration" "glue" {
  name = "glue-flight-radar-stream-cdc-security-config"

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

# ── Glue Connection (VPC) ────────────────────────────────────────────────────

resource "aws_glue_connection" "vpc" {
  name            = "glue-flight-radar-stream-cdc-vpc"
  connection_type = "NETWORK"

  physical_connection_requirements {
    availability_zone = data.aws_subnet.all[keys(data.aws_subnet.all)[0]].availability_zone
    # Prefer private subnets; fall back to any subnet if none are private
    subnet_id              = length(data.aws_subnets.private.ids) > 0 ? data.aws_subnets.private.ids[0] : data.aws_subnets.all.ids[0]
    security_group_id_list = [data.aws_security_group.default.id]
  }

  tags = merge(local.common_tags, {
    Name = "glue-flight-radar-stream-cdc-vpc"
  })
}

# ── Glue Job — Full-Load Batch ──────────────────────────────────────────────

resource "aws_glue_job" "full_load_batch" {
  name              = var.full_load_job_name
  role_arn          = data.aws_iam_role.datalake_analytics.arn
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
    "--continuous-log-logGroup"          = "/aws-glue/jobs/${var.full_load_job_name}"
    "--enable-auto-scaling"              = "true"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"

    # Job configuration
    "--config_s3_path" = local.config_s3_path
    "--extra-py-files" = local.extra_py_files
    "--mode"           = "batch"

    # Delta Lake support (writer.py importa delta.tables)
    "--datalake-formats" = "delta"

    # Spark configs passed via --conf (parsed dynamically by main.py)
    "--conf" = local.spark_conf

    # Spark UI
    "--enable-spark-ui"       = "true"
    "--spark-event-logs-path" = "s3://${local.buckets.workspace}/spark-logs/${var.full_load_job_name}/"

    # Security configuration
    "--encryption-type" = "sse-s3-kms"
  }

  # Associate Glue connection if provided
  connections = length(var.glue_connections) > 0 ? var.glue_connections : [aws_glue_connection.vpc.name]

  tags = merge(local.common_tags, {
    Name = var.full_load_job_name
  })
}

# ── Glue Job — Streaming CDC ────────────────────────────────────────────────

resource "aws_glue_job" "streaming_minibatch" {
  name              = var.glue_job_name
  role_arn          = data.aws_iam_role.datalake_analytics.arn
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
    "--continuous-log-logGroup"          = "/aws-glue/jobs/${var.glue_job_name}"
    "--enable-auto-scaling"              = "true"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"

    # Job configuration
    "--config_s3_path" = local.config_s3_path
    "--extra-py-files" = local.extra_py_files
    "--mode"           = "streaming"

    # Delta Lake support (writer.py importa delta.tables)
    "--datalake-formats" = "delta"

    # Spark configs passed via --conf (parsed dynamically by main.py)
    "--conf" = local.spark_conf

    # Spark UI
    "--enable-spark-ui"       = "true"
    "--spark-event-logs-path" = "s3://${local.buckets.workspace}/spark-logs/${var.glue_job_name}/"

    # Security configuration
    "--encryption-type" = "sse-s3-kms"
  }

  # Associate Glue connection if provided
  connections = length(var.glue_connections) > 0 ? var.glue_connections : [aws_glue_connection.vpc.name]

  tags = merge(local.common_tags, {
    Name = var.glue_job_name
  })
}

# ── Glue Workflow — Full Load → Streaming (native sequencing) ────────────────
#
# A Lambda dispara o workflow via StartWorkflowRun. O workflow usa um
# trigger ON_DEMAND para iniciar o batch job e um trigger CONDITIONAL
# (nativo do Glue) para iniciar o streaming apenas quando o batch
# SUCCEEDED. Como o batch é iniciado por um trigger do mesmo workflow,
# o trigger condicional dispara normalmente — sem polling na Lambda e
# sem execução simultânea dos dois jobs.

resource "aws_glue_workflow" "dms_full_load" {
  name = "${var.full_load_job_name}-workflow"

  tags = merge(local.common_tags, {
    Name = "${var.full_load_job_name}-workflow"
  })
}

# ── On-demand Trigger — Start Full-Load Batch ────────────────────────────────
# Fires when the workflow run starts (via StartWorkflowRun).

resource "aws_glue_trigger" "start_full_load" {
  name          = "${var.full_load_job_name}-start-full-load"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.dms_full_load.name

  actions {
    job_name = aws_glue_job.full_load_batch.name
  }

  tags = merge(local.common_tags, {
    Name = "${var.full_load_job_name}-start-full-load"
  })
}

# ── Conditional Trigger — Start Streaming After Full Load ────────────────────
# Fires when the full-load batch job succeeds within the workflow.

resource "aws_glue_trigger" "start_streaming_after_full_load" {
  name          = "${var.full_load_job_name}-start-streaming"
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
    Name = "${var.full_load_job_name}-start-streaming"
  })
}