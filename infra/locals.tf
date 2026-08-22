#===============================================================================
# Locals — Computed values for the Glue Streaming Mini-Batch Module
#===============================================================================

locals {
  # Resolve account ID from the current AWS caller identity
  account_id = data.aws_caller_identity.current.account_id

  # S3 bucket names with account ID resolved
  buckets = {
    landing   = "lakehouse-landing-${local.account_id}"
    raw       = "lakehouse-raw-${local.account_id}"
    trusted   = "lakehouse-trusted-${local.account_id}"
    business  = "lakehouse-business-${local.account_id}"
    workspace = "lakehouse-workspace-${local.account_id}"
  }

  # Glue script and config S3 paths (defaults if not explicitly provided)
  script_location = var.glue_script_location != "" ? var.glue_script_location : "s3://${local.buckets.workspace}/aws-glue/jobs/flight-radar/src/main.py"
  config_s3_path  = var.glue_config_s3_path != "" ? var.glue_config_s3_path : "s3://${local.buckets.workspace}/aws-glue/jobs/flight-radar/src/dependencies/config/config.json"
  extra_py_files  = var.glue_extra_py_files != "" ? var.glue_extra_py_files : "s3://${local.buckets.workspace}/aws-glue/jobs/flight-radar/src/dependencies/helpers.zip"

  # Single objective, two job definitions derived from glue_job_name.
  # The processes are differentiated in code/classes via --mode (batch|streaming).
  glue_batch_job_name     = "${var.glue_job_name}-batch"
  glue_streaming_job_name = "${var.glue_job_name}-streaming"
  glue_full_load_job_name = var.full_load_job_name != "" ? var.full_load_job_name : local.glue_batch_job_name

  # KMS key alias
  kms_key_alias = "alias/glue-flight-radar"

  # Spark configuration properties (applied at runtime by main.py via
  # spark.conf.set()). NOT passed via --conf: Glue 5.0's PrepareLaunch only
  # accepts a single key=value per --conf and rejects space-separated lists
  # ("Invalid input to --conf"). Delta extensions/catalog are configured by
  # Glue itself through the --datalake-formats job argument.
  spark_properties = {
    "spark.sql.adaptive.enabled"                      = "true"
    "spark.sql.adaptive.coalescePartitions.enabled"   = "true"
    "spark.sql.adaptive.skewJoin.enabled"             = "true"
    "spark.sql.adaptive.advisoryPartitionSizeInBytes" = "128MB"
    "spark.sql.shuffle.partitions"                    = "200"
    "spark.sql.parquet.compression.codec"             = "snappy"
    "spark.sql.streaming.schemaInference"             = "true"
    "spark.sql.parquet.mergeSchema"                   = "false"
    # Delta Lake / Lakehouse configs (runtime-settable)
    "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite" = "true"
    "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact"   = "true"
  }

  # Common tags merged with environment
  common_tags = merge(var.tags, {
    Environment = var.environment
  })
}
