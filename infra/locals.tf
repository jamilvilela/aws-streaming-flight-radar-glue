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

  # Spark configuration properties passed directly to Glue through --conf.
  # Join/broadcast and shuffle settings are intentionally omitted because this
  # job does not perform joins or aggregations.
  spark_properties = {
    # Delta Lake extensions and catalog (REQUIRED for Delta tables via Glue Catalog).
    # These MUST be set via --conf at session bootstrap; cannot be set at runtime.
    "spark.sql.extensions"                             = "io.delta.sql.DeltaSparkSessionExtension"
    "spark.sql.catalog.spark_catalog"                  = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    "spark.sql.catalogImplementation"                  = "hive"
    "spark.hadoop.hive.metastore.client.factory.class" = "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"

    # File discovery and small-file grouping during reads.
    "spark.sql.files.maxPartitionBytes"                        = "256MB"
    "spark.sql.files.openCostInBytes"                          = "32MB"
    "spark.sql.files.maxPartitionNum"                          = "2000"
    "spark.sql.sources.parallelPartitionDiscovery.threshold"   = "32"
    "spark.sql.sources.parallelPartitionDiscovery.parallelism" = "10000"
    "spark.sql.parquet.filterPushdown"                         = "true"
    "spark.sql.parquet.enableVectorizedReader"                 = "true"

    # General adaptive execution.
    "spark.sql.adaptive.enabled"                      = "true"
    "spark.sql.adaptive.coalescePartitions.enabled"   = "true"
    "spark.sql.adaptive.advisoryPartitionSizeInBytes" = "128MB"

    # Delta writes, compression and automatic small-file compaction.
    "spark.databricks.delta.optimizeWrite.enabled"   = "true"
    "spark.databricks.delta.autoCompact.enabled"     = "true"
    "spark.databricks.delta.autoCompact.minNumFiles" = "10"
    "spark.databricks.delta.autoCompact.maxFileSize" = "134217728"
    "spark.sql.parquet.compression.codec"            = "snappy"
  }

  # Glue expects --conf as a Spark-submit-style string, not a JSON object.
  spark_conf_argument = join(" ", [
    for key, value in local.spark_properties : "--conf ${key}=${value}"
  ])

  # Common tags merged with environment
  common_tags = merge(var.tags, {
    Environment = var.environment
  })
}
