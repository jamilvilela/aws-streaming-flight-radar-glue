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

  # KMS key alias
  kms_key_alias = "alias/glue-flight-radar-stream-cdc"

  # Spark configuration properties (passed via --conf)
  spark_properties = {
    "spark.sql.adaptive.enabled"                      = "true"
    "spark.sql.adaptive.coalescePartitions.enabled"   = "true"
    "spark.sql.adaptive.skewJoin.enabled"             = "true"
    "spark.sql.adaptive.advisoryPartitionSizeInBytes" = "128MB"
    "spark.sql.shuffle.partitions"                    = "200"
    "spark.sql.parquet.compression.codec"             = "snappy"
    "spark.executor.memory"                           = "4g"
    "spark.driver.memory"                             = "4g"
    "spark.executor.memoryOverhead"                   = "2g"
    "spark.driver.memoryOverhead"                     = "2g"
    "spark.memory.offHeap.enabled"                    = "true"
    "spark.memory.offHeap.size"                       = "2g"
    "spark.dynamicAllocation.enabled"                 = "true"
    "spark.dynamicAllocation.shuffleTracking.enabled" = "true"
    "spark.sql.streaming.schemaInference"             = "true"
    "spark.sql.parquet.mergeSchema"                   = "false"
    "spark.glue.disable.optimization"                 = "false"
    # Delta Lake / Lakehouse configs
    "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite" = "true"
    "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact"   = "true"
    # Delta Lake Spark extension + catalog (obrigatório para DeltaTable e formato delta)
    "spark.sql.extensions"            = "io.delta.sql.DeltaSparkSessionExtension"
    "spark.sql.catalog.spark_catalog" = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    "spark.delta.logStore.class"      = "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore"
  }

  # Build the --conf string: key=value key=value ...
  spark_conf = join(" ", [
    for k, v in local.spark_properties : "${k}=${v}"
  ])

  # Common tags merged with environment
  common_tags = merge(var.tags, {
    Environment = var.environment
  })
}
