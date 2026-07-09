#===============================================================================
# Outputs — Glue Streaming Mini-Batch DMS Module
#===============================================================================

output "glue_job_id" {
  description = "Glue job ID"
  value       = aws_glue_job.streaming_minibatch_dms.id
}

output "glue_job_name" {
  description = "Glue job name"
  value       = aws_glue_job.streaming_minibatch_dms.name
}

output "glue_job_arn" {
  description = "Glue job ARN"
  value       = aws_glue_job.streaming_minibatch_dms.arn
}

output "glue_security_configuration_name" {
  description = "Glue security configuration name"
  value       = aws_glue_security_configuration.glue.name
}

output "glue_connection_name" {
  description = "Glue VPC connection name"
  value       = aws_glue_connection.vpc.name
}

output "kms_key_id" {
  description = "KMS key ID"
  value       = aws_kms_key.glue.key_id
}

output "kms_key_arn" {
  description = "KMS key ARN"
  value       = aws_kms_key.glue.arn
}

output "spark_conf" {
  description = "Spark configuration string passed via --conf"
  value       = local.spark_conf
}

output "script_location" {
  description = "S3 path to the Glue job script"
  value       = local.script_location
}

output "config_s3_path" {
  description = "S3 path to the unified config.json file"
  value       = local.config_s3_path
}

output "full_load_job_name" {
  description = "Full-load batch Glue job name"
  value       = aws_glue_job.full_load_batch.name
}

output "full_load_job_arn" {
  description = "Full-load batch Glue job ARN"
  value       = aws_glue_job.full_load_batch.arn
}

output "eventbridge_rule_name" {
  description = "EventBridge rule name for DMS full load completion"
  value       = aws_cloudwatch_event_rule.dms_full_load_complete.name
}

output "glue_trigger_name" {
  description = "Glue trigger name for starting streaming after full load"
  value       = aws_glue_trigger.start_streaming_after_full_load.name
}

output "vpc_id" {
  description = "VPC ID used for the Glue connection"
  value       = data.aws_vpc.default.id
}

output "subnet_ids" {
  description = "Subnet IDs used for the Glue connection"
  value       = data.aws_subnets.private.ids
}

output "security_group_id" {
  description = "Security group ID for the Glue connection"
  value       = data.aws_security_group.default.id
}
