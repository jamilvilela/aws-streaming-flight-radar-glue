#===============================================================================
# Outputs — Glue Streaming Mini-Batch Module
#===============================================================================

output "glue_job_id" {
  description = "Streaming Glue job ID"
  value       = aws_glue_job.streaming_minibatch.id
}

output "glue_job_name" {
  description = "Streaming Glue job name"
  value       = aws_glue_job.streaming_minibatch.name
}

output "glue_job_arn" {
  description = "Streaming Glue job ARN"
  value       = aws_glue_job.streaming_minibatch.arn
}

output "full_load_job_id" {
  description = "Full-load batch Glue job ID"
  value       = aws_glue_job.full_load_batch.id
}

output "full_load_job_name" {
  description = "Full-load batch Glue job name"
  value       = aws_glue_job.full_load_batch.name
}

output "full_load_job_arn" {
  description = "Full-load batch Glue job ARN"
  value       = aws_glue_job.full_load_batch.arn
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


output "eventbridge_rule_name" {
  description = "EventBridge rule name for full load completion"
  value       = aws_cloudwatch_event_rule.full_load_complete.name
}

output "glue_workflow_name" {
  description = "Glue workflow name orchestrating full load -> streaming"
  value       = aws_glue_workflow.dms_full_load.name
}

output "workflow_lock_table" {
  description = "DynamoDB table used to start the workflow exactly once"
  value       = aws_dynamodb_table.workflow_lock.name
}

output "glue_trigger_name" {
  description = "Glue conditional trigger name for starting streaming after full load"
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
