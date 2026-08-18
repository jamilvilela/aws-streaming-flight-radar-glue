#===============================================================================
# CloudWatch / EventBridge — Rules and Targets
#===============================================================================

# ── EventBridge Rule — Full Load Complete ───────────────────────────────────
# A DMS replication task of type full-load-and-cdc does NOT stop after the full
# load — it transitions to "Load complete, replication ongoing" and keeps
# applying CDC. It therefore never emits the REPLICATION_TASK_STOPPED /
# "Stop Reason FULL_LOAD_ONLY_FINISHED" event (that event only fires for
# full-load-only tasks), so no event-driven rule can signal full-load
# completion. Instead, this rule polls the DMS task status on a schedule and
# the Lambda decides (via describe-replication-tasks) when the full-load phase
# is done.

resource "aws_cloudwatch_event_rule" "full_load_complete" {
  name        = "${local.glue_full_load_job_name}-complete"
  description = "Poll DMS replication task every ${var.full_load_check_interval} minutes and start the full-load workflow once the full-load phase completes"

  schedule_expression = "rate(${var.full_load_check_interval} minutes)"

  tags = merge(local.common_tags, {
    Name = "${local.glue_full_load_job_name}-complete"
  })
}

resource "aws_cloudwatch_event_target" "start_glue_batch" {
  rule      = aws_cloudwatch_event_rule.full_load_complete.name
  target_id = "StartGlueBatchJob"
  arn       = aws_lambda_function.glue_starter.arn
}