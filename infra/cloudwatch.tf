#===============================================================================
# CloudWatch / EventBridge — Rules and Targets
#===============================================================================

# ── EventBridge Rule — Full Load Complete ───────────────────────────────────

resource "aws_cloudwatch_event_rule" "full_load_complete" {
  name        = "${local.glue_full_load_job_name}-complete"
  description = "Triggered when DMS full load completes for flight_radar"

  # DMS não emite detail-type "DMS Full Load Completed". O evento real de
  # full load concluído é "DMS Replication Task State Change" com
  # eventType REPLICATION_TASK_STOPPED / detailMessage "Stop Reason FULL_LOAD_ONLY_FINISHED".
  event_pattern = jsonencode({
    source      = ["aws.dms"]
    detail-type = ["DMS Replication Task State Change"]
    detail = {
      type          = ["REPLICATION_TASK"]
      category      = ["StateChange"]
      eventType     = ["REPLICATION_TASK_STOPPED"]
      eventId       = ["DMS-EVENT-0079"]
      detailMessage = ["Stop Reason FULL_LOAD_ONLY_FINISHED"]
    }
  })

  tags = merge(local.common_tags, {
    Name = "${local.glue_full_load_job_name}-complete"
  })
}

resource "aws_cloudwatch_event_target" "start_glue_batch" {
  rule      = aws_cloudwatch_event_rule.full_load_complete.name
  target_id = "StartGlueBatchJob"
  arn       = aws_lambda_function.glue_starter.arn
}