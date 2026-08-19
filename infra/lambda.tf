#===============================================================================
# Lambda — Function that starts the Glue workflow (batch then streaming)
#===============================================================================

# ── Lambda Function — Start Glue Full-Load Job ──────────────────────────────

data "archive_file" "lambda_glue_starter" {
  type        = "zip"
  source_file = "${path.module}/../app/aws-lambda/start_workflow/start_glue_job.py"
  output_path = "${path.module}/../.terraform/lambda_glue_starter.zip"
}

resource "aws_lambda_function" "glue_starter" {
  filename         = data.archive_file.lambda_glue_starter.output_path
  source_code_hash = data.archive_file.lambda_glue_starter.output_base64sha256
  function_name    = "${local.glue_full_load_job_name}-starter"
  role             = aws_iam_role.lambda_glue_starter.arn
  handler          = "start_glue_job.lambda_handler"
  runtime          = "python3.9"
  timeout          = 30

  environment {
    variables = {
      GLUE_WORKFLOW_NAME         = "${local.glue_full_load_job_name}-workflow"
      DMS_REPLICATION_CONFIG_ARN = var.dms_replication_config_arn
      DMS_WORKFLOW_LOCK_TABLE    = aws_dynamodb_table.workflow_lock.name
    }
  }

  tags = merge(local.common_tags, {
    Name = "${local.glue_full_load_job_name}-starter"
  })
}

# ── CloudWatch Log Group — Lambda ────────────────────────────────────────────
# Pre-create the Lambda log group so log-based monitors do not fail with
# ResourceNotFoundException before the function is first invoked.

resource "aws_cloudwatch_log_group" "glue_starter" {
  name              = "/aws/lambda/${local.glue_full_load_job_name}-starter"
  retention_in_days = 14

  tags = merge(local.common_tags, {
    Name = "${local.glue_full_load_job_name}-starter-logs"
  })
}

# ── EventBridge Permission — Invoke Lambda ───────────────────────────────────

resource "aws_lambda_permission" "eventbridge_invoke_glue_starter" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.glue_starter.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.full_load_complete.arn
}