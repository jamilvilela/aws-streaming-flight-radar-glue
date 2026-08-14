#===============================================================================
# Lambda — Function that starts the Glue workflow (batch then streaming)
#===============================================================================

# ── Lambda Function — Start Glue Full-Load Job ──────────────────────────────

data "archive_file" "lambda_glue_starter" {
  type        = "zip"
  source_file = "${path.module}/../app/src/lambdas/start_glue_job.py"
  output_path = "${path.module}/../.terraform/lambda_glue_starter.zip"
}

resource "aws_lambda_function" "glue_starter" {
  filename         = data.archive_file.lambda_glue_starter.output_path
  source_code_hash = data.archive_file.lambda_glue_starter.output_base64sha256
  function_name    = "${var.full_load_job_name}-starter"
  role             = aws_iam_role.lambda_glue_starter.arn
  handler          = "start_glue_job.lambda_handler"
  runtime          = "python3.9"
  timeout          = 30

  environment {
    variables = {
      GLUE_WORKFLOW_NAME = "${var.full_load_job_name}-workflow"
    }
  }

  tags = merge(local.common_tags, {
    Name = "${var.full_load_job_name}-starter"
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