#===============================================================================
# DynamoDB — Workflow lock table
#===============================================================================
# The batch-starter Lambda is invoked on a schedule. The lock table guarantees
# the Glue workflow is started exactly once per DMS full load: the Lambda
# inserts a row keyed by the replication task ARN with a conditional
# attribute_not_exists PutItem. Once present, subsequent polls skip.

resource "aws_dynamodb_table" "workflow_lock" {
  name         = "glue-flight-radar-workflow-lock"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "task_arn"

  attribute {
    name = "task_arn"
    type = "S"
  }

  tags = merge(local.common_tags, {
    Name = "glue-flight-radar-workflow-lock"
  })
}