#===============================================================================
# KMS — Encryption Keys
#===============================================================================

# KMS Key for Glue encryption
# Used for SSE-KMS (S3, CloudWatch) and CSE-KMS (job bookmarks) encryption.
# Key rotation enabled, 30-day deletion window.
resource "aws_kms_key" "glue" {
  description             = "KMS key for Glue Streaming Mini-Batch job encryption"
  deletion_window_in_days = 30
  enable_key_rotation     = true

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "EnableIAMAdminAccess"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${local.account_id}:root"
        }
        Action   = "kms:*"
        Resource = "*"
      },
      {
        Sid    = "AllowGlueServiceAccess"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = "*"
      },
      {
        Sid    = "AllowCloudWatchLogsAccess"
        Effect = "Allow"
        Principal = {
          Service = "logs.amazonaws.com"
        }
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = "*"
      }
    ]
  })

  tags = merge(local.common_tags, {
    Name = local.kms_key_alias
  })
}

# KMS alias for easier reference
resource "aws_kms_alias" "glue" {
  name          = local.kms_key_alias
  target_key_id = aws_kms_key.glue.key_id
}