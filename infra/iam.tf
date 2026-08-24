#===============================================================================
# IAM — Roles and Policies
#===============================================================================

# ── Glue Job Role — Dedicated ────────────────────────────────────────────────
# Dedicated role for the Glue jobs (batch + streaming) and interactive
# sessions. Owned by this module — unlike role-datalake-analytics, which is
# managed by the data-lake repository. Trusted by the Glue service.

resource "aws_iam_role" "glue_job" {
  name = var.glue_job_role_name

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      },
    ]
  })

  tags = merge(local.common_tags, {
    Name = var.glue_job_role_name
  })
}

# ── Glue Job Role — Catalog & Connections ─────────────────────────────────────
# The dedicated Glue role runs the jobs, which use a VPC NETWORK connection.
# Glue must be able to read connection metadata from the Data Catalog
# (glue:GetConnection/GetConnections).

resource "aws_iam_role_policy" "glue_catalog_connections" {
  name = "glue-catalog-connections"
  role = aws_iam_role.glue_job.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:GetConnection",
          "glue:GetConnections",
        ]
        Resource = [
          "arn:aws:glue:${var.region}:${local.account_id}:catalog",
          "arn:aws:glue:${var.region}:${local.account_id}:connection/*",
        ]
      },
    ]
  })
}

# ── Glue Job Role — Data Catalog tables ───────────────────────────────────────
# Spark resolves existing Delta tables through the Glue Catalog. Data writes
# go to the registered table locations; this role must not create or mutate
# Glue Catalog metadata.

resource "aws_iam_role_policy" "glue_catalog_tables" {
  name = "glue-catalog-tables"
  role = aws_iam_role.glue_job.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:GetUserDefinedFunction",
          "glue:GetDataCatalogEncryptionSettings",
          "glue:BatchCreatePartition",
          "glue:BatchGetPartition",
          "glue:CreatePartition",
          "glue:UpdatePartition",
          ]
        Resource = [
          "arn:aws:glue:${var.region}:${local.account_id}:catalog",
          "arn:aws:glue:${var.region}:${local.account_id}:database/*",
          "arn:aws:glue:${var.region}:${local.account_id}:table/*/*",
          "arn:aws:glue:${var.region}:${local.account_id}:userDefinedFunction/*/*/*",
        ]
      },
    ]
  })
}

# ── Glue Job Role — S3, KMS and CloudWatch Logs ──────────────────────────────
# The Glue jobs read full-load/CDC parquet from landing, write Delta to raw,
# and read scripts/config/dependencies from workspace. Writes use SSE-KMS
# (glue KMS key) and job logs go to CloudWatch.

resource "aws_iam_role_policy" "glue_data_access" {
  name = "glue-data-access"
  role = aws_iam_role.glue_job.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:ListBucketMultipartUploads",
          "s3:ListMultipartUploadParts",
          "s3:AbortMultipartUpload",
        ]
        Resource = [
          "arn:aws:s3:::${local.buckets.landing}",
          "arn:aws:s3:::${local.buckets.landing}/*",
          "arn:aws:s3:::${local.buckets.raw}",
          "arn:aws:s3:::${local.buckets.raw}/*",
          "arn:aws:s3:::${local.buckets.workspace}",
          "arn:aws:s3:::${local.buckets.workspace}/*",
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:Encrypt",
          "kms:GenerateDataKey",
          "kms:ReEncryptFrom",
          "kms:ReEncryptTo",
          "kms:DescribeKey",
        ]
        Resource = [
          aws_kms_key.glue.arn,
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:AssociateKmsKey",
        ]
        Resource = [
          "arn:aws:logs:${var.region}:${local.account_id}:log-group:/aws-glue/jobs/*",
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = [
          "arn:aws:logs:${var.region}:${local.account_id}:log-group:/aws-glue/jobs/*:*",
        ]
      },
      {
        # Glue runtime metrics reporter (GlueCloudWatchReporter) sends job
        # metrics to CloudWatch every ~30s when --enable-metrics=true. Without
        # this permission it logs a 403 ERROR on every report cycle.
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData",
        ]
        Resource = [
          "*",
        ]
      },
    ]
  })
}

# ── Glue Job Role — VPC Networking (ENI management) ──────────────────────────
# The Glue jobs run inside a VPC (NETWORK connection). Glue must be able to
# describe the VPC/subnets/security groups and create/delete elastic network
# interfaces (ENIs) for the job's Spark executors.

resource "aws_iam_role_policy" "glue_vpc_networking" {
  name = "glue-vpc-networking"
  role = aws_iam_role.glue_job.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ec2:DescribeVpcs",
          "ec2:DescribeSubnets",
          "ec2:DescribeSecurityGroups",
          "ec2:DescribeAvailabilityZones",
          "ec2:DescribeNetworkInterfaces",
          "ec2:DescribeVpcEndpoints",
          "ec2:DescribeRouteTables",
          "ec2:DescribePrefixLists",
          "ec2:CreateNetworkInterface",
          "ec2:DeleteNetworkInterface",
          "ec2:ModifyNetworkInterfaceAttribute",
        ]
        Resource = ["*"]
      },
      {
        # Glue taggea as ENIs que cria ao iniciar os workers no VPC.
        Effect = "Allow"
        Action = [
          "ec2:CreateTags",
          "ec2:DeleteTags",
        ]
        Resource = [
          "arn:aws:ec2:*:*:network-interface/*",
          "arn:aws:ec2:*:*:vpc/*",
          "arn:aws:ec2:*:*:security-group/*",
          "arn:aws:ec2:*:*:subnet/*",
        ]
      },
    ]
  })
}

# ── Glue Interactive Sessions — Session role permissions ─────────────────────
# The role also executes AWS Glue interactive sessions (Jupyter notebooks that
# test this pipeline). It needs the same interactive-sessions actions plus the
# ability to tag sessions (%%tags) and write session logs to CloudWatch.

resource "aws_iam_role_policy" "glue_interactive_sessions" {
  name = "glue-interactive-sessions"
  role = aws_iam_role.glue_job.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:CreateSession",
          "glue:GetSession",
          "glue:ListSessions",
          "glue:StopSession",
          "glue:GetStatement",
          "glue:ListStatements",
          "glue:RunStatement",
          "glue:TagResource",
          "glue:UntagResource",
        ]
        Resource = [
          "arn:aws:glue:${var.region}:${local.account_id}:catalog",
          "arn:aws:glue:${var.region}:${local.account_id}:database/*",
          "arn:aws:glue:${var.region}:${local.account_id}:table/*/*",
          "arn:aws:glue:${var.region}:${local.account_id}:session/*",
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "iam:PassRole",
        ]
        Resource = [
          aws_iam_role.glue_job.arn,
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = [
          "arn:aws:logs:${var.region}:${local.account_id}:log-group:/aws-glue/sessions/*:*",
        ]
      },
    ]
  })
}

# ── Interactive Sessions — Caller PassRole ───────────────────────────────────
# The identity that starts a notebook (members of the datalake-admins group)
# must be allowed to pass the dedicated Glue role when creating an interactive
# session (glue:CreateSession requires iam:PassRole on the role).

resource "aws_iam_group_policy" "interactive_sessions_passrole" {
  name  = "glue-interactive-sessions-passrole"
  group = "datalake-admins"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "iam:PassRole",
        ]
        Resource = [
          aws_iam_role.glue_job.arn,
        ]
      },
    ]
  })
}

# ── Lambda IAM Role ──────────────────────────────────────────────────────────
# Role for the Lambda function that starts the Glue full-load batch job.

resource "aws_iam_role" "lambda_glue_starter" {
  name = "role-lambda-start-${local.glue_full_load_job_name}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = merge(local.common_tags, {
    Name = "role-lambda-start-${local.glue_full_load_job_name}"
  })
}

resource "aws_iam_role_policy" "lambda_glue_starter" {
  name = "lambda-start-glue-policy"
  role = aws_iam_role.lambda_glue_starter.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartWorkflowRun",
        ]
        Resource = [
          aws_glue_workflow.dms_full_load.arn,
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "dms:DescribeReplications",
        ]
        Resource = [
          var.dms_replication_config_arn != "" ? var.dms_replication_config_arn : "*",
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
        ]
        Resource = [
          aws_dynamodb_table.workflow_lock.arn,
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = [
          "arn:aws:logs:${var.region}:${local.account_id}:log-group:/aws/lambda/${local.glue_full_load_job_name}-starter:*",
        ]
      },
    ]
  })
}