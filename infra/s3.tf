#===============================================================================
# S3 — Artifact Upload (scripts, dependencies, configs)
#===============================================================================

# Archive (helpers.zip)
# Creates a zip of the Glue source package using hashicorp/archive provider.
# The zip root contains the `src` package (src/__init__.py + src/dependencies/...)
# so runtime imports resolve as `src.dependencies.<module>`.
data "archive_file" "helpers" {
  type        = "zip"
  source_dir  = "${path.module}/../app/aws-glue/"
  output_path = "${path.module}/../.terraform/helpers.zip"
  excludes    = ["tests", "tests/**", "__pycache__", "**/__pycache__", "*.pyc", "**/*.pyc", "src/main.py"]
}

# S3 Artifact Upload
# Uploads scripts, dependencies, and configs to the workspace bucket
# using declarative aws_s3_object resources.

resource "aws_s3_object" "main_py" {
  bucket      = local.buckets.workspace
  key         = "aws-glue/jobs/flight-radar/src/main.py"
  source      = "${path.module}/../app/aws-glue/src/main.py"
  source_hash = filemd5("${path.module}/../app/aws-glue/src/main.py")
  tags        = local.common_tags
}

resource "aws_s3_object" "helpers_zip" {
  bucket      = local.buckets.workspace
  key         = "aws-glue/jobs/flight-radar/src/dependencies/helpers.zip"
  source      = data.archive_file.helpers.output_path
  source_hash = data.archive_file.helpers.output_md5
  tags        = local.common_tags
}

resource "aws_s3_object" "config_json" {
  bucket       = local.buckets.workspace
  key          = "aws-glue/jobs/flight-radar/src/dependencies/config/config.json"
  content      = replace(file("${path.module}/../app/aws-glue/src/dependencies/config/config.json"), "{account_id}", local.account_id)
  content_type = "application/json"
  tags         = local.common_tags
}

resource "aws_s3_object" "lambda_start_glue_job" {
  bucket      = local.buckets.workspace
  key         = "aws-lambda/flight-radar/start_workflow/start_glue_job.py"
  source      = "${path.module}/../app/aws-lambda/start_workflow/start_glue_job.py"
  source_hash = filemd5("${path.module}/../app/aws-lambda/start_workflow/start_glue_job.py")
  tags        = local.common_tags
}