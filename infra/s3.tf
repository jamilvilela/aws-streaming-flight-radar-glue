#===============================================================================
# S3 — Artifact Upload (scripts, dependencies, configs)
#===============================================================================

# ── Archive (helpers.zip) ──────────────────────────────────────────────────
# Creates a zip of the dependencies directory using hashicorp/archive provider.

data "archive_file" "helpers" {
  type        = "zip"
  source_dir  = "${path.module}/../app/src/dependencies/"
  output_path = "${path.module}/../.terraform/helpers.zip"
  excludes    = ["__pycache__", "*.pyc"]
}

# ── S3 Artifact Upload ─────────────────────────────────────────────────────
# Uploads scripts, dependencies, and configs to the workspace bucket
# using declarative aws_s3_object resources.

resource "aws_s3_object" "main_py" {
  bucket      = local.buckets.workspace
  key         = "scripts/glue-flight-radar-stream-cdc/main.py"
  source      = "${path.module}/../app/src/main.py"
  source_hash = filemd5("${path.module}/../app/src/main.py")
  tags        = local.common_tags
}

resource "aws_s3_object" "helpers_zip" {
  bucket      = local.buckets.workspace
  key         = "dependencies/helpers.zip"
  source      = data.archive_file.helpers.output_path
  source_hash = data.archive_file.helpers.output_md5
  tags        = local.common_tags
}

resource "aws_s3_object" "config_json" {
  bucket       = local.buckets.workspace
  key          = "config/config.json"
  content      = replace(file("${path.module}/../app/src/dependencies/config/config.json"), "{account_id}", local.account_id)
  content_type = "application/json"
  tags         = local.common_tags
}