#===============================================================================
# Terraform Variable Values — Glue Streaming Mini-Batch DMS
#===============================================================================

region = "us-east-1"

environment = "dev"

control_account = "331504768406"

databases = {
  landing  = ""
  raw      = "db_raw"
  trusted  = "db_trusted"
  business = "db_business"
}

tables = {
  tbl_opensky_flights = "tbl_opensky_flights"
  etl_control         = "etl_control"
  data_quality        = "data_quality_metrics"
}

buckets = {
  landing   = "lakehouse-landing-331504768406"
  raw       = "lakehouse-raw-331504768406"
  trusted   = "lakehouse-trusted-331504768406"
  business  = "lakehouse-business-331504768406"
  workspace = "lakehouse-workspace-331504768406"
}

glue_job_name = "glue-streaming-minibatch-dms"

glue_iam_role_name = "role-datalake-analytics"

glue_worker_type = "G.1X"

glue_number_of_workers = 2

glue_job_timeout = 2880

glue_script_location = ""

glue_extra_py_files = ""

glue_origins_s3_path = ""

glue_target_s3_path  = ""

glue_connections = []

tags = {
  ManagedBy   = "terraform"
  Environment = "dev"
  Project     = "aws-streaming-flight-radar-glue"
}
