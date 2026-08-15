#===============================================================================
# Terraform Variable Values — Glue Streaming Mini-Batch
#===============================================================================

region      = "us-east-1"
environment = "prod"

databases = {
  raw      = "db_raw"
  trusted  = "db_trusted"
  business = "db_business"
}

tables = {
  tbl_aircraft           = "tbl_aircraft"
  tbl_airports           = "tbl_airports"
  tbl_airlines           = "tbl_airlines"
  tbl_flights            = "tbl_flights"
  tbl_aircraft_positions = "tbl_aircraft_positions"
  tbl_countries          = "tbl_countries"
  tbl_aircraft_types     = "tbl_aircraft_types"
  tbl_routes             = "tbl_routes"
  etl_control            = "etl_control"
  data_quality           = "data_quality_metrics"
}


glue_job_name = "glue-flight-radar-stream-cdc"

glue_iam_role_name = "role-datalake-analytics"

glue_worker_type = "G.1X"

glue_number_of_workers = 2

glue_job_timeout = 2880

glue_script_location = ""

glue_extra_py_files = ""

glue_config_s3_path = ""

glue_connections = []

tags = {
  ManagedBy   = "terraform"
  Environment = "prod"
  Project     = "aws-streaming-flight-radar-glue"
}
