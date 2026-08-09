#===============================================================================
# Variables — Glue Streaming Mini-Batch Module
#===============================================================================

variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (dev, prod, etc.)"
  type        = string
  default     = "dev"
}

variable "databases" {
  description = "Glue Data Catalog database names"
  type = object({
    raw      = string
    trusted  = string
    business = string
  })
  default = {
    raw      = "db_raw"
    trusted  = "db_trusted"
    business = "db_business"
  }
}

variable "tables" {
  description = "Glue Data Catalog table names"
  type = object({
    tbl_aircraft           = string
    tbl_airports           = string
    tbl_airlines           = string
    tbl_flights            = string
    tbl_aircraft_positions = string
    tbl_countries          = string
    tbl_aircraft_types     = string
    tbl_routes             = string
    etl_control            = string
    data_quality           = string
  })
  default = {
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
}

variable "glue_job_name" {
  description = "Name of the streaming Glue job (CDC)"
  type        = string
  default     = "glue-flight-radar-stream-cdc"
}

variable "full_load_job_name" {
  description = "Name of the full-load batch Glue job"
  type        = string
  default     = "glue-flight-radar-full-load"
}

variable "glue_iam_role_name" {
  description = "Name of the existing IAM role for Glue"
  type        = string
  default     = "role-datalake-analytics"
}

variable "glue_worker_type" {
  description = "[DEPRECATED] Glue worker type — use *_worker_type variables below"
  type        = string
  default     = "G.1X"
}

variable "glue_number_of_workers" {
  description = "[DEPRECATED] Number of Glue workers — use *_number_of_workers variables below"
  type        = number
  default     = 2
}

variable "full_load_worker_type" {
  description = "Glue worker type for the full-load batch job"
  type        = string
  default     = "G.1X"
}

variable "full_load_number_of_workers" {
  description = "Number of Glue workers for the full-load batch job"
  type        = number
  default     = 4
}

variable "streaming_worker_type" {
  description = "Glue worker type for the streaming (CDC) job"
  type        = string
  default     = "G.1X"
}

variable "streaming_number_of_workers" {
  description = "Number of Glue workers for the streaming (CDC) job"
  type        = number
  default     = 2
}

variable "glue_job_timeout" {
  description = "Glue job timeout in minutes"
  type        = number
  default     = 2880
}

variable "glue_script_location" {
  description = "S3 path to the main.py script"
  type        = string
  default     = ""
}

variable "glue_extra_py_files" {
  description = "S3 path to extra Python dependencies zip"
  type        = string
  default     = ""
}

variable "glue_config_s3_path" {
  description = "S3 path to the unified config.json file (all tables with sources + targets)"
  type        = string
  default     = ""
}

variable "glue_connections" {
  description = "List of Glue connection names to associate with the job"
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Common tags applied to all resources"
  type        = map(string)
  default = {
    ManagedBy   = "terraform"
    Environment = "dev"
    Project     = "aws-streaming-flight-radar-glue"
  }
}
