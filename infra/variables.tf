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
    fr_aircraft           = string
    fr_airports           = string
    fr_airlines           = string
    fr_flights            = string
    fr_aircraft_positions = string
    fr_countries          = string
    fr_aircraft_types     = string
    fr_routes             = string
    etl_control            = string
    data_quality           = string
  })
  default = {
    fr_aircraft           = "fr_aircraft"
    fr_airports           = "fr_airports"
    fr_airlines           = "fr_airlines"
    fr_flights            = "fr_flights"
    fr_aircraft_positions = "fr_aircraft_positions"
    fr_countries          = "fr_countries"
    fr_aircraft_types     = "fr_aircraft_types"
    fr_routes             = "fr_routes"
    etl_control            = "etl_control"
    data_quality           = "data_quality_metrics"
  }
}

variable "glue_job_name" {
  description = "Base name of the Glue job (single objective). Both the batch and streaming job definitions derive from this value."
  type        = string
  default     = "glue-flight-radar"
}

variable "full_load_job_name" {
  description = "Name of the full-load batch Glue job (derived from glue_job_name)"
  type        = string
  default     = ""
}

variable "glue_job_role_name" {
  description = "Name of the dedicated IAM role created for the Glue jobs and interactive sessions"
  type        = string
  default     = "role-glue-job-flight-radar"
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

variable "dms_replication_config_arn" {
  description = "ARN of the DMS Serverless replication config that loads flight_radar. Used by the batch-starter Lambda to check full-load completion. Leave empty to auto-detect the single replication in the account."
  type        = string
  default     = ""
}

variable "full_load_check_interval" {
  description = "Minutes between full-load completion checks (EventBridge schedule that invokes the batch-starter Lambda)"
  type        = number
  default     = 5
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
