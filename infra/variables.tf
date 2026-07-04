#===============================================================================
# Variables — Glue Streaming Mini-Batch DMS Module
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

variable "control_account" {
  description = "AWS account ID for the data lake"
  type        = string
  default     = "331504768406"
}

variable "databases" {
  description = "Glue Data Catalog database names"
  type        = object({
    landing = string
    raw     = string
    trusted = string
    business = string
  })
  default = {
    landing  = "db_landing"
    raw      = "db_raw"
    trusted  = "db_trusted"
    business = "db_business"
  }
}

variable "tables" {
  description = "Glue Data Catalog table names"
  type        = object({
    tbl_opensky_flights = string
    etl_control         = string
    data_quality        = string
  })
  default = {
    tbl_opensky_flights = "tbl_opensky_flights"
    etl_control         = "etl_control"
    data_quality        = "data_quality_metrics"
  }
}

variable "buckets" {
  description = "S3 bucket names (without account ID suffix)"
  type        = object({
    landing   = string
    raw       = string
    trusted   = string
    business  = string
    workspace = string
  })
  default = {
    landing   = "lakehouse-landing-331504768406"
    raw       = "lakehouse-raw-331504768406"
    trusted   = "lakehouse-trusted-331504768406"
    business  = "lakehouse-business-331504768406"
    workspace = "lakehouse-workspace-331504768406"
  }
}

variable "glue_job_name" {
  description = "Name of the Glue job"
  type        = string
  default     = "glue-streaming-minibatch-dms"
}

variable "glue_iam_role_name" {
  description = "Name of the existing IAM role for Glue"
  type        = string
  default     = "role-datalake-analytics"
}

variable "glue_worker_type" {
  description = "Glue worker type (G.1X, G.2X, etc.)"
  type        = string
  default     = "G.1X"
}

variable "glue_number_of_workers" {
  description = "Number of Glue workers"
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
  description = "S3 path to the origins.json config file"
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
