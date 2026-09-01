---
name: glue-streaming-agents
description: Specialized agents for implementing the Glue Job streaming mini-batch DMS CDC (flight_radar tables)
---

# Agents — Glue Streaming Mini-Batch DMS CDC

## Agent: `glue-job-builder`
**Purpose:** Implement Glue Job PySpark code (main.py, processor, config, reader, data_quality, writer, etl_control, quality_metrics, rejected_records)

**Skills:**
- PySpark 4.0 / AWS Glue 5.0 (pure Spark — no GlueContext, DynamicFrame or Job)
- Streaming reading with `spark.readStream` and `trigger(processingTime=...)`
- S3 checkpointing with `cleanSource=archive` (no job bookmarks)
- DataFrame API: schema validation, type casting
- **Delta Lake**: `DeltaTable.forName()` resolved via the Glue Data Catalog, MERGE by PK, partitioning
- Bootstrap of the Delta table on first write; MERGE on subsequent writes
- Partitioned write to Delta Lake with MERGE (cross-batch dedup)
- Python dataclasses with type hints (SourceConfig, TargetConfig)
- Spark optimization configs (AQE, shuffle, off-heap, dynamic allocation, Delta auto-optimize)
- Configs passed via `--conf` and applied dynamically

**Prompt pattern to invoke:**
```
Use the glue-job-builder agent to implement the {ClassName} class 
in app/aws-glue/src/dependencies/{file_name}.py with the following requirements:
- {requirement 1}
- {requirement 2}
...
```

## Agent: `terraform-infra`
**Purpose:** Create and maintain AWS infrastructure resources via Terraform

**Skills:**
- AWS Glue Job definitions (Glue 5.0, Python 3.9)
- Glue Security Configuration and KMS keys
- Glue Connection type NETWORK (VPC)
- S3 Buckets — artifact upload via `aws_s3_object` resources (declarative, no upload in scripts)
- IAM roles and policies (role-glue-job-flight-radar, lambda_glue_starter)
- `ci-cd/deploy.sh` and `ci-cd/rollback.sh` scripts (setup and rollback only)

## Agent: `data-quality-spec`
**Purpose:** Define and implement data quality rules for the flight_radar tables

**Skills:**
- Schema validation (types, nullability, constraints)
- 4-stage pipeline: cast_types, check_nulls, check_enums, validate_timestamps
- **No explicit dedup** — delegated to Delta MERGE on write
- Quality metrics generation via `QualityMetrics`
- Data structure for rejected records (`_reject_table`, `_reject_rule`, `_reject_timestamp`)

## Agent: `config-designer`
**Purpose:** Design and validate JSON configuration files (single file)

**Skills:**
- **config.json**: unified configuration (list of 8 tables, each with embedded source + target)
- Clear separation between connection config and destination config
- S3 locations (source, cdc_source, target, rejected, checkpoint)
- Deployed to `aws-glue/jobs/flight-radar/src/dependencies/config/config.json`

## Agent: `test-builder`
**Purpose:** Implement unit and integration tests

**Skills:**
- pytest with fixtures and mocks
- Mock SparkSession (no GlueContext), boto3 (S3, Glue)
- Code coverage (pytest-cov)
- Integration tests with real boto3
- Validation of data written to S3 and Glue Catalog
- Tests for EtlControl and QualityMetrics

## Agent: `lambda-starter`
**Purpose:** Implement and maintain the Lambda that starts the full-load Glue workflow

**Skills:**
- Python 3.9 Lambda handler triggered by EventBridge
- `glue.start_workflow_run()` to start the full-load workflow
- Native Glue sequencing (on-demand + conditional triggers) — no polling
- Source kept in `app/aws-lambda/start_workflow/` (outside `app/aws-glue/src/`)
- Deployed via `infra/lambda.tf` and uploaded to `aws-lambda/flight-radar/start_workflow/`

## Agent: `rejected-records-designer`
**Purpose:** Design and implement centralized rejected records handling

**Skills:**
- Single table for all entities (`db_raw.rejected_records`)
- JSON payload column for schema-flexible rejected record storage
- Partitioned by reference_date for query performance
- Append-only write to S3 with partitionBy (no merge)
- Resolved through Glue Data Catalog
- Glue Catalog partition updates via `update_table_partitions`

## How to use

### To generate code for a specific class:
```
@glue-job-builder Create the Reader class in app/aws-glue/src/dependencies/reader.py 
that reads Parquet data from S3 in streaming mode (no Glue APIs, no job bookmarks).
```

### To create infrastructure:
```
@terraform-infra Create the aws_glue_job resource for the streaming-minibatch-dms job 
with worker_type G.1X, glue_version 5.0, and script_location in the workspace bucket.
```

### To validate quality:
```
@data-quality-spec Define the quality rules for the fr_flights table 
with the 4-stage pipeline.
```

### To configure source:
```
@config-designer Create config.json with the list of tables (each with source + target)
for the flight_radar destination schemas.
```

### To create tests:
```
@test-builder Create unit tests for the DataQuality class 
with SparkSession mock, covering type validation, enums and rejects.
```

### To implement rejected records:
```
@rejected-records-designer Create the RejectedRecords class 
with centralized table write and JSON payload.
```