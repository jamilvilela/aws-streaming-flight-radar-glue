---
name: glue-streaming-agents
description: Specialized agents for implementing the Glue Job streaming mini-batch DMS CDC (tbl_opensky_flights)
---

# Agents — Glue Streaming Mini-Batch DMS CDC (tbl_opensky_flights)

## Agent: `glue-job-builder`
**Purpose:** Implement Glue Job PySpark code (main.py, processor, config, reader, data_quality, writer, etl_control, quality_metrics)

**Skills:**
- PySpark 4.0 / AWS Glue 5.1 (pure Spark — no GlueContext, DynamicFrame or Job)
- Streaming reading with `spark.readStream` and `forEachBatch`
- S3 checkpointing with `cleanSource=archive` (no job bookmarks)
- DataFrame API: schema validation, type casting
- **Delta Lake**: `DeltaTable.forName()` via Glue Catalog, MERGE by PK, partitioning
- Partitioned write to Delta Lake with MERGE (cross-batch dedup)
- Python dataclasses with type hints (SourceConfig, TargetConfig)
- Spark optimization configs (AQE, shuffle, off-heap, dynamic allocation, Delta auto-optimize)
- Configs passed via `--conf` and applied dynamically

**Prompt pattern to invoke:**
```
Use the glue-job-builder agent to implement the {ClassName} class 
in app/src/dependencies/{file_name}.py with the following requirements:
- {requirement 1}
- {requirement 2}
...
```

## Agent: `terraform-infra`
**Purpose:** Create and maintain AWS infrastructure resources via Terraform

**Skills:**
- AWS Glue Job definitions (Glue 5.1)
- Glue Security Configuration and KMS keys
- Glue Connection type NETWORK (VPC)
- S3 Buckets — artifact upload via `aws_s3_object` resources
- IAM roles and policies (role-datalake-analytics)
- Bash setup and rollback scripts (no artifact upload in scripts)

## Agent: `data-quality-spec`
**Purpose:** Define and implement data quality rules for tbl_opensky_flights

**Skills:**
- Schema validation (types, nullability, constraints)
- 4-stage pipeline: cast_types, check_nulls, check_enums, validate_timestamps
- **No explicit dedup** — delegated to Delta MERGE on write
- Quality metrics generation via `QualityMetrics`
- Data structure for rejected records (`_reject_table`, `_reject_rule`, `_reject_timestamp`)

## Agent: `config-designer`
**Purpose:** Design and validate JSON configuration files (single file)

**Skills:**
- **config.json**: unified configuration (list of tables, each with embedded source + target)
- Clear separation between connection config and destination config
- S3 locations (source, target, rejected, checkpoint)
- Deployed to `glue-jobs/flight-radar/src/dependencies/config/config.json`

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
- Source kept in `app/lambdas/` (outside `app/src/`)
- Deployed via `infra/lambda.tf` and uploaded to `lambdas/flight-radar/start_workflow/`

## How to use

### To generate code for a specific class:
```
@glue-job-builder Create the Reader class in app/src/dependencies/reader.py 
that reads Parquet data from S3 in pure streaming (no Glue, no batch, no bookmarks).
```

### To create infrastructure:
```
@terraform-infra Create the aws_glue_job resource for the streaming-minibatch-dms job 
with worker_type G.1X, glue_version 5.0, and script_location in the workspace bucket.
```

### To validate quality:
```
@data-quality-spec Define the quality rules for the tbl_opensky_flights table 
with a 5-stage pipeline.
```

### To configure source:
```
@config-designer Create config.json with the list of tables (each with source + target)
for the tbl_opensky_flights destination schema.
```

### To create tests:
```
@test-builder Create unit tests for the DataQuality class 
with SparkSession mock, covering type validation, enums and rejects.
```
