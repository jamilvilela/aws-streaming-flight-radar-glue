---
id: spec-glue-streaming-dms-cdc
title: Specification — Glue Job Batch + Streaming DMS CDC
status: draft
version: 1.0
created: 2026-07-02
author: Data Engineering Team
---

# Specification — Glue Batch + Streaming DMS CDC

> Generated from `agents.md` and `PRD.md`.

## 1. Overview

CDC (Change Data Capture) processing pipeline using **AWS Glue 5.0 (PySpark 4.0)** with **pure Spark** (no GlueContext, DynamicFrame, or Job API). Operates in **two modes**:

- **Batch mode (full load):** Runs when DMS completes the initial load. Processes all tables **sequentially** in the order defined by the `order` field in config.json.
- **Streaming mode (CDC):** Runs continuously after the full load. Starts **N concurrent queries** (one per table), each reading its CDC prefix via `readStream` with `forEachBatch`.

Both modes share the same `main.py` script, differentiated by the `--mode` parameter.

### Technology Stack

| Component | Version |
|------------|--------|
| AWS Glue | 5.0 |
| Apache Spark | 4.0 |
| Python | 3.9 |
| Format | **Delta Lake** (target) / Parquet (rejects) |
| Infrastructure | Terraform ≥ 1.5 |
| Tests | pytest + boto3 |

## 2. Architecture

### Diagrama de Arquitetura (Mermaid)

```mermaid
flowchart TD
    DMS["DMS Task<br/>(full-load-and-cdc)"]
    LAND["S3 Landing<br/>(prefixos por tabela + CDC)"]
    EVB["EventBridge<br/>(DMS Full Load Complete)"]
    LAMBDA["Lambda glue_starter"]
    BATCH["Glue Batch Job<br/>--mode=batch · sequencial"]
    TRIG["Glue Trigger<br/>(CONDITIONAL)"]
    STREAM["Glue Streaming Job<br/>--mode=streaming · concorrente"]
    READER["Reader"]
    DQ["DataQuality<br/>(4 estágios)"]
    WRITER["Writer<br/>(Delta MERGE)"]
    RAW["S3 Raw<br/>(tabelas Delta)"]
    ETL["EtlControl"]
    QM["QualityMetrics"]

    DMS --> LAND
    LAND --> EVB
    EVB --> LAMBDA
    LAMBDA --> BATCH
    BATCH --> TRIG
    TRIG --> STREAM
    LAND --> READER
    BATCH --> READER
    STREAM --> READER
    READER --> DQ
    DQ --> WRITER
    WRITER --> RAW
    DQ -.-> ETL
    DQ -.-> QM
```

## 3. Components

### 3.1. Config — `app/aws-glue/src/dependencies/config_models.py` (+ `config/` JSON)

**Dataclasses:**
- `SchemaField(name, type, comment, nullable)`
- `PartitionKey(name, type)`
- `CdcConfig(op_column, timestamp_column, delete_strategy)`
- `SourceConfig(source, order, source_location, cdc_source_location, format, cdc_config, checkpoint_location, target)`
- `TargetConfig(catalog, location, rejected_location, format, compression, partition_keys, schema, primary_key, enum_columns, cod_unico_expr)`
- `Config` with `_sources: List[SourceConfig]`, `source` property (first), `sources` (all, sorted by order), `get_source(name)` method, and `from_files()`, `from_s3()`, `to_dict()` methods

**Configuration files:**
| File | Purpose | Content |
|---------|-----------|----------|
| `app/aws-glue/src/dependencies/config/config.json` | Unified configuration | List of tables with embedded source + target |

### 3.2. Reader — `app/aws-glue/src/dependencies/reader.py`
- Method `read(source, mode="streaming")` — dispatcher for batch or streaming mode
- **Streaming mode:** `spark.readStream.format("parquet")` with `maxFilesPerTrigger=1`, `cleanSource=archive`, `sourceArchiveDir`, `includeExistingFiles=false`, reading from `source.cdc_source_location` (CDC-only prefix)
- **Batch mode:** `spark.read.format("parquet").load(source.source_location)` — reads all existing full-load files
- S3 checkpoint (`source.checkpoint_location`) — streaming only

### 3.3. AwsHelper — `app/aws-glue/src/dependencies/aws_helper.py`
- Reusable boto3 utility class, decoupled from the pipeline
- Lazy clients: `s3`, `athena`, `glue`, `sts`, `cloudwatch`
- `get_account_id()` → returns account ID via STS
- `run_athena_query(spark, query, database, workgroup, location)` → executes Athena query and returns Spark DataFrame
- `move_s3_objects(source_bucket, source_prefix, dest_bucket, dest_prefix, pattern, delete_source)` → moves S3 objects with regex filter
- `put_metric(namespace, metric_name, value, unit, dimensions)` → publishes CloudWatch metric

### 3.4. DataQuality — `app/aws-glue/src/dependencies/data_quality.py`
- 4-stage pipeline executed on raw DataFrame
- Uses `TargetConfig` schema for dynamic validation
- **No explicit dedup** — uniqueness guaranteed by Delta MERGE on write
- Rejected records enriched with `_reject_table`, `_reject_rule`, `_reject_timestamp`
- Returns `(valid_df, rejects_df)`

### 3.5. Writer — `app/aws-glue/src/dependencies/writer.py`
- **Delta Lake** write resolved by path with `DeltaTable.forPath()` (independent of catalog metadata)
- Generates `cod_unico` via `F.concat_ws("_", *pk_cols)` for merge key
- Derives `event_date` via `F.to_date(F.col(timestamp_col))`
- Bootstraps the physical Delta table (`_delta_log`) at `target.location` on first write, then MERGE on subsequent writes
- Maps DMS CDC short names (`Op` / `dms_timestamp`) to catalog column names (`cdc_operation` / `cdc_timestamp`) via `_map_cdc_columns`
- MERGE: `WHEN NOT MATCHED AND Op <> 'D' THEN INSERT` / `WHEN MATCHED AND Op = 'D' THEN DELETE` / `WHEN MATCHED THEN UPDATE`
- **No manual compaction** — Delta manages via auto-optimize
- `write_rejects()` for rejected records (Parquet format)

### 3.6. EtlControl — `app/aws-glue/src/dependencies/etl_control.py`
- Separate class for registering in `etl_control`
- Method `register(execution_id, source_name, status, records_read, records_written, records_rejected, target, elapsed_seconds, error_message)`
- Resolves account_id via boto3 STS

### 3.7. QualityMetrics — `app/aws-glue/src/dependencies/quality_metrics.py`
- Separate class for metrics in `data_quality_metrics`
- Method `save(target, status, records_read, records_written, records_rejected)`
- Resolves account_id via boto3 STS

### 3.8. Processor — `app/aws-glue/src/dependencies/processor.py`
- Pipeline orchestrator
- Method `run(source, target, mode="streaming", dataframe=None)`
- In batch mode with pre-read `dataframe`, skips the read step
- 7-stage pipeline:
  1. Read via Reader (or uses pre-read DataFrame in batch mode)
  2. DataQuality.validate()
  3. Writer.write() valid data
  4. Writer.write_rejects() rejects
  5. EtlControl.register()
  6. QualityMetrics.save()
  7. Logging

### 3.9. Main — `app/aws-glue/src/main.py`
- Argument parsing via `argparse`: `--config_s3_path`, `--conf`, `--mode` (batch|streaming)
- `main()`: loads config (`Config.from_s3(args.config_s3_path)`), gets sorted list via `config.sources`, dispatches `_run_batch()` or `_run_streaming()`
- `_run_batch()`: iterates sources in `order`, reads batch of each table and processes via `Processor.run(mode="batch", dataframe=raw_df)`. Errors in one table do not block the others.
- `_run_streaming()`: starts **one streaming query per table**, each with `trigger(processingTime="5 minutes")` and own checkpoint. `awaitTermination()` keeps the job alive.
- `_parse_conf()` converts string "key=val key=val" to dict
- `_init_spark()` applies configs dynamically

### 3.10. Lambda — `app/aws-lambda/start_workflow/start_glue_job.py`
- Lives **outside** `app/aws-glue/src` (kept separate from the Glue job source)
- Triggered by EventBridge when the DMS full load completes
- Starts the full-load Glue workflow via `glue.start_workflow_run()`
- The workflow uses native Glue sequencing (on-demand + conditional triggers) to start streaming after the batch succeeds, avoiding polling and concurrent writes

## 4. Infrastructure (Terraform)

### Resources (organizados por serviço em `infra/`)

| Name | File | Type | Description |
|------|------|------|-----------|
| `aws_kms_key.glue` | `kms.tf` | KMS Key | SSE-KMS/CSE-KMS encryption |
| `aws_kms_alias.glue` | `kms.tf` | KMS Alias | `alias/glue-flight-radar` |
| `aws_glue_security_configuration.glue` | `glue.tf` | Security Config | CloudWatch SSE-KMS, bookmarks CSE-KMS, S3 SSE-KMS |
| `aws_glue_connection.vpc` | `glue.tf` | Glue Connection | NETWORK, private subnet, default SG |
| `aws_glue_job.full_load_batch` | `glue.tf` | Glue Job (batch) | `glue-flight-radar-batch` — Glue 5.0, Python 3.9, `--mode=batch` — processes N tables sequentially |
| `aws_glue_job.streaming_minibatch` | `glue.tf` | Glue Job (streaming) | `glue-flight-radar-streaming` — Glue 5.0, Python 3.9, `--mode=streaming` — N concurrent queries |
| `aws_glue_workflow.dms_full_load` | `glue.tf` | Glue Workflow | `glue-flight-radar-batch-workflow` — orchestrates full load → streaming |
| `aws_glue_trigger.start_full_load` | `glue.tf` | Glue Trigger | ON_DEMAND — starts the batch job on workflow run |
| `aws_glue_trigger.start_streaming_after_full_load` | `glue.tf` | Glue Trigger | CONDITIONAL — starts streaming CDC after batch succeed |
| `aws_iam_role.lambda_glue_starter` | `iam.tf` | IAM Role | Role for Lambda to start the full-load Glue workflow |
| `aws_iam_role_policy.lambda_glue_starter` | `iam.tf` | IAM Policy | Permission `glue:StartWorkflowRun` on the full-load workflow |
| `aws_lambda_function.glue_starter` | `lambda.tf` | Lambda Function | Starts the full-load Glue workflow (EventBridge target) |
| `aws_lambda_permission.eventbridge_invoke_glue_starter` | `lambda.tf` | Lambda Permission | Allows EventBridge to invoke the Lambda |
| `aws_cloudwatch_event_rule.full_load_complete` | `cloudwatch.tf` | EventBridge Rule | DMS full load completed event (source=aws.dms) |
| `aws_cloudwatch_event_target.start_glue_batch` | `cloudwatch.tf` | EventBridge Target | Triggers the Lambda via InvokeFunction |
| `data.archive_file.helpers` | `s3.tf` | Archive Data | Creates helpers.zip from `app/aws-glue/` (contains the `src` package) |
| `aws_s3_object.main_py` | `s3.tf` | S3 Object | Uploads `main.py` to `aws-glue/jobs/flight-radar/src/` |
| `aws_s3_object.helpers_zip` | `s3.tf` | S3 Object | Uploads `helpers.zip` to `aws-glue/jobs/flight-radar/src/dependencies/` |
| `aws_s3_object.config_json` | `s3.tf` | S3 Object | Uploads `config.json` (with `{account_id}` resolved) to `aws-glue/jobs/flight-radar/src/dependencies/config/` |
| `aws_s3_object.lambda_start_glue_job` | `s3.tf` | S3 Object | Uploads Lambda source to `aws-lambda/flight-radar/start_workflow/` |

### Deployment Structure (Workspace Bucket)

Artifacts are published to the workspace bucket mirroring the project layout:

```
lakehouse-workspace-{account_id}/
├── aws-glue/jobs/flight-radar/src/
│   ├── main.py
│   └── dependencies/
│       ├── helpers.zip
│       └── config/config.json
└── aws-lambda/flight-radar/start_workflow/
    └── start_glue_job.py
```

The Glue job is named for a **single objective** (`glue_job_name = glue-flight-radar`).
Two job definitions are derived from it and differentiated by process in
code/classes via the `--mode` argument:
- `glue-flight-radar-batch` — full load (`--mode=batch`)
- `glue-flight-radar-streaming` — streaming CDC (`--mode=streaming`)

### Dynamic Spark Configs (`locals.tf → spark_conf`)
- `spark.sql.adaptive.enabled` = true
- `spark.sql.adaptive.coalescePartitions.enabled` = true
- `spark.sql.adaptive.skewJoin.enabled` = true
- `spark.sql.adaptive.advisoryPartitionSizeInBytes` = 128MB
- `spark.sql.shuffle.partitions` = 200
- `spark.sql.parquet.compression.codec` = snappy
- `spark.executor.memory` = 4g
- `spark.driver.memory` = 4g
- `spark.executor.memoryOverhead` = 2g
- `spark.driver.memoryOverhead` = 2g
- `spark.memory.offHeap.enabled` = true
- `spark.memory.offHeap.size` = 2g
- `spark.dynamicAllocation.enabled` = true
- `spark.dynamicAllocation.shuffleTracking.enabled` = true
- `spark.sql.streaming.schemaInference` = true
- **Delta Lake configs:**
  - `spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite` = true
  - `spark.databricks.delta.properties.defaults.autoOptimize.autoCompact` = true
  - `spark.sql.extensions` = `io.delta.sql.DeltaSparkSessionExtension`
  - `spark.sql.catalog.spark_catalog` = `org.apache.spark.sql.delta.catalog.DeltaCatalog`
  - `spark.delta.logStore.class` = `org.apache.spark.sql.delta.storage.S3SingleDriverLogStore`

### Existing Data Sources
- IAM Role: `role-datalake-analytics`
- VPC: default (`vpc-022139f6bee3cbdd5`)
- Subnets: private in us-east-1a/b/c
- Security Group: default (`sg-0f885f9d1473a7777`)

> ⚠️ Glue Catalog databases and tables are not managed by this module.

## 5. Scripts

### `ci-cd/deploy.sh`
- Checks prerequisites (Terraform, AWS CLI, jq)
- Initializes Terraform (local state)
- Selects workspace (`-e ENV`)
- Applies Terraform (artifact upload is done by `aws_s3_object` resources)

### `ci-cd/rollback.sh`
- Confirms rollback (or `--yes`)
- Runs `terraform destroy`
- Does **not** remove S3 files

## 6. Glue Job Parameters

| Parameter | Description | Required | Default |
|-----------|-----------|-------------|---------|
| `--config_s3_path` | S3 path to config.json (all tables with source + target) | Yes | — |
| `--conf` | Spark configs (key=val key=val) | No | "" |
| `--mode` | Execution mode: `batch` (sequential) \| `streaming` (concurrent) | No | `streaming` |

## 7. Data Lake Tables

| Table | Database | Purpose | Partition |
|--------|----------|-----------|----------|
| `tbl_aircraft` | `db_raw` | Aircraft registry | `event_date` |
| `tbl_airports` | `db_raw` | Airports | `event_date` |
| `tbl_airlines` | `db_raw` | Airlines | `event_date` |
| `tbl_flights` | `db_raw` | Flights fact table | `event_date` |
| `tbl_aircraft_positions` | `db_raw` | Positions (high volume) | `event_date` |
| `tbl_countries` | `db_raw` | Countries | `event_date` |
| `tbl_aircraft_types` | `db_raw` | Aircraft types | `event_date` |
| `tbl_routes` | `db_raw` | Routes | `event_date` |
| `etl_control` | `db_raw` | Execution control | `reference_date` |
| `data_quality_metrics` | `db_raw` | Quality metrics | `reference_date` |

## 8. Conventions

- **Buckets**: named with account ID: `lakehouse-{tier}-{account_id}`
- **Format**: Delta Lake (target) + Snappy; Parquet for rejects
- **IAM**: role `role-datalake-analytics`
- **Spark**: pure Spark, no Glue APIs
- **Configs**: unified config.json (8 tables with embedded source + target)
- **Streaming + Batch**: same `main.py` script, differentiated by `--mode`
- **Bookmarks**: not used (uses `cleanSource=archive`)
- **Separate S3 prefixes**: DMS `CdcPath` writes CDC to a distinct prefix to avoid reprocessing
- **Two job definitions**: batch and streaming — share the same script
- **Deploy layout**: Glue artifacts under `aws-glue/jobs/flight-radar/` and Lambda under `aws-lambda/flight-radar/start_workflow/` in the workspace bucket
- **Lambda location**: source in `app/aws-lambda/start_workflow/`, outside `app/aws-glue/src/`

## 9. Event Flow

```mermaid
sequenceDiagram
    participant DMS as DMS Task
    participant EB as EventBridge
    participant Lambda as Lambda glue_starter
    participant Batch as Glue Batch Job
    participant Trigger as Glue Trigger
    participant Stream as Glue Streaming Job

    DMS->>EB: Full Load Completed (8 tables)
    EB->>Lambda: InvokeFunction
    Lambda->>Batch: StartWorkflowRun (--mode=batch)
    Note over Batch: Processes tables SEQUENTIALLY (order 1..8)
    Batch-->>Trigger: Job Succeeded
    Trigger->>Stream: StartJobRun (--mode=streaming)
    Note over Stream: Starts N CONCURRENT queries (one per table)
    Note over DMS: DMS continues writing CDC to separate prefixes (CdcPath)
```

## 10. Data Quality

4-stage pipeline in `DataQuality`:

| Stage | Function | Description |
|-------|--------|-----------|
| 1 | `_cast_types` | Converts columns to target schema types |
| 2 | `_check_nulls` | Removes records with nulls in required fields |
| 3 | `_check_enums` | Validates values against allowed list in `enum_columns` |
| 4 | `_validate_timestamps` | Validates timestamps (current pass-through) |

## 11. Tests

### Unit (`tests/unit/`)
- `test_config.py`: from_files, from_s3, to_dict, invalid JSON
- `test_reader.py`: streaming read, checkpoint config, schema
- `test_data_quality.py`: 4 stages, rejects, types, enums, nulls
- `test_writer.py`: Delta write, partitions, rejects, bootstrap
- `test_processor.py`: full mocked pipeline

### Integration (`tests/integration/`)
- `conftest.py`: boto3 fixtures (S3, Glue)
- `test_s3_landing.py`: landing bucket structure
- `test_glue_catalog.py`: databases and tables existence
- `test_pipeline_e2e.py`: complete pipeline with real data