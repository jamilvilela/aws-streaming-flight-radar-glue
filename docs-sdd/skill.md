---
name: glue-streaming-cdc
description: >-
  Glue Job streaming for CDC data ingestion from AWS DMS,
  with S3 reading, quality validation and writing to Data Lake (raw layer)
  in Delta Lake format with MERGE for cross-batch dedup.
---

# Skill: Glue Streaming CDC

## Purpose

Implement an **AWS Glue 5.0 (PySpark 4.0)** job to process **CDC (Change Data Capture)** data replicated by **AWS DMS Serverless** in the **landing** bucket — the `flight_radar` tables — apply quality rules, and write to the **Data Lake** (raw layer) in **Delta Lake** format.

## Technology Stack

| Component | Version |
|------------|--------|
| AWS Glue | **5.0** |
| Apache Spark | **4.0** |
| Python | **3.9** |
| Format | **Delta Lake** (target) / Parquet (rejects) |
| Tests | pytest + boto3 (integration) |
| Glue APIs | **NOT used** (pure Spark) |

## Job Architecture

### Diagrama de Arquitetura (Mermaid)

```mermaid
flowchart TD
    DMS["DMS Task<br/>(full-load-and-cdc)"]
    LAND["S3 Landing<br/>(DMS Parquet)"]
    SCHED["EventBridge Schedule<br/>rate({interval} min)"]
    LAMBDA["Lambda glue_starter"]
    LOCK["DynamoDB Lock<br/>glue-flight-radar-workflow-lock"]
    WF["Glue Workflow<br/>glue-flight-radar-batch-workflow"]
    ON_DEMAND["Glue Trigger<br/>ON_DEMAND"]
    BATCH["Glue Batch Job<br/>--mode=batch"]
    TRIG["Glue Trigger<br/>CONDITIONAL"]
    STREAM["Glue Streaming Job<br/>--mode=streaming"]
    READER["Reader"]
    DQ["DataQuality<br/>(4 estágios)"]
    WRITER["Writer<br/>(Delta MERGE por PK)"]
    RAW["S3 Raw<br/>(tabelas Delta)"]
    ETL["EtlControl<br/>(execution log)"]
    QM["QualityMetrics<br/>(quality metrics)"]
    REJ["RejectedRecords<br/>(centralized table)"]

    DMS --> LAND
    SCHED --> LAMBDA
    LAMBDA -->|describe_replications<br/>FullLoadProgressPercent==100| DMS
    LAMBDA -->|put_item<br/>attribute_not_exists| LOCK
    LAMBDA -->|start_workflow_run| WF
    WF --> ON_DEMAND
    ON_DEMAND --> BATCH
    BATCH --> TRIG
    TRIG --> STREAM
    LAND --> READER
    BATCH --> READER
    STREAM --> READER
    READER --> DQ
    DQ --> WRITER
    DQ --> REJ
    WRITER --> RAW
    DQ -.-> ETL
    DQ -.-> QM
```

## Infrastructure (Terraform)

The Terraform module in `infra/` manages all required AWS resources, organized by service:

| Resource | File | Description |
|---------|------|-----------|
| `aws_kms_key.glue` | `kms.tf` | KMS key for SSE-KMS/CSE-KMS encryption |
| `aws_glue_security_configuration.glue` | `glue.tf` | Security config (CloudWatch, bookmarks, S3) |
| `aws_glue_connection.vpc` | `glue.tf` | VPC connection (private subnet, default security group) |
| `aws_glue_workflow.dms_full_load` | `glue.tf` | Glue workflow orchestrating full load → streaming |
| `aws_glue_job.full_load_batch` | `glue.tf` | Full-load batch Glue job (Glue 5.0, Spark 4.0, Python 3.9) |
| `aws_glue_job.streaming` | `glue.tf` | Streaming CDC Glue job (Glue 5.0, Spark 4.0, Python 3.9) |
| `aws_glue_trigger.start_streaming_after_full_load` | `glue.tf` | CONDITIONAL trigger — starts streaming after batch |
| `aws_iam_role.lambda_glue_starter` + policy | `iam.tf` | IAM role/policy for Lambda → Glue |
| `aws_lambda_function.glue_starter` + permission | `lambda.tf` | Lambda that starts the full-load workflow (EventBridge schedule target) |
| `aws_cloudwatch_event_rule.full_load_complete` + target | `cloudwatch.tf` | EventBridge schedule rule/target — polls DMS task status until full load completes |
| `aws_dynamodb_table.workflow_lock` | `dynamodb.tf` | DynamoDB lock keyed by task ARN — single workflow start per full load |
| `data.archive_file.helpers` + `aws_s3_object.*` | `s3.tf` | Creates helpers.zip and declarative upload of main.py, config.json and Lambda source to the workspace bucket |

> ⚠️ Glue Catalog databases and tables are not managed by this module — they already exist in the Data Lake.

### Dynamic Spark Configs

Spark configs are defined in `locals.tf` as a `spark_properties` map and converted to a `--conf` string (format `key=value key=value ...`). `main.py` parses this string via `_parse_conf()` and applies each property dynamically to `SparkSession.builder`, with no hardcoded values.

Data sources: default VPC, subnets, security group (the Glue role `role-glue-job-flight-radar` is created by this module in `infra/iam.tf`).

## Components

### 1. `main.py` — Entry Point
- Initializes pure `SparkSession` (no GlueContext, no Glue Job API)
- Parses arguments: `--config_s3_path`, `--conf`, `--mode` (batch|streaming), `--generate-test-rejects`
- Applies **Spark optimization configs** dynamically via `--conf`
- Batch mode: processes N tables **sequentially** (`order` field)
- Streaming mode: starts **N concurrent queries** (one per table) with `trigger(processingTime="5 minutes")`
- Instantiates the `Processor` class and executes the pipeline in each micro-batch

### 2. `config_models.py` — Configuration Models (dataclasses)
- Defines dataclasses for `SchemaField`, `PartitionKey`, `CdcConfig`, `TargetConfig`, `SourceConfig`, `Config`
- Reads a **single** `config.json` with all tables (embedded source + target)
- `SourceConfig`: `source`, `order`, `source_location`, `cdc_source_location`, `archive_location`, `format`, `filter`, `cdc_config`, `checkpoint_location`, `target`
- `TargetConfig`: `catalog` (database, table), `format`, `compression`, `partition_keys`, `schema`, `primary_key`, `enum_columns`, `cod_unique_expr`
- `Config`: `sources` (list sorted by `order`), methods `from_file()` (local) and `from_s3()` (S3)

### 3. `Reader` — Data Reading (batch + streaming)
- Reads Parquet files from S3 (DMS CDC) in **batch** or **streaming** mode
- **Streaming:** uses `spark.readStream` with `cleanSource=archive`, `sourceArchiveDir`, `includeExistingFiles=true`, reading the CDC-only prefix, `maxFilesPerTrigger=1000`, `pathGlobFilter=2*.parquet`
- **Batch:** uses `spark.read.format("parquet").load()` to read all existing full-load files
- Returns raw Spark `DataFrame`
- Schema inference for `aircraft_positions` table (handles FIXED_LEN_BYTE_ARRAY)
- Filter support via `source.filter`

### 4. `data_quality.py` — Quality and Validation
- Receives raw DataFrame and target schema (from `TargetConfig`)
- 4-stage pipeline:
  1. **Cast types** — converts columns according to target schema
  2. **Null check** — filters nulls in NOT NULL fields
  3. **Enum validation** — validates `enum_columns` values
  4. **Timestamp validation** — validates timestamps (pass-through)
- **No explicit dedup** — uniqueness guaranteed by Delta MERGE on write (Writer)
- **Invalid** records are written to centralized `rejected_records` table with metadata (`_reject_table`, `_reject_rule`, `_reject_timestamp`)
- Returns tuple: `(valid DataFrame, rejects DataFrame)`
- **Dynamic operation**: reads schema from JSON config, allowing validation of any source without code changes

### 5. `writer.py` — Data Lake Write (Delta Lake)
- Receives valid DataFrame and target table metadata
- Generates `cod_unique` column via `F.concat_ws("_", *pk_cols)` for merge key
- Writes in **Delta Lake** format to the **raw** bucket
- Resolves the table via the Glue Data Catalog (`DeltaTable.forName`) and performs **Delta MERGE** based on composite PK for cross-batch uniqueness
- Bootstraps the physical Delta table on first write; MERGE on subsequent writes
- Maps DMS CDC columns (`Op` / `dms_timestamp`) to catalog names (`cdc_operation` / `cdc_timestamp`)
- Partitions data by `event_date` (derived from timestamp column via `F.to_date()`) or `aircraft_icao24` for positions table
- **No compaction needed** — Delta Lake manages optimization automatically via auto-optimize
- Supports writing rejects with `write_rejects()` (Parquet format)

### 6. `rejected_records.py` — Centralized Rejected Records
- Single table for all entities: `db_raw.rejected_records`
- JSON payload column (`rejected_record_json`) for schema-flexible rejected record storage
- Metadata columns: `execution_id`, `execution_timestamp`, `source_database`, `source_table`, `target_database`, `target_table`, `reject_rule`, `reject_reason`, `reference_date`
- Partitioned by `reference_date` for query performance
- Append-only write to S3 with `partitionBy("reference_date")` (no merge)
- Resolved through Glue Data Catalog
- Updates Glue Catalog partitions after write via `update_table_partitions`

### 7. `etl_control.py` — Execution Log
- Separate class responsible for writing metadata to the `etl_control` table
- Records: `execution_id`, `job_name`, `source`, `execution_start`, `execution_end`, `status`, `records_read`, `records_written`, `records_rejected`, `target_partition`, `error_message`, `reference_date`
- Writes to the `db_raw.etl_control` catalog table via `saveAsTable` (Parquet, partitioned by `reference_date`)

### 8. `quality_metrics.py` — Quality Metrics
- Separate class responsible for saving metrics to `data_quality_metrics`
- Records: `database`, `table`, `processing_timestamp`, `metric`, `rule`, `status`, `failure_reason`, `partition`, `technology`, `reference_date`
- Uses `SparkSession` to write partitioned Parquet to the raw layer

### 9. `processor.py` — Orchestrator
- Coordinates the full pipeline: Config → Reader → DataQuality → Writer → EtlControl → QualityMetrics
- Method `run(source, target, mode, dataframe)` executes the pipeline: Read → Validate → Write Rejects → Write (Delta MERGE) → Register → Metrics
- Delegates `_register_execution` to `EtlControl` and `_save_quality_metrics` to `QualityMetrics`

### 10. `aws_helper.py` — Reusable AWS Helper
- Lazy boto3 clients: `s3`, `athena`, `glue`, `sts`, `cloudwatch`
- `get_account_id()` via STS
- `run_athena_query()` returns Spark DataFrame
- `move_s3_objects()` with regex filter
- `put_metric()` for CloudWatch
- `get_json_from_s3()` for config loading
- `get_table_location()` for Glue Catalog table locations
- `update_table_partitions()` for partition discovery and creation

### 11. Lambda — `app/aws-lambda/start_workflow/start_glue_job.py`
- Kept **outside** `app/aws-glue/src`, separate from the Glue job source
- Invoked on a **schedule** (EventBridge `rate()` rule) — polls the DMS replication task status
- Starts the workflow only when the full-load phase completes (`FullLoadProgressPercent == 100` and `TablesLoading == 0`)
- Uses a **DynamoDB lock** (conditional `attribute_not_exists` on the task ARN) to guarantee a single start per full load
- Starts the full-load Glue workflow via `glue.start_workflow_run()`
- Native Glue sequencing (on-demand + conditional triggers) starts streaming after the batch succeeds — no polling in the Lambda

## Deployment Structure (Workspace Bucket)

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

## Spark Optimization Configs

Spark configs are defined in `infra/locals.tf` in the `spark_properties` map:

- `spark.sql.extensions` = `io.delta.sql.DeltaSparkSessionExtension`
- `spark.sql.catalog.spark_catalog` = `org.apache.spark.sql.delta.catalog.DeltaCatalog`
- `spark.sql.catalogImplementation` = `hive`
- `spark.hadoop.hive.metastore.client.factory.class` = `com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory`
- `spark.sql.files.maxPartitionBytes` = `256MB`
- `spark.sql.files.openCostInBytes` = `32MB`
- `spark.sql.files.maxPartitionNum` = `2000`
- `spark.sql.sources.parallelPartitionDiscovery.threshold` = `32`
- `spark.sql.sources.parallelPartitionDiscovery.parallelism` = `10000`
- `spark.sql.parquet.filterPushdown` = `true`
- `spark.sql.parquet.enableVectorizedReader` = `true`
- `spark.sql.adaptive.enabled` = `true`
- `spark.sql.adaptive.coalescePartitions.enabled` = `true`
- `spark.sql.adaptive.advisoryPartitionSizeInBytes` = `128MB`
- `spark.databricks.delta.optimizeWrite.enabled` = `true`
- `spark.databricks.delta.autoCompact.enabled` = `true`
- `spark.databricks.delta.autoCompact.minNumFiles` = `10`
- `spark.databricks.delta.autoCompact.maxFileSize` = `134217728`
- `spark.sql.parquet.compression.codec` = `snappy`

## Data Lake Conventions

| Layer | Bucket | Database | Format |
|--------|--------|----------|---------|
| Landing | `lakehouse-landing-{account_id}` | — | Parquet (DMS) |
| Raw | `lakehouse-raw-{account_id}` | `db_raw` | **Delta Lake** (target) / Parquet (rejects) |
| Workspace | `lakehouse-workspace-{account_id}` | — | Scripts, configs, checkpoints |

## Involved Tables

| Table | Purpose | Partitions |
|--------|-----------|-----------|
| `fr_aircraft` | Aircraft registry | `event_date` |
| `fr_airports` | Airports | `event_date` |
| `fr_airlines` | Airlines | `event_date` |
| `fr_flights` | Flights fact table | `event_date` |
| `fr_aircraft_positions` | Positions (high volume) | `aircraft_icao24` |
| `fr_countries` | Countries | `event_date` |
| `fr_aircraft_types` | Aircraft types | `event_date` |
| `fr_routes` | Routes | `event_date` |
| `etl_control` | Glue Job execution control | `reference_date` |
| `data_quality_metrics` | Quality metrics | `reference_date` |
| `rejected_records` | Centralized rejected records | `reference_date` |

## Glue Job Parameters

| Parameter | Description | Example |
|-----------|-----------|---------|
| `--config_s3_path` | S3 path to unified config.json (all tables) | `s3://.../aws-glue/jobs/flight-radar/src/dependencies/config/config.json` |
| `--mode` | Execution mode: `batch` or `streaming` | `batch` |
| `--conf` | Dynamic Spark configs (key=val key=val ...) | `spark.sql.shuffle.partitions=200 ...` |
| `--generate-test-rejects` | Generate test rejected records | `true` |

## IAM Role

`role-glue-job-flight-radar` — dedicated role created by this module with minimum permissions to read landing, write raw, access Glue Catalog and KMS.