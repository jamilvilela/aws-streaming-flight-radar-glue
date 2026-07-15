---
id: feature-glue-streaming-dms-cdc
title: Glue Job Streaming Mini-Batch for DMS CDC (entity tbl_opensky_flights)
status: draft
version: 3.0
created: 2026-06-29
updated: 2026-07-02
author: Data Engineering Team
---

# Feature: Glue Streaming Mini-Batch with DMS CDC — `tbl_opensky_flights`

## 1. Description
**AWS Glue 5.1 (PySpark 4.0)** job to process Change Data Capture (CDC) data replicated by **AWS DMS Serverless** in the **landing** bucket, apply quality rules and store in the **Data Lake** (raw layer — `tbl_opensky_flights`) in **Delta Lake** format, partitioned and cataloged in **Glue Data Catalog**.

> ⚠️ **Pure Spark:** The job does **not** use Glue APIs (`GlueContext`, `DynamicFrame`, `Job`, `getResolvedOptions`). All processing is done with pure SparkSession.

> **Scope:** This job covers **only** the `tbl_opensky_flights` entity. Other entities will be handled in separate processes.

## 2. Context
AWS DMS Serverless continuously replicates data from an Aurora PostgreSQL source to the S3 landing bucket in Parquet format. Files are organized by table folder and date (`YYYY/MM/DD/`), with operation column (`Op`) and DMS timestamp (`dms_timestamp`).

The Glue Job processes these files in **mini-batches** (60s), ensuring:
- Streaming read with S3 checkpoint (`cleanSource=archive`) — **no job bookmarks**
- Data validation and cleansing (type casting, nulls, enums)
- Write to raw bucket in **Delta Lake** with **MERGE by PK** for cross-batch uniqueness
- Quality metrics tracking via `QualityMetrics`
- Execution log in control table via `EtlControl`

## 3. Data Source (config.json)

### Source Configuration (config.json)
```json
[{
  "source": "flights",
  "source_location": "s3://lakehouse-landing-{account_id}/dms/flightradar/flight_radar/",
  "format": "parquet",
  "cdc_config": { "op_column": "Op", "timestamp_column": "dms_timestamp" },
  "checkpoint_location": "s3://lakehouse-workspace-{account_id}/checkpoints/flights/"
}]
```

### CDC Behavior
- **Full load** (`LOAD*.parquet`): initial snapshot with `Op` = `I`
- **Incremental**: files with maximum interval of **60 seconds**
- `Op` = `I` (insert), `U` (update), `D` (delete)
- `dms_timestamp` marks the moment of change capture

## 4. Destination — `tbl_opensky_flights` (embedded in config.json)

### Target Schema
| Column | Type | Description |
|--------|------|-----------|
| `icao24` | `string` | ICAO aircraft code |
| `callsign` | `string` | Flight identifier |
| `origin_country` | `string` | Country of origin |
| `latitude` | `double` | Latitude |
| `longitude` | `double` | Longitude |
| `altitude` | `double` | Altitude |
| `velocity` | `double` | Velocity |
| `heading` | `double` | Direction |
| `last_contact` | `bigint` | Last contact |
| `event_time` | `string` | Event timestamp |
| `location` | `string` | Location |
| `cod_unico` | `string` | PK concatenation (icao24 + event_time) for merge |

**Partition:** `event_date` (date).  
**Format:** Delta Lake.  
**PK:** `icao24`, `event_time`.  
**Dedup:** Delta MERGE (cross-batch, no `row_number()` or `dropDuplicates`).  
**Location:** `s3://{bucket_raw}/tables/opensky/flights/`.

## 5. Job Structure

### 5.1. Configuration Files (single file)
| File | Content | Class |
|---------|----------|--------|
| `app/src/dependencies/config/config.json` | Unified configuration (source + target) | `Config` |

### 5.2. Classes

| Class | File | Responsibility |
|--------|---------|-----------------|
| `Config` | `config_models.py` | Read config JSON, return `SourceConfig`/`TargetConfig` (dataclasses) |
| `Reader` | `reader.py` | Read Parquet data from S3 with streaming (no bookmarks) |
| `DataQuality` | `data_quality.py` | Validate, cleanse and convert data; record rejections |
| `Writer` | `writer.py` | Write valid data to raw layer |
| `EtlControl` | `etl_control.py` | Register execution in `etl_control` |
| `QualityMetrics` | `quality_metrics.py` | Save metrics to `data_quality_metrics` |
| `Processor` | `processor.py` | Orchestrate full pipeline (delegates EtlControl/QualityMetrics) |
| `main.py` | `main.py` | Entry point: init Spark 4.0 / Glue 5.1, parse args via argparse, run |

### 4.2. Infrastructure (Terraform)

| Resource | `infra/` File | Description |
|---------|-----------------|-----------|
| KMS Key | `main.tf` | KMS key for SSE-KMS/CSE-KMS encryption |
| Security Config | `main.tf` | Encryption: CloudWatch, job bookmarks, S3 |
| Glue Connection | `main.tf` | VPC connection (private subnet + security group) |
| Glue Job | `main.tf` | Glue 5.1 / Spark 4.0 / Python 3.10 job |

> ⚠️ Glue Catalog databases and tables are not created by Terraform — they already exist in the Data Lake. Names are only for code reference.

#### Dynamic Spark Configs

Spark configs are defined in the Terraform module's `locals.tf` as a `spark_properties` map and converted to a single `--conf` string. `main.py` parses this string and applies each property dynamically to `SparkSession.builder`, eliminating hardcoded values.

Delta Lake specific configs are included: `spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite` and `spark.databricks.delta.properties.defaults.autoOptimize.autoCompact`.

### 4.3. Pipeline
```
main.py
  └── Processor.run("flights")
        ├── 1. Config.from_s3(path) → SourceConfig
        ├── 2. Reader.stream(config) → raw DataFrame
        ├── 3. DataQuality.validate(df, config) → (valid_df, rejects_df)
        ├── 4. Writer.write_rejects(rejects_df, config)
        ├── 5. Writer.write(valid_df, config)  → Delta MERGE by PK
        ├── 6. etl_control.register_execution(...)
        └── 7. data_quality_metrics.save_metrics(...)
```

## 5. Quality Rules (DataQuality)

| Rule | Description | Action for invalid |
|-------|-----------|-------------------|
| Null in PK | `icao24` or `event_time` null | Reject |
| Enum `Op` | Value outside [I, U, D] | Reject |
| `timestamp` type | Columns with invalid format | Reject |
| `double` type | Non-convertible numeric columns | Reject |

> **Note:** Duplicate PK validation is delegated to **Delta MERGE** on write — the `WHEN NOT MATCHED THEN INSERT` clause ensures only records with new PKs are inserted, and `WHEN MATCHED THEN UPDATE` updates existing records. No need for `row_number()` or `dropDuplicates` in the pipeline.

## 6. Support Tables

### 6.1. `etl_control` — Execution Control

Suggested schema (enriched):

| Column | Type | Description |
|--------|------|-----------|
| `execution_id` | `STRING` | Execution UUID |
| `job_name` | `STRING` | Glue Job name |
| `source` | `STRING` | Processed source (e.g. "flights") |
| `execution_start` | `TIMESTAMP` | Execution start |
| `execution_end` | `TIMESTAMP` | Execution end |
| `status` | `STRING` | `running`, `success`, `failed` |
| `records_read` | `BIGINT` | Records read |
| `records_written` | `BIGINT` | Records written to raw |
| `records_rejected` | `BIGINT` | Rejected records |
| `target_partition` | `STRING` | Target partition (e.g. "year=2026/month=06/day=21") |
| `error_message` | `STRING` | Error message (if any) |
| `reference_date` | `DATE` | Partition (reference date) |

### 6.2. `data_quality_metrics` — Quality Metrics

| Column | Type | Description |
|--------|------|-----------|
| `database` | `STRING` | Evaluated database |
| `table` | `STRING` | Evaluated table |
| `processing_timestamp` | `TIMESTAMP` | Processing timestamp |
| `metric` | `STRING` | Evaluated metric |
| `rule` | `STRING` | Applied rule |
| `status` | `STRING` | `passed`, `failed` |
| `failure_reason` | `STRING` | Failure reason |
| `partition` | `STRING` | Evaluated partition |
| `technology` | `STRING` | Technology (e.g. "glue") |
| `reference_date` | `DATE` | Partition |

## 7. Additional Artifacts

| Artifact | Description |
|----------|-----------|
| `tests/unit/` | Unit tests with **pytest** mocking Spark, Glue and AWS |
| `tests/integration/` | Integration tests with **boto3** (S3, Glue Catalog) |
| `infra/` | Complete Terraform module (Glue job, KMS, Security Config, Connection, Databases) |
| `scripts/setup-env.sh` | Bash script for AWS environment setup via Terraform (points to `infra/`) |
| `scripts/rollback-setup.sh` | Bash script for AWS environment rollback |
| `app/src/dependencies/config/config.json` | Unified configuration (JSON) |

## 8. Acceptance Criteria

| # | Criterion | Status |
|---|----------|--------|
| 1 | Job reads Parquet files from DMS (`flights`) in the landing bucket | ☐ |
| 2 | Job processes streaming data in mini-batches with checkpointing | ☐ |
| 3 | Job uses Glue 5.1 with Spark 4.0 | ☐ |
| 4 | Spark optimization configs applied (AQE, memory, shuffle) | ☐ |
| 5 | Job applies quality rules and rejects invalid records | ☐ |
| 6 | Rejected records are moved to `Rejected/flights/` folder | ☐ |
| 7 | Job writes validated data in Parquet + Snappy to raw bucket | ☐ |
| 8 | Data is partitioned by `year/month/day` | ☐ |
| 9 | `tbl_opensky_flights` table registered in Glue Catalog (db_raw) | ☐ |
| 10 | Quality metrics saved to `data_quality_metrics` | ☐ |
| 11 | Execution control registered in `etl_control` (enriched schema) | ☐ |
| 12 | Unit tests with pytest with ≥ 100% module coverage | ☐ |
| 13 | Integration tests with boto3 validate S3 and Glue Catalog | ☐ |
| 14 | `setup-env.sh` and `rollback-setup.sh` scripts functional | ☐ |
| 15 | Terraform module `infra/` with KMS, Security Config, Connection and Databases | ☐ |
| 16 | Glue job associated with security configuration and VPC connection | ☐ |

## 9. Dependências
- Bucket S3 `lakehouse-landing-{account_id}` com dados DMS da tabela `flights`
- Bucket S3 `lakehouse-raw-{account_id}` para escrita
- Bucket S3 `lakehouse-workspace-{account_id}` para scripts, configs e checkpoints
- Role IAM `role-datalake-analytics`
- Default VPC com subnets privadas e security group default
- KMS key para criptografia (criada pelo módulo `infra/`)
- Glue Catalog database `db_raw` (já existe no Data Lake, não é criado pelo módulo `infra/`)
- Tabelas Glue Catalog em `scripts/*.tf`
