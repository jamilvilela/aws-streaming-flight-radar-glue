---
id: arquitetura-glue-streaming-dms-cdc
title: Arquitetura — AWS Data Lakehouse para Flight Radar
status: approved
version: 2.0
created: 2026-06-28
updated: 2026-08-31
author: Data Engineering Team
---

# AWS Data Architecture — Technical Detail for Data Engineers

**Author:** Jamil  
**Date:** 28-06-2026  
**Version:** v2.0

## 1 Executive Overview

**Objective:** AWS Data Lakehouse platform that supports hybrid ingestion (batch, CDC, streaming), layered curation (Landing → Bronze → Silver → Gold → Workspace) and consumption by BI/ML. **Audience:** data engineers responsible for implementation and operation.

## 2 Components and Responsibilities

- **Sources:** RDBMS (CDC), legacy files, SaaS APIs, apps/sites.
- **Connectors:** DMS (CDC), Transfer Family (SFTP), AppFlow, API Gateway + Lambda, EventBridge.
- **Ingestion:** Kinesis / Firehose, MSK (Kafka), Flink (KDA).
- **Lakehouse (S3):** **Landing**, **Bronze**, **Silver**, **Gold**, **Workspace**.
- **Processing:** Glue (batch/streaming), EMR (Spark for >10TB), Databricks (notebooks/Delta).
- **Governance:** DataZone, Glue Data Catalog, Lake Formation.
- **Consumption:** Redshift, Athena, SageMaker, APIs.
- **Security/Observability:** IAM, KMS, Secrets Manager, CloudWatch, DataDog, Terraform (IaC).

## 3 Flows and Patterns

- **Batch:** Transfer → S3 Landing → Glue Batch/EMR → Bronze → Silver → Gold.
- **CDC:** RDBMS → DMS → MSK/Kinesis → stream processors → Bronze→Silver.
- **Streaming:** App/API → API Gateway/Lambda → Kinesis → Flink/Glue Streaming → Bronze.
**Patterns:** medallion pattern; event-driven; CDC with idempotency; schema evolution via Glue Catalog.

## 4 Modeling and Best Practices

- **Partitioning:** `ingestion_date` and `event_date`.
- **Format:** **Parquet** + **Snappy** (batch) / **Delta Lake** (streaming CDC).
- **Practices:** partition pruning, periodic compaction, small-file mitigation, Delta MERGE for cross-batch dedup.

**DDL Example (Gold)**

```sql
CREATE TABLE gold.events (
  event_id STRING,
  user_id STRING,
  event_type STRING,
  event_time TIMESTAMP,
  payload MAP<STRING,STRING>,
  source_system STRING,
  ingestion_time TIMESTAMP
)
PARTITIONED BY (ingestion_date DATE)
STORED AS PARQUET;
```

## 5 Glue Job Examples (PySpark)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("streaming-minibatch-dms") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

df = spark.readStream.format("parquet") \
    .option("maxFilesPerTrigger", 1000) \
    .option("cleanSource", "archive") \
    .option("includeExistingFiles", "true") \
    .option("sourceArchiveDir", "s3://bucket/archive/") \
    .load("s3://bucket/landing/flights_cdc/")

def write_batch(df, epoch_id):
    # Delta MERGE by PK resolved via the Glue Data Catalog (forName) — cross-batch dedup
    writer.write(df, target, source)

df.writeStream \
    .trigger(processingTime="5 minutes") \
    .foreachBatch(write_batch) \
    .option("checkpointLocation", "s3://bucket/checkpoints/flights/") \
    .start() \
    .awaitTermination()
```

**Idempotency:** Delta MERGE based on composite PK (via `cod_unique`) to ensure cross-batch uniqueness (no need for `dropDuplicates`).

## 6 Orchestration and Observability

- **Orchestration:** Airflow (DAGs), EventBridge, Step Functions, Glue Workflows + Triggers.
- **Essential metrics:** throughput, consumer lag, job duration, data quality score.
- **Alerts:** lag > threshold, job failures, quality regression; integrate PagerDuty/Slack.

## 7 Security and Governance

- **IAM least-privilege**, **SSE‑KMS**, Secrets Manager, Lake Formation for column/row control.
- **PII Masking** in Silver layer; retention and audit policies via CloudTrail.

## 8 SLAs, Costs and Optimizations

- **Streaming SLA:** <30s end‑to‑end; **daily batch:** 2–4h.
- **Optimizations:** lifecycle (Landing 30d → Glacier), spot instances EMR, partition pruning, compaction.

## 9 Risks and Recommendations

- Mitigate single-point-of-failure in topics; implement DLQs, exponential retries, canary jobs; IaC mandatory.

## 10 Roadmap (phases)

1. Foundations (S3, KMS, Catalog). 2. Batch ingestion. 3. Streaming + CDC. 4. Silver/Gold curation. 5. Consumption and ML. 6. Observability and cost.

## Appendices

**Minimal IAM policy (Glue job)**

```json
{
  "Version":"2012-10-17",
  "Statement":[
    {"Effect":"Allow","Action":["s3:GetObject","s3:ListBucket"],"Resource":["arn:aws:s3:::my-bucket-bronze","arn:aws:s3:::my-bucket-bronze/*"]},
    {"Effect":"Allow","Action":["s3:PutObject"],"Resource":["arn:aws:s3:::my-bucket-silver/*"]},
    {"Effect":"Allow","Action":["kms:Decrypt","kms:Encrypt"],"Resource":["arn:aws:kms:region:acct:key/key-id"]}
  ]
}
```

**Terraform snippet (S3 + lifecycle + KMS)**

```hcl
resource "aws_kms_key" "data" { description = "S3 KMS key" }
resource "aws_s3_bucket" "data" { bucket = "my-bucket" }
resource "aws_s3_bucket_lifecycle_configuration" "lc" {
  bucket = aws_s3_bucket.data.id
  rule {
    id = "landing-lifecycle"
    status = "Enabled"
    filter { prefix = "landing/" }
    transition { days = 30, storage_class = "GLACIER" }
  }
}
```

## 11 Solution Flow — Flight Radar CDC Pipeline (Mermaid)

```mermaid
flowchart TD
    AURO["Aurora PostgreSQL<br/>(schema flight_radar)"]
    DMS["AWS DMS Serverless<br/>(full-load-and-cdc)"]
    LAND["S3 Landing<br/>lakehouse-landing-{account_id}<br/>dms/flightradar/flight_radar/"]
    SCHED["EventBridge Schedule<br/>rate({interval} min)"]
    LAMBDA["Lambda glue_starter<br/>start_glue_job.py"]
    LOCK["DynamoDB Lock<br/>glue-flight-radar-workflow-lock"]
    WF["Glue Workflow<br/>glue-flight-radar-batch-workflow"]
    ON_DEMAND["Glue Trigger<br/>ON_DEMAND"]
    BATCH["Glue Job Batch<br/>glue-flight-radar-batch<br/>--mode=batch"]
    TRIG["Glue Trigger<br/>CONDITIONAL"]
    STREAM["Glue Job Streaming<br/>glue-flight-radar-streaming<br/>--mode=streaming"]
    READER["Reader"]
    DQ["DataQuality<br/>(4 estágios)"]
    WRITER["Writer<br/>(Delta MERGE por PK)"]
    REJ["RejectedRecords<br/>(Parquet centralizado)"]
    RAW["S3 Raw<br/>tabelas Delta (db_raw)"]
    ETL["EtlControl"]
    QM["QualityMetrics"]

    AURO --> DMS
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

## 12 Event Flow (Mermaid)

```mermaid
sequenceDiagram
    participant SCHED as EventBridge Schedule
    participant LAMBDA as Lambda glue_starter
    participant DMS as AWS DMS
    participant LOCK as DynamoDB Lock
    participant WF as Glue Workflow
    participant BATCH as Glue Batch Job
    participant TRIG as Glue Trigger
    participant STREAM as Glue Streaming Job

    loop A cada {interval} min
        SCHED->>LAMBDA: InvokeFunction (rate)
        LAMBDA->>DMS: describe_replications
        alt Full load completo (100% e 0 tabelas carregando)
            LAMBDA->>LOCK: put_item (attribute_not_exists task_arn)
            LOCK-->>LAMBDA: lock adquirido (disparo único)
            LAMBDA->>WF: start_workflow_run
            WF->>BATCH: ON_DEMAND trigger (--mode=batch)
            Note over BATCH: Processa tabelas SEQUENCIALMENTE (order 1..8)
            BATCH-->>TRIG: SUCCEEDED
            TRIG->>STREAM: start_job_run (--mode=streaming)
            Note over STREAM: N queries CONCORRENTES (uma por tabela)
        else Full load ainda em andamento
            LAMBDA-->>SCHED: skip (aguarda próximo ciclo)
        end
    end
```

## 13 Pipeline Internal Flow (Mermaid)

```mermaid
flowchart TD
    MAIN["main.py"]
    PROC["Processor.run(source, target)"]
    CONFIG["1. Config.from_s3(path) → SourceConfig"]
    READER["2. Reader.read(source, mode) → raw DataFrame"]
    DQ["3. DataQuality.validate(df, target, source) → (valid_df, rejects_df)"]
    REJ["4. RejectedRecords.write(rejects_df, ...)"]
    WRITE["5. Writer.write(valid_df, target, source) → Delta MERGE by PK"]
    ETL["6. EtlControl.register(...)"]
    QM["7. QualityMetrics.save(...)"]

    MAIN --> PROC
    PROC --> CONFIG
    CONFIG --> READER
    READER --> DQ
    DQ --> REJ
    DQ --> WRITE
    WRITE --> ETL
    ETL --> QM
```

## 14 Data Lake Layer Mapping

| Layer | Bucket | Database | Format | Purpose |
|-------|--------|----------|--------|---------|
| Landing | `lakehouse-landing-{account_id}` | — | Parquet (DMS) | Raw DMS CDC output |
| Raw (Bronze) | `lakehouse-raw-{account_id}` | `db_raw` | **Delta Lake** / Parquet (rejects) | Validated, partitioned, cataloged |
| Workspace | `lakehouse-workspace-{account_id}` | — | Scripts, configs, checkpoints | Job artifacts, configs, checkpoints |

## 15 Tables in Raw Layer (db_raw)

| Table | Partition | Primary Key | Format | Description |
|-------|-----------|-------------|--------|-------------|
| `fr_aircraft` | `event_date` | `icao24` | Delta | Aircraft registry |
| `fr_airports` | `event_date` | `icao_code` | Delta | Airports |
| `fr_airlines` | `event_date` | `icao_code` | Delta | Airlines |
| `fr_flights` | `event_date` (from `scheduled_departure`) | `flight_id` | Delta | Flights fact table |
| `fr_aircraft_positions` | `aircraft_icao24` | `position_id`, `recorded_at` | Delta | Positions (high volume) |
| `fr_countries` | `event_date` | `id` | Delta | Countries |
| `fr_aircraft_types` | `event_date` | `icao_code` | Delta | Aircraft types |
| `fr_routes` | `event_date` | `id` | Delta | Routes |
| `etl_control` | `reference_date` | — | Parquet | Execution control |
| `data_quality_metrics` | `reference_date` | — | Parquet | Quality metrics |
| `rejected_records` | `reference_date` | — | Parquet | Centralized rejected records |

## 16 Deployment Structure (Workspace Bucket)

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

---

**Fim do documento.** Salve como `arquitetura-aws-detalhamento-engenheiros.md`.