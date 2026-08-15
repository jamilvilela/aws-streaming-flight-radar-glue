**Summary:** **Technical document ready for data engineers** detailing AWS architecture (medallion), batch/CDC/streaming flows, DDL examples, Glue job, minimal IAM policy and Terraform snippet; **date:** 28-06-2026; **version:** v2.0.

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
    .option("maxFilesPerTrigger", 1) \
    .load("s3://bucket/landing/flights/")

def write_batch(df, epoch_id):
    df.write.format("delta") \
        .mode("append") \
        .save("s3://bucket/raw/flights/")

df.writeStream \
    .trigger(processingTime="60 seconds") \
    .foreachBatch(write_batch) \
    .option("checkpointLocation", "s3://bucket/checkpoints/flights/") \
    .start() \
    .awaitTermination()
```
**Idempotency:** use Delta MERGE based on composite PK to ensure cross-batch uniqueness (no need for `dropDuplicates`).

## 6 Orchestration and Observability
- **Orchestration:** Airflow (DAGs), EventBridge, Step Functions.  
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

**Diagrama de Arquitetura (Mermaid)**

```mermaid
flowchart LR
    subgraph SRC["Fontes de Dados"]
        RDBMS["RDBMS (CDC)"]
        LEGACY["Arquivos Legados"]
        SAAS["SaaS APIs"]
        APPS["Apps / Sites"]
    end

    subgraph CON["Conectores"]
        DMS["AWS DMS"]
        SFTP["Transfer Family (SFTP)"]
        APPFLOW["AppFlow"]
        GW["API Gateway + Lambda"]
        EVB["EventBridge"]
    end

    subgraph ING["Ingestão"]
        KIN["Kinesis / Firehose"]
        MSK["MSK (Kafka)"]
        FLINK["Flink (KDA)"]
    end

    subgraph LAKE["Lakehouse S3 (Medallion)"]
        LAND["Landing"]
        BRONZE["Bronze"]
        SILVER["Silver"]
        GOLD["Gold"]
        WS["Workspace"]
    end

    subgraph PROC["Processamento"]
        GLUE["Glue (batch/streaming)"]
        EMR["EMR (Spark)"]
        DBX["Databricks"]
    end

    subgraph GOV["Governança"]
        CAT["Glue Data Catalog"]
        LF["Lake Formation"]
        DZ["DataZone"]
    end

    subgraph CONSUME["Consumo"]
        RS["Redshift"]
        ATH["Athena"]
        SM["SageMaker"]
        API["APIs"]
    end

    RDBMS --> DMS
    LEGACY --> SFTP
    SAAS --> APPFLOW
    APPS --> GW
    DMS --> MSK
    SFTP --> KIN
    APPFLOW --> KIN
    GW --> KIN
    EVB --> GLUE
    KIN --> LAND
    MSK --> LAND
    LAND --> GLUE
    LAND --> EMR
    LAND --> DBX
    GLUE --> BRONZE
    EMR --> BRONZE
    DBX --> BRONZE
    BRONZE --> SILVER
    SILVER --> GOLD
    GOLD --> WS
    CAT -.-> GLUE
    LF -.-> SILVER
    GOLD --> RS
    GOLD --> ATH
    GOLD --> SM
    GOLD --> API
```

**Fluxo da Solução (Mermaid)**

```mermaid
flowchart TD
    AURO["Aurora PostgreSQL<br/>(schema flight_radar)"]
    DMS["AWS DMS Serverless<br/>(full-load-and-cdc)"]
    LAND["S3 Landing<br/>lakehouse-landing-{account_id}<br/>dms/flightradar/flight_radar/"]
    EVB["EventBridge<br/>DMS Full Load Completed"]
    LAMBDA["Lambda glue_starter<br/>start_glue_job.py"]
    BATCH["Glue Job Batch<br/>glue-flight-radar-full-load<br/>--mode=batch"]
    TRIG["Glue Trigger<br/>CONDITIONAL"]
    STREAM["Glue Job Streaming<br/>glue-flight-radar-streaming<br/>--mode=streaming"]
    READER["Reader"]
    DQ["DataQuality<br/>(4 estágios)"]
    WRITER["Writer<br/>(Delta MERGE por PK)"]
    REJ["Rejected/ (Parquet)"]
    RAW["S3 Raw<br/>tbl_opensky_flights (Delta)"]
    ETL["EtlControl"]
    QM["QualityMetrics"]

    AURO --> DMS
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
    DQ --> REJ
    WRITER --> RAW
    DQ -.-> ETL
    DQ -.-> QM
```

**Fluxo de Eventos (Mermaid)**

```mermaid
sequenceDiagram
    participant DMS as AWS DMS
    participant LAND as S3 Landing
    participant EB as EventBridge
    participant LAMBDA as Lambda glue_starter
    participant BATCH as Glue Batch Job
    participant TRIG as Glue Trigger
    participant STREAM as Glue Streaming Job

    DMS->>LAND: Full load + CDC (Parquet, 5 tabelas)
    DMS->>EB: DMS Full Load Completed
    EB->>LAMBDA: InvokeFunction
    LAMBDA->>BATCH: start_job_run(--mode=batch)
    Note over BATCH: Processa tabelas SEQUENCIALMENTE (order 1..5)
    BATCH-->>TRIG: SUCCEEDED
    TRIG->>STREAM: start_job_run(--mode=streaming)
    Note over STREAM: N queries CONCORRENTES (uma por tabela)
```

---  
**Fim do documento.** Salve como `arquitetura-aws-detalhamento-engenheiros.md`.