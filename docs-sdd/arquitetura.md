**Resumo:** **Documento técnico pronto para engenheiros de dados** com detalhamento da arquitetura AWS (medallion), fluxos batch/CDC/streaming, exemplos de DDL, job Glue, política IAM mínima e snippet Terraform; **data:** 28-06-2026; **versão:** v2.0.

# Arquitetura de Dados AWS — Detalhamento Técnico para Engenheiros de Dados  
**Autor:** Jamil  
**Data:** 28-06-2026  
**Versão:** v2.0

## 1 Visão Executiva
**Objetivo:** plataforma Data Lakehouse em AWS que suporta ingestão híbrida (batch, CDC, streaming), curadoria por camadas (Landing → Bronze → Silver → Gold → Workspace) e consumo por BI/ML. **Público:** engenheiros de dados responsáveis por implementação e operação.

## 2 Componentes e Responsabilidades
- **Fontes:** RDBMS (CDC), arquivos legados, APIs SaaS, apps/sites.  
- **Conectores:** DMS (CDC), Transfer Family (SFTP), AppFlow, API Gateway + Lambda, EventBridge.  
- **Ingestão:** Kinesis / Firehose, MSK (Kafka), Flink (KDA).  
- **Lakehouse (S3):** **Landing**, **Bronze**, **Silver**, **Gold**, **Workspace**.  
- **Processamento:** Glue (batch/streaming), EMR (Spark para >10TB), Databricks (notebooks/Delta).  
- **Governança:** DataZone, Glue Data Catalog, Lake Formation.  
- **Consumo:** Redshift, Athena, SageMaker, APIs.  
- **Segurança/Observabilidade:** IAM, KMS, Secrets Manager, CloudWatch, DataDog, Terraform (IaC).

## 3 Fluxos e Padrões
- **Batch:** Transfer → S3 Landing → Glue Batch/EMR → Bronze → Silver → Gold.  
- **CDC:** RDBMS → DMS → MSK/Kinesis → stream processors → Bronze→Silver.  
- **Streaming:** App/API → API Gateway/Lambda → Kinesis → Flink/Glue Streaming → Bronze.  
**Padrões:** medallion pattern; event-driven; CDC com idempotência; schema evolution via Glue Catalog.

## 4 Modelagem e Boas Práticas
- **Particionamento:** `ingestion_date` e `event_date`.  
- **Formato:** **Parquet** + **Snappy** (batch) / **Delta Lake** (streaming CDC).  
- **Práticas:** partition pruning, compaction periódica, small-file mitigation, Delta MERGE para dedup cross-batch.

**Exemplo DDL (Gold)**
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

## 5 Exemplos de Job Glue (PySpark)
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
**Idempotência:** usar Delta MERGE com base na PK composta para garantir unicidade cross-batch (sem necessidade de `dropDuplicates`).

## 6 Orquestração e Observabilidade
- **Orquestração:** Airflow (DAGs), EventBridge, Step Functions.  
- **Métricas essenciais:** throughput, consumer lag, job duration, data quality score.  
- **Alertas:** lag > threshold, job failures, regressão de qualidade; integrar PagerDuty/Slack.

## 7 Segurança e Governança
- **IAM least-privilege**, **SSE‑KMS**, Secrets Manager, Lake Formation para controle coluna/linha.  
- **Masking PII** na camada Silver; políticas de retenção e auditoria via CloudTrail.

## 8 SLAs, Custos e Otimizações
- **SLA streaming:** <30s end‑to‑end; **batch diário:** 2–4h.  
- **Otimizações:** lifecycle (Landing 30d → Glacier), spot instances EMR, partition pruning, compaction.

## 9 Riscos e Recomendações
- Mitigar single-point-of-failure em tópicos; implementar DLQs, retries exponenciais, canary jobs; IaC obrigatório.

## 10 Roadmap (fases)
1. Fundamentos (S3, KMS, Catalog). 2. Batch ingestion. 3. Streaming + CDC. 4. Curadoria Silver/Gold. 5. Consumo e ML. 6. Observabilidade e custo.

## Anexos

**IAM policy mínima (Glue job)**
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

**Diagrama ASCII**
```
[SOURCES] -> [CONNECTORS] -> [Kinesis/MSK] -> [S3 Landing] -> [Bronze] -> [Silver] -> [Gold] -> [Redshift/Athena/SageMaker]
```

---  
**Fim do documento.** Salve como `arquitetura-aws-detalhamento-engenheiros.md`.