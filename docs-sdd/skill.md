---
name: glue-streaming-minibatch-dms-cdc
description: >-
  Criação de Glue Job streaming mini-batch para ingestão de dados CDC do AWS DMS
  (entidade tbl_opensky_flights), com leitura em S3, validação de qualidade e escrita no
  Data Lake (camada raw).
---

# Skill: Glue Streaming Mini-Batch com DMS CDC

## Propósito
Implementar um job **AWS Glue 5.1 (PySpark 4.0)** para processar dados de **CDC (Change Data Capture)** replicados pelo **AWS DMS Serverless** no bucket **landing** — tabela `tbl_opensky_flights` — aplicar regras de qualidade, e escrever no **Data Lake** (camada raw) no formato **Parquet + Snappy**.

## Stack Tecnológica

| Componente | Versão |
|------------|--------|
| AWS Glue | **5.1** |
| Apache Spark | **4.0** |
| Python | **3.10+** |
| Formato | Parquet + Snappy |
| Testes | pytest + boto3 (integração) |
| APIs Glue | **NÃO utilizadas** (pure Spark) |

## Arquitetura do Job

```
S3 Landing (DMS Parquet — tbl_opensky_flights/)
        │
        ▼
   ┌──────────┐
   │  Reader   │ (Spark readStream — streaming-only, sem bookmarks)
   └─────┬────┘
         │ DataFrame bruto
         ▼
   ┌───────────┐
   │ DataQuality│ (cleansing, validação de schema, rejects)
   └─────┬─────┘
         │ DataFrame validado
         ▼
   ┌──────────┐
   │  Writer   │ (escrita raw em Parquet + partições)
   └─────┬────┘
         │
         ▼
   S3 Raw (tabela Glue Catalog — tbl_opensky_flights)

   ┌──────────────┐
   │  EtlControl   │  ← registro de cada execução (classe separada)
   └──────────────┘
   ┌──────────────────────┐
   │  QualityMetrics      │  ← métricas de qualidade (classe separada)
   └──────────────────────┘
```

## Infraestrutura (Terraform)

O módulo Terraform em `infra/` gerencia todos os recursos AWS necessários:

| Recurso | Descrição |
|---------|-----------|
| `aws_kms_key.glue` | KMS key para criptografia SSE-KMS/CSE-KMS |
| `aws_glue_security_configuration.glue` | Security config (CloudWatch, bookmarks, S3) |
| `aws_glue_connection.vpc` | Conexão VPC (subnet privada, security group default) |
| `aws_glue_job.streaming_minibatch_dms` | Glue job (Glue 5.1, Spark 4.0, Python 3.10) |
| `null_resource.upload_artifacts` | Upload de scripts Python e configs JSON para S3 |

> ⚠️ Databases e tabelas Glue Catalog não são gerenciados por este módulo — já existem no Data Lake.

### Spark Configs Dinâmicas

As configurações Spark são definidas no `locals.tf` como um mapa `spark_properties` e convertidas em string `--conf` (formato `key=value key=value ...`). O `main.py` faz o parse desta string via `_parse_conf()` e aplica cada propriedade dinamicamente ao `SparkSession.builder`, sem valores hardcoded.

Data sources: IAM role `role-datalake-analytics`, default VPC, subnets, security group.

## Componentes

### 1. `main.py` — Entry Point
- Inicializa `SparkSession` puro (sem GlueContext, sem Glue Job API)
- Parseia argumentos: `--origins_s3_path`, `--target_s3_path`, `--conf`
- Aplica **Spark configs de otimização** dinamicamente via `--conf`
- Executa streaming com `forEachBatch` (trigger `processingTime="60 seconds"`)
- Instancia a classe `Processor` e executa o pipeline em cada micro-batch

### 2. `config.py` — Leitura de Configuração (dataclasses)
- Lê **dois** arquivos JSON separados: `origins.json` (conexão origem DMS) e `target.json` (schema destino)
- Utiliza **dataclasses** do Python para modelar `SourceConfig` e `TargetConfig`
- `SourceConfig`: `source`, `source_location`, `format`, `cdc_config`, `checkpoint_location`
- `TargetConfig`: `catalog` (database, table), `location`, `rejected_location`, `format`, `compression`, `partition_keys`, `schema`, `primary_key`, `enum_columns`
- Métodos `from_files()` (local) e `from_s3()` (S3)

### 3. `Reader` — Leitura dos Dados (streaming-only)
- Lê arquivos Parquet do S3 (DMS CDC) exclusivamente em modo **streaming**
- **Sem suporte batch** — sempre usa `spark.readStream`
- **Sem job bookmarks** — usa `cleanSource=archive` e checkpoint location no S3
- `maxFilesPerTrigger=1` para controle de micro-batches
- `includeExistingFiles=true` para processar arquivos existentes
- Retorna `DataFrame` Spark bruto

### 4. `data_quality.py` — Qualidade e Validação
- Recebe DataFrame bruto e schema alvo (do `TargetConfig`)
- Pipeline de 5 etapas:
  1. **Cast de tipos** — converte colunas conforme schema do target
  2. **Null check** — filtra nulos em campos NOT NULL
  3. **Enum validation** — valida valores de `enum_columns`
  4. **Duplicate removal** — dedup por `primary_key` usando `row_number()` + `cdc_config.order`
  5. **Timestamp validation** — valida timestamps (pass-through)
- Registros **inválidos** são escritos em `Rejected/` com metadados (`_reject_table`, `_reject_rule`, `_reject_timestamp`)
- Retorna tuple: `(DataFrame válido, DataFrame rejects)`
- **Funcionamento dinâmico**: lê o schema do JSON de configuração, permitindo validar qualquer origem sem alteração de código

### 5. `writer.py` — Escrita no Data Lake
- Recebe DataFrame válido e metadados da tabela alvo
- Escreve no formato **Parquet + Snappy** no bucket **raw**
- Particiona os dados por `event_date` (derivado de coluna timestamp via `F.to_date()`)
- Usa `mode = append` para dados incrementais
- Implementa **compaction** para coalescer small files
- Suporta escrita de rejects com `write_rejects()`

### 6. `etl_control.py` — Registro de Execução
- Classe separada responsável por escrever metadados na tabela `etl_control`
- Registra: `execution_id`, `source_name`, `status`, `records_read`, `records_written`, `records_rejected`, `elapsed_seconds`, `error_message`
- Resolve caminhos S3 e account_id dinamicamente via `boto3`

### 7. `quality_metrics.py` — Métricas de Qualidade
- Classe separada responsável por salvar métricas em `data_quality_metrics`
- Registra: `rows_read`, `rows_written`, `rows_rejected`, `pipeline_status`
- Utiliza `SparkSession` para escrever no Glue Catalog

### 8. `processor.py` — Orquestrador
- Coordena o pipeline completo: Config → Reader → DataQuality → Writer → EtlControl → QualityMetrics
- Método `run(source, target)` executa 8 etapas sequenciais
- Delega `_register_execution` ao `EtlControl` e `_save_quality_metrics` ao `QualityMetrics`

## Spark Configs de Otimização

As configurações Spark são definidas em `infra/locals.tf` no mapa `spark_properties`:

- `spark.sql.adaptive.enabled` = true (AQE)
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

## Convenções do Data Lake

| Camada | Bucket | Database | Formato |
|--------|--------|----------|---------|
| Landing | `lakehouse-landing-{account_id}` | `db_landing` | Parquet (DMS) |
| Raw | `lakehouse-raw-{account_id}` | `db_raw` | Parquet + Snappy |
| Workspace | `lakehouse-workspace-{account_id}` | — | Scripts, configs, checkpoints |

## Tabelas Envolvidas

| Tabela | Finalidade | Partições |
|--------|-----------|-----------|
| `tbl_opensky_flights` (raw) | Dados de voos processados do CDC | `event_date` |
| `etl_control` (raw) | Controle de execuções do Glue Job | `reference_date` |
| `data_quality_metrics` (raw) | Métricas de qualidade | `reference_date` |

## Parâmetros do Glue Job

| Parâmetro | Descrição | Exemplo |
|-----------|-----------|---------|
| `--origins_s3_path` | Caminho S3 do JSON de configuração da origem | `s3://.../config/origins.json` |
| `--target_s3_path` | Caminho S3 do JSON de configuração do target | `s3://.../config/target.json` |
| `--conf` | Spark configs dinâmicas (key=val key=val ...) | `spark.sql.shuffle.partitions=200 ...` |

## IAM Role
`role-datalake-analytics` — permissões mínimas para ler landing, escrever raw, acessar Glue Catalog e KMS.
