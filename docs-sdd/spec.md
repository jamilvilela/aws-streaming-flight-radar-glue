---
id: spec-glue-streaming-dms-cdc
title: Specification — Glue Job Streaming Mini-Batch DMS CDC (tbl_opensky_flights)
status: draft
version: 1.0
created: 2026-07-02
updated: 2026-07-02
author: Data Engineering Team
---

# Specification — Glue Streaming Mini-Batch DMS CDC

> Gerado a partir de `agents.md` e `PRD.md`.

## 1. Visão Geral

Pipeline de processamento CDC (Change Data Capture) usando **AWS Glue 5.1 (PySpark 4.0)** com **pure Spark** (sem GlueContext, DynamicFrame ou Job API). Leitura streaming de arquivos Parquet do bucket landing, validação em 5 etapas, escrita no Data Lake (camada raw) e registro de métricas/controle.

### Stack Tecnológica

| Componente | Versão |
|------------|--------|
| AWS Glue | 5.1 |
| Apache Spark | 4.0 |
| Python | 3.10+ |
| Formato | **Delta Lake** (target) / Parquet (rejects) |
| Infraestrutura | Terraform ≥ 1.5 |
| Testes | pytest + boto3 |

## 2. Arquitetura

```
┌─────────────────────────────────────────────────────────────────────┐
│                        S3 Landing (DMS Parquet)                     │
│  s3://lakehouse-landing-{account}/dms/flightradar/flight_radar/    │
└────────────────────────┬────────────────────────────────────────────┘
                         │ readStream (maxFilesPerTrigger=1)
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Reader (app/src/dependencies/reader.py)                           │
│  • Spark readStream puro (sem Glue)                                 │
│  • cleanSource=archive, checkpoint S3                               │
│  • Sem job bookmarks, sem batch fallback                            │
└────────────────────────┬────────────────────────────────────────────┘
                         │ DataFrame bruto
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  DataQuality (app/src/dependencies/data_quality.py)                │
│  Pipeline de 4 etapas:                                              │
│  1. _cast_types — converter tipos conforme schema target            │
│  2. _check_nulls — filtrar nulos em campos NOT NULL                 │
│  3. _check_enums — validar valores de enum_columns                  │
│  4. _validate_timestamps — pass-through                             │
│  **Sem dedup** — unicidade garantida pelo Delta MERGE na escrita    │
│  Retorna: (valid_df, rejects_df) com metadados de rejeição          │
└──────────────────────┬──────────────────┬───────────────────────────┘
                       │ valid_df         │ rejects_df
                       ▼                  ▼
┌──────────────────────────────┐   ┌──────────────────────────┐
│  Writer (app/src/dependencies/   │   │  Writer.write_rejects()  │
│         writer.py)           │   │  • Salva em Rejected/    │
│  • **Delta Lake**            │   │  • Com metadados         │
│  • MERGE por PK composta     │   └──────────────────────────┘
│  • Partition by event_date   │
│  • Gera cod_unico            │
└──────────┬───────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────────┐
│  S3 Raw (tbl_opensky_flights)                                       │
│  s3://lakehouse-raw-{account}/tables/opensky/flights/              │
│  Partição: event_date                                               │
└─────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────┐   ┌──────────────────────────────────┐
│  EtlControl                  │   │  QualityMetrics                  │
│  (app/src/dependencies/     │   │  (app/src/dependencies/         │
│    etl_control.py)           │   │    quality_metrics.py)           │
│  • etl_control table         │   │  • data_quality_metrics          │
│  • execution_id,             │   │  • rows_read/written/            │
│    records, status           │   │    rejected, status              │
└──────────────────────────────┘   └──────────────────────────────────┘
```

## 3. Componentes

### 3.1. Config — `app/src/dependencies/config/` (subpacote)

**Dataclasses:**
- `SchemaField(name, type, comment, nullable)`
- `PartitionKey(name, type)`
- `CdcConfig(op_column, timestamp_column, order)`
- `SourceConfig(source, source_location, format, cdc_config, checkpoint_location)`
- `TargetConfig(catalog, location, rejected_location, format, compression, partition_keys, schema, primary_key, enum_columns, cod_unico_expr)`
- `Config` com métodos `from_files()`, `from_s3()`, `to_dict()`

**Arquivos de configuração:**
| Arquivo | Propósito | Conteúdo |
|---------|-----------|----------|
| `app/src/dependencies/config/origins.json` | Conexão DMS | source, source_location, format, cdc_config, checkpoint_location |
| `app/src/dependencies/config/target.json` | Schema destino | catalog, location, partition_keys, schema, primary_key, enum_columns |

### 3.2. Reader — `app/src/dependencies/reader.py`
- Leitura **streaming-only** via `spark.readStream.format("parquet")`
- Configurações: `maxFilesPerTrigger=1`, `cleanSource=archive`, `includeExistingFiles=true`
- Checkpoint no S3 (`source.checkpoint_location`)
- **Sem suporte batch**

### 3.3. DataQuality — `app/src/dependencies/data_quality.py`
- Pipeline de 4 etapas executado sobre DataFrame bruto
- Usa schema do `TargetConfig` para validação dinâmica
- **Sem dedup explícito** — unicidade garantida pelo Delta MERGE na escrita
- Registros rejeitados enriquecidos com `_reject_table`, `_reject_rule`, `_reject_timestamp`
- Retorna `(valid_df, rejects_df)`

### 3.4. Writer — `app/src/dependencies/writer.py`
- Escrita **Delta Lake** com `DeltaTable.forName()` via Glue Catalog
- Gera `cod_unico` via `F.concat_ws("_", *pk_cols)` para chave do merge
- Deriva `event_date` via `F.to_date(F.col(partition_col))`
- MERGE: `WHEN NOT MATCHED THEN INSERT` / `WHEN MATCHED THEN UPDATE`
- **Sem compaction manual** — Delta gerencia via auto-optimize
- `write_rejects()` para registros rejeitados (formato Parquet)

### 3.5. EtlControl — `app/src/dependencies/etl_control.py`
- Classe separada para registro em `etl_control`
- Método `register(execution_id, source_name, status, records_read, records_written, records_rejected, target, elapsed_seconds, error_message)`
- Resolve account_id via boto3 STS

### 3.6. QualityMetrics — `app/src/dependencies/quality_metrics.py`
- Classe separada para métricas em `data_quality_metrics`
- Método `save(target, status, records_read, records_written, records_rejected)`
- Resolve account_id via boto3 STS

### 3.7. Processor — `app/src/dependencies/processor.py`
- Orquestrador do pipeline (8 etapas):
  1. Load configs (origins + target)
  2. Read streaming data
  3. DataQuality.validate()
  4. Writer.write() dados válidos
  5. Writer.write_rejects() rejects
  6. EtlControl.register()
  7. QualityMetrics.save()
  8. Logging

### 3.8. Main — `app/src/main.py`
- Parse de argumentos via `argparse`: `--origins_s3_path`, `--target_s3_path`, `--conf`
- `_parse_conf()` converte string "key=val key=val" em dict
- `_init_spark()` aplica configs dinamicamente
- Streaming com `forEachBatch` e `trigger(processingTime="60 seconds")`

## 4. Infraestrutura (Terraform)

### Recursos (`infra/main.tf`)
| Nome | Tipo | Descrição |
|------|------|-----------|
| `aws_kms_key.glue` | KMS Key | Criptografia SSE-KMS/CSE-KMS |
| `aws_kms_alias.glue` | KMS Alias | `alias/glue-streaming-minibatch-dms` |
| `aws_glue_security_configuration.glue` | Security Config | CloudWatch SSE-KMS, bookmarks CSE-KMS, S3 SSE-KMS |
| `aws_glue_connection.vpc` | Glue Connection | NETWORK, subnet privada, SG default |
| `aws_glue_job.streaming_minibatch_dms` | Glue Job | Glue 5.1, Python 3.10, G.1X, 2 workers |
| `data.archive_file.helpers` | Archive Data | Cria helpers.zip a partir de `app/src/dependencies/` |
| `aws_s3_object.main_py` | S3 Object | Upload de `main.py` para scripts path |
| `aws_s3_object.helpers_zip` | S3 Object | Upload de `helpers.zip` para dependencies path |
| `aws_s3_object.origins_json` | S3 Object | Upload de `origins.json` (com `{account_id}` resolvido) |
| `aws_s3_object.target_json` | S3 Object | Upload de `target.json` (com `{account_id}` resolvido) |

### Spark Configs Dinâmicas (`locals.tf → spark_conf`)
- `spark.sql.adaptive.enabled` = true
- `spark.sql.adaptive.coalescePartitions.enabled` = true
- `spark.sql.adaptive.skewJoin.enabled` = true
- `spark.sql.adaptive.advisoryPartitionSizeInBytes` = 128MB
- `spark.sql.shuffle.partitions` = 200
- `spark.executor.memory` = 4g
- `spark.driver.memory` = 4g
- `spark.executor.memoryOverhead` = 2g
- `spark.driver.memoryOverhead` = 2g
- `spark.memory.offHeap.enabled` = true
- `spark.memory.offHeap.size` = 2g
- `spark.dynamicAllocation.enabled` = true
- **Delta Lake configs:**
  - `spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite` = true
  - `spark.databricks.delta.properties.defaults.autoOptimize.autoCompact` = true

### Dados Existentes (data sources)
- IAM Role: `role-datalake-analytics`
- VPC: default (`vpc-022139f6bee3cbdd5`)
- Subnets: privadas em us-east-1a/b/c
- Security Group: default (`sg-0f885f9d1473a7777`)

> ⚠️ Databases e tabelas Glue Catalog não são gerenciados por este módulo.

## 5. Scripts

### `scripts/setup-env.sh`
- Verifica pré-requisitos (Terraform, AWS CLI, jq)
- Inicializa Terraform com backend S3
- Seleciona workspace
- Aplica Terraform (upload de artefatos é feito pelos `aws_s3_object` resources)

### `scripts/rollback-setup.sh`
- Confirma rollback
- Executa `terraform destroy`
- **Não** remove arquivos do S3

## 6. Glue Job Parâmetros

| Parâmetro | Descrição | Obrigatório |
|-----------|-----------|-------------|
| `--origins_s3_path` | Caminho S3 do origins.json | Sim |
| `--target_s3_path` | Caminho S3 do target.json | Sim |
| `--conf` | Spark configs (key=val key=val) | Não |

## 7. Tabelas no Data Lake

| Tabela | Database | Finalidade | Partição |
|--------|----------|-----------|----------|
| `tbl_opensky_flights` | `db_raw` | Dados de voos processados | `event_date` |
| `etl_control` | `db_raw` | Controle de execuções | `reference_date` |
| `data_quality_metrics` | `db_raw` | Métricas de qualidade | `reference_date` |

## 8. Convenções

- **Buckets**: nomeados com account ID: `lakehouse-{tier}-{account_id}`
- **Formato**: Parquet + Snappy
- **IAM**: role `role-datalake-analytics`
- **Spark**: pure Spark, sem APIs Glue
- **Configs**: dois JSONs separados (origem vs destino)
- **Streaming**: apenas readStream, sem batch
- **Bookmarks**: não utilizado (usa `cleanSource=archive`)

## 9. Qualidade de Dados

Pipeline de 5 etapas em `DataQuality`:

| Etapa | Função | Descrição |
|-------|--------|-----------|
| 1 | `_cast_types` | Converte colunas para os tipos do target schema |
| 2 | `_check_nulls` | Remove registros com nulos em campos obrigatórios |
| 3 | `_check_enums` | Valida valores contra lista permitida em `enum_columns` |
| 4 | `_remove_duplicates` | Remove duplicatas por `primary_key`, ordenando por CDC |
| 5 | `_validate_timestamps` | Valida timestamps (pass-through atual) |

## 10. Testes

### Unitários (`tests/unit/`)
- `test_config.py`: from_files, from_s3, to_dict, JSON inválido
- `test_reader.py`: streaming read, checkpoint config, schema
- `test_data_quality.py`: 5 etapas, rejects, tipos, enums, nulos, dedup
- `test_writer.py`: escrita Parquet, partições, compaction, rejects
- `test_processor.py`: pipeline completo mockado

### Integração (`tests/integration/`)
- `conftest.py`: fixtures boto3 (S3, Glue)
- `test_s3_landing.py`: estrutura do bucket landing
- `test_glue_catalog.py`: existência de databases e tabelas
- `test_pipeline_e2e.py`: pipeline completo com dados reais
