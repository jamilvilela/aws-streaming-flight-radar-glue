---
id: plan-glue-streaming-dms-cdc
title: Plano de Implementação — Glue Job Streaming Mini-Batch (tbl_opensky_flights)
status: migrated-to-delta
version: 3.0
created: 2026-06-29
updated: 2026-07-02
author: Data Engineering Team
---

# Plano de Implementação

> Gerado a partir de `spec.md`. Status: **migrado para Delta Lake**.

## Estrutura do Projeto

```
app/
├── src/
│   ├── main.py              # Entry point do Glue Job (argparse, --conf dinâmico)
│   ├── __init__.py
│   └── dependencies/        # Módulos de suporte (empacotados como helpers.zip)
│       ├── __init__.py
│       ├── config/          # Dataclasses de configuração + JSONs (subpacote)
│       │   ├── __init__.py  #   → Config, SourceConfig, TargetConfig, etc.
│       │   ├── origins.json #   → Configuração da origem DMS (conexão)
│       │   └── target.json  #   → Configuração do target (schema, partições, PK)
│       ├── reader.py        # Leitor de dados streaming-only (Reader)
│       ├── data_quality.py  # Validação e qualidade (DataQuality) — 4 etapas
│       ├── writer.py        # Escrita no Data Lake (Writer) — Delta Lake
│       ├── processor.py     # Orquestrador (Processor)
│       ├── etl_control.py   # Registro de execução em etl_control
│       └── quality_metrics.py # Métricas de qualidade em data_quality_metrics
└── tests/                    # Testes unitários e de integração
    ├── __init__.py
    ├── conftest.py            # Adiciona app/ ao sys.path
    ├── unit/
    │   ├── __init__.py
    │   ├── test_config.py
    │   ├── test_reader.py
    │   ├── test_data_quality.py
    │   ├── test_writer.py
    │   └── test_processor.py
    └── integration/
        ├── __init__.py
        ├── conftest.py
        ├── test_s3_landing.py
        ├── test_glue_catalog.py
        └── test_pipeline_e2e.py
infra/                     # Módulo Terraform completo
├── main.tf               # Recursos: Glue job, KMS, Security Config, Connection, Upload
├── variables.tf          # Variáveis de entrada
├── outputs.tf            # Outputs do módulo
├── data.tf               # Data sources (VPC, IAM Role, Subnets, SG)
├── locals.tf             # Locals computados (buckets, spark_conf)
├── versions.tf           # Provider e backend S3
└── terraform.tfvars      # Valores default das variáveis
scripts/
├── setup-env.sh          # Setup do ambiente AWS via Terraform (upload via null_resource)
└── rollback-setup.sh     # Rollback do ambiente AWS (sem limpeza S3)
docs-sdd/
├── skill.md              # Skill document (atualizado v3)
├── feature.md            # Feature document (atualizado v3)
├── prd.md                # PRD document (atualizado v3)
├── agents.md             # Agents document (atualizado v3)
├── spec.md               # Specification (novo — a partir de agents + PRD)
├── plan.md               # Plano de implementação (atualizado — a partir de spec)
├── arquitetura.md        # Arquitetura geral do Data Lake
├── schema_source_files.md # Schema das fontes DMS
├── structure_source_directory.md # Estrutura do bucket landing
└── feature_skill.txt     # Especificação inicial da feature
```

## Configuração (JSON)

### `app/src/dependencies/config/origins.json` — Origem DMS (conexão)
```json
[{
  "source": "flights",
  "source_location": "s3://lakehouse-landing-{account_id}/dms/flightradar/flight_radar/",
  "format": "parquet",
  "cdc_config": { "op_column": "Op", "timestamp_column": "dms_timestamp" },
  "checkpoint_location": "s3://lakehouse-workspace-{account_id}/checkpoints/flights/"
}]
```

### `app/src/dependencies/config/target.json` — Destino (schema, partições, PK)
```json
{
  "catalog": { "database": "db_raw", "table": "tbl_opensky_flights" },
  "location": "s3://lakehouse-raw-{account_id}/tables/opensky/flights/",
  "rejected_location": "s3://lakehouse-landing-{account_id}/dms/flightradar/flight_radar/Rejected/",
  "format": "delta",
  "compression": "snappy",
  "partition_keys": [{ "name": "event_date", "type": "date" }],
  "schema": [
    { "name": "icao24", "type": "string", "comment": "Código ICAO24 da aeronave" },
    { "name": "callsign", "type": "string", "comment": "Callsign do voo" },
    { "name": "origin_country", "type": "string", "comment": "País de origem" },
    { "name": "latitude", "type": "double", "comment": "Latitude" },
    { "name": "longitude", "type": "double", "comment": "Longitude" },
    { "name": "altitude", "type": "double", "comment": "Altitude em pés" },
    { "name": "velocity", "type": "double", "comment": "Velocidade em m/s" },
    { "name": "heading", "type": "double", "comment": "Direção em graus" },
    { "name": "last_contact", "type": "bigint", "comment": "Último contato (epoch)" },
    { "name": "event_time", "type": "string", "comment": "Horário do evento" },
    { "name": "location", "type": "string", "comment": "Localização" },
    { "name": "cod_unico", "type": "string", "comment": "Concatenação da PK (icao24_event_time)" }
  ],
  "primary_key": ["icao24", "event_time"],
  "cod_unico_expr": { "columns": ["icao24", "event_time"], "separator": "_" },
  "enum_columns": {}
}
```

## Implementação dos Módulos Core

### `config.py` — Classe Config com dataclasses

**Dataclasses:**
| Classe | Campos |
|--------|--------|
| `SchemaField` | name, type, comment, nullable |
| `PartitionKey` | name, type |
| `CdcConfig` | op_column, timestamp_column, order |
| `SourceConfig` | source, source_location, format, cdc_config, checkpoint_location |
| `TargetConfig` | catalog, location, rejected_location, format, compression, partition_keys, schema, primary_key, enum_columns, cod_unico_expr |
| `Config` | from_files(), from_s3(), to_dict() |

### `reader.py` — Classe Reader (streaming-only)
- **Sem job bookmarks**, sem fallback batch
- `spark.readStream.format("parquet")` com `maxFilesPerTrigger=1`
- `cleanSource=archive`, `includeExistingFiles=true`
- Checkpoint em `source.checkpoint_location`

### `data_quality.py` — Classe DataQuality (4 etapas)
| Etapa | Método | Descrição |
|-------|--------|-----------|
| 1 | `_cast_types` | Converte colunas para tipos do target schema |
| 2 | `_check_nulls` | Remove registros com nulos em campos obrigatórios |
| 3 | `_check_enums` | Valida contra lista permitida em `enum_columns` |
| 4 | `_validate_timestamps` | Pass-through (reservado) |

> **Nota:** A etapa `_remove_duplicates` foi removida — a unicidade é garantida pelo Delta MERGE na escrita (Writer).

### `writer.py` — Classe Writer
- Escrita **Delta Lake** com `DeltaTable.forName()` via Glue Catalog
- Gera `cod_unico` via `F.concat_ws("_", *pk_cols)` para chave do merge
- Deriva `event_date` via `F.to_date()`
- MERGE: `WHEN NOT MATCHED THEN INSERT` / `WHEN MATCHED THEN UPDATE`
- **Sem compaction manual** — Delta gerencia via auto-optimize
- `write_rejects()` com metadados `_reject_table`, `_reject_rule`, `_reject_timestamp` (formato Parquet)

### `etl_control.py` — Classe EtlControl
- Método `register()` escreve em `etl_control`
- Resolve account_id via boto3 STS
- Schema: execution_id, source_name, status, records_read, records_written, records_rejected, elapsed_seconds, error_message

### `quality_metrics.py` — Classe QualityMetrics
- Método `save()` escreve em `data_quality_metrics`
- Resolve account_id via boto3 STS
- Schema: rows_read, rows_written, rows_rejected, pipeline_status

### `processor.py` — Classe Processor
- Pipeline de 6 etapas no método `run(source, target)`:
  1. Load configs (origins + target) via `Config`
  2. Read streaming via `Reader`
  3. Validate via `DataQuality` (4 etapas — sem dedup)
  4. Write rejects via `Writer.write_rejects()`
  5. Write valid data via `Writer` (Delta MERGE por PK)
  6. Register execution via `EtlControl` + Save quality metrics via `QualityMetrics`

### `main.py` — Entry Point
- Parse de argumentos via `argparse`: `--origins_s3_path`, `--target_s3_path`, `--conf`
- `_parse_conf()`: converte string `"key=val key=val"` em dict
- `_init_spark()`: aplica configs dinamicamente ao `SparkSession.builder`
- Streaming com `forEachBatch` e `trigger(processingTime="60 seconds")`
- **Sem GlueContext, DynamicFrame, Job, getResolvedOptions**

## Infraestrutura como Código (Terraform)

### Recursos (`infra/main.tf`)

| Recurso | Tipo | Descrição |
|---------|------|-----------|
| `aws_kms_key.glue` | KMS Key | SSE-KMS/CSE-KMS, rotação 30 dias |
| `aws_kms_alias.glue` | KMS Alias | `alias/glue-streaming-minibatch-dms` |
| `aws_glue_security_configuration.glue` | Security Config | CloudWatch SSE-KMS, bookmarks CSE-KMS, S3 SSE-KMS |
| `aws_glue_connection.vpc` | Glue Connection | NETWORK, subnet privada, SG default |
| `aws_glue_job.streaming_minibatch_dms` | Glue Job | Glue 5.1, Python 3.10, G.1X, 2 workers |
| `data.archive_file.helpers` + `aws_s3_object.*` | Archive + S3 Objects | Declarativo — helpers.zip + main.py + configs JSON |

### Spark Configs (--conf)

Definidas em `locals.tf` como `spark_properties` e convertidas em string `spark_conf`:

- AQE habilitado, shuffle=200, memória 4g executor/driver
- Off-heap 2g, dynamic allocation, Snappy compression
- Aplicadas dinamicamente no `main.py` via `_parse_conf()` + `SparkSession.builder.config()`

### Scripts

| Script | Funcionalidade |
|--------|---------------|
| `scripts/setup-env.sh` | Terraform init/apply + upload de artefatos via `aws_s3_object` |
| `scripts/rollback-setup.sh` | Terraform destroy **sem** limpeza de S3 |

## Cronograma (Executado)

| Fase | Tarefas | Status |
|------|---------|--------|
| Fase 1 | Estrutura de diretórios, config JSON | ✅ Completo |
| Fase 2 | Implementação dos 8 módulos Python | ✅ Completo |
| Fase 3 | Terraform + upload S3 via null_resource | ✅ Completo |
| Fase 4 | Testes unitários + integração | ✅ Completo |
| Fase 5 | Scripts setup-env.sh + rollback-setup.sh | ✅ Completo |

## Entregáveis

| # | Entregável | Status |
|---|-----------|--------|
| 1 | `app/src/main.py` + 7 módulos em `app/src/dependencies/` (config/ é subpacote) | ✅ |
| 2 | `app/src/dependencies/config/origins.json` + `app/src/dependencies/config/target.json` (configs separadas) | ✅ |
| 3 | `infra/` módulo Terraform completo (Glue Job, KMS, Security Config, Connection, Upload) | ✅ |
| 4 | `tests/unit/` (100% cobertura) | ✅ |
| 5 | `tests/integration/` (boto3) | ✅ |
| 6 | `scripts/setup-env.sh` + `rollback-setup.sh` (sem upload S3 nos scripts) | ✅ |
| 7 | Pipeline validado e em produção | ⏳ Pendente |
| 8 | `docs-sdd/` sincronizado com código atual | ✅ |
