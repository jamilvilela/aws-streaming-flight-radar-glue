---
id: feature-glue-streaming-dms-cdc
title: Glue Job Streaming Mini-Batch para DMS CDC (entidade tbl_opensky_flights)
status: draft
version: 3.0
created: 2026-06-29
updated: 2026-07-02
author: Data Engineering Team
---

# Feature: Glue Streaming Mini-Batch com DMS CDC — `tbl_opensky_flights`

## 1. Descrição
Job **AWS Glue 5.1 (PySpark 4.0)** para processar dados de Change Data Capture (CDC) replicados pelo **AWS DMS Serverless** no bucket **landing**, aplicar regras de qualidade e armazenar no **Data Lake** (camada raw — `tbl_opensky_flights`) no formato **Parquet + Snappy**, particionado e catalogado no **Glue Data Catalog**.

> ⚠️ **Pure Spark:** O job **não** utiliza APIs do Glue (`GlueContext`, `DynamicFrame`, `Job`, `getResolvedOptions`). Todo o processamento é feito com SparkSession puro.

> **Escopo:** Este job contempla **apenas** a entidade `tbl_opensky_flights`. Demais entidades serão tratadas em processos separados.

## 2. Contexto
O AWS DMS Serverless replica continuamente dados de uma origem Aurora PostgreSQL para o bucket S3 de landing no formato Parquet. Os arquivos são organizados por pasta de tabela e data (`YYYY/MM/DD/`), com colunas de operação (`Op`) e timestamp DMS (`dms_timestamp`).

O Glue Job processa esses arquivos em **mini-batches** (60s), garantindo:
- Leitura streaming com checkpoint S3 (`cleanSource=archive`) — **sem job bookmarks**
- Validação e limpeza dos dados (cast de tipos, nulls, enums, dedup)
- Escrita eficiente no bucket raw (Parquet + Snappy particionado)
- Rastreamento de métricas de qualidade via `QualityMetrics`
- Registro de execução em tabela de controle via `EtlControl`

## 3. Fonte de Dados (origins.json)

### Configuração da Origem (origins.json)
```json
[{
  "source": "flights",
  "source_location": "s3://lakehouse-landing-{account_id}/dms/flightradar/flight_radar/",
  "format": "parquet",
  "cdc_config": { "op_column": "Op", "timestamp_column": "dms_timestamp" },
  "checkpoint_location": "s3://lakehouse-workspace-{account_id}/checkpoints/flights/"
}]
```

### Comportamento CDC
- **Full load** (`LOAD*.parquet`): snapshot inicial com `Op` = `I`
- **Incrementais**: arquivos com intervalo máximo de **60 segundos**
- `Op` = `I` (insert), `U` (update), `D` (delete)
- `dms_timestamp` marca o momento da captura da mudança

## 4. Destino — `tbl_opensky_flights` (target.json)

### Schema Destino
| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `icao24` | `string` | Código ICAO da aeronave |
| `callsign` | `string` | Identificador do voo |
| `origin_country` | `string` | País de origem |
| `latitude` | `double` | Latitude |
| `longitude` | `double` | Longitude |
| `altitude` | `double` | Altitude |
| `velocity` | `double` | Velocidade |
| `heading` | `double` | Direção |
| `last_contact` | `bigint` | Último contato |
| `event_time` | `string` | Horário do evento |
| `location` | `string` | Localização |

**Partição:** `event_date` (date).  
**Formato:** Parquet + Snappy.  
**PK:** `icao24`, `event_time`.  
**Localização:** `s3://{bucket_raw}/tables/opensky/flights/`.

## 5. Estrutura do Job

### 5.1. Arquivos de Configuração (separados)
| Arquivo | Conteúdo | Classe |
|---------|----------|--------|
| `config/origins.json` | Configuração da origem DMS (conexão) | `SourceConfig` |
| `config/target.json` | Schema e metadados da tabela destino | `TargetConfig` |

### 5.2. Classes

| Classe | Arquivo | Responsabilidade |
|--------|---------|-----------------|
| `Config` | `config.py` | Ler JSONs de configuração, retornar `SourceConfig`/`TargetConfig` (dataclasses) |
| `Reader` | `reader.py` | Ler dados Parquet do S3 com streaming (sem bookmarks) |
| `DataQuality` | `data_quality.py` | Validar, limpar e converter dados; registrar rejeições |
| `Writer` | `writer.py` | Escrever dados válidos em Parquet no raw |
| `EtlControl` | `etl_control.py` | Registrar execução em `etl_control` |
| `QualityMetrics` | `quality_metrics.py` | Salvar métricas em `data_quality_metrics` |
| `Processor` | `processor.py` | Orquestrar pipeline completo (delega EtlControl/QualityMetrics) |
| `main.py` | `main.py` | Entry point: init Spark 4.0 / Glue 5.1, parse args via argparse, run |

### 4.2. Infraestrutura (Terraform)

| Recurso | Arquivo `infra/` | Descrição |
|---------|-----------------|-----------|
| KMS Key | `main.tf` | Chave KMS para criptografia SSE-KMS/CSE-KMS |
| Security Config | `main.tf` | Criptografia: CloudWatch, job bookmarks, S3 |
| Glue Connection | `main.tf` | Conexão VPC (subnet privada + security group) |
| Glue Job | `main.tf` | Job Glue 5.1 / Spark 4.0 / Python 3.10 |

> ⚠️ Databases e tabelas Glue Catalog não são criados pelo Terraform — já existem no Data Lake. Os nomes são apenas para referência no código.

#### Spark Configs Dinâmicas

As configurações Spark são definidas no `locals.tf` do módulo Terraform como um mapa `spark_properties` e convertidas em uma string única `--conf`. O `main.py` parseia essa string e aplica cada propriedade dinamicamente ao `SparkSession.builder`, eliminando valores hardcoded no código.

### 4.3. Pipeline
```
main.py
  └── Processor.run("flights")
        ├── 1. Config.from_s3(path) → SourceConfig
        ├── 2. Reader.stream(config) → DataFrame bruto
        ├── 3. DataQuality.validate(df, config) → (valid_df, rejects_df)
        ├── 4. Writer.write_rejects(rejects_df, config)
        ├── 5. Writer.write(valid_df, config)
        ├── 6. etl_control.registrar_execucao(...)
        └── 7. data_quality_metrics.salvar_metricas(...)
```

## 5. Regras de Qualidade (DataQuality)

| Regra | Descrição | Ação para inválidos |
|-------|-----------|-------------------|
| PK duplicada | `flight_id` duplicado | Rejeitar (manter primeira ocorrência) |
| Null em PK | `flight_id` nulo | Rejeitar |
| Enum `status` | Valor fora de [scheduled, active, landed, cancelled, diverted] | Rejeitar |
| Enum `Op` | Valor fora de [I, U, D] | Rejeitar |
| Tipo `timestamp` | Colunas TIMESTAMPTZ com formato inválido | Rejeitar |
| Tipo `bigint` | `flight_id` não numérico | Rejeitar |

## 6. Tabelas de Suporte

### 6.1. `etl_control` — Controle de Execuções

Schema sugerido (enriquecido):

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `execution_id` | `STRING` | UUID da execução |
| `job_name` | `STRING` | Nome do Glue Job |
| `source` | `STRING` | Origem processada (ex: "flights") |
| `execution_start` | `TIMESTAMP` | Início da execução |
| `execution_end` | `TIMESTAMP` | Fim da execução |
| `status` | `STRING` | `running`, `success`, `failed` |
| `records_read` | `BIGINT` | Registros lidos |
| `records_written` | `BIGINT` | Registros escritos no raw |
| `records_rejected` | `BIGINT` | Registros rejeitados |
| `target_partition` | `STRING` | Partição target (ex: "year=2026/month=06/day=21") |
| `error_message` | `STRING` | Mensagem de erro (se houver) |
| `reference_date` | `DATE` | Partição (data de referência) |

### 6.2. `data_quality_metrics` — Métricas de Qualidade

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `database` | `STRING` | Database avaliada |
| `table` | `STRING` | Tabela avaliada |
| `processing_timestamp` | `TIMESTAMP` | Timestamp do processamento |
| `metric` | `STRING` | Métrica avaliada |
| `rule` | `STRING` | Regra aplicada |
| `status` | `STRING` | `passed`, `failed` |
| `failure_reason` | `STRING` | Motivo da falha |
| `partition` | `STRING` | Partição avaliada |
| `technology` | `STRING` | Tecnologia (ex: "glue") |
| `reference_date` | `DATE` | Partição |

## 7. Artefatos Complementares

| Artefato | Descrição |
|----------|-----------|
| `tests/unit/` | Testes unitários com **pytest** mockando Spark, Glue e AWS |
| `tests/integration/` | Testes de integração com **boto3** (S3, Glue Catalog) |
| `infra/` | Módulo Terraform completo (Glue job, KMS, Security Config, Connection, Databases) |
| `scripts/setup-env.sh` | Script bash para setup do ambiente AWS via Terraform (aponta para `infra/`) |
| `scripts/rollback-setup.sh` | Script bash para rollback do ambiente AWS |
| `config/origins.json` | Configuração das origens (JSON) |

## 8. Critérios de Aceitação

| # | Critério | Status |
|---|----------|--------|
| 1 | Job lê arquivos Parquet do DMS (`flights`) no bucket landing | ☐ |
| 2 | Job processa dados streaming em mini-batches com checkpointing | ☐ |
| 3 | Job utiliza Glue 5.1 com Spark 4.0 | ☐ |
| 4 | Spark configs de otimização aplicadas (AQE, memória, shuffle) | ☐ |
| 5 | Job aplica regras de qualidade e rejeita registros inválidos | ☐ |
| 6 | Registros rejeitados são movidos para pasta `Rejected/flights/` | ☐ |
| 7 | Job escreve dados validados em Parquet + Snappy no bucket raw | ☐ |
| 8 | Dados são particionados por `year/month/day` | ☐ |
| 9 | Tabela `tbl_opensky_flights` registrada no Glue Catalog (db_raw) | ☐ |
| 10 | Métricas de qualidade salvas em `data_quality_metrics` | ☐ |
| 11 | Controle de execução registrado em `etl_control` (schema enriquecido) | ☐ |
| 12 | Testes unitários com pytest com cobertura ≥ 100% dos módulos | ☐ |
| 13 | Testes de integração com boto3 validam S3 e Glue Catalog | ☐ |
| 14 | Scripts `setup-env.sh` e `rollback-setup.sh` funcionais | ☐ |
| 15 | Módulo Terraform `infra/` com KMS, Security Config, Connection e Databases | ☐ |
| 16 | Glue job associado a security configuration e VPC connection | ☐ |

## 9. Dependências
- Bucket S3 `lakehouse-landing-{account_id}` com dados DMS da tabela `flights`
- Bucket S3 `lakehouse-raw-{account_id}` para escrita
- Bucket S3 `lakehouse-workspace-{account_id}` para scripts, configs e checkpoints
- Role IAM `role-datalake-analytics`
- Default VPC com subnets privadas e security group default
- KMS key para criptografia (criada pelo módulo `infra/`)
- Glue Catalog databases `db_landing` e `db_raw` (criadas pelo módulo `infra/`)
- Tabelas Glue Catalog em `scripts/*.tf`
