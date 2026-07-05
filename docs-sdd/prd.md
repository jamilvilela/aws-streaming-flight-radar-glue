---
id: prd-glue-streaming-dms-cdc
title: PRD — Glue Job Streaming Mini-Batch para DMS CDC (tbl_opensky_flights)
status: draft
version: 3.0
created: 2026-06-29
updated: 2026-07-02
author: Data Engineering Team
---

# Product Requirements Document (PRD)

## 1. Objetivo
Construir um pipeline de processamento de dados **CDC (Change Data Capture)** da tabela **`tbl_opensky_flights`** no Data Lake, utilizando **AWS Glue 5.1 (PySpark 4.0)** com processamento **streaming mini-batch** (pure Spark — sem APIs Glue), garantindo qualidade, rastreabilidade e baixa latência.

## 2. Problema
Os dados replicados pelo DMS no bucket landing precisam ser processados antes de estarem prontos para consumo analítico. Atualmente:
- Dados brutos no bucket landing sem validação de schema
- Sem rastreamento de qualidade dos dados ingeridos
- Sem processo de rejeição de registros inválidos
- Sem checkpointing para retomada de processamento
- Necessidade de schema enriquecido para tabela de controle `etl_control`

## 3. Requisitos Funcionais

### RF01 — Leitura Streaming (sem Glue APIs)
- Job deve ler arquivos Parquet do DMS no bucket landing
- Leitura **streaming-only** com `spark.readStream` (sem suporte batch)
- `maxFilesPerTrigger=1` para controle de micro-batches
- Checkpoint S3 via `cleanSource=archive` (sem job bookmarks do Glue)
- `includeExistingFiles=true` para processar dados históricos

### RF02 — Configuração Dinâmica via JSON (separada)
- Dois arquivos JSON no S3:
  - **origins.json**: configuração da origem DMS (conexão, source_location, checkpoint)
  - **target.json**: schema destino com colunas, tipos, partições, PK, enums
- Job lê ambos os arquivos em runtime usando **dataclasses** Python
- Schema deve incluir tipos (string, double, bigint, etc.), partition_keys, primary_key, enum_columns

### RF03 — Qualidade de Dados (4 etapas)
- Pipeline de validação com 4 etapas:
  1. **Cast de tipos** — converte colunas conforme schema do target
  2. **Null check** — filtra nulos em campos NOT NULL
  3. **Enum validation** — valida valores de `enum_columns`
  4. **Timestamp validation** — valida timestamps
- **Sem dedup explícito** — unicidade garantida pelo Delta MERGE na escrita
- Registrar métricas de qualidade em `data_quality_metrics` via classe `QualityMetrics`

### RF04 — Rejeição de Registros
- Registros que não passarem nas validações devem ser salvos em `Rejected/`
- Cada registro rejeitado deve incluir metadados: `_reject_table`, `_reject_rule`, `_reject_timestamp`

### RF05 — Escrita no Data Lake (Delta Lake)
- Formato: **Delta Lake** para dados válidos (rejects em Parquet)
- Particionamento por `event_date` (derivado de coluna timestamp)
- Escrita via **Delta MERGE** (`WHEN NOT MATCHED THEN INSERT` / `WHEN MATCHED THEN UPDATE`) com base na PK
- Geração de `cod_unico` (concatenação da PK) como chave do merge
- **Sem necessidade de compaction** — Delta Lake gerencia otimização via auto-optimize
- Suporte a escrita de rejects via `Writer.write_rejects()` (formato Parquet)

### RF06 — Rastreabilidade (EtlControl)
- Classe `EtlControl` separada para escrever metadados em `etl_control`
- Schema: execution_id, source_name, status, records_read, records_written, records_rejected, elapsed_seconds, error_message
- Resolve caminhos S3 e account_id dinamicamente via `boto3`

### RF07 — Orquestração com Componentes Separados
- `Processor` como classe central que coordena:
  1. `Config.from_files()` / `Config.from_s3()` — carregar configurações
  2. `Reader.read(source)` — streaming read
  3. `DataQuality.validate(df, target, source)` — validação
  4. `Writer.write(valid_df, target, source)` — escrita dados válidos
  5. `Writer.write_rejects(rejects_df, target)` — escrita rejects
  6. `EtlControl.register(...)` — registro de execução
  7. `QualityMetrics.save(target, ...)` — métricas de qualidade

### RF08 — Infraestrutura como Código
- Módulo Terraform completo em `infra/` com:
  - KMS key para criptografia dos dados do Glue job
  - Glue Security Configuration (SSE-KMS para CloudWatch, SSE-KMS para S3)
  - Glue Connection tipo NETWORK para acesso à VPC
  - Glue Job com Spark configs passadas via `--conf` (AQE, shuffle, memória, compressão)
  - `data.archive_file.helpers` + `aws_s3_object.*` para upload declarativo de scripts Python e configs JSON ao S3
  - Databases e tabelas Glue Catalog **não são criados** — já existem no Data Lake

### RF09 — Spark Configs Dinâmicas (--conf)
- Configurações Spark definidas no Terraform (`locals.spark_properties`) e convertidas em string `--conf`
- `main.py` parseia o argumento `--conf` via `_parse_conf()` e aplica cada propriedade dinamicamente ao `SparkSession.builder`
- Nenhum valor Spark hardcoded no código Python — totalmente configurável via Terraform
- Logging estruturado com CloudWatch
- Tratamento de erros com retry e notificação

### RF10 — Spark Configs de Otimização
- AQE (Adaptive Query Execution) habilitado
- Configurações de memória do executor e driver (4g cada, 2g overhead)
- Off-heap memory habilitado
- Dynamic allocation com shuffle tracking
- Configurações de shuffle e paralelismo (200 partições)
- Compressão Snappy

### RF11 — Testes
- Testes unitários com **pytest** mockando Spark e serviços AWS
- Cobertura mínima de **100%** do código
- Testes de integração com **boto3** validando S3, Glue Catalog e dados

### RF12 — Setup e Rollback via Scripts
- `scripts/setup-env.sh`: provisionar ambiente AWS via Terraform (upload de artefatos é feito pelo Terraform)
- `scripts/rollback-setup.sh`: destruir recursos Terraform (sem limpeza de S3)

## 4. Requisitos Não-Funcionais

| ID | Requisito | Descrição |
|----|-----------|-----------|
| RNF01 | Latência | Processamento em mini-batches de até 60s |
| RNF02 | Escalabilidade | Job deve escalar horizontalmente com Spark 4.0 + AQE |
| RNF03 | Tolerância a falhas | Checkpointing S3 para recovery automático |
| RNF04 | Custo | Uso de spot instances quando possível |
| RNF05 | Segurança | Dados criptografados com KMS, IAM least-privilege |
| RNF06 | Pure Spark | Nenhuma API Glue utilizada (GlueContext, DynamicFrame, Job) |
| RNF06 | Manutenibilidade | Código modular com dataclasses, type hints e classes separadas |
| RNF07 | Testabilidade | Testes unitários com mock e testes de integração reais |

## 5. Stack Tecnológica

| Componente | Tecnologia |
|------------|-----------|
| Processamento | **AWS Glue 5.1** (PySpark 4.0) |
| Linguagem | Python 3.10+ com dataclasses e type hints |
| Armazenamento | Amazon S3 (Delta Lake para target / Parquet para rejects) |
| Catálogo | AWS Glue Data Catalog |
| Orquestração | Glue Job Triggers / EventBridge |
| Monitoramento | CloudWatch Logs + Metrics |
| Infraestrutura | Terraform (IaC) |
| Testes unitários | pytest + unittest.mock |
| Testes integração | pytest + boto3 |
| Segurança | AWS KMS + IAM + Lake Formation |

## 6. Métricas de Sucesso

| Métrica | Alvo |
|---------|------|
| Registros processados/min | ≥ 10.000 |
| Taxa de validade | ≥ 99% |
| Latência ponta-a-ponta | < 120s |
| Cobertura de testes unitários | ≥ 100% |
| Uptime do job | ≥ 99.9% |
| Cobertura de validações | 100% das colunas do schema |

## 7. Stakeholders
- **Engenharia de Dados**: Implementação e manutenção
- **Analytics**: Consumo dos dados da tabela `flights` na camada raw
- **Data Governance**: Qualidade e linhagem dos dados
- **FinOps**: Otimização de custos
