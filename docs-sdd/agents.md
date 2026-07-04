---
name: glue-streaming-agents
description: Agentes especializados para implementação do Glue Job streaming mini-batch DMS CDC (tbl_opensky_flights)
---

# Agents — Glue Streaming Mini-Batch DMS CDC (tbl_opensky_flights)

## Agente: `glue-job-builder`
**Propósito:** Implementar o código PySpark do Glue Job (main.py, processor, config, reader, data_quality, writer, etl_control, quality_metrics)

**Habilidades:**
- PySpark 4.0 / AWS Glue 5.1 (pure Spark — sem GlueContext, DynamicFrame ou Job)
- Leitura streaming com `spark.readStream` e `forEachBatch`
- Checkpointing S3 com `cleanSource=archive` (sem job bookmarks)
- DataFrame API: schema validation, type casting, dedup com `row_number()`
- Escrita particionada em Parquet + Snappy
- Dataclasses Python com type hints (SourceConfig, TargetConfig)
- Spark configs de otimização (AQE, shuffle, off-heap, dynamic allocation)
- Configs passadas via `--conf` e aplicadas dinamicamente

**Prompt pattern para invocar:**
```
Use o agente glue-job-builder para implementar a classe {ClassName} 
em src/{file_name}.py com os seguintes requisitos:
- {requisito 1}
- {requisito 2}
...
```

## Agente: `terraform-infra`
**Propósito:** Criar e manter recursos de infraestrutura AWS via Terraform

**Habilidades:**
- AWS Glue Job definitions (Glue 5.1)
- Glue Security Configuration e KMS keys
- Glue Connection tipo NETWORK (VPC)
- Buckets S3 — upload de artefatos via `null_resource`
- IAM roles e policies (role-datalake-analytics)
- Scripts bash de setup e rollback (sem upload de artefatos nos scripts)

## Agente: `data-quality-spec`
**Propósito:** Definir e implementar regras de qualidade de dados para tbl_opensky_flights

**Habilidades:**
- Validação de schemas (tipos, nulabilidade, constraints)
- Pipeline de 5 etapas: cast_types, check_nulls, check_enums, remove_duplicates, validate_timestamps
- Geração de métricas de qualidade via `QualityMetrics`
- Estrutura de dados para rejected records (`_reject_table`, `_reject_rule`, `_reject_timestamp`)
- Dedup com base em primary_key composta e ordenação CDC

## Agente: `config-designer`
**Propósito:** Projetar e validar arquivos de configuração JSON (separados)

**Habilidades:**
- **origins.json**: configuração da origem DMS (source, source_location, format, cdc_config, checkpoint_location)
- **target.json**: schema destino (catalog, location, partition_keys, schema completo, primary_key, enum_columns)
- Separação clara entre config de conexão e config de destino
- Localizações S3 (source, target, rejected, checkpoint)

## Agente: `test-builder`
**Propósito:** Implementar testes unitários e de integração

**Habilidades:**
- pytest com fixtures e mocks
- Mock de SparkSession (sem GlueContext), boto3 (S3, Glue)
- Cobertura de código (pytest-cov)
- Testes de integração com boto3 real
- Validação de dados escritos no S3 e Glue Catalog
- Testes para EtlControl e QualityMetrics

## Como usar

### Para gerar código de uma classe específica:
```
@glue-job-builder Crie a classe Reader em src/reader.py 
que lê dados Parquet do S3 em streaming puro (sem Glue, sem batch, sem bookmarks).
```

### Para criar infraestrutura:
```
@terraform-infra Crie o resource aws_glue_job para o job streaming-minibatch-dms 
com worker_type G.1X, glue_version 5.0, e script_location no bucket workspace.
```

### Para validar qualidade:
```
@data-quality-spec Defina as regras de qualidade para a tabela tbl_opensky_flights 
com pipeline de 5 etapas.
```

### Para configurar origem:
```
@config-designer Crie origins.json para a origem DMS e target.json 
para o schema destino tbl_opensky_flights.
```

### Para criar testes:
```
@test-builder Crie testes unitários para a classe DataQuality 
com mock do SparkSession, cobrindo validação de tipos, enums e rejects.
```
