## Schema dos arquivos gerados pelo DMS no Bucket Landing

O DMS Serverless está configurado para replicar o schema `flight_radar` do Aurora PostgreSQL para o bucket S3 **`lakehouse-landing-<account_id>`** no formato **Parquet** (v2.0) com compressão **gzip**.

Cada tabela gera seu próprio conjunto de arquivos Parquet. Como `include_op_for_full_load = true`, os arquivos de full load incluem uma coluna adicional `Op` indicando o tipo de operação (`I`, `U`, `D`).

### Colunas por tabela (arquivo Parquet)

#### `aircraft.parquet` — Registro de aeronaves
| Coluna | Tipo | Descrição |
|---|---|---|
| `icao24` | `VARCHAR(6)` | Código ICAO da aeronave (PK) |
| `registration` | `VARCHAR(20)` | Matrícula |
| `aircraft_type` | `VARCHAR(10)` | Tipo (ex: B738, A320) |
| `manufacturer` | `VARCHAR(50)` | Fabricante |
| `model` | `VARCHAR(50)` | Modelo |
| `serial_number` | `VARCHAR(30)` | Número de série |
| `operator_icao` | `VARCHAR(3)` | Código ICAO da operadora |
| `operator_name` | `VARCHAR(100)` | Nome da operadora |
| `first_flight_date` | `DATE` | Data do primeiro voo |
| `created_at` | `TIMESTAMPTZ` | Data de criação |
| `updated_at` | `TIMESTAMPTZ` | Data de atualização |
| `dms_timestamp` | `TIMESTAMPTZ` | Timestamp DMS (full load) |
| `Op` | `CHAR(1)` | `I` (insert) — incluído via `include_op_for_full_load` |

#### `airports.parquet` — Aeroportos
| Coluna | Tipo | Descrição |
|---|---|---|
| `icao_code` | `VARCHAR(4)` | Código ICAO (PK) |
| `iata_code` | `VARCHAR(3)` | Código IATA |
| `name` | `VARCHAR(200)` | Nome do aeroporto |
| `city` | `VARCHAR(100)` | Cidade |
| `country` | `VARCHAR(100)` | País |
| `country_code` | `VARCHAR(2)` | Código do país (ISO) |
| `latitude` | `DECIMAL(10,7)` | Latitude |
| `longitude` | `DECIMAL(10,7)` | Longitude |
| `elevation_ft` | `INTEGER` | Elevação em pés |
| `timezone` | `VARCHAR(50)` | Fuso horário |
| `created_at` | `TIMESTAMPTZ` | Data de criação |
| `dms_timestamp` | `TIMESTAMPTZ` | Timestamp DMS |
| `Op` | `CHAR(1)` | Operação |

#### `airlines.parquet` — Companhias aéreas
| Coluna | Tipo | Descrição |
|---|---|---|
| `icao_code` | `VARCHAR(3)` | Código ICAO (PK) |
| `iata_code` | `VARCHAR(2)` | Código IATA |
| `name` | `VARCHAR(200)` | Nome da companhia |
| `country` | `VARCHAR(100)` | País |
| `callsign` | `VARCHAR(50)` | Callsign |
| `is_active` | `BOOLEAN` | Ativo? |
| `created_at` | `TIMESTAMPTZ` | Data de criação |
| `dms_timestamp` | `TIMESTAMPTZ` | Timestamp DMS |
| `Op` | `CHAR(1)` | Operação |

#### `flights.parquet` — Tabela fato de voos
| Coluna | Tipo | Descrição |
|---|---|---|
| `flight_id` | `BIGINT` | ID do voo (PK) |
| `flight_number` | `VARCHAR(10)` | Número do voo (ex: AA1234) |
| `airline_icao` | `VARCHAR(3)` | FK → `airlines` |
| `aircraft_icao24` | `VARCHAR(6)` | FK → `aircraft` |
| `origin_airport` | `VARCHAR(4)` | FK → `airports` (origem) |
| `destination_airport` | `VARCHAR(4)` | FK → `airports` (destino) |
| `scheduled_departure` | `TIMESTAMPTZ` | Partida programada |
| `scheduled_arrival` | `TIMESTAMPTZ` | Chegada programada |
| `actual_departure` | `TIMESTAMPTZ` | Partida real |
| `actual_arrival` | `TIMESTAMPTZ` | Chegada real |
| `status` | `VARCHAR(20)` | Status (`scheduled`, `active`, `landed`, `cancelled`, `diverted`) |
| `created_at` | `TIMESTAMPTZ` | Data de criação |
| `updated_at` | `TIMESTAMPTZ` | Data de atualização |
| `dms_timestamp` | `TIMESTAMPTZ` | Timestamp DMS |
| `Op` | `CHAR(1)` | Operação |

#### `aircraft_positions.parquet` — Posições (alta volumetria, streaming)
| Coluna | Tipo | Descrição |
|---|---|---|
| `position_id` | `BIGINT` | ID da posição (PK) |
| `aircraft_icao24` | `VARCHAR(6)` | FK → `aircraft` |
| `flight_id` | `BIGINT` | FK → `flights` |
| `latitude` | `DECIMAL(10,7)` | Latitude |
| `longitude` | `DECIMAL(10,7)` | Longitude |
| `altitude_ft` | `INTEGER` | Altitude em pés |
| `velocity_kts` | `DECIMAL(7,2)` | Velocidade em nós |
| `heading` | `DECIMAL(5,2)` | Rumo (graus) |
| `vertical_rate_fpm` | `DECIMAL(7,2)` | Razão de subida/descida (ft/min) |
| `on_ground` | `BOOLEAN` | Em solo? |
| `recorded_at` | `TIMESTAMPTZ` | Momento da captura |
| `ingested_at` | `TIMESTAMPTZ` | Momento da ingestão |
| `dms_timestamp` | `TIMESTAMPTZ` | Timestamp DMS |
| `Op` | `CHAR(1)` | Operação |

### Comportamento CDC (streaming)

Os arquivos na pasta `cdc/` seguem o mesmo schema das tabelas, mas com estas diferenças:

- **`Op`** passa a registrar: `I` (insert), `U` (update), `D` (delete)
- Os arquivos são escritos com intervalo máximo de **60 segundos** (`cdc_max_batch_interval`)
- A coluna `dms_timestamp` indica quando o DMS capturou a mudança
- `preserve_transactions = false` → cada registro é independente (sem agrupamento transacional)

### Resumo

O generate_dms_data.py popula exatamente essas 5 tabelas no schema `flight_radar` do Aurora PostgreSQL, e o DMS Serverless replica tudo para o bucket landing no formato **Parquet + gzip**, particionado por data (`YYYYMMDD`), com uma coluna `Op` para rastrear o tipo de operação e `dms_timestamp` para o momento da captura.