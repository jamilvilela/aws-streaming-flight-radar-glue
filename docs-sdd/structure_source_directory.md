Aqui está a estrutura completa do bucket `lakehouse-landing-331504768406` em formato de árvore:

```
lakehouse-landing-331504768406/
│
├── dms/
│   └── flightradar/
│       └── flight_radar/
│           │
│           ├── aircraft/
│           │   ├── LOAD00000001.parquet
│           │   └── 2026/
│           │       └── 06/
│           │           ├── 21/
│           │           │   ├── 20260621-175008375.parquet
│           │           │   └── 20260621-180445067.parquet
│           │           └── 22/
│           │               ├── 20260622-221241295.parquet
│           │               └── 20260622-224610534.parquet
│           │
│           ├── aircraft_positions/
│           │   ├── LOAD00000001.parquet  (7.9 MiB)
│           │   └── 2026/
│           │       └── 06/
│           │           ├── 21/
│           │           │   ├── 20260621-175111770.parquet
│           │           │   ├── 20260621-180448944.parquet
│           │           │   └── 20260621-180549270.parquet
│           │           └── 22/
│           │               ├── 20260622-221254605.parquet
│           │               ├── 20260622-221354931.parquet
│           │               ├── 20260622-221455278.parquet
│           │               ├── 20260622-221605128.parquet
│           │               ├── 20260622-221705269.parquet
│           │               ├── 20260622-224614578.parquet
│           │               └── 20260622-224724640.parquet
│           │
│           ├── airlines/
│           │   ├── LOAD00000001.parquet
│           │   └── 2026/
│           │       └── 06/
│           │           ├── 21/
│           │           │   ├── 20260621-171301295.parquet
│           │           │   ├── 20260621-172032946.parquet
│           │           │   ├── 20260621-172144302.parquet
│           │           │   ├── 20260621-175002546.parquet
│           │           │   └── 20260621-180439278.parquet
│           │           └── 22/
│           │               ├── 20260622-221235560.parquet
│           │               └── 20260622-224604743.parquet
│           │
│           ├── airports/
│           │   ├── LOAD00000001.parquet
│           │   └── 2026/
│           │       └── 06/
│           │           ├── 21/
│           │           │   ├── 20260621-174956188.parquet
│           │           │   └── 20260621-180432719.parquet
│           │           └── 22/
│           │               ├── 20260622-221229309.parquet
│           │               └── 20260622-224558193.parquet
│           │
│           └── flights/
│               ├── LOAD00000001.parquet  (187.7 KiB)
│               └── 2026/
│                   └── 06/
│                       ├── 21/
│                       │   ├── 20260621-175011538.parquet
│                       │   ├── 20260621-175111876.parquet
│                       │   ├── 20260621-180448905.parquet
│                       │   └── 20260621-180549387.parquet
│                       └── 22/
│                           ├── 20260622-221244612.parquet
│                           ├── 20260622-221344828.parquet
│                           ├── 20260622-221445144.parquet
│                           ├── 20260622-221545285.parquet
│                           ├── 20260622-221645432.parquet
│                           ├── 20260622-224614617.parquet
│                           └── 20260622-224724746.parquet
│
└── opensky/
    │
    ├── flights-enriched-raw/
    │   └── dt=2026-06-07-20/
    │   │   ├── _SUCCESS
    │   │   └── part-9ba756aa-949e-48ad-8c78-2748cf8b692c-0-0
    │   └── dt=2026-06-11-23/
    │   │   ├── _SUCCESS
    │   │   ├── part-ef966fe7-3c18-4ac3-b4d2-6147aaf685c7-0-0
    │   │   ├── part-ef966fe7-3c18-4ac3-b4d2-6147aaf685c7-0-1
    │   │   ├── part-ef966fe7-3c18-4ac3-b4d2-6147aaf685c7-0-2
    │   │   └── part-ef966fe7-3c18-4ac3-b4d2-6147aaf685c7-0-3
    │   └── dt=2026-06-21-13/
    │       ├── _SUCCESS
    │       └── part-a5514f4c-8ca0-4303-a3e4-b193ea47d164-0-0
    │
    └── flights/
        └── year=2026/
            ├── month=04/
            │   └── day=16/
            │       └── hour=10/
            │           ├── flight-radar-firehose-flights-1-2026-04-16-10-57-11-019a814f-...
            │           ├── flight-radar-firehose-flights-1-2026-04-16-10-57-11-1d7893e3-...
            │           ├── ... (8 arquivos JSON no total)
            │
            ├── month=05/
            │   ├── day=02/
            │   │   ├── hour=19/  (8 arquivos JSON)
            │   │   ├── hour=20/  (1 arquivo JSON)
            │   │   ├── hour=22/  (6 arquivos JSON)
            │   │   └── hour=23/  (4 arquivos JSON)
            │   ├── day=03/
            │   │   └── hour=00/  (1 arquivo JSON)
            │   ├── day=05/
            │   │   └── hour=22/  (9 arquivos JSON)
            │   ├── day=06/
            │   │   ├── hour=22/  (1 arquivo JSON)
            │   │   └── hour=23/  (1 arquivo JSON)
            │   ├── day=10/
            │   │   └── hour=18/  (1 arquivo JSON)
            │   ├── day=17/
            │   │   ├── hour=16/  (1 arquivo JSON)
            │   │   ├── hour=18/  (3 arquivos JSON)
            │   │   └── hour=19/  (1 arquivo JSON)
            │   ├── day=18/
            │   │   ├── hour=12/  (2 arquivos JSON)
            │   │   ├── hour=18/  (2 arquivos JSON)
            │   │   ├── hour=19/  (1 arquivo JSON)
            │   │   └── hour=20/  (3 arquivos JSON)
            │   └── day=19/
            │       ├── hour=20/  (1 arquivo JSON)
            │       └── hour=22/  (1 arquivo JSON)
            │
            └── month=06/
                └── (sem dados em opensky/flights/ ainda)
```

---

### Resumo

| Caminho | Total de Objetos |
|---|---|
| **Total geral** | **105 objetos — 20.9 MiB** |
| `dms/flightradar/flight_radar/` (DMS Streaming — Parquet) | 5 tabelas: `aircraft`, `aircraft_positions`, `airlines`, `airports`, `flights` |
| `opensky/flights-enriched-raw/` (particionado por `dt=`) | 3 partições (07, 11, 21 de Junho) |
| `opensky/flights/` (particionado `year/month/day/hour` — JSON) | Registros de Abr a Mai/2026 |

**Observações:**
- A pasta `dms/flightradar/flight_radar/` contém dados do **DMS CDC Streaming** no formato **Parquet**, com arquivos de carga inicial (`LOAD...`) e incrementais particionados por data (`2026/06/21` e `22`).
- A pasta `opensky/flights-enriched-raw/` contém saída de **ETL Glue** (provavelmente) com dados enriquecidos no formato de texto particionado.
- A pasta `opensky/flights/` contém dados brutos do **OpenSky Network** ingeridos via **Firehose** no formato **JSON**, particionados hierarquicamente por `year=.../month=.../day=.../hour=...`.