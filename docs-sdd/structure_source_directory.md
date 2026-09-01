# Structure of the Landing Bucket

Here is the complete structure of the `lakehouse-landing-${local.account_id}` bucket in tree format:

```
lakehouse-landing-${local.account_id}/
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
│           ├── countries/
│           │   ├── LOAD00000001.parquet
│           │   └── 2026/
│           │       └── 06/
│           │           ├── 21/
│           │           │   ├── 20260621-174942000.parquet
│           │           │   └── 20260621-180418921.parquet
│           │           └── 22/
│           │               ├── 20260622-221215482.parquet
│           │               └── 20260622-224541014.parquet
│           │
│           ├── aircraft_types/
│           │   ├── LOAD00000001.parquet
│           │   └── 2026/
│           │       └── 06/
│           │           ├── 21/
│           │           │   ├── 20260621-174955001.parquet
│           │           │   └── 20260621-180431892.parquet
│           │           └── 22/
│           │               ├── 20260622-221228110.parquet
│           │               └── 20260622-224556903.parquet
│           │
│           ├── routes/
│           │   ├── LOAD00000001.parquet
│           │   └── 2026/
│           │       └── 06/
│           │           ├── 21/
│           │           │   ├── 20260621-175009203.parquet
│           │           │   └── 20260621-180443815.parquet
│           │           └── 22/
│           │               ├── 20260622-221243275.parquet
│           │               └── 20260622-224613201.parquet
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
│           └── *_cdc/ (CDC streaming, via CdcPath — e.g. aircraft_cdc/, flights_cdc/)
│               └── 2026/
│                   └── 06/
│                       ├── 21/
│                       │   └── 20260621-180600124.parquet
│                       └── 22/
│                           └── 20260622-224800451.parquet
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

## Summary

| Path | Total Objects |
|---|---|
| **Grand total** | **130 objects — 21.6 MiB** |
| `dms/flightradar/flight_radar/` (DMS Streaming — Parquet) | 8 tables: `aircraft`, `aircraft_positions`, `airlines`, `airports`, `countries`, `aircraft_types`, `routes`, `flights` |
| `dms/flightradar/flight_radar/*_cdc/` (DMS CDC Streaming — Parquet) | CDC-only prefixes per table (via `CdcPath`), consumed by the streaming job |
| `opensky/flights-enriched-raw/` (partitioned by `dt=`) | 3 partitions (June 07, 11, 21) |
| `opensky/flights/` (partitioned `year/month/day/hour` — JSON) | Records from Apr to May/2026 |

**Notes:**
- The `dms/flightradar/flight_radar/` folder contains **DMS CDC Streaming** data in **Parquet** format, with initial load files (`LOAD...`) and incremental files partitioned by date (`2026/06/21` and `22`). Each table also has a dedicated `*_cdc/` prefix written by DMS via the `CdcPath` parameter — the streaming job reads only these prefixes.
- The `opensky/flights-enriched-raw/` folder contains **Glue ETL** output (likely) with enriched data in partitioned text format.
- The `opensky/flights/` folder contains raw **OpenSky Network** data ingested via **Firehose** in **JSON** format, hierarchically partitioned by `year=.../month=.../day=.../hour=...`.

## Config.json Path Mapping

The `config.json` maps each table to its respective paths:

| Table | source_location (batch) | cdc_source_location (streaming) | archive_location | checkpoint_location |
|-------|------------------------|--------------------------------|------------------|---------------------|
| aircraft | `.../aircraft/LOAD*.parquet` | `.../aircraft/2*/*/*/*/*.parquet` | `.../aircraft_archive/` | `.../checkpoints/aircraft/` |
| airports | `.../airports/LOAD*.parquet` | `.../airports/2*/*/*/*/*.parquet` | `.../airports_archive/` | `.../checkpoints/airports/` |
| airlines | `.../airlines/LOAD*.parquet` | `.../airlines/2*/*/*/*/*.parquet` | `.../airlines_archive/` | `.../checkpoints/airlines/` |
| flights | `.../flights/LOAD*.parquet` | `.../flights/2*/*/*/*/*.parquet` | `.../flights_archive/` | `.../checkpoints/flights/` |
| aircraft_positions | `.../aircraft_positions_2026_*/LOAD*.parquet` | `.../aircraft_positions_2*/2*/*/*/*/*.parquet` | `.../aircraft_positions_archive/` | `.../checkpoints/aircraft_positions/` |
| countries | `.../countries/LOAD*.parquet` | `.../countries/2*/*/*/*/*.parquet` | `.../countries_archive/` | `.../checkpoints/countries/` |
| aircraft_types | `.../aircraft_types/LOAD*.parquet` | `.../aircraft_types/2*/*/*/*/*.parquet` | `.../aircraft_types_archive/` | `.../checkpoints/aircraft_types/` |
| routes | `.../routes/LOAD*.parquet` | `.../routes/2*/*/*/*/*.parquet` | `.../routes_archive/` | `.../checkpoints/routes/` |

The `{account_id}` placeholder is resolved at deploy time by Terraform (`infra/s3.tf`).