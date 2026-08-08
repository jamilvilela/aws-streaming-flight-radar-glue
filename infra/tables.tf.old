#===============================================================================
# Glue Data Catalog Tables — DMS CDC Streaming (flight_radar schema)
#===============================================================================
#
# Defines all target tables written by the Glue job in the raw layer (db_raw).
# Data tables use Delta format; support tables (etl_control, data_quality_metrics)
# use Parquet format.
#
# Tables must be pre-created because the Glue job uses DeltaTable.forName()
# and Delta MERGE which require existing Catalog entries.
#===============================================================================

# ------------------------------------------------------------------------------
# tbl_aircraft — Aircraft registry
# Source: DMS CDC from flight_radar.aircraft (Aurora PostgreSQL)
# Format: Delta Lake
# PK: icao24
# ------------------------------------------------------------------------------
resource "aws_glue_catalog_table" "tbl_aircraft" {
  name          = var.tables.tbl_aircraft
  database_name = var.databases.raw

  table_type = "EXTERNAL_TABLE"

  parameters = {
    classification  = "delta"
    table_type      = "delta"
    compressionType = "snappy"
  }

  partition_keys {
    name = "event_date"
    type = "date"
  }

  storage_descriptor {
    location      = "s3://${var.buckets.raw}/tables/tbl_aircraft/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "DeltaLakeSerDe"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "icao24"
      type    = "string"
      comment = "ICAO aircraft address (hex)"
    }
    columns {
      name    = "registration"
      type    = "string"
      comment = "Aircraft registration/tail number"
    }
    columns {
      name    = "aircraft_type"
      type    = "string"
      comment = "Aircraft type ICAO code (e.g. B738, A320)"
    }
    columns {
      name    = "serial_number"
      type    = "string"
      comment = "Manufacturer serial number"
    }
    columns {
      name    = "operator_icao"
      type    = "string"
      comment = "Operator ICAO code"
    }
    columns {
      name    = "operator_name"
      type    = "string"
      comment = "Operator name"
    }
    columns {
      name    = "year_built"
      type    = "int"
      comment = "Year of manufacture"
    }
    columns {
      name    = "created_at"
      type    = "timestamp"
      comment = "Record creation timestamp"
    }
    columns {
      name    = "updated_at"
      type    = "timestamp"
      comment = "Record last update timestamp"
    }
    columns {
      name    = "cod_unico"
      type    = "string"
      comment = "PK concatenation (icao24)"
    }
  }
}

# ------------------------------------------------------------------------------
# tbl_airports — Airports reference
# Source: DMS CDC from flight_radar.airports (Aurora PostgreSQL)
# Format: Delta Lake
# PK: icao_code (natural key)
# ------------------------------------------------------------------------------
resource "aws_glue_catalog_table" "tbl_airports" {
  name          = var.tables.tbl_airports
  database_name = var.databases.raw

  table_type = "EXTERNAL_TABLE"

  parameters = {
    classification  = "delta"
    table_type      = "delta"
    compressionType = "snappy"
  }

  partition_keys {
    name = "event_date"
    type = "date"
  }

  storage_descriptor {
    location      = "s3://${var.buckets.raw}/tables/tbl_airports/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "DeltaLakeSerDe"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "id"
      type    = "int"
      comment = "Airport internal ID (PK)"
    }
    columns {
      name    = "ident"
      type    = "string"
      comment = "Airport identifier code"
    }
    columns {
      name    = "type"
      type    = "string"
      comment = "Airport type (large_airport, medium_airport, small_airport, heliport...)"
    }
    columns {
      name    = "name"
      type    = "string"
      comment = "Airport name"
    }
    columns {
      name    = "latitude_deg"
      type    = "decimal(10,7)"
      comment = "Latitude in degrees"
    }
    columns {
      name    = "longitude_deg"
      type    = "decimal(10,7)"
      comment = "Longitude in degrees"
    }
    columns {
      name    = "elevation_ft"
      type    = "int"
      comment = "Elevation in feet"
    }
    columns {
      name    = "continent"
      type    = "string"
      comment = "Continent code"
    }
    columns {
      name    = "iso_country"
      type    = "string"
      comment = "ISO country code"
    }
    columns {
      name    = "iso_region"
      type    = "string"
      comment = "ISO region code"
    }
    columns {
      name    = "municipality"
      type    = "string"
      comment = "Municipality/city"
    }
    columns {
      name    = "scheduled_service"
      type    = "boolean"
      comment = "Has scheduled service?"
    }
    columns {
      name    = "icao_code"
      type    = "string"
      comment = "ICAO airport code (natural key)"
    }
    columns {
      name    = "iata_code"
      type    = "string"
      comment = "IATA airport code"
    }
    columns {
      name    = "gps_code"
      type    = "string"
      comment = "GPS code"
    }
    columns {
      name    = "local_code"
      type    = "string"
      comment = "Local airport code"
    }
    columns {
      name    = "home_link"
      type    = "string"
      comment = "Airport homepage URL"
    }
    columns {
      name    = "wikipedia_link"
      type    = "string"
      comment = "Wikipedia page URL"
    }
    columns {
      name    = "cod_unico"
      type    = "string"
      comment = "PK concatenation (icao_code)"
    }
  }
}

# ------------------------------------------------------------------------------
# tbl_airlines — Airlines reference
# Source: DMS CDC from flight_radar.airlines (Aurora PostgreSQL)
# Format: Delta Lake
# PK: icao_code (natural key)
# ------------------------------------------------------------------------------
resource "aws_glue_catalog_table" "tbl_airlines" {
  name          = var.tables.tbl_airlines
  database_name = var.databases.raw

  table_type = "EXTERNAL_TABLE"

  parameters = {
    classification  = "delta"
    table_type      = "delta"
    compressionType = "snappy"
  }

  partition_keys {
    name = "event_date"
    type = "date"
  }

  storage_descriptor {
    location      = "s3://${var.buckets.raw}/tables/tbl_airlines/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "DeltaLakeSerDe"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "id"
      type    = "int"
      comment = "Airline internal ID (PK)"
    }
    columns {
      name    = "name"
      type    = "string"
      comment = "Airline name"
    }
    columns {
      name    = "alias"
      type    = "string"
      comment = "Airline alias/alternative name"
    }
    columns {
      name    = "iata_code"
      type    = "string"
      comment = "IATA airline code (2-letter)"
    }
    columns {
      name    = "icao_code"
      type    = "string"
      comment = "ICAO airline code (3-letter, natural key)"
    }
    columns {
      name    = "callsign"
      type    = "string"
      comment = "Airline callsign"
    }
    columns {
      name    = "country"
      type    = "string"
      comment = "Country of incorporation"
    }
    columns {
      name    = "is_active"
      type    = "boolean"
      comment = "Is the airline active?"
    }
    columns {
      name    = "created_at"
      type    = "timestamp"
      comment = "Record creation timestamp"
    }
    columns {
      name    = "cod_unico"
      type    = "string"
      comment = "PK concatenation (icao_code)"
    }
  }
}

# ------------------------------------------------------------------------------
# tbl_flights — Flights fact table
# Source: DMS CDC from flight_radar.flights (Aurora PostgreSQL)
# Format: Delta Lake
# PK: flight_id
# ------------------------------------------------------------------------------
resource "aws_glue_catalog_table" "tbl_flights" {
  name          = var.tables.tbl_flights
  database_name = var.databases.raw

  table_type = "EXTERNAL_TABLE"

  parameters = {
    classification  = "delta"
    table_type      = "delta"
    compressionType = "snappy"
  }

  partition_keys {
    name = "event_date"
    type = "date"
  }

  storage_descriptor {
    location      = "s3://${var.buckets.raw}/tables/tbl_flights/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "DeltaLakeSerDe"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "flight_id"
      type    = "bigint"
      comment = "Flight ID (PK)"
    }
    columns {
      name    = "flight_number"
      type    = "string"
      comment = "Flight number (e.g. AA1234)"
    }
    columns {
      name    = "airline_icao"
      type    = "string"
      comment = "Airline ICAO code (FK → airlines)"
    }
    columns {
      name    = "aircraft_icao24"
      type    = "string"
      comment = "Aircraft ICAO24 address (FK → aircraft)"
    }
    columns {
      name    = "origin_airport"
      type    = "string"
      comment = "Origin airport ICAO code"
    }
    columns {
      name    = "destination_airport"
      type    = "string"
      comment = "Destination airport ICAO code"
    }
    columns {
      name    = "scheduled_departure"
      type    = "timestamp"
      comment = "Scheduled departure time"
    }
    columns {
      name    = "scheduled_arrival"
      type    = "timestamp"
      comment = "Scheduled arrival time"
    }
    columns {
      name    = "actual_departure"
      type    = "timestamp"
      comment = "Actual departure time"
    }
    columns {
      name    = "actual_arrival"
      type    = "timestamp"
      comment = "Actual arrival time"
    }
    columns {
      name    = "status"
      type    = "string"
      comment = "Flight status (scheduled, active, landed, cancelled, diverted)"
    }
    columns {
      name    = "created_at"
      type    = "timestamp"
      comment = "Record creation timestamp"
    }
    columns {
      name    = "updated_at"
      type    = "timestamp"
      comment = "Record last update timestamp"
    }
    columns {
      name    = "cod_unico"
      type    = "string"
      comment = "PK concatenation (flight_id)"
    }
  }
}

# ------------------------------------------------------------------------------
# tbl_aircraft_positions — Aircraft positions (high volume, streaming)
# Source: DMS CDC from flight_radar.aircraft_positions (Aurora PostgreSQL)
# Format: Delta Lake
# PK: position_id, recorded_at
# ------------------------------------------------------------------------------
resource "aws_glue_catalog_table" "tbl_aircraft_positions" {
  name          = var.tables.tbl_aircraft_positions
  database_name = var.databases.raw

  table_type = "EXTERNAL_TABLE"

  parameters = {
    classification  = "delta"
    table_type      = "delta"
    compressionType = "snappy"
  }

  partition_keys {
    name = "event_date"
    type = "date"
  }

  storage_descriptor {
    location      = "s3://${var.buckets.raw}/tables/tbl_aircraft_positions/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "DeltaLakeSerDe"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "position_id"
      type    = "bigint"
      comment = "Position ID (PK)"
    }
    columns {
      name    = "aircraft_icao24"
      type    = "string"
      comment = "Aircraft ICAO24 address (FK → aircraft)"
    }
    columns {
      name    = "flight_id"
      type    = "bigint"
      comment = "Flight ID (FK → flights)"
    }
    columns {
      name    = "latitude"
      type    = "decimal(10,7)"
      comment = "Latitude"
    }
    columns {
      name    = "longitude"
      type    = "decimal(10,7)"
      comment = "Longitude"
    }
    columns {
      name    = "altitude_ft"
      type    = "int"
      comment = "Altitude in feet"
    }
    columns {
      name    = "velocity_kts"
      type    = "decimal(7,2)"
      comment = "Velocity in knots"
    }
    columns {
      name    = "heading"
      type    = "decimal(5,2)"
      comment = "Heading in degrees"
    }
    columns {
      name    = "vertical_rate_fpm"
      type    = "decimal(7,2)"
      comment = "Vertical rate in ft/min"
    }
    columns {
      name    = "on_ground"
      type    = "boolean"
      comment = "Is the aircraft on ground?"
    }
    columns {
      name    = "recorded_at"
      type    = "timestamp"
      comment = "Position recording timestamp"
    }
    columns {
      name    = "ingested_at"
      type    = "timestamp"
      comment = "Ingestion timestamp"
    }
    columns {
      name    = "dms_operation"
      type    = "string"
      comment = "DMS CDC operation (I/U/D)"
    }
    columns {
      name    = "dms_timestamp"
      type    = "timestamp"
      comment = "DMS capture timestamp"
    }
    columns {
      name    = "cod_unico"
      type    = "string"
      comment = "PK concatenation (position_id_recorded_at)"
    }
  }
}

# ------------------------------------------------------------------------------
# tbl_countries — Countries reference table
# Source: DMS CDC from flight_radar.countries (Aurora PostgreSQL)
# Format: Delta Lake
# PK: id
# ------------------------------------------------------------------------------
resource "aws_glue_catalog_table" "tbl_countries" {
  name          = var.tables.tbl_countries
  database_name = var.databases.raw

  table_type = "EXTERNAL_TABLE"

  parameters = {
    classification  = "delta"
    table_type      = "delta"
    compressionType = "snappy"
  }

  partition_keys {
    name = "event_date"
    type = "date"
  }

  storage_descriptor {
    location      = "s3://${var.buckets.raw}/tables/tbl_countries/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "DeltaLakeSerDe"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "id"
      type    = "int"
      comment = "Country internal ID (PK)"
    }
    columns {
      name    = "code"
      type    = "string"
      comment = "ISO 2-letter country code"
    }
    columns {
      name    = "name"
      type    = "string"
      comment = "Country name"
    }
    columns {
      name    = "continent"
      type    = "string"
      comment = "Continent code"
    }
    columns {
      name    = "wikipedia_link"
      type    = "string"
      comment = "Wikipedia page URL"
    }
    columns {
      name    = "cod_unico"
      type    = "string"
      comment = "PK concatenation (id)"
    }
  }
}

# ------------------------------------------------------------------------------
# tbl_aircraft_types — Aircraft type catalog (models)
# Source: DMS CDC from flight_radar.aircraft_types (Aurora PostgreSQL)
# Format: Delta Lake
# PK: icao_code
# ------------------------------------------------------------------------------
resource "aws_glue_catalog_table" "tbl_aircraft_types" {
  name          = var.tables.tbl_aircraft_types
  database_name = var.databases.raw

  table_type = "EXTERNAL_TABLE"

  parameters = {
    classification  = "delta"
    table_type      = "delta"
    compressionType = "snappy"
  }

  partition_keys {
    name = "event_date"
    type = "date"
  }

  storage_descriptor {
    location      = "s3://${var.buckets.raw}/tables/tbl_aircraft_types/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "DeltaLakeSerDe"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "icao_code"
      type    = "string"
      comment = "ICAO aircraft type code (e.g. B738, A320)"
    }
    columns {
      name    = "iata_code"
      type    = "string"
      comment = "IATA aircraft type code (e.g. 738, 320)"
    }
    columns {
      name    = "name"
      type    = "string"
      comment = "Aircraft model name"
    }
    columns {
      name    = "manufacturer"
      type    = "string"
      comment = "Manufacturer name"
    }
    columns {
      name    = "cod_unico"
      type    = "string"
      comment = "PK concatenation (icao_code)"
    }
  }
}

# ------------------------------------------------------------------------------
# tbl_routes — Route catalog between airports
# Source: DMS CDC from flight_radar.routes (Aurora PostgreSQL)
# Format: Delta Lake
# PK: id
# ------------------------------------------------------------------------------
resource "aws_glue_catalog_table" "tbl_routes" {
  name          = var.tables.tbl_routes
  database_name = var.databases.raw

  table_type = "EXTERNAL_TABLE"

  parameters = {
    classification  = "delta"
    table_type      = "delta"
    compressionType = "snappy"
  }

  partition_keys {
    name = "event_date"
    type = "date"
  }

  storage_descriptor {
    location      = "s3://${var.buckets.raw}/tables/tbl_routes/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "DeltaLakeSerDe"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "id"
      type    = "bigint"
      comment = "Route ID (PK)"
    }
    columns {
      name    = "airline_iata"
      type    = "string"
      comment = "Airline IATA code"
    }
    columns {
      name    = "airline_id"
      type    = "int"
      comment = "Airline internal ID (FK)"
    }
    columns {
      name    = "src_airport"
      type    = "string"
      comment = "Source airport IATA code"
    }
    columns {
      name    = "src_airport_id"
      type    = "int"
      comment = "Source airport internal ID (FK)"
    }
    columns {
      name    = "dst_airport"
      type    = "string"
      comment = "Destination airport IATA code"
    }
    columns {
      name    = "dst_airport_id"
      type    = "int"
      comment = "Destination airport internal ID (FK)"
    }
    columns {
      name    = "codeshare"
      type    = "string"
      comment = "Codeshare indicator"
    }
    columns {
      name    = "stops"
      type    = "int"
      comment = "Number of stops"
    }
    columns {
      name    = "equipment"
      type    = "string"
      comment = "ICAO aircraft type codes"
    }
    columns {
      name    = "duration_minutes"
      type    = "int"
      comment = "Route duration in minutes"
    }
    columns {
      name    = "created_at"
      type    = "timestamp"
      comment = "Record creation timestamp"
    }
    columns {
      name    = "cod_unico"
      type    = "string"
      comment = "PK concatenation (id)"
    }
  }
}

# ------------------------------------------------------------------------------
# etl_control — Pipeline execution control table
# Source: Written by EtlControl class (Parquet append)
# Format: Parquet
# Partition: reference_date
# ------------------------------------------------------------------------------
resource "aws_glue_catalog_table" "etl_control" {
  name          = var.tables.etl_control
  database_name = var.databases.raw

  table_type = "EXTERNAL_TABLE"

  parameters = {
    classification  = "parquet"
    compressionType = "snappy"
  }

  partition_keys {
    name = "reference_date"
    type = "date"
  }

  storage_descriptor {
    location      = "s3://${var.buckets.raw}/tables/etl_control/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "execution_id"
      type    = "string"
      comment = "Execution UUID"
    }
    columns {
      name    = "job_name"
      type    = "string"
      comment = "Glue Job name"
    }
    columns {
      name    = "source"
      type    = "string"
      comment = "Processed source (e.g. flights)"
    }
    columns {
      name    = "execution_start"
      type    = "timestamp"
      comment = "Execution start"
    }
    columns {
      name    = "execution_end"
      type    = "timestamp"
      comment = "Execution end"
    }
    columns {
      name    = "status"
      type    = "string"
      comment = "running, success, failed"
    }
    columns {
      name    = "records_read"
      type    = "bigint"
      comment = "Records read"
    }
    columns {
      name    = "records_written"
      type    = "bigint"
      comment = "Records written to raw"
    }
    columns {
      name    = "records_rejected"
      type    = "bigint"
      comment = "Rejected records"
    }
    columns {
      name    = "target_partition"
      type    = "string"
      comment = "Target partition (e.g. event_date=)"
    }
    columns {
      name    = "error_message"
      type    = "string"
      comment = "Error message (if any)"
    }
  }
}

# ------------------------------------------------------------------------------
# data_quality_metrics — Data quality metrics table
# Source: Written by QualityMetrics class (Parquet append)
# Format: Parquet
# Partition: reference_date
# ------------------------------------------------------------------------------
resource "aws_glue_catalog_table" "data_quality_metrics" {
  name          = var.tables.data_quality
  database_name = var.databases.raw

  table_type = "EXTERNAL_TABLE"

  parameters = {
    classification  = "parquet"
    compressionType = "snappy"
  }

  partition_keys {
    name = "reference_date"
    type = "date"
  }

  storage_descriptor {
    location      = "s3://${var.buckets.raw}/tables/data_quality_metrics/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "database"
      type    = "string"
      comment = "Evaluated database"
    }
    columns {
      name    = "table"
      type    = "string"
      comment = "Evaluated table"
    }
    columns {
      name    = "processing_timestamp"
      type    = "timestamp"
      comment = "Processing timestamp"
    }
    columns {
      name    = "metric"
      type    = "string"
      comment = "Evaluated metric"
    }
    columns {
      name    = "rule"
      type    = "string"
      comment = "Applied rule"
    }
    columns {
      name    = "status"
      type    = "string"
      comment = "passed, failed"
    }
    columns {
      name    = "failure_reason"
      type    = "string"
      comment = "Failure reason"
    }
    columns {
      name    = "partition"
      type    = "string"
      comment = "Evaluated partition"
    }
    columns {
      name    = "technology"
      type    = "string"
      comment = "Technology (e.g. glue)"
    }
    columns {
      name    = "reference_date"
      type    = "date"
      comment = "Partition date"
    }
  }
}
