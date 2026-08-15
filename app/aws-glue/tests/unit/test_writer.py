"""
Unit tests for writer.py — Writer class for Delta Lake writing.

Early-return and wiring paths are tested with mocked DataFrames (no JVM
required). The tests that exercise real Spark SQL (partition extraction and
cod_unico generation) use the shared ``spark`` fixture, which skips cleanly
when no JVM is available and runs fully in CI/Glue.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from src.dependencies.config import CdcConfig, PartitionKey, SchemaField, SourceConfig, TargetConfig
from src.dependencies.writer import Writer


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def flights_source():
    return SourceConfig(
        source="flights",
        source_location="s3://landing/dms/flightradar/flight_radar/",
        format="parquet",
        cdc_config=CdcConfig(op_column="Op", timestamp_column="dms_timestamp"),
    )


@pytest.fixture
def flights_target():
    return TargetConfig(
        catalog={"database": "db_raw", "table": "tbl_flights"},
        location="s3://raw/tables/tbl_flights/",
        rejected_location="s3://landing/dms/flightradar/flight_radar/Rejected/",
        format="delta",
        compression="snappy",
        partition_keys=[PartitionKey("event_date", "date")],
        schema={
            "flight_id": SchemaField(type="bigint", nullable=False, comment="PK"),
            "airline_code": SchemaField(type="string", nullable=True, comment="IATA"),
            "status": SchemaField(type="string", nullable=True, comment="Status"),
            "dms_timestamp": SchemaField(type="timestamp", nullable=True, comment="CDC ts"),
        },
        primary_key=["flight_id"],
        enum_columns={"status": ["active"]},
        cod_unico_expr={"columns": ["flight_id"], "separator": "_"},
    )


@pytest.fixture
def writer():
    """Writer with a mocked SparkSession (no JVM required)."""
    return Writer(MagicMock())


# ── Tests ────────────────────────────────────────────────────────────────────

class TestWriter:
    def test_write_empty(self, writer, flights_source, flights_target):
        """Writing an empty DataFrame should do nothing."""
        df = MagicMock()
        df.isEmpty.return_value = True
        # Should not raise
        writer.write(df, flights_target, flights_source)
        df.isEmpty.assert_called_once()

    def test_prepare_partitions(self, spark, flights_source, flights_target):
        """_prepare_with_partitions should add event_date from timestamp column."""
        from datetime import datetime
        row = (1, "AA", "active", datetime(2026, 6, 30, 10, 0, 0))
        schema = StructType([
            StructField("flight_id", LongType(), True),
            StructField("airline_code", StringType(), True),
            StructField("status", StringType(), True),
            StructField("dms_timestamp", TimestampType(), True),
        ])
        df = spark.createDataFrame([row], schema)

        result = Writer._prepare_with_partitions(df, flights_target, flights_source)
        columns = result.columns
        assert "event_date" in columns

        row_out = result.collect()[0]
        assert str(row_out.event_date) == "2026-06-30"

    def test_prepare_partitions_skip_existing(self, writer, flights_source, flights_target):
        """If partition columns already exist, skip extraction."""
        df = MagicMock()
        df.columns = ["flight_id", "event_date"]
        result = writer._prepare_with_partitions(df, flights_target, flights_source)
        assert result is df
        df.withColumn.assert_not_called()

    def test_write_rejects_empty(self, writer, flights_target):
        """Writing empty rejects should do nothing."""
        df = MagicMock()
        df.isEmpty.return_value = True
        writer.write_rejects(df, flights_target)  # Should not raise

    def test_write_empty_delta(self, writer, flights_source, flights_target):
        """Writing empty DataFrame with Delta should not fail."""
        df = MagicMock()
        df.isEmpty.return_value = True
        # Should not raise (returns early for empty df)
        writer.write(df, flights_target, flights_source)

    def test_generate_cod_unico_from_pk(self, spark, flights_target):
        """_generate_cod_unico should create cod_unico from PK columns."""
        rows = [(1, "AA")]
        schema = StructType([
            StructField("flight_id", LongType(), True),
            StructField("airline_code", StringType(), True),
        ])
        df = spark.createDataFrame(rows, schema)
        result = Writer._generate_cod_unico(df, flights_target)
        assert "cod_unico" in result.columns
        assert result.collect()[0].cod_unico == "1"

    def test_generate_cod_unico_skips_existing(self, writer, flights_target):
        """_generate_cod_unico should skip if cod_unico already exists."""
        df = MagicMock()
        df.columns = ["flight_id", "cod_unico"]
        result = writer._generate_cod_unico(df, flights_target)
        assert result is df
        df.withColumn.assert_not_called()