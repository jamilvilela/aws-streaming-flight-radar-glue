"""
Unit tests for writer.py — Writer class for Delta Lake writing.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from src.dependencies.config import CdcConfig, PartitionKey, SchemaField, SourceConfig, TargetConfig
from src.dependencies.writer import Writer, WriterError


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[2]") \
        .appName("test-writer") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()


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
def writer(spark):
    return Writer(spark)


# ── Tests ────────────────────────────────────────────────────────────────────

class TestWriter:
    def test_write_empty(self, writer, flights_source, flights_target):
        """Writing an empty DataFrame should do nothing."""
        schema = StructType([StructField("dummy", StringType(), True)])
        df = writer._spark.createDataFrame([], schema)
        # Should not raise
        writer.write(df, flights_target, flights_source)

    def test_prepare_partitions(self, spark, writer, flights_source, flights_target):
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

        result = writer._prepare_with_partitions(df, flights_source, flights_target)
        columns = result.columns
        assert "event_date" in columns

        row_out = result.collect()[0]
        assert str(row_out.event_date) == "2026-06-30"

    def test_prepare_partitions_skip_existing(self, spark, writer, flights_source, flights_target):
        """If partition columns already exist, skip extraction."""
        from datetime import date
        rows = [(1, date(2026, 6, 30))]
        schema = StructType([
            StructField("flight_id", LongType(), True),
            StructField("event_date", StringType(), True),
        ])
        df = spark.createDataFrame(rows, schema)
        result = writer._prepare_with_partitions(df, flights_source, flights_target)
        assert str(result.collect()[0].event_date) == "2026-06-30"

    def test_write_rejects_empty(self, writer, flights_target):
        """Writing empty rejects should do nothing."""
        schema = StructType([StructField("_reject_rule", StringType(), True)])
        df = writer._spark.createDataFrame([], schema)
        writer.write_rejects(df, flights_target)  # Should not raise

    def test_write_empty_delta(self, writer, flights_source, flights_target):
        """Writing empty DataFrame with Delta should not fail."""
        schema = StructType([StructField("dummy", StringType(), True)])
        df = writer._spark.createDataFrame([], schema)
        # Should not raise (returns early for empty df)
        writer.write(df, flights_target, flights_source)

    def test_generate_cod_unico_from_pk(self, writer, flights_target):
        """_generate_cod_unico should create cod_unico from PK columns."""
        rows = [(1, "AA")]
        schema = StructType([
            StructField("flight_id", LongType(), True),
            StructField("airline_code", StringType(), True),
        ])
        df = writer._spark.createDataFrame(rows, schema)
        result = writer._generate_cod_unico(df, flights_target)
        assert "cod_unico" in result.columns
        assert result.collect()[0].cod_unico == "1"

    def test_generate_cod_unico_skips_existing(self, writer, flights_target):
        """_generate_cod_unico should skip if cod_unico already exists."""
        rows = [(1, "existing_val")]
        schema = StructType([
            StructField("flight_id", LongType(), True),
            StructField("cod_unico", StringType(), True),
        ])
        df = writer._spark.createDataFrame(rows, schema)
        result = writer._generate_cod_unico(df, flights_target)
        assert result.collect()[0].cod_unico == "existing_val"
