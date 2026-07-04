"""
Unit tests for writer.py — Writer class for Parquet+Snappy writing.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from src.config import CdcConfig, PartitionKey, SchemaField, SourceConfig, TargetConfig
from src.writer import Writer, WriterError


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
        catalog={"database": "db_raw", "table": "tbl_opensky_flights"},
        location="s3://raw/tables/opensky/flights/",
        rejected_location="s3://landing/dms/flightradar/flight_radar/Rejected/",
        format="parquet",
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

    def test_compact_empty(self, writer, flights_target, flights_source):
        """Compaction on empty/non-existent location should not fail."""
        # Mock the read to return empty
        with patch.object(writer._spark.read, "format") as mock_read:
            mock_reader = MagicMock()
            mock_read.return_value.load.return_value = writer._spark.createDataFrame([], StructType([]))
            writer.compact(flights_target)

    def test_write_accepts_source_and_target(self, writer, flights_source, flights_target):
        """Writer.write accepts both source and target configs."""
        schema = StructType([StructField("dummy", StringType(), True)])
        df = writer._spark.createDataFrame([], schema)
        # Should not raise
        writer.write(df, flights_target, flights_source)
