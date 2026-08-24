"""
Unit tests for data_quality.py — DataQuality validation pipeline.

Uses a real local SparkSession for DataFrame operations.
"""

from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.dependencies.config import CdcConfig, PartitionKey, SchemaField, SourceConfig, TargetConfig
from src.dependencies.data_quality import DataQuality


@pytest.fixture
def flights_target():
    return TargetConfig(
        catalog={"database": "db_raw", "table": "fr_flights"},
        format="parquet",
        compression="snappy",
        partition_keys=[PartitionKey("event_date", "date")],
        schema={
            "flight_id": SchemaField(type="bigint", nullable=False, comment="PK"),
            "airline_code": SchemaField(type="string", nullable=True, comment="IATA"),
            "status": SchemaField(type="string", nullable=True, comment="Status"),
            "cdc_timestamp": SchemaField(type="timestamp", nullable=True, comment="CDC ts"),
        },
        primary_key=["flight_id"],
        enum_columns={"status": ["scheduled", "active", "landed", "cancelled", "diverted", "unknown"],
                      "cdc_operation": ["I", "U", "D"]},
    )


@pytest.fixture
def flights_source():
    return SourceConfig(
        source="flights",
        source_location="s3://landing/dms/flightradar/flight_radar/",
        format="parquet",
        cdc_config=CdcConfig(op_column="Op", timestamp_column="dms_timestamp"),
    )


@pytest.fixture
def dq(spark):
    return DataQuality(spark)


class TestDataQuality:
    def test_empty_dataframe(self, spark, dq, flights_target, flights_source):
        """An empty DataFrame should pass through with no rejects."""
        schema = StructType([StructField("flight_id", LongType(), True)])
        df = spark.createDataFrame([], schema)
        valid, rejects = dq.validate(df, flights_target, flights_source)
        assert valid.collect() == []
        assert rejects.collect() == []

    def test_valid_data(self, spark, dq, flights_target, flights_source):
        """Valid rows should pass all checks."""
        rows = [
            (1, "AA", "active", datetime(2026, 6, 30, 10, 0, 0)),
            (2, "DL", "landed", datetime(2026, 6, 30, 11, 0, 0)),
        ]
        schema = StructType([
            StructField("flight_id", LongType(), True),
            StructField("airline_code", StringType(), True),
            StructField("status", StringType(), True),
            StructField("dms_timestamp", TimestampType(), True),
        ])
        df = spark.createDataFrame(rows, schema)
        valid, rejects = dq.validate(df, flights_target, flights_source)
        assert valid.count() == 2
        assert rejects.isEmpty()

    def test_type_cast_rejects(self, spark, dq, flights_target, flights_source):
        """Rows with null PK after cast should be rejected."""
        rows = [
            (None, "AA", "active", datetime(2026, 6, 30, 10, 0, 0)),
            (2, "DL", "landed", datetime(2026, 6, 30, 11, 0, 0)),
        ]
        schema = StructType([
            StructField("flight_id", LongType(), True),
            StructField("airline_code", StringType(), True),
            StructField("status", StringType(), True),
            StructField("dms_timestamp", TimestampType(), True),
        ])
        df = spark.createDataFrame(rows, schema)
        valid, rejects = dq.validate(df, flights_target, flights_source)
        assert valid.count() == 1
        assert rejects.count() >= 1

    def test_null_check(self, spark, dq, flights_target, flights_source):
        """Rows with null in non-nullable PK should be rejected."""
        rows = [
            (1, None, "active", datetime(2026, 6, 30, 10, 0, 0)),
        ]
        schema = StructType([
            StructField("flight_id", LongType(), True),
            StructField("airline_code", StringType(), True),
            StructField("status", StringType(), True),
            StructField("dms_timestamp", TimestampType(), True),
        ])
        df = spark.createDataFrame(rows, schema)
        # Force flight_id to null
        from pyspark.sql import functions as F
        df = df.withColumn("flight_id", F.lit(None).cast(LongType()))
        valid, rejects = dq.validate(df, flights_target, flights_source)
        assert valid.isEmpty()
        assert rejects.isEmpty() is False  # null_check catches it

    def test_enum_validation(self, spark, dq, flights_target, flights_source):
        """Rows with invalid enum values should be rejected."""
        rows = [
            (1, "AA", "invalid_status", datetime(2026, 6, 30, 10, 0, 0)),
            (2, "DL", "active", datetime(2026, 6, 30, 11, 0, 0)),
        ]
        schema = StructType([
            StructField("flight_id", LongType(), True),
            StructField("airline_code", StringType(), True),
            StructField("status", StringType(), True),
            StructField("dms_timestamp", TimestampType(), True),
        ])
        df = spark.createDataFrame(rows, schema)
        valid, rejects = dq.validate(df, flights_target, flights_source)
        assert valid.count() == 1
        assert rejects.count() >= 1
        reject_rules = [r._reject_rule for r in rejects.collect()]
        assert "enum_check" in reject_rules

    def test_reject_enrichment(self, spark, dq, flights_target, flights_source):
        """Rejected rows should have _reject_* metadata columns."""
        rows = [(1, "AA", "bad_status", datetime(2026, 6, 30, 10, 0, 0))]
        schema = StructType([
            StructField("flight_id", LongType(), True),
            StructField("airline_code", StringType(), True),
            StructField("status", StringType(), True),
            StructField("dms_timestamp", TimestampType(), True),
        ])
        df = spark.createDataFrame(rows, schema)
        _, rejects = dq.validate(df, flights_target, flights_source)
        if not rejects.isEmpty():
            row = rejects.collect()[0]
            assert hasattr(row, "_reject_table")
            assert row._reject_table == "fr_flights"
            assert hasattr(row, "_reject_rule")

    def test_resolve_type(self):
        """_resolve_type should map schema type strings to Spark DataTypes."""
        from pyspark.sql.types import LongType, StringType, TimestampType
        assert isinstance(DataQuality._resolve_type("bigint"), LongType)
        assert isinstance(DataQuality._resolve_type("string"), StringType)
        assert isinstance(DataQuality._resolve_type("timestamp"), TimestampType)
        assert isinstance(DataQuality._resolve_type("unknown"), StringType)  # default fallback

    def test_reject_schema(self):
        """_reject_schema should return the expected StructType."""
        schema = DataQuality._reject_schema()
        field_names = [f.name for f in schema.fields]
        assert "_reject_table" in field_names
        assert "_reject_rule" in field_names
        assert "_reject_timestamp" in field_names

    def test_data_without_pk(self, spark, dq):
        """Config without PK should pass through without duplicate removal."""
        target = TargetConfig(
            catalog={"database": "db_raw", "table": "fr_test"},
            format="parquet",
            compression="snappy",
            partition_keys=[],
            schema={"col1": SchemaField(type="string", nullable=True, comment="")},
            primary_key=[],
            enum_columns={},
        )
        source = SourceConfig(
            source="test",
            source_location="s3://bucket/",
            target=target,
        )
        schema = StructType([StructField("col1", StringType(), True)])
        df = spark.createDataFrame([("hello",)], schema)
        valid, rejects = dq.validate(df, target, source)
        assert valid.count() == 1
        assert rejects.isEmpty()
