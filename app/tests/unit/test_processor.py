"""
Unit tests for processor.py — Processor pipeline orchestrator.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

from src.dependencies.config import CdcConfig, PartitionKey, SchemaField, SourceConfig, TargetConfig
from src.dependencies.processor import Processor, ProcessorError


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[2]") \
        .appName("test-processor") \
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
        enum_columns={"status": ["active", "landed"]},
        cod_unico_expr={"columns": ["flight_id"], "separator": "_"},
    )


@pytest.fixture
def processor(spark):
    return Processor(spark)


# ── Tests ────────────────────────────────────────────────────────────────────

class TestProcessor:
    def test_init(self, spark, processor):
        """Processor should initialise with all sub-components."""
        assert processor._reader is not None
        assert processor._data_quality is not None
        assert processor._writer is not None
        assert processor._etl_control is not None
        assert processor._quality_metrics is not None
        assert processor._execution_id == ""

    @patch("delta.tables.DeltaTable.forName")
    def test_run_success(self, mock_delta, spark, processor, flights_source, flights_target):
        """A successful pipeline run should complete without errors."""
        # Mock DeltaTable.forName to avoid requiring Delta Lake binaries
        mock_delta.return_value.alias.return_value.merge.return_value \
            .whenNotMatchedInsertAll.return_value \
            .whenMatchedUpdateAll.return_value.execute.return_value = None

        # Create minimal input data
        schema = StructType([
            StructField("flight_id", LongType(), True),
            StructField("airline_code", StringType(), True),
            StructField("status", StringType(), True),
            StructField("dms_timestamp", StringType(), True),
        ])
        df = spark.createDataFrame([(1, "AA", "active", "2026-06-30")], schema)

        # Mock reader to return our test DataFrame
        processor._reader.read = MagicMock(return_value=df)

        # Run pipeline
        processor.run(flights_source, flights_target)

        assert processor._execution_id != ""
        assert processor._records_read == 1
        assert processor._records_written == 1
        assert processor._records_rejected == 0

    @patch("delta.tables.DeltaTable.forName")
    def test_run_with_rejects(self, mock_delta, spark, processor, flights_source, flights_target):
        """Pipeline should handle rows that fail validation."""
        # Mock DeltaTable.forName to avoid requiring Delta Lake binaries
        mock_delta.return_value.alias.return_value.merge.return_value \
            .whenNotMatchedInsertAll.return_value \
            .whenMatchedUpdateAll.return_value.execute.return_value = None

        schema = StructType([
            StructField("flight_id", LongType(), True),
            StructField("airline_code", StringType(), True),
            StructField("status", StringType(), True),
            StructField("dms_timestamp", StringType(), True),
        ])
        # Invalid status
        df = spark.createDataFrame([(1, "AA", "invalid_status", "2026-06-30")], schema)

        processor._reader.read = MagicMock(return_value=df)
        processor.run(flights_source, flights_target)

        assert processor._records_read == 1
        assert processor._records_rejected >= 1
        assert processor._records_written == 0

    @patch("delta.tables.DeltaTable.forName")
    def test_run_empty_dataframe(self, mock_delta, spark, processor, flights_source, flights_target):
        """Empty input should be handled gracefully."""
        # Mock DeltaTable.forName to avoid requiring Delta Lake binaries
        mock_delta.return_value.alias.return_value.merge.return_value \
            .whenNotMatchedInsertAll.return_value \
            .whenMatchedUpdateAll.return_value.execute.return_value = None

        schema = StructType([
            StructField("flight_id", LongType(), True),
        ])
        df = spark.createDataFrame([], schema)

        processor._reader.read = MagicMock(return_value=df)

        # Should not raise
        processor.run(flights_source, flights_target)
        assert processor._records_read == 0

    def test_run_reader_error(self, processor, flights_source, flights_target):
        """Reader failure should raise ProcessorError."""
        processor._reader.read = MagicMock(side_effect=Exception("Read failed"))
        with pytest.raises(ProcessorError):
            processor.run(flights_source, flights_target)

    def test_is_streaming(self, processor):
        """_is_streaming should detect streaming DataFrames."""
        from pyspark.sql import DataFrame
        assert processor._is_streaming(MagicMock(spec=DataFrame)) is False

    def test_sub_components_created(self, processor):
        """All sub-components should be properly instantiated."""
        from src.dependencies.etl_control import EtlControl
        from src.dependencies.quality_metrics import QualityMetrics
        assert isinstance(processor._etl_control, EtlControl)
        assert isinstance(processor._quality_metrics, QualityMetrics)
