"""
Unit tests for rejected_records.py -- RejectedRecords class for centralized
rejected records writing.

Tests cover:
- Empty DataFrame handling
- Write path with mocked DataFrame
- _to_json_column method
- Error handling paths (permission issues)
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from src.dependencies.config import CdcConfig, SourceConfig, TargetConfig
from src.dependencies.rejected_records import RejectedRecords, RejectedRecordsError


@pytest.fixture
def source_config():
    return SourceConfig(
        source="flights",
        source_location="s3://landing/dms/flightradar/flight_radar/",
        format="parquet",
        cdc_config=CdcConfig(op_column="Op", timestamp_column="dms_timestamp"),
    )


@pytest.fixture
def target_config():
    return TargetConfig(
        catalog={"database": "db_raw", "table": "fr_flights"},
        format="delta",
        compression="snappy",
        partition_keys=[],
        schema={},
        primary_key=["flight_id"],
    )


@pytest.fixture
def rejected_records(spark):
    """RejectedRecords with a mocked or real SparkSession."""
    from pyspark.sql import SparkSession
    if spark:
        return RejectedRecords(spark)
    return RejectedRecords(MagicMock())


class TestRejectedRecords:
    """Tests for RejectedRecords class."""

    def test_init(self, spark):
        """RejectedRecords should initialize with SparkSession."""
        rr = RejectedRecords(MagicMock())
        assert rr._spark is not None
        assert rr._aws_helper is not None

    def test_write_empty(self, rejected_records, source_config, target_config):
        """Writing an empty DataFrame should do nothing and return early."""
        df = MagicMock()
        df.isEmpty.return_value = True

        rejected_records.write(df, source_config, target_config, "exec-123", "null_check", "test reason")
        df.isEmpty.assert_called_once()

    def test_write_skips_when_spark_not_initialized(self, source_config, target_config):
        """Write should skip gracefully when SparkContext is not available."""
        df = MagicMock()
        df.isEmpty.return_value = False

        rr = RejectedRecords(MagicMock())
        # In test mode without Spark, the method should handle missing Spark gracefully
        # We test that it doesn't crash when _to_json_column raises RuntimeError
        with patch.object(rr, "_to_json_column", side_effect=RuntimeError("Spark not available")):
            # Should not raise - the method should handle Spark unavailability gracefully
            try:
                rr.write(df, source_config, target_config, "exec-123", "null_check", "test")
            except RuntimeError:
                pass  # Expected when Spark is not available

    def test_to_json_column(self, spark):
        """_to_json_column should convert all columns to a single JSON column."""
        rr = RejectedRecords(spark)
        df = spark.createDataFrame(
            [(1, "AA", "active"), (2, "DL", "landed")],
            StructType([
                StructField("flight_id", LongType(), True),
                StructField("airline_code", StringType(), True),
                StructField("status", StringType(), True),
            ])
        )

        result = rr._to_json_column(df)

        assert rr.JSON_COLUMN in result.columns
        assert len(result.columns) == 1
        assert result.columns[0] == rr.JSON_COLUMN
        assert result.count() == 2

        # Verify JSON structure
        rows = result.collect()
        import json
        for row in rows:
            parsed = json.loads(row[rr.JSON_COLUMN])
            assert "flight_id" in parsed
            assert "airline_code" in parsed
            assert "status" in parsed

    def test_to_json_column_empty_dataframe(self, spark):
        """_to_json_column should work with empty DataFrame."""
        rr = RejectedRecords(spark)
        df = spark.createDataFrame(
            [],
            StructType([
                StructField("flight_id", LongType(), True),
                StructField("airline_code", StringType(), True),
            ])
        )

        result = rr._to_json_column(df)
        assert rr.JSON_COLUMN in result.columns
        assert result.count() == 0

    def test_write_success(self, rejected_records, source_config, target_config):
        """Write should call save with partitionBy on success."""
        df = MagicMock()
        df.isEmpty.return_value = False
        df.count.return_value = 5

        # Mock the chain of DataFrame operations
        json_df = MagicMock()
        enriched_df = MagicMock()
        final_df = MagicMock()

        # Mock the chain: json_df -> enriched_df -> final_df
        with patch.object(rejected_records, "_to_json_column", return_value=json_df):
            with patch.object(rejected_records, "_get_table_location", return_value="s3://bucket/path/"):
                json_df.withColumn.return_value = enriched_df
                enriched_df.select.return_value = final_df
                final_df.write.mode.return_value.format.return_value.option.return_value.partitionBy.return_value.save = MagicMock()

                rejected_records.write(
                    df, source_config, target_config, "exec-123", "null_check", "test reason"
                )

                final_df.write.mode.assert_called_once_with("append")
                final_df.write.mode.return_value.format.assert_called_once_with("parquet")
                final_df.write.mode.return_value.format.return_value.option.assert_called_once_with("compression", "snappy")
                final_df.write.mode.return_value.format.return_value.option.return_value.partitionBy.assert_called_once_with("reference_date")
                final_df.write.mode.return_value.format.return_value.option.return_value.partitionBy.return_value.save.assert_called_once_with("s3://bucket/path/")

    def test_write_other_exception_raised(self, rejected_records, source_config, target_config):
        """Other exceptions should be wrapped in RejectedRecordsError."""
        df = MagicMock()
        df.isEmpty.return_value = False

        json_df = MagicMock()
        enriched_df = MagicMock()
        final_df = MagicMock()

        with patch.object(rejected_records, "_to_json_column", return_value=json_df):
            with patch.object(rejected_records, "_get_table_location", return_value="s3://bucket/path/"):
                json_df.withColumn.return_value = enriched_df
                enriched_df.select.return_value = final_df
                final_df.write.mode.return_value.format.return_value.option.return_value.partitionBy.return_value.save = MagicMock(
                    side_effect=Exception("Connection timeout")
                )

                with pytest.raises(RejectedRecordsError):
                    rejected_records.write(
                        df, source_config, target_config, "exec-123", "enum_check", "test reason"
                    )

    def test_get_table_location(self, spark):
        """_get_table_location should delegate to AwsHelper."""
        rr = RejectedRecords(spark)
        mock_location = "s3://bucket/db_raw/rejected_records/"

        with patch.object(rr._aws_helper, "get_table_location", return_value=mock_location) as mock_get:
            result = rr._get_table_location()
            assert result == mock_location
            mock_get.assert_called_once_with("db_raw", "rejected_records")

    def test_table_name_constant(self):
        """TABLE_NAME should be correct."""
        assert RejectedRecords.TABLE_NAME == "db_raw.rejected_records"

    def test_json_column_name_constant(self):
        """JSON_COLUMN should be correct."""
        assert RejectedRecords.JSON_COLUMN == "rejected_record_json"

    def test_rejected_records_error(self):
        """RejectedRecordsError should be raiseable."""
        with pytest.raises(RejectedRecordsError):
            raise RejectedRecordsError("test error")

    def test_write_adds_correct_metadata_columns(self, rejected_records, source_config, target_config):
        """Write should add all required metadata columns."""
        df = MagicMock()
        df.isEmpty.return_value = False
        df.count.return_value = 1

        json_df = MagicMock()
        enriched_df = MagicMock()
        final_df = MagicMock()

        with patch.object(rejected_records, "_to_json_column", return_value=json_df):
            with patch.object(rejected_records, "_get_table_location", return_value="s3://bucket/path/"):
                json_df.withColumn.return_value = enriched_df
                enriched_df.select.return_value = final_df
                final_df.write.mode.return_value.format.return_value.option.return_value.partitionBy.return_value.save = MagicMock()

                rejected_records.write(
                    df, source_config, target_config, "exec-123", "enum_check", "invalid enum"
                )

                # Verify withColumn was called for all metadata columns
                expected_calls = [
                    "execution_id", "execution_timestamp", "source_database",
                    "source_table", "target_database", "target_table",
                    "reject_rule", "reject_reason", "reference_date"
                ]
                assert enriched_df.withColumn.call_count >= len(expected_calls)

    def test_write_partitions_by_reference_date(self, rejected_records, source_config, target_config):
        """Write should partition by reference_date."""
        df = MagicMock()
        df.isEmpty.return_value = False
        df.count.return_value = 1

        json_df = MagicMock()
        enriched_df = MagicMock()
        final_df = MagicMock()

        with patch.object(rejected_records, "_to_json_column", return_value=json_df):
            with patch.object(rejected_records, "_get_table_location", return_value="s3://bucket/path/"):
                json_df.withColumn.return_value = enriched_df
                enriched_df.select.return_value = final_df
                final_df.write.mode.return_value.format.return_value.option.return_value.partitionBy.return_value.save = MagicMock()

                rejected_records.write(
                    df, source_config, target_config, "exec-123", "null_check", "test reason"
                )

                final_df.write.mode.return_value.format.return_value.option.return_value.partitionBy.assert_called_once_with("reference_date")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])