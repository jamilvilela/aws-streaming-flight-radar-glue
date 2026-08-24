"""
Unit tests for writer.py — Writer class for Delta Lake writing.

Early-return and wiring paths are tested with mocked DataFrames (no JVM
required). The tests that exercise real Spark SQL (partition extraction and
cod_unique generation) use the shared ``spark`` fixture, which skips cleanly
when no JVM is available and runs fully in CI/Glue.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from src.dependencies.config import CdcConfig, PartitionKey, SchemaField, SourceConfig, TargetConfig
from src.dependencies.writer import CDC_OP_COLUMN, CDC_TIMESTAMP_COLUMN, Writer


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
        rejected_location="s3://landing/dms/flightradar/flight_radar/Rejected/",
        format="delta",
        compression="snappy",
        partition_keys=[PartitionKey("event_date", "date")],
        schema={
            "flight_id": SchemaField(type="bigint", nullable=False, comment="PK"),
            "airline_code": SchemaField(type="string", nullable=True, comment="IATA"),
            "status": SchemaField(type="string", nullable=True, comment="Status"),
            "cdc_timestamp": SchemaField(type="timestamp", nullable=True, comment="CDC ts"),
        },
        primary_key=["flight_id"],
        enum_columns={"status": ["active"]},
        cod_unique_expr={"columns": ["flight_id"], "separator": "_"},
    )


@pytest.fixture
def writer():
    """Writer with a mocked SparkSession (no JVM required)."""
    return Writer(MagicMock())


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

    def test_prepare_partitions_from_source_column(self, spark, flights_source):
        """_prepare_with_partitions should derive event_date from a source_column."""
        from datetime import datetime
        flights_target = TargetConfig(
            catalog={"database": "db_raw", "table": "tbl_flights"},
            rejected_location="s3://landing/dms/flightradar/flight_radar/Rejected/",
            format="delta",
            compression="snappy",
            partition_keys=[PartitionKey("event_date", "date", source_column="scheduled_departure")],
            schema={},
            primary_key=["flight_id"],
            enum_columns={},
            cod_unique_expr={"columns": ["flight_id"], "separator": "_"},
        )
        row = (1, datetime(2026, 6, 30, 8, 30, 0))
        schema = StructType([
            StructField("flight_id", LongType(), True),
            StructField("scheduled_departure", TimestampType(), True),
        ])
        df = spark.createDataFrame([row], schema)

        result = Writer._prepare_with_partitions(df, flights_target, flights_source)
        assert "event_date" in result.columns
        assert str(result.collect()[0].event_date) == "2026-06-30"

    def test_write_empty_delta(self, writer, flights_source, flights_target):
        """Writing empty DataFrame with Delta should not fail."""
        df = MagicMock()
        df.isEmpty.return_value = True
        # Should not raise (returns early for empty df)
        writer.write(df, flights_target, flights_source)

    def test_generate_cod_unique_from_pk(self, spark, flights_target):
        """_generate_cod_unique should create cod_unique from PK columns."""
        rows = [(1, "AA")]
        schema = StructType([
            StructField("flight_id", LongType(), True),
            StructField("airline_code", StringType(), True),
        ])
        df = spark.createDataFrame(rows, schema)
        result = Writer._generate_cod_unique(df, flights_target)
        assert "cod_unique" in result.columns
        assert result.collect()[0].cod_unique == "1"

    def test_generate_cod_unique_skips_existing(self, writer, flights_target):
        """_generate_cod_unique should skip if cod_unique already exists."""
        df = MagicMock()
        df.columns = ["flight_id", "cod_unique"]
        result = writer._generate_cod_unique(df, flights_target)
        assert result is df
        df.withColumn.assert_not_called()

    def test_select_and_cast_target_schema(self, spark, flights_target):
        """Target selection drops extras and casts configured and partition columns."""
        df = spark.createDataFrame([(1, "AA", "active", "ignored")], [
            "flight_id", "airline_code", "status", "extra",
        ])

        result = Writer._select_and_cast_target_schema(df, flights_target)

        assert result.columns == [
            "flight_id", "airline_code", "status", "event_date",
        ]
        assert result.schema["flight_id"].dataType.simpleString() == "bigint"
        assert result.schema["event_date"].dataType.simpleString() == "date"

    def test_map_cdc_columns_renames(self, spark, flights_source):
        """CDC short names (Op/dms_timestamp) should be renamed to catalog names."""
        rows = [(1, "I", None)]
        schema = StructType([
            StructField("flight_id", LongType(), True),
            StructField("Op", StringType(), True),
            StructField("dms_timestamp", TimestampType(), True),
        ])
        df = spark.createDataFrame(rows, schema)
        result = Writer._map_cdc_columns(df, flights_source)
        assert CDC_OP_COLUMN in result.columns
        assert CDC_TIMESTAMP_COLUMN in result.columns
        assert "Op" not in result.columns
        assert "dms_timestamp" not in result.columns

    def test_map_cdc_columns_no_source(self, writer, flights_target):
        """Without a source config, columns should be left untouched."""
        df = MagicMock()
        df.columns = ["flight_id", "Op"]
        result = writer._map_cdc_columns(df, None)
        assert result is df

    def test_bootstrap_table_partitioned(self, writer, spark, flights_target):
        """_bootstrap_table should write delta with the configured partitions."""
        row = (1, "AA", "active", None)
        schema = StructType([
            StructField("flight_id", LongType(), True),
            StructField("airline_code", StringType(), True),
            StructField("status", StringType(), True),
            StructField("event_date", TimestampType(), True),
        ])
        df = spark.createDataFrame([row], schema)
        with patch.object(df.write, "format", return_value=df.write) as mock_format, \
             patch.object(df.write, "save") as mock_save:
            Writer._bootstrap_table(df, ["event_date"], "s3://raw/tables/tbl_flights/")
            mock_format.assert_called_once_with("delta")
            df.write.mode.assert_called_once_with("overwrite")
            df.write.partitionBy.assert_called_once_with("event_date")
            mock_save.assert_called_once_with("s3://raw/tables/tbl_flights/")

    def test_write_bootstraps_missing_delta_table(self, writer, flights_source, flights_target):
        """A non-Delta catalog table should be bootstrapped and skip the MERGE."""
        df = MagicMock()
        df.isEmpty.return_value = False
        df.columns = ["flight_id", "event_date", "cod_unique", CDC_OP_COLUMN]
        df.select.return_value = df
        df.withColumnRenamed.return_value = df
        df.withColumn.return_value = df

        with patch.object(writer, "_is_delta_table", return_value=False) as mock_is_delta, \
             patch.object(writer, "_bootstrap_table") as mock_bootstrap, \
             patch.object(writer, "_select_and_cast_target_schema", return_value=df), \
             patch("src.dependencies.writer.AwsHelper.get_table_location", return_value="s3://raw/tables/tbl_flights/"), \
             patch("src.dependencies.writer.DeltaTable.forName") as mock_for_name:
            writer.write(df, flights_target, flights_source)
            mock_is_delta.assert_called_once_with("db_raw.tbl_flights")
            mock_bootstrap.assert_called_once()
            mock_for_name.assert_not_called()

    def test_write_merges_when_delta_table_exists(self, writer, flights_source, flights_target):
        """When the catalog table is already Delta, resolve by name and MERGE."""
        df = MagicMock()
        df.isEmpty.return_value = False
        df.columns = ["flight_id", "event_date", "cod_unique", CDC_OP_COLUMN]
        df.select.return_value = df
        df.withColumnRenamed.return_value = df
        df.withColumn.return_value = df

        delta_table_mock = MagicMock()
        delta_table_mock.toDF.return_value.columns = ["flight_id", "event_date", "cod_unique", CDC_OP_COLUMN]

        with patch.object(writer, "_is_delta_table", return_value=True) as mock_is_delta, \
             patch("src.dependencies.writer.DeltaTable.forName", return_value=delta_table_mock) as mock_for_name, \
               patch.object(writer, "_select_and_cast_target_schema", return_value=df), \
             patch.object(writer, "_bootstrap_table") as mock_bootstrap:
            writer.write(df, flights_target, flights_source)
            mock_is_delta.assert_called_once_with("db_raw.tbl_flights")
            mock_bootstrap.assert_not_called()
            mock_for_name.assert_called_once_with(writer._spark, "db_raw.tbl_flights")