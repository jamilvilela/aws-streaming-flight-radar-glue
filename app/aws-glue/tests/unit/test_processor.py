"""
Unit tests for processor.py — Processor pipeline orchestrator.

The orchestration logic is tested with fully mocked collaborators so the
tests run without a JVM. Real Spark/SQL semantics are covered by the
Spark-based tests in test_data_quality.py and test_writer.py (which are
skipped automatically when no JVM is available).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.dependencies.config import CdcConfig, PartitionKey, SchemaField, SourceConfig, TargetConfig
from src.dependencies.processor import Processor, ProcessorError


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
def processor():
    """Processor whose collaborators are all MagicMock objects (no JVM)."""
    return Processor(MagicMock())


@pytest.fixture
def run_mocks(processor):
    """Wire Processor.run's collaborators with configurable mocks."""
    raw_df = MagicMock()
    raw_df.isStreaming = False
    valid_df = MagicMock()
    rejects_df = MagicMock()

    processor._reader.read = MagicMock(return_value=raw_df)
    processor._data_quality.validate = MagicMock(return_value=(valid_df, rejects_df))
    processor._writer.write = MagicMock()
    processor._writer.write_rejects = MagicMock()
    processor._etl_control.register = MagicMock()
    processor._quality_metrics.save = MagicMock()

    return {"raw": raw_df, "valid": valid_df, "rejects": rejects_df}


class TestProcessor:
    def test_init(self, processor):
        """Processor should initialise with all sub-components."""
        assert processor._reader is not None
        assert processor._data_quality is not None
        assert processor._writer is not None
        assert processor._etl_control is not None
        assert processor._quality_metrics is not None
        assert processor._execution_id == ""

    def test_run_success(self, processor, flights_source, flights_target, run_mocks):
        """A successful pipeline run should complete without errors."""
        run_mocks["raw"].count.return_value = 1
        run_mocks["valid"].isEmpty.return_value = False
        run_mocks["valid"].count.return_value = 1
        run_mocks["rejects"].isEmpty.return_value = True

        processor.run(flights_source, flights_target)

        assert processor._execution_id != ""
        assert processor._records_read == 1
        assert processor._records_written == 1
        assert processor._records_rejected == 0

    def test_run_with_rejects(self, processor, flights_source, flights_target, run_mocks):
        """Pipeline should handle rows that fail validation."""
        run_mocks["raw"].count.return_value = 1
        run_mocks["valid"].isEmpty.return_value = True
        run_mocks["rejects"].isEmpty.return_value = False
        run_mocks["rejects"].count.return_value = 1

        processor.run(flights_source, flights_target)

        assert processor._records_read == 1
        assert processor._records_rejected == 1
        assert processor._records_written == 0

    def test_run_empty_dataframe(self, processor, flights_source, flights_target, run_mocks):
        """Empty input should be handled gracefully."""
        run_mocks["raw"].count.return_value = 0
        run_mocks["valid"].isEmpty.return_value = True
        run_mocks["rejects"].isEmpty.return_value = True

        processor.run(flights_source, flights_target)

        assert processor._records_read == 0
        assert processor._records_written == 0
        assert processor._records_rejected == 0

    def test_run_reader_error(self, processor, flights_source, flights_target):
        """Reader failure should raise ProcessorError."""
        processor._reader.read = MagicMock(side_effect=Exception("Read failed"))
        with pytest.raises(ProcessorError):
            processor.run(flights_source, flights_target)

    def test_is_streaming(self, processor):
        """_is_streaming should detect streaming DataFrames."""
        stream_df = MagicMock()
        stream_df.isStreaming = True
        batch_df = MagicMock()
        batch_df.isStreaming = False
        assert processor._is_streaming(stream_df) is True
        assert processor._is_streaming(batch_df) is False

    def test_sub_components_created(self, processor):
        """All sub-components should be properly instantiated."""
        from src.dependencies.etl_control import EtlControl
        from src.dependencies.quality_metrics import QualityMetrics
        assert isinstance(processor._etl_control, EtlControl)
        assert isinstance(processor._quality_metrics, QualityMetrics)