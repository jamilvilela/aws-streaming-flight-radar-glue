"""
Unit tests for reader.py — Reader class with streaming and batch modes.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.dependencies.config import CdcConfig, SourceConfig
from src.dependencies.reader import Reader, ReaderError


@pytest.fixture
def spark_mock():
    return MagicMock()


@pytest.fixture
def reader(spark_mock):
    return Reader(spark_mock)


@pytest.fixture
def flights_source():
    return SourceConfig(
        source="flights",
        source_location="s3://landing/dms/flightradar/flight_radar/",
        format="parquet",
        cdc_config=CdcConfig(op_column="Op", timestamp_column="dms_timestamp"),
        checkpoint_location="s3://workspace/checkpoints/flights/",
    )


class TestReader:
    def _mock_stream_builder(self, spark_mock, df_mock):
        """Configure the readStream mock chain so option() returns the builder."""
        builder = spark_mock.readStream.format.return_value
        builder.option.return_value = builder
        builder.load.return_value = df_mock
        return builder

    def test_read_streaming(self, reader, spark_mock, flights_source):
        """Reader should always use streaming mode."""
        df_mock = MagicMock()
        builder = self._mock_stream_builder(spark_mock, df_mock)

        result = reader.read(flights_source)

        spark_mock.readStream.format.assert_called_once_with("parquet")
        args, _ = builder.load.call_args
        assert args[0] == flights_source.source_location
        assert result == df_mock

    def test_read_streaming_options(self, reader, spark_mock, flights_source):
        """Streaming options should include maxFilesPerTrigger, cleanSource, etc."""
        df_mock = MagicMock()
        self._mock_stream_builder(spark_mock, df_mock)

        reader.read(flights_source)

        opts = spark_mock.readStream.format.return_value
        opts.option.assert_any_call("maxFilesPerTrigger", 1)
        opts.option.assert_any_call("cleanSource", "archive")
        opts.option.assert_any_call("includeExistingFiles", "false")

    def test_read_streaming_error(self, reader, spark_mock, flights_source):
        """Reader should raise ReaderError on streaming failure."""
        spark_mock.readStream.format.side_effect = Exception("Stream error")

        with pytest.raises(ReaderError):
            reader.read(flights_source)

    def test_read_always_streaming(self, reader, spark_mock):
        """Reader has no batch mode — read() always calls _read_streaming."""
        source = SourceConfig(
            source="test",
            source_location="s3://bucket/path/",
        )
        df_mock = MagicMock()
        self._mock_stream_builder(spark_mock, df_mock)

        result = reader.read(source)
        spark_mock.readStream.format.assert_called_once()
        assert result == df_mock

    def test_read_without_checkpoint(self, reader, spark_mock):
        """Reader still uses streaming even without checkpoint_location."""
        source = SourceConfig(
            source="test",
            source_location="s3://bucket/path/",
            format="parquet",
        )
        df_mock = MagicMock()
        self._mock_stream_builder(spark_mock, df_mock)

        result = reader.read(source)
        spark_mock.readStream.format.assert_called_once()
        assert result == df_mock
