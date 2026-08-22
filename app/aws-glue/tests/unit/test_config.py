"""
Unit tests for config.py — Config, SourceConfig, TargetConfig dataclasses.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from src.dependencies.config import (
    CdcConfig,
    Config,
    PartitionKey,
    SchemaField,
    SourceConfig,
    TargetConfig,
)


class TestSchemaField:
    def test_default_nullable(self):
        sf = SchemaField(type="string", comment="test")
        assert sf.nullable is True

    def test_explicit_nullable(self):
        sf = SchemaField(type="bigint", nullable=False, comment="PK")
        assert sf.nullable is False

    def test_to_dict(self):
        sf = SchemaField(type="string", comment="col", nullable=True)
        d = sf.to_dict()
        assert d["type"] == "string"
        assert d["nullable"] is True
        assert d["comment"] == "col"


class TestPartitionKey:
    def test_defaults(self):
        pk = PartitionKey(name="year", type="string")
        assert pk.name == "year"
        assert pk.type == "string"
        assert pk.source_column is None

    def test_to_dict(self):
        pk = PartitionKey(name="month", type="string")
        d = pk.to_dict()
        assert d == {"name": "month", "type": "string"}

    def test_to_dict_with_source_column(self):
        pk = PartitionKey(name="event_date", type="date", source_column="scheduled_departure")
        d = pk.to_dict()
        assert d == {"name": "event_date", "type": "date", "source_column": "scheduled_departure"}


class TestCdcConfig:
    def test_defaults(self):
        cfg = CdcConfig(op_column="Op", timestamp_column="dms_timestamp")
        assert cfg.op_column == "Op"
        assert cfg.timestamp_column == "dms_timestamp"
        assert cfg.delete_strategy == "soft_delete"

    def test_to_dict(self):
        cfg = CdcConfig(op_column="Op", timestamp_column="ts", delete_strategy="hard")
        d = cfg.to_dict()
        assert d["op_column"] == "Op"
        assert d["delete_strategy"] == "hard"


class TestSourceConfig:
    def test_defaults(self):
        cfg = SourceConfig(source="flights")
        assert cfg.source == "flights"
        assert cfg.format == "parquet"
        assert cfg.source_location == ""
        assert cfg.filter == ""
        assert cfg.cdc_config is None
        assert cfg.checkpoint_location == ""

    def test_to_dict_roundtrip(self):
        cfg = SourceConfig(
            source="flights",
            source_location="s3://bucket/path",
            format="parquet",
            filter="status = 'active'",
            cdc_config=CdcConfig(op_column="Op", timestamp_column="dms_timestamp"),
            checkpoint_location="s3://bucket/checkpoint/",
        )
        d = cfg.to_dict()
        assert d["source"] == "flights"
        assert d["source_location"] == "s3://bucket/path"
        assert d["filter"] == "status = 'active'"
        assert d["cdc_config"]["op_column"] == "Op"
        assert d["checkpoint_location"] == "s3://bucket/checkpoint/"

    def test_to_dict_no_cdc(self):
        cfg = SourceConfig(source="flights")
        d = cfg.to_dict()
        assert d["cdc_config"] is None


class TestTargetConfig:
    SAMPLE_SCHEMA = {
        "icao24": SchemaField(type="string", comment="ICAO"),
        "latitude": SchemaField(type="double", nullable=False, comment="Lat"),
    }

    def test_defaults(self):
        cfg = TargetConfig()
        assert cfg.format == "delta"
        assert cfg.compression == "snappy"
        assert cfg.catalog == {}

    def test_database_table_properties(self):
        cfg = TargetConfig(catalog={"database": "db_raw", "table": "tbl_flights"})
        assert cfg.database == "db_raw"
        assert cfg.table == "tbl_flights"

    def test_to_dict_roundtrip(self):
        cfg = TargetConfig(
            catalog={"database": "db_raw", "table": "tbl_test"},
            rejected_location="s3://raw/tables/test/Rejected/",
            format="parquet",
            compression="snappy",
            partition_keys=[PartitionKey("event_date", "date")],
            schema=self.SAMPLE_SCHEMA,
            primary_key=["icao24", "event_time"],
            enum_columns={"status": ["active", "landed"]},
        cod_unique_expr={"columns": ["icao24", "event_time"], "separator": "_"},
        )
        d = cfg.to_dict()
        assert d["catalog"]["database"] == "db_raw"
        assert d["primary_key"] == ["icao24", "event_time"]
        assert d["enum_columns"]["status"] == ["active", "landed"]
        assert d["cod_unique_expr"] == {"columns": ["icao24", "event_time"], "separator": "_"}
        assert len(d["partition_keys"]) == 1
        assert d["partition_keys"][0]["name"] == "event_date"


class TestConfig:
    # Single source dict with embedded target (new config.json format)
    SOURCE_WITH_TARGET = {
        "source": "flights",
        "order": 1,
        "source_location": "s3://landing/dms/flightradar/flight_radar/",
        "format": "parquet",
        "cdc_config": {
            "op_column": "Op",
            "timestamp_column": "dms_timestamp",
        },
        "checkpoint_location": "s3://workspace/checkpoints/flights/",
        "target": {
            "catalog": {"database": "db_raw", "table": "tbl_flights"},
            "rejected_location": "s3://landing/dms/flightradar/flight_radar/Rejected/",
            "format": "parquet",
            "compression": "snappy",
            "partition_keys": [{"name": "event_date", "type": "date"}],
            "schema": {
                "icao24": {"type": "string", "nullable": True, "comment": "ICAO24"},
                "latitude": {"type": "double", "nullable": True, "comment": "Latitude"},
            },
            "primary_key": ["icao24", "event_time"],
            "cod_unique_expr": {"columns": ["icao24", "event_time"], "separator": "_"},
            "enum_columns": {},
        },
    }

    def test_from_dicts(self):
        config = Config.from_dicts(self.SOURCE_WITH_TARGET)
        assert config.source.source == "flights"
        assert config.target.database == "db_raw"
        assert config.target.table == "tbl_flights"

    def test_from_dicts_list_source(self):
        """Source as a list should be parsed correctly."""
        config = Config.from_dicts([self.SOURCE_WITH_TARGET])
        assert config.source.source == "flights"

    def test_from_dicts_dict_with_sources_key(self):
        """Legacy format with 'sources' key should still work."""
        legacy = {"sources": [self.SOURCE_WITH_TARGET]}
        config = Config.from_dicts(legacy)
        assert config.source.source == "flights"

    def test_from_files(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump([self.SOURCE_WITH_TARGET], f)
            path = f.name
        try:
            config = Config.from_file(path)
            assert config.source.source == "flights"
            assert config.target.table == "tbl_flights"
        finally:
            Path(path).unlink()

    def test_from_invalid_json(self):
        with pytest.raises(Exception):
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
                f.write("{invalid}")
                path = f.name
            try:
                Config.from_file(path)
            finally:
                Path(path).unlink()

    def test_parse_source_from_list(self):
        result = Config._parse_sources([self.SOURCE_WITH_TARGET])
        assert isinstance(result, list)
        assert result[0].source == "flights"

    def test_parse_source_from_empty_list(self):
        result = Config._parse_sources([])
        assert isinstance(result, list)
        assert len(result) == 0

    def test_parse_source_from_dict_with_sources(self):
        result = Config._parse_sources({"sources": [self.SOURCE_WITH_TARGET]})
        assert result[0].source == "flights"


class TestParseS3Path:
    def test_valid_s3_path(self):
        bucket, key = Config._parse_s3_path("s3://my-bucket/path/to/file.json")
        assert bucket == "my-bucket"
        assert key == "path/to/file.json"

    def test_s3_path_root(self):
        bucket, key = Config._parse_s3_path("s3://my-bucket/")
        assert bucket == "my-bucket"
        assert key == ""

    def test_invalid_path(self):
        bucket, key = Config._parse_s3_path("/local/path")
        assert bucket is None
        assert key is None
