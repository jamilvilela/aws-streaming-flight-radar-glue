"""
Config module — Reads and parses JSON configuration files.

Uses Python dataclasses to model source (origin) and target (table)
configurations with full type hints, loaded from separate JSON files.
Supports loading from local file paths or S3 URIs.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────
# Dataclasses
# ──────────────────────────────────────────────────────────────────────

@dataclass
class SchemaField:
    """Represents a single field in the source schema."""
    type: str
    nullable: bool = True
    comment: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {"type": self.type, "nullable": self.nullable, "comment": self.comment}


@dataclass
class PartitionKey:
    """Represents a partition key definition."""
    name: str
    type: str = "string"

    def to_dict(self) -> Dict[str, str]:
        return {"name": self.name, "type": self.type}


@dataclass
class CdcConfig:
    """CDC-specific configuration."""
    op_column: str = "Op"
    timestamp_column: str = "dms_timestamp"
    delete_strategy: str = "soft_delete"

    def to_dict(self) -> Dict[str, str]:
        return {
            "op_column": self.op_column,
            "timestamp_column": self.timestamp_column,
            "delete_strategy": self.delete_strategy,
        }


# ──────────────────────────────────────────────────────────────────────
# Source Config (origins.json)
# ──────────────────────────────────────────────────────────────────────

@dataclass
class SourceConfig:
    """
    Configuration for a single data source (DMS CDC origin).

    Contains only the connection/location information needed to
    locate and read the raw Parquet data arriving via DMS in the
    landing bucket. No schema, partition, or quality information
    — those belong in TargetConfig.
    """
    source: str = ""
    source_location: str = ""
    format: str = "parquet"
    cdc_config: Optional[CdcConfig] = None
    checkpoint_location: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "source_location": self.source_location,
            "format": self.format,
            "cdc_config": self.cdc_config.to_dict() if self.cdc_config else None,
            "checkpoint_location": self.checkpoint_location,
        }


# ──────────────────────────────────────────────────────────────────────
# Target Config (target.json)
# ──────────────────────────────────────────────────────────────────────

@dataclass
class TargetConfig:
    """
    Configuration for the target Data Lake table.

    Describes where and how to write the processed data, the
    expected schema (matching the Glue Catalog table definition),
    partition keys, primary key for dedup, and enum columns for
    validation.
    """
    catalog: Dict[str, str] = field(default_factory=dict)
    location: str = ""
    rejected_location: str = ""
    format: str = "parquet"
    compression: str = "snappy"
    partition_keys: List[PartitionKey] = field(default_factory=list)
    schema: Dict[str, SchemaField] = field(default_factory=dict)
    primary_key: List[str] = field(default_factory=list)
    enum_columns: Dict[str, List[str]] = field(default_factory=dict)

    @property
    def database(self) -> str:
        return self.catalog.get("database", "")

    @property
    def table(self) -> str:
        return self.catalog.get("table", "")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "catalog": dict(self.catalog),
            "location": self.location,
            "rejected_location": self.rejected_location,
            "format": self.format,
            "compression": self.compression,
            "partition_keys": [pk.to_dict() for pk in self.partition_keys],
            "schema": {k: v.to_dict() for k, v in self.schema.items()},
            "primary_key": list(self.primary_key),
            "enum_columns": dict(self.enum_columns),
        }


# ──────────────────────────────────────────────────────────────────────
# Top-Level Config
# ──────────────────────────────────────────────────────────────────────

@dataclass
class Config:
    """
    Top-level configuration holding one source config and one target config.

    Loaded from two separate JSON files: origins.json (source/DMS origin)
    and target.json (destination table).
    """
    source: SourceConfig = field(default_factory=SourceConfig)
    target: TargetConfig = field(default_factory=TargetConfig)

    # ── Factory methods ──────────────────────────────────────────────

    @classmethod
    def from_dicts(cls, source_data: Any, target_data: dict) -> "Config":
        """Build Config from raw dicts for source and target."""
        source_cfg = cls._parse_source(source_data)
        target_cfg = cls._parse_target(target_data)
        return cls(source=source_cfg, target=target_cfg)

    @classmethod
    def from_files(cls, source_path: str, target_path: str) -> "Config":
        """Load configuration from two local JSON files."""
        logger.info("Loading config from files: %s / %s", source_path, target_path)
        with open(source_path, "rt", encoding="utf-8") as f:
            source_data = json.load(f)
        with open(target_path, "rt", encoding="utf-8") as f:
            target_data = json.load(f)
        return cls.from_dicts(source_data, target_data)

    @classmethod
    def from_s3(cls, source_s3_path: str, target_s3_path: str) -> "Config":
        """
        Load configuration from two S3 URIs using boto3.

        Each file is fetched via get_object and parsed as JSON.
        """
        logger.info("Loading config from S3: %s / %s", source_s3_path, target_s3_path)
        import boto3
        s3 = boto3.client("s3")

        def _read_json(s3_path: str):
            bucket, key = cls._parse_s3_path(s3_path)
            obj = s3.get_object(Bucket=bucket, Key=key)
            return json.loads(obj["Body"].read().decode("utf-8"))

        source_data = _read_json(source_s3_path)
        target_data = _read_json(target_s3_path)
        return cls.from_dicts(source_data, target_data)

    # ── Public methods ───────────────────────────────────────────────

    @property
    def source_name(self) -> str:
        return self.source.source

    # ── Internal helpers ─────────────────────────────────────────────

    @staticmethod
    def _parse_source(item: Any) -> SourceConfig:
        """
        Parse source config — accepts either a dict or a list.

        - If a list, extracts the first element.
        - If a dict with 'sources' key, extracts the first value.
        """
        if isinstance(item, list):
            item = item[0] if item else {}
        elif isinstance(item, dict) and "sources" in item:
            sources_list = item["sources"]
            if isinstance(sources_list, dict):
                first_key = next(iter(sources_list))
                item = sources_list[first_key]
            elif isinstance(sources_list, list):
                item = sources_list[0] if sources_list else {}

        raw_cdc = item.get("cdc_config")
        cdc_config: Optional[CdcConfig] = None
        if raw_cdc:
            cdc_config = CdcConfig(
                op_column=raw_cdc.get("op_column", "Op"),
                timestamp_column=raw_cdc.get("timestamp_column", "dms_timestamp"),
                delete_strategy=raw_cdc.get("delete_strategy", "soft_delete"),
            )

        return SourceConfig(
            source=item.get("source", ""),
            source_location=item.get("source_location", ""),
            format=item.get("format", "parquet"),
            cdc_config=cdc_config,
            checkpoint_location=item.get("checkpoint_location", ""),
        )

    @staticmethod
    def _parse_target(item: dict) -> TargetConfig:
        """Convert a raw dict into a TargetConfig dataclass."""
        raw_partitions: List[Dict[str, str]] = item.get("partition_keys", [])
        parsed_partitions = [
            PartitionKey(name=p.get("name", ""), type=p.get("type", "string"))
            for p in raw_partitions
        ]

        raw_schema: Dict[str, Any] = item.get("schema", {})
        parsed_schema: Dict[str, SchemaField] = {}
        for field_name, field_def in raw_schema.items():
            if isinstance(field_def, dict):
                parsed_schema[field_name] = SchemaField(
                    type=field_def.get("type", "string"),
                    nullable=field_def.get("nullable", True),
                    comment=field_def.get("comment", ""),
                )
            else:
                parsed_schema[field_name] = SchemaField(type=str(field_def))

        return TargetConfig(
            catalog=item.get("catalog", {}),
            location=item.get("location", ""),
            rejected_location=item.get("rejected_location", ""),
            format=item.get("format", "parquet"),
            compression=item.get("compression", "snappy"),
            partition_keys=parsed_partitions,
            schema=parsed_schema,
            primary_key=item.get("primary_key", []),
            enum_columns=item.get("enum_columns", {}),
        )

    @staticmethod
    def _parse_s3_path(s3_path: str) -> tuple:
        """Parse 's3://bucket/key' into (bucket, key)."""
        path = s3_path.replace("s3://", "")
        parts = path.split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        return bucket, key
