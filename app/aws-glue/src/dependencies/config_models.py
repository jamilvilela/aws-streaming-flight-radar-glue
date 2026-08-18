"""
config_models.py — Dataclasses for multi-table configuration.

Each source carries its own target definition, order, and CDC path,
allowing one Glue job to process multiple tables sequentially (batch)
or concurrently (streaming).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# Dataclasses


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
    source_column: Optional[str] = None

    def to_dict(self) -> Dict[str, str]:
        d = {"name": self.name, "type": self.type}
        if self.source_column:
            d["source_column"] = self.source_column
        return d


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


@dataclass
class TargetConfig:
    """
    Configuration for the target Data Lake table.

    Embedded inside each source entry in config.json, it holds the
    catalog reference, storage location, schema, partition keys and
    primary key used to write to the raw layer.
    """

    catalog: Dict[str, str] = field(default_factory=dict)
    location: str = ""
    rejected_location: str = ""
    format: str = "delta"
    compression: str = "snappy"
    partition_keys: List[PartitionKey] = field(default_factory=list)
    schema: Dict[str, SchemaField] = field(default_factory=dict)
    primary_key: List[str] = field(default_factory=list)
    enum_columns: Dict[str, List[str]] = field(default_factory=dict)
    cod_unique_expr: Optional[Dict[str, Any]] = None

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
            "cod_unique_expr": dict(self.cod_unique_expr) if self.cod_unique_expr else None,
        }


@dataclass
class SourceConfig:
    """
    Configuration for a single data source.

    Includes:
    - ``order`` for batch sequencing
    - ``cdc_source_location`` for streaming reads (separate CDC prefix)
    - ``target`` embedded (schema, partitions, PK) for independent writing
    """

    source: str = ""
    order: int = 0
    source_location: str = ""
    cdc_source_location: str = ""
    format: str = "parquet"
    cdc_config: Optional[CdcConfig] = None
    checkpoint_location: str = ""
    target: TargetConfig = field(default_factory=TargetConfig)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "order": self.order,
            "source_location": self.source_location,
            "cdc_source_location": self.cdc_source_location,
            "format": self.format,
            "cdc_config": self.cdc_config.to_dict() if self.cdc_config else None,
            "checkpoint_location": self.checkpoint_location,
            "target": self.target.to_dict(),
        }


# Top-Level Config


@dataclass
class Config:
    """
    Top-level configuration holding all table definitions.

    Loaded from a single JSON file (config.json) that contains a
    list of sources, each with its own source path, CDC path,
    order, and embedded target config.
    """

    _sources: List[SourceConfig] = field(default_factory=list)


    @property
    def source(self) -> SourceConfig:
        """Return the first configured source."""
        return self._sources[0] if self._sources else SourceConfig()

    @property
    def sources(self) -> List[SourceConfig]:
        """Return all parsed sources sorted by ``order``."""
        return sorted(self._sources, key=lambda s: s.order)

    @property
    def target(self) -> TargetConfig:
        """Return the first source's target."""
        return self.source.target

    def get_source(self, name: str) -> Optional[SourceConfig]:
        """
        Find a source by its ``source`` field value.

        Args:
            name: The source name to look for (e.g. ``"aircraft"``).

        Returns:
            The matching SourceConfig or None if not found.
        """
        for s in self._sources:
            if s.source == name:
                return s
        return None


    @classmethod
    def from_dicts(cls, data: Any) -> "Config":
        """
        Build Config from a raw dict / list (parsed JSON).

        Accepts a list of source dicts, a single source dict, or a dict
        with a ``sources`` key.
        """
        sources = cls._parse_sources(data)
        return cls(_sources=sources)

    @classmethod
    def from_file(cls, path: str) -> "Config":
        """Load configuration from a single local JSON file."""
        logger.info("Loading config from file: %s", path)
        with open(path, "rt", encoding="utf-8") as f:
            data = json.load(f)
        return cls.from_dicts(data)

    @classmethod
    def from_s3(cls, s3_path: str) -> "Config":
        """
        Load configuration from a single S3 URI using boto3.

        Args:
            s3_path: S3 URI like ``s3://bucket/key/config.json``.
        """
        logger.info("Loading config from S3: %s", s3_path)
        import boto3

        s3 = boto3.client("s3")
        bucket, key = cls._parse_s3_path(s3_path)
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        return cls.from_dicts(data)


    @staticmethod
    def _parse_sources(data: Any) -> List[SourceConfig]:
        """
        Parse a list of source configs from config.json.

        Accepts:
        - A list of dicts (standard format)
        - A single dict
        - A dict with 'sources' key
        """
        items: List[dict] = []

        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            if "sources" in data:
                sources_val = data["sources"]
                if isinstance(sources_val, list):
                    items = sources_val
                elif isinstance(sources_val, dict):
                    items = list(sources_val.values())
            else:
                items = [data]

        sources: List[SourceConfig] = []
        for item in items:
            # --- CDC config ---
            raw_cdc = item.get("cdc_config")
            cdc_config: Optional[CdcConfig] = None
            if raw_cdc:
                cdc_config = CdcConfig(
                    op_column=raw_cdc.get("op_column", "Op"),
                    timestamp_column=raw_cdc.get("timestamp_column", "dms_timestamp"),
                    delete_strategy=raw_cdc.get("delete_strategy", "soft_delete"),
                )

            # --- Embedded target config ---
            raw_target = item.get("target", {})
            parsed_partitions = [
                PartitionKey(
                    name=p.get("name", ""),
                    type=p.get("type", "string"),
                    source_column=p.get("source_column"),
                )
                for p in raw_target.get("partition_keys", [])
            ]

            raw_schema: Dict[str, Any] = raw_target.get("schema", {})
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

            target = TargetConfig(
                catalog=raw_target.get("catalog", {}),
                location=raw_target.get("location", ""),
                rejected_location=raw_target.get("rejected_location", ""),
                format=raw_target.get("format", "delta"),
                compression=raw_target.get("compression", "snappy"),
                partition_keys=parsed_partitions,
                schema=parsed_schema,
                primary_key=raw_target.get("primary_key", []),
                enum_columns=raw_target.get("enum_columns", {}),
                cod_unique_expr=raw_target.get("cod_unique_expr"),
            )

            # --- SourceConfig ---
            sources.append(
                SourceConfig(
                    source=item.get("source", ""),
                    order=item.get("order", 0),
                    source_location=item.get("source_location", ""),
                    cdc_source_location=item.get("cdc_source_location", ""),
                    format=item.get("format", "parquet"),
                    cdc_config=cdc_config,
                    checkpoint_location=item.get("checkpoint_location", ""),
                    target=target,
                )
            )

        return sources

    @staticmethod
    def _parse_s3_path(s3_path: str) -> tuple:
        """Parse ``s3://bucket/key`` into ``(bucket, key)``."""
        if not s3_path.startswith("s3://"):
            return None, None
        path = s3_path.replace("s3://", "")
        parts = path.split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        return bucket, key
