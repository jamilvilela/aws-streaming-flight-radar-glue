"""
Reader module — Reads source data from S3 (Parquet CDC).

Uses Spark readStream with checkpoint-based fault-tolerance.
No Glue-specific APIs are used.
"""

from __future__ import annotations

import logging
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from .config import SourceConfig, SchemaField

logger = logging.getLogger(__name__)


class ReaderError(Exception):
    """Raised when a read operation fails."""
    pass


class Reader:
    """
    Reads Parquet data from S3 (CDC) using Spark readStream.

    Supports:
    - Streaming reads via Spark readStream with S3 checkpointing
    """

    def __init__(self, spark: SparkSession):
        """
        Initialize the Reader.

        Args:
            spark: Active SparkSession.
        """
        self._spark = spark


    def read(self, source: SourceConfig, mode: str = "streaming") -> DataFrame:
        """
        Read data from the configured source.

        In ``streaming`` mode (default) uses Spark ``readStream`` with
        checkpoint-based fault-tolerance, reading from
        ``source.cdc_source_location`` (the CDC-only prefix).

        In ``batch`` mode reads all existing files at once from
        ``source.source_location`` (the full-load path).

        Args:
            source: SourceConfig describing the source origin to read from.
            mode: ``"streaming"`` (default) or ``"batch"``.

        Returns:
            Streaming or static DataFrame with the raw source data.
        """
        location = source.cdc_source_location if mode == "streaming" else source.source_location
        logger.info("Reading %s from %s", mode, location)
        if mode == "batch":
            return self._read_batch(source)
        return self._read_streaming(source)


    def _read_batch(self, source: SourceConfig) -> DataFrame:
        """Read all existing data from source location as a static DataFrame."""
        try:
            df = (
                self._spark.read
                .format(source.format)
                .load(source.source_location)
            )
            if source.filter:
                df = df.filter(source.filter)
            logger.info("Batch DataFrame created from %s", source.source_location)
            return df
        except Exception as exc:
            raise ReaderError(
                f"Failed to create batch read: {exc}"
            ) from exc

    @staticmethod
    def _to_spark_schema(target_schema: Dict[str, SchemaField]) -> StructType:
        """Convert target schema dict to Spark StructType."""
        type_map = {
            "bigint": LongType(),
            "int": LongType(),
            "integer": LongType(),
            "smallint": LongType(),
            "tinyint": LongType(),
            "double": DoubleType(),
            "float": DoubleType(),
            "decimal": DoubleType(),
            "string": StringType(),
            "varchar": StringType(),
            "char": StringType(),
            "timestamp": TimestampType(),
            "date": DateType(),
            "boolean": BooleanType(),
        }
        fields = []
        for field_name, field_info in target_schema.items():
            spark_type = type_map.get(field_info.type.lower(), StringType())
            fields.append(StructField(field_name, spark_type, field_info.nullable))
        return StructType(fields)

    def _read_streaming(self, source: SourceConfig) -> DataFrame:
        """
        Read data in streaming mode using Spark readStream.

        Uses ``cdc_source_location`` — the CDC-only prefix. The source path
        must be a base directory (no directory-level globs): Spark's
        FileStreamSource does not recurse into directories matched by a
        glob. Full-load ``LOAD*.parquet`` files in the root are excluded via
        ``pathGlobFilter`` (CDC files are named like ``20260814-*.parquet``).

        ``includeExistingFiles=true`` makes the first run pick up existing
        CDC files; processed files are moved to ``archive_location``
        (must live OUTSIDE the source path, otherwise archived files are
        re-discovered on the next trigger).
        """
        cdc_path = source.cdc_source_location or source.source_location
        archive_path = source.archive_location.rstrip("/") + "/"
        is_aircraft_positions = source.source == "aircraft_positions"
        try:
            schema = self._to_spark_schema(source.target.schema)
            options = {
                "maxFilesPerTrigger": 1000,
                "pathGlobFilter": "2*.parquet",
                "cleanSource": "archive",
                "sourceArchiveDir": archive_path,
                "includeExistingFiles": "true",
            }
            if is_aircraft_positions:
                # aircraft_positions Parquet stores latitude/longitude as FIXED_LEN_BYTE_ARRAY.
                # Spark cannot convert that physical type to the target schema at read time,
                # so infer the file schema via a batch read (no conversion) and use it for
                # streaming. _cast_types converts the inferred types to target types later.
                try:
                    schema = (
                        self._spark.read
                        .format("parquet")
                        .option("mergeSchema", "true")
                        .load(cdc_path)
                        .schema
                    )
                    logger.info("Inferred schema from %s for streaming", cdc_path)
                except Exception as infer_exc:
                    logger.warning(
                        "Schema inference failed (%s), falling back to target schema", infer_exc
                    )
            reader = self._spark.readStream.format("parquet").options(**options)
            if schema is not None:
                reader = reader.schema(schema)
            stream_df = reader.load(cdc_path)
            if source.filter:
                stream_df = stream_df.filter(source.filter)
            logger.info("Streaming reader created for %s", cdc_path)
            return stream_df
        except Exception as exc:
            raise ReaderError(
                f"Failed to create streaming read: {exc}"
            ) from exc
