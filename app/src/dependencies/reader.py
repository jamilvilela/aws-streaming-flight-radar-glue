"""
Reader module — Reads source data from S3 (DMS Parquet CDC).

Uses Spark readStream with checkpoint-based fault-tolerance.
No Glue-specific APIs are used.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from .config import SourceConfig

logger = logging.getLogger(__name__)


class ReaderError(Exception):
    """Raised when a read operation fails."""
    pass


class Reader:
    """
    Reads Parquet data from S3 (DMS CDC) using Spark readStream.

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

    # ── Public API ───────────────────────────────────────────────────

    def read(self, source: SourceConfig) -> DataFrame:
        """
        Read data from the configured source in streaming mode.

        The checkpoint_location in the source config is used for
        fault-tolerant recovery.

        Args:
            source: SourceConfig describing the DMS origin to read from.

        Returns:
            Streaming DataFrame with the raw source data.
        """
        logger.info("Reading streaming from %s", source.source_location)
        return self._read_streaming(source)

    # ── Internal methods ─────────────────────────────────────────────

    def _read_streaming(self, source: SourceConfig) -> DataFrame:
        """
        Read data in streaming mode using Spark readStream.

        Relies on Parquet's self-describing schema — no explicit schema needed.
        Checkpoint location in the source config enables fault-tolerance.
        """
        try:
            stream_df = (
                self._spark.readStream
                .format("parquet")
                .option("maxFilesPerTrigger", 1)
                .option("cleanSource", "archive")
                .option("sourceArchiveDir", source.source_location + "_archive/")
                .option("includeExistingFiles", "true")
                .load(source.source_location)
            )
            logger.debug("Streaming reader created for %s", source.source_location)
            return stream_df
        except Exception as exc:
            raise ReaderError(
                f"Failed to create streaming read: {exc}"
            ) from exc
