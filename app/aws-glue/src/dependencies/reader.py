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


    def read(self, source: SourceConfig, mode: str = "streaming") -> DataFrame:
        """
        Read data from the configured source.

        In ``streaming`` mode (default) uses Spark ``readStream`` with
        checkpoint-based fault-tolerance, reading from
        ``source.cdc_source_location`` (the CDC-only prefix).

        In ``batch`` mode reads all existing files at once from
        ``source.source_location`` (the full-load path).

        Args:
            source: SourceConfig describing the DMS origin to read from.
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
            logger.info("Batch read %d rows from %s", df.count(), source.source_location)
            return df
        except Exception as exc:
            raise ReaderError(
                f"Failed to create batch read: {exc}"
            ) from exc

    def _read_streaming(self, source: SourceConfig) -> DataFrame:
        """
        Read data in streaming mode using Spark readStream.

        Uses ``cdc_source_location`` — the CDC-only prefix written by
        DMS via the ``CdcPath`` parameter. ``includeExistingFiles=false``
        ensures already-processed full-load files are not re-read.
        """
        cdc_path = source.cdc_source_location or source.source_location
        archive_path = cdc_path.rstrip("/") + "_archive/"
        try:
            stream_df = (
                self._spark.readStream
                .format("parquet")
                .option("maxFilesPerTrigger", 1)
                .option("cleanSource", "archive")
                .option("sourceArchiveDir", archive_path)
                .option("includeExistingFiles", "false")
                .load(cdc_path)
            )
            logger.debug("Streaming reader created for %s", cdc_path)
            return stream_df
        except Exception as exc:
            raise ReaderError(
                f"Failed to create streaming read: {exc}"
            ) from exc
