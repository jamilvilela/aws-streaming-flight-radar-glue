"""
Writer module — Writes validated DataFrames to the raw Data Lake layer
in Parquet + Snappy format, with dynamic partitioning and optional compaction.
"""

from __future__ import annotations

import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config import SourceConfig, TargetConfig

logger = logging.getLogger(__name__)


class WriterError(Exception):
    """Raised when a write operation fails."""
    pass


class Writer:
    """
    Writes validated DataFrames to the raw S3 bucket.

    Features:
    - Dynamic partitioning by year/month/day from dms_timestamp
    - Parquet + Snappy format
    - Append mode for incremental data
    - Compaction to mitigate small files
    - Rejects writing with metadata
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark

    # ── Public API ───────────────────────────────────────────────────

    def write(self, df: DataFrame, target: TargetConfig, source: Optional[SourceConfig] = None) -> None:
        """
        Write a validated DataFrame to the target raw location.

        Automatically extracts partition columns from dms_timestamp
        and writes in Parquet + Snappy format.

        Args:
            df: Validated DataFrame to write.
            target: TargetConfig with location, partition_keys, compression.
            source: Optional SourceConfig for CDC timestamp resolution.
        """
        if df.rdd.isEmpty():
            logger.info("Empty DataFrame — nothing to write for %s", target.table)
            return

        df_to_write = self._prepare_with_partitions(df, target, source)

        partition_cols = [pk.name for pk in target.partition_keys if pk.name in df_to_write.columns]

        try:
            (
                df_to_write
                .write
                .mode("append")
                .format(target.format)
                .partitionBy(*partition_cols)
                .option("compression", target.compression)
                .save(target.location)
            )
            logger.info(
                "Wrote %d rows to %s (partitions: %s)",
                df_to_write.count(),
                target.location,
                partition_cols,
            )
        except Exception as exc:
            raise WriterError(f"Failed to write to {target.location}: {exc}") from exc

    def write_rejects(self, df: DataFrame, target: TargetConfig) -> None:
        """
        Write rejected records to the rejected location.

        Args:
            df: Rejected DataFrame (with _reject_* metadata columns).
            target: TargetConfig with rejected_location.
        """
        if df.rdd.isEmpty():
            logger.info("No rejected records for %s", target.table)
            return

        try:
            (
                df
                .write
                .mode("append")
                .format(target.format)
                .option("compression", target.compression)
                .save(target.rejected_location)
            )
            logger.info("Wrote %d rejected rows to %s", df.count(), target.rejected_location)
        except Exception as exc:
            raise WriterError(f"Failed to write rejects to {target.rejected_location}: {exc}") from exc

    def compact(
        self,
        target: TargetConfig,
        max_files_per_partition: int = 1,
    ) -> None:
        """
        Compact small Parquet files in the target location.

        Reads existing data, coalesces, and overwrites the partition(s).
        Use periodically to mitigate the small-files problem.

        Args:
            target: TargetConfig with location and partition_keys.
            max_files_per_partition: Target max number of files per partition.
        """
        try:
            df = (
                self._spark.read
                .format(target.format)
                .load(target.location)
            )
            if df.rdd.isEmpty():
                logger.info("No data to compact in %s", target.location)
                return

            partition_cols = [pk.name for pk in target.partition_keys if pk.name in df.columns]

            num_partitions = df.rdd.getNumPartitions()
            target_partitions = max(num_partitions // 2, max_files_per_partition)

            (
                df.coalesce(target_partitions)
                .write
                .mode("overwrite")
                .format(target.format)
                .partitionBy(*partition_cols)
                .option("compression", target.compression)
                .save(target.location)
            )
            logger.info(
                "Compaction completed for %s (%d → %d partitions)",
                target.location,
                num_partitions,
                target_partitions,
            )
        except Exception as exc:
            raise WriterError(f"Compaction failed for {target.location}: {exc}") from exc

    # ── Internal helpers ─────────────────────────────────────────────

    @staticmethod
    def _prepare_with_partitions(
        df: DataFrame,
        target: TargetConfig,
        source: Optional[SourceConfig] = None,
    ) -> DataFrame:
        """
        Add partition columns required by the target table.

        Derives ``event_date`` (date) from the CDC timestamp column
        if not already present in the DataFrame.
        """
        # Determine which partition columns already exist
        needed = [pk for pk in target.partition_keys if pk.name not in df.columns]
        if not needed:
            return df

        # Resolve timestamp column
        ts_col: Optional[str] = None
        if source and source.cdc_config and source.cdc_config.timestamp_column:
            ts_col = source.cdc_config.timestamp_column
        elif "dms_timestamp" in df.columns:
            ts_col = "dms_timestamp"

        if ts_col is None or ts_col not in df.columns:
            logger.warning("No timestamp column found for partition extraction")
            return df

        result = df
        for pk in needed:
            if pk.name == "event_date":
                result = result.withColumn("event_date", F.to_date(F.col(ts_col)))
            else:
                logger.warning("Unknown partition key '%s' — skipping", pk.name)

        return result

        return result
