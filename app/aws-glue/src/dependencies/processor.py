"""
Processor module — Orchestrates the full ETL pipeline.

Coordinates: Reader → DataQuality → Writer,
with execution logging (EtlControl) and quality metrics (QualityMetrics).
Uses pure Spark APIs — no Glue dependencies.
"""

from __future__ import annotations

import logging
import time
import uuid
from typing import Optional

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession

from .config import SourceConfig, TargetConfig
from .reader import Reader
from .data_quality import DataQuality
from .writer import Writer
from .rejected_records import RejectedRecords
from .etl_control import EtlControl
from .quality_metrics import QualityMetrics

logger = logging.getLogger(__name__)


class ProcessorError(Exception):
    """Raised when the pipeline processing fails."""
    pass


class Processor:
    """
    Orchestrates the end-to-end data processing pipeline.

    Pipeline steps:
    1. Read raw data via Reader (streaming)
    2. Validate & cleanse via DataQuality
    3. Write rejects via RejectedRecords (centralized table)
    4. Write valid data via Writer
    5. Register execution via EtlControl
    6. Persist quality metrics via QualityMetrics
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark
        self._reader = Reader(spark)
        self._data_quality = DataQuality(spark)
        self._writer = Writer(spark)
        self._rejected_records = RejectedRecords(spark)
        self._etl_control = EtlControl(spark)
        self._quality_metrics = QualityMetrics(spark)

        # Pipeline state
        self._execution_id: str = ""
        self._source: Optional[SourceConfig] = None
        self._target: Optional[TargetConfig] = None
        self._records_read: int = 0
        self._records_written: int = 0
        self._records_rejected: int = 0


    def run(
        self,
        source: SourceConfig,
        target: TargetConfig,
        mode: str = "streaming",
        dataframe: Optional[DataFrame] = None,
    ) -> None:
        """
        Execute the full processing pipeline for a given source/target.

        Args:
            source: SourceConfig describing the source origin.
            target: TargetConfig describing the destination table.
            mode: ``\"streaming\"`` (default) or ``\"batch\"``.
            dataframe: Optional pre-read DataFrame. When provided in batch
                mode, the Reader step is skipped and this DataFrame is used
                directly.
        """
        self._execution_id = str(uuid.uuid4())
        self._source = source
        self._target = target

        is_batch = mode == "batch"

        logger.info(
            "Starting pipeline | execution_id=%s | source=%s | target=%s.%s | mode=%s",
            self._execution_id,
            source.source,
            target.database,
            target.table,
            mode,
        )

        start_time = time.time()
        pipeline_status = "running"
        exc: Optional[Exception] = None
        cached_frames: list[DataFrame] = []

        try:
            # 1. Read (skip if a pre-read dataframe was provided in batch mode)
            if dataframe is not None and is_batch:
                raw_df = dataframe
            else:
                raw_df = self._reader.read(source, mode=mode)
            if is_batch:
                raw_df = raw_df.persist(StorageLevel.MEMORY_AND_DISK)
                cached_frames.append(raw_df)
                self._records_read = raw_df.count()
            else:
                # Streaming: avoid count() for low latency
                self._records_read = 0
            logger.info("Read %d records from %s (%s)", self._records_read, source.source, mode)

            # 2. Validate
            valid_df, rejects_df = self._data_quality.validate(raw_df, target, source)
            if is_batch:
                valid_df = valid_df.persist(StorageLevel.MEMORY_AND_DISK)
                rejects_df = rejects_df.persist(StorageLevel.MEMORY_AND_DISK)
                cached_frames.extend((valid_df, rejects_df))
                # Compute counts once and reuse - avoid multiple count() calls
                self._records_rejected = rejects_df.count()
                self._records_written = valid_df.count()
            else:
                # Streaming: avoid count() on streaming DataFrames for low latency
                # Metrics will be tracked via EtlControl/QualityMetrics via separate mechanisms
                self._records_rejected = 0
                self._records_written = 0
            logger.info(
                "Validation complete: %d valid, %d rejected",
                self._records_written,
                self._records_rejected,
            )

            # 3. Write rejects (centralized table)
            # The DataQuality returns rejects_df with _reject_rule column indicating which rule failed
            # We need to extract the reject rule and write to centralized table
            if not rejects_df.isEmpty():
                # Get the reject rule from the first row (all rows should have same rule for batch)
                # For streaming, we'll process per batch
                reject_rule = "validation_failed"
                reject_reason = "Record failed data quality validation"
                try:
                    # Try to get the reject rule from the DataFrame
                    rule_col = [c for c in rejects_df.columns if c.startswith("_reject_rule")]
                    if rule_col:
                        rule_row = rejects_df.select(rule_col[0]).first()
                        if rule_row and rule_row[0]:
                            reject_rule = rule_row[0]
                except Exception:
                    pass  # fallback to default

                self._rejected_records.write(
                    rejected_df=rejects_df,
                    source=source,
                    target=target,
                    execution_id=self._execution_id,
                    reject_rule=reject_rule,
                    reject_reason="Record failed data quality validation",
                )

            # 4. Write valid data
            self._writer.write(valid_df, target, source)

            pipeline_status = "success"
            logger.info("Pipeline completed successfully for %s (%s)", source.source, mode)

        except Exception as pipeline_exc:
            exc = pipeline_exc
            pipeline_status = "failed"
            logger.error("Pipeline failed for %s: %s", source.source, exc, exc_info=True)
            raise ProcessorError(f"Pipeline failed for '{source.source}': {exc}") from exc

        finally:
            try:
                elapsed = time.time() - start_time
                self._register_execution(
                    status=pipeline_status,
                    elapsed_seconds=elapsed,
                    error_message=str(exc) if pipeline_status == "failed" else None,
                )
                self._save_quality_metrics(pipeline_status)
            finally:
                for cached_frame in cached_frames:
                    cached_frame.unpersist()


    def _register_execution(
        self,
        status: str,
        elapsed_seconds: float,
        error_message: Optional[str] = None,
    ) -> None:
        """Delegate execution registration to EtlControl."""
        if not self._source or not self._target:
            return
        self._etl_control.register(
            execution_id=self._execution_id,
            source_name=self._source.source,
            status=status,
            records_read=self._records_read,
            records_written=self._records_written,
            records_rejected=self._records_rejected,
            target=self._target,
            elapsed_seconds=elapsed_seconds,
            error_message=error_message,
        )

    def _save_quality_metrics(self, status: str) -> None:
        """Delegate quality metrics persistence to QualityMetrics."""
        if not self._target:
            return
        self._quality_metrics.save(
            target=self._target,
            status=status,
            records_read=self._records_read,
            records_written=self._records_written,
            records_rejected=self._records_rejected,
        )


    @staticmethod
    def _is_streaming(df: DataFrame) -> bool:
        """Check if the DataFrame is a streaming DataFrame."""
        return df.isStreaming if hasattr(df, "isStreaming") else False
