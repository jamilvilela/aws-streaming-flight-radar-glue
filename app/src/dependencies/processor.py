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

from pyspark.sql import DataFrame, SparkSession

from .config import SourceConfig, TargetConfig
from .reader import Reader
from .data_quality import DataQuality
from .writer import Writer
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
    3. Write rejects via Writer
    4. Write valid data via Writer
    5. Register execution via EtlControl
    6. Persist quality metrics via QualityMetrics
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark
        self._reader = Reader(spark)
        self._data_quality = DataQuality(spark)
        self._writer = Writer(spark)
        self._etl_control = EtlControl(spark)
        self._quality_metrics = QualityMetrics(spark)

        # Pipeline state
        self._execution_id: str = ""
        self._source: Optional[SourceConfig] = None
        self._target: Optional[TargetConfig] = None
        self._records_read: int = 0
        self._records_written: int = 0
        self._records_rejected: int = 0

    # ── Public API ───────────────────────────────────────────────────

    def run(
        self,
        source: SourceConfig,
        target: TargetConfig,
    ) -> None:
        """
        Execute the full processing pipeline for a given source/target.

        Args:
            source: SourceConfig describing the DMS origin.
            target: TargetConfig describing the destination table.
        """
        self._execution_id = str(uuid.uuid4())
        self._source = source
        self._target = target

        logger.info(
            "Starting pipeline | execution_id=%s | source=%s | target=%s.%s",
            self._execution_id,
            source.source,
            target.database,
            target.table,
        )

        start_time = time.time()
        pipeline_status = "running"

        try:
            # 1. Read
            raw_df = self._reader.read(source)
            self._records_read = raw_df.count() if not self._is_streaming(raw_df) else 0
            logger.info("Read %d records from %s", self._records_read, source.source)

            # 2. Validate
            valid_df, rejects_df = self._data_quality.validate(raw_df, target, source)
            self._records_rejected = rejects_df.count() if not rejects_df.isEmpty() else 0
            self._records_written = (valid_df.count() if not self._is_streaming(valid_df)
                                     else max(0, self._records_read - self._records_rejected))
            logger.info(
                "Validation complete: %d valid, %d rejected",
                self._records_written,
                self._records_rejected,
            )

            # 3. Write rejects
            self._writer.write_rejects(rejects_df, target)

            # 4. Write valid data
            self._writer.write(valid_df, target, source)

            pipeline_status = "success"
            logger.info("Pipeline completed successfully for %s", source.source)

        except Exception as exc:
            pipeline_status = "failed"
            logger.error("Pipeline failed for %s: %s", source.source, exc, exc_info=True)
            raise ProcessorError(f"Pipeline failed for '{source.source}': {exc}") from exc

        finally:
            elapsed = time.time() - start_time
            self._register_execution(
                status=pipeline_status,
                elapsed_seconds=elapsed,
                error_message=str(exc) if pipeline_status == "failed" else None,
            )
            self._save_quality_metrics(pipeline_status)

    # ── Execution logging ────────────────────────────────────────────

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

    # ── Internal helpers ─────────────────────────────────────────────

    @staticmethod
    def _is_streaming(df: DataFrame) -> bool:
        """Check if the DataFrame is a streaming DataFrame."""
        return df.isStreaming if hasattr(df, "isStreaming") else False
