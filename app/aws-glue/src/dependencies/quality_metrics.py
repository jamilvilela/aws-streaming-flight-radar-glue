"""
QualityMetrics module — Saves data quality metrics to the
data_quality_metrics table (Parquet in the raw layer, resolved via
the Glue Data Catalog).
"""

from __future__ import annotations

import logging
from datetime import datetime

from pyspark.sql import SparkSession

from .aws_helper import AwsHelper
from .config import TargetConfig

logger = logging.getLogger(__name__)


class QualityMetricsError(Exception):
    """Raised when saving quality metrics fails."""
    pass


class QualityMetrics:
    """
    Persists quality metrics for each pipeline execution.

    Metrics include rows_read, rows_written, rows_rejected, and
    overall pipeline_status — all stored partitioned by reference_date
    in the ``db_raw.data_quality_metrics`` catalog table.
    """

    DATABASE = "db_raw"
    TABLE = "data_quality_metrics"

    def __init__(self, spark: SparkSession):
        """Initialize the QualityMetrics.

        Args:
            spark: Active SparkSession.
        """
        self._spark = spark
        self._aws_helper = AwsHelper()

    def save(
        self,
        target: TargetConfig,
        status: str,
        records_read: int,
        records_written: int,
        records_rejected: int,
    ) -> None:
        """Append quality metrics to the data_quality_metrics table.

        Args:
            target: TargetConfig for database/table names and partition keys.
            status: 'success' or 'failed'.
            records_read: Number of raw records read.
            records_written: Number of valid records written.
            records_rejected: Number of rejected records.
        """
        metrics = [
            ("rows_read", str(records_read), "success" if status == "success" else "failed"),
            ("rows_written", str(records_written), "success" if status == "success" else "failed"),
            ("rows_rejected", str(records_rejected), "success"),
            ("pipeline_status", status, "success"),
        ]

        now = datetime.utcnow()
        partition_value = self._build_partition_value(target)
        rows = []
        for metric_name, metric_value, metric_status in metrics:
            rows.append((
                target.database,
                target.table,
                now,
                metric_name,
                metric_name,
                metric_status,
                metric_value,
                partition_value,
                "glue",
                now.date(),
            ))

        try:
            df = self._spark.createDataFrame(
                rows,
                schema=[
                    "database", "table", "processing_timestamp",
                    "metric", "rule", "status", "failure_reason",
                    "partition", "technology", "reference_date",
                ],
            )

            metrics_location = self._aws_helper.get_table_location(self.DATABASE, self.TABLE)
            (df.coalesce(1).write
             .mode("append")
             .format("parquet")
             .option("compression", "snappy")
             .partitionBy("reference_date")
             .save(metrics_location)
            )
            self._aws_helper.update_table_partitions(self.DATABASE, self.TABLE)

            logger.info("Quality metrics saved to data_quality_metrics")
        except Exception as exc:
            logger.warning("Failed to save quality metrics: %s", exc)

    @staticmethod
    def _build_partition_value(target: TargetConfig) -> str:
        """Build a partition description string, e.g. 'event_date='.

        Args:
            target: TargetConfig with partition_keys.

        Returns:
            Partition description string.
        """
        if not target.partition_keys:
            return ""
        return "/".join(f"{pk.name}=" for pk in target.partition_keys)