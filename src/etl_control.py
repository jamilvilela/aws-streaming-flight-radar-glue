"""
EtlControl module — Registers pipeline execution records in the
etl_control table (Parquet in the raw layer).
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession

from src.config import TargetConfig

logger = logging.getLogger(__name__)


class EtlControlError(Exception):
    """Raised when registering an execution fails."""
    pass


class EtlControl:
    """
    Writes execution metadata to the etl_control table for observability.

    Each call to ``register()`` appends one row with execution_id,
    job_name, source, timestamps, row counts, status, and partition info.
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark

    def register(
        self,
        execution_id: str,
        source_name: str,
        status: str,
        records_read: int,
        records_written: int,
        records_rejected: int,
        target: TargetConfig,
        elapsed_seconds: float,
        error_message: Optional[str] = None,
    ) -> None:
        """
        Append an execution record to the etl_control table.

        Args:
            execution_id: Unique execution UUID.
            source_name: Source name (e.g. 'flights').
            status: 'success' or 'failed'.
            records_read: Number of raw records read.
            records_written: Number of valid records written.
            records_rejected: Number of rejected records.
            target: TargetConfig for table name and partition keys.
            elapsed_seconds: Total pipeline duration in seconds.
            error_message: Error description if status is 'failed'.
        """
        now = datetime.utcnow()
        partition_value = self._build_partition_value(target)

        data = [(
            execution_id,
            "glue-streaming-minibatch-dms",
            source_name,
            datetime.fromtimestamp(
                (now.timestamp() - elapsed_seconds)
            ),
            now,
            status,
            records_read,
            records_written,
            records_rejected,
            partition_value,
            error_message or "",
            now.date(),
        )]

        try:
            df = self._spark.createDataFrame(
                data,
                schema=[
                    "execution_id", "job_name", "source",
                    "execution_start", "execution_end",
                    "status", "records_read", "records_written",
                    "records_rejected", "target_partition",
                    "error_message", "reference_date",
                ],
            )

            path = self._resolve_path()
            (
                df.write
                .mode("append")
                .format("parquet")
                .partitionBy("reference_date")
                .option("compression", "snappy")
                .save(path)
            )
            logger.info("Execution registered in etl_control: %s", execution_id)
        except Exception as exc:
            logger.warning("Failed to register execution in etl_control: %s", exc)

    # ── Internal helpers ─────────────────────────────────────────────

    def _resolve_path(self) -> str:
        """Resolve the S3 path for the etl_control table."""
        account_id = self._get_account_id()
        return f"s3://lakehouse-raw-{account_id}/tables/etl_control/"

    @staticmethod
    def _build_partition_value(target: TargetConfig) -> str:
        """Build a partition description string, e.g. 'event_date='."""
        if not target.partition_keys:
            return ""
        return "/".join(f"{pk.name}=" for pk in target.partition_keys)

    @staticmethod
    def _get_account_id() -> str:
        """Try to get the AWS account ID from boto3 or environment."""
        try:
            import boto3
            return boto3.client("sts").get_caller_identity()["Account"]
        except Exception:
            return "000000000000"
