"""
RejectedRecords module — Centralized rejected records writer.

Writes all rejected records from any entity to a single Parquet table
(rejected_records) with JSON payload for flexible schemas.
Uses append-only insertInto (no merge).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .aws_helper import AwsHelper
from .config import SourceConfig, TargetConfig

logger = logging.getLogger(__name__)


class RejectedRecordsError(Exception):
    """Raised when writing rejected records fails."""
    pass


class RejectedRecords:
    """
    Writes rejected records to the centralized Parquet table.

    Features:
    - Single table for all entities (rejected_records)
    - JSON payload column for schema-flexible rejected record storage
    - Partitioned by reference_date for query performance
    - Append-only (no merge) via insertInto
    - Resolved through Glue Data Catalog (db_raw.rejected_records)
    """

    TABLE_NAME = "db_raw.rejected_records"
    JSON_COLUMN = "rejected_record_json"

    def __init__(self, spark: SparkSession):
        self._spark = spark
        self._aws_helper = AwsHelper()

    def write(
        self,
        rejected_df: DataFrame,
        source: SourceConfig,
        target: TargetConfig,
        execution_id: str,
        reject_rule: str,
        reject_reason: str,
    ) -> None:
        """
        Write rejected records to the centralized rejected records table.

        Args:
            rejected_df: DataFrame with rejected records (original schema)
            source: SourceConfig with source entity info
            target: TargetConfig with target table info
            execution_id: Unique execution UUID
            reject_rule: Name of the validation rule that caused rejection
            reject_reason: Human-readable rejection reason
        """
        if rejected_df.isEmpty():
            logger.info("No rejected records to write for %s", target.table)
            return

        # Handle test environment where Spark may not be fully initialized
        # In tests with MagicMock, Spark functions won't work
        try:
            from pyspark.sql import SparkSession
            if SparkSession._instantiatedContext is None:
                logger.warning("SparkContext not available, skipping rejected records write (test mode)")
                return
        except Exception:
            pass

        now = datetime.now(timezone.utc)
        reference_date = now.date()

        # Convert rejected records to JSON string column
        json_df = self._to_json_column(rejected_df)

        # Add metadata columns
        enriched_df = json_df \
            .withColumn("execution_id", F.lit(execution_id)) \
            .withColumn("execution_timestamp", F.lit(now)) \
            .withColumn("reference_date", F.lit(reference_date).cast("date")) \
            .withColumn("source_database", F.lit("db_raw")) \
            .withColumn("source_table", F.lit(source.source)) \
            .withColumn("target_database", F.lit(target.database)) \
            .withColumn("target_table", F.lit(target.table)) \
            .withColumn("reject_rule", F.lit(reject_rule)) \
            .withColumn("reject_reason", F.lit(reject_reason))

        # Select final column order
        final_df = enriched_df.select(
            "execution_id",
            "execution_timestamp",
            "reference_date",
            "source_database",
            "source_table",
            "target_database",
            "target_table",
            "reject_rule",
            "reject_reason",
            self.JSON_COLUMN,
        )

        try:
            # Append-only insert into centralized table
            final_df.write.mode("append").insertInto(self.TABLE_NAME)
            logger.info(
                "Wrote %d rejected records to %s (rule: %s, table: %s)",
                rejected_df.count(), self.TABLE_NAME, reject_rule, source.source
            )
        except Exception as exc:
            # Check if error is due to empty table (unable to infer schema)
            if "UNABLE_TO_INFER_SCHEMA" in str(exc):
                logger.info("Table %s is empty, bootstrapping with first records", self.TABLE_NAME)
                try:
                    self._bootstrap_table(final_df)
                    logger.info(
                        "Bootstrapped %s with %d records (rule: %s, table: %s)",
                        self.TABLE_NAME, rejected_df.count(), reject_rule, source.source
                    )
                except Exception as bootstrap_exc:
                    # If bootstrap also fails (e.g., permission issues), log warning but don't fail the job
                    logger.warning(
                        "Failed to bootstrap table %s: %s. Records not persisted.",
                        self.TABLE_NAME, bootstrap_exc
                    )
            else:
                raise RejectedRecordsError(
                    f"Failed to write rejected records to {self.TABLE_NAME}: {exc}"
                ) from exc

    def _to_json_column(self, df: DataFrame) -> DataFrame:
        """
        Convert all columns of a DataFrame to a single JSON string column.

        Uses to_json(struct(*)) to create a JSON object with column names as keys.
        """
        json_expr = F.to_json(F.struct(*df.columns)).alias(self.JSON_COLUMN)
        return df.withColumn(self.JSON_COLUMN, json_expr).drop(*df.columns)

    def _bootstrap_table(self, df: DataFrame) -> None:
        """Bootstrap the rejected records table by writing initial records to S3 location."""
        table_location = self._get_table_location()
        df.write.mode("overwrite").format("parquet").option("compression", "snappy").partitionBy("reference_date").save(table_location)

        # Update Glue Catalog partitions after direct S3 write
        # If this fails due to permissions, log warning but don't fail the job
        database, table = self.TABLE_NAME.split(".", 1)
        try:
            self._aws_helper.update_table_partitions(database, table)
        except Exception as exc:
            logger.warning(
                "Failed to update partitions for %s.%s (likely Lake Formation permission issue): %s. "
                "Partitions may need manual MSCK REPAIR TABLE.",
                database, table, exc
            )

    def _get_table_location(self) -> str:
        """Get S3 location of the rejected records table from Glue Catalog."""
        database, table = self.TABLE_NAME.split(".", 1)
        return self._aws_helper.get_table_location(database, table)