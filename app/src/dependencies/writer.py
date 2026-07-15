"""
Writer module — Writes validated DataFrames to the raw Data Lake layer
in Delta Lake format with MERGE for cross-batch dedup.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import SourceConfig, TargetConfig

logger = logging.getLogger(__name__)


class WriterError(Exception):
    """Raised when a write operation fails."""
    pass


class Writer:
    """
    Writes validated DataFrames to the raw S3 bucket (Delta Lake).

    Features:
    - Delta Lake format with MERGE (insert/update) by primary key
    - Automatic generation of cod_unico from PK columns
    - Dynamic partitioning by event_date
    - Rejects writing with metadata (Parquet format)
    - No manual compaction needed — Delta handles via auto-optimize
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark

    # ── Public API ───────────────────────────────────────────────────

    def write(self, df: DataFrame, target: TargetConfig, source: Optional[SourceConfig] = None) -> None:
        """
        Write a validated DataFrame using Delta MERGE via Glue Catalog.

        Automatically generates ``cod_unico`` from primary key columns
        (via ``cod_unico_expr`` or by concatenating PK columns with "_"),
        then performs a Delta MERGE using the table name resolved through
        the **Glue Catalog** (``DeltaTable.forName``):
          - ``WHEN NOT MATCHED THEN INSERT`` — new records
          - ``WHEN MATCHED THEN UPDATE SET *`` — existing records updated

        The target table (``{database}.{table}``) must already exist in
        the Glue Catalog — it is a prerequisite and is NOT created here.

        Args:
            df: Validated DataFrame to write.
            target: TargetConfig with catalog (database, table), location,
                    partition_keys, primary_key, cod_unico_expr.
            source: Optional SourceConfig for CDC timestamp resolution.
        """
        if df.isEmpty():
            logger.info("Empty DataFrame — nothing to write for %s", target.table)
            return

        df_with_pk = self._generate_cod_unico(df, target)
        df_to_write = self._prepare_with_partitions(df_with_pk, target, source)

        table_name = f"{target.database}.{target.table}"
        partition_cols = [pk.name for pk in target.partition_keys if pk.name in df_to_write.columns]

        try:
            delta_table = DeltaTable.forName(self._spark, table_name)

            merge_condition = "source.cod_unico = target.cod_unico"

            (delta_table.alias("target")
             .merge(df_to_write.alias("source"), merge_condition)
             .whenNotMatchedInsertAll()
             .whenMatchedUpdateAll()
             .execute())

            logger.info("Delta MERGE completed for %s (via Glue Catalog)", table_name)

        except Exception as exc:
            raise WriterError(
                f"Failed to merge into Delta table {table_name} at {target.location}: {exc}"
            ) from exc

    def write_rejects(self, df: DataFrame, target: TargetConfig) -> None:
        """
        Write rejected records to the rejected location.

        Args:
            df: Rejected DataFrame (with _reject_* metadata columns).
            target: TargetConfig with rejected_location.
        """
        if df.isEmpty():
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

    # ── Internal helpers ─────────────────────────────────────────────

    @staticmethod
    def _generate_cod_unico(df: DataFrame, target: TargetConfig) -> DataFrame:
        """
        Generate the ``cod_unico`` column by concatenating primary key columns.

        Uses ``cod_unico_expr`` from TargetConfig if available, otherwise
        concatenates primary key columns with "_" separator.
        """
        if "cod_unico" in df.columns:
            return df

        # Determine PK columns and separator
        expr_config: Optional[Dict[str, Any]] = target.cod_unico_expr
        if expr_config:
            pk_cols = expr_config.get("columns", target.primary_key)
            separator = expr_config.get("separator", "_")
        else:
            pk_cols = target.primary_key
            separator = "_"

        pk_cols_in_df = [c for c in pk_cols if c in df.columns]
        if not pk_cols_in_df:
            logger.warning("No PK columns found in DataFrame for cod_unico generation")
            return df

        return df.withColumn("cod_unico", F.concat_ws(separator, *pk_cols_in_df))

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
