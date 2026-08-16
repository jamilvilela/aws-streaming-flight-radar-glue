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


# Physical (catalog) column names for the DMS CDC metadata. The Glue Catalog
# tables in the data lakehouse are defined with these names, while DMS CDC
# files carry the short names (Op / dms_timestamp). The writer renames the
# DataFrame columns to match the catalog schema before writing.
CDC_OP_COLUMN = "cdc_operation"
CDC_TIMESTAMP_COLUMN = "cdc_timestamp"


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
          - ``WHEN NOT MATCHED AND Op <> 'D' THEN INSERT`` — new records
          - ``WHEN MATCHED AND Op = 'D' THEN DELETE`` — DMS deletes
          - ``WHEN MATCHED THEN UPDATE SET *`` — existing records updated

        The target table (``{database}.{table}``) must already be registered
        in the Glue Catalog (created in the data lakehouse repo). On the first
        write, if the physical Delta table (``_delta_log``) does not exist yet,
        it is bootstrapped at ``target.location``; subsequent writes perform a
        Delta MERGE.

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
        df_to_write = self._map_cdc_columns(df_to_write, source)

        table_name = f"{target.database}.{target.table}"
        partition_cols = [pk.name for pk in target.partition_keys if pk.name in df_to_write.columns]

        try:
            # Bootstrap check — the Glue Catalog table is only metadata until
            # the first physical write creates _delta_log at the location.
            if not DeltaTable.isDeltaTable(self._spark, target.location):
                self._bootstrap_table(df_to_write, target, partition_cols)
                logger.info("Created Delta table %s at %s (bootstrap)", table_name, target.location)
                return

            # Resolve by path — independent of the Glue Catalog table being
            # recognized as a Delta table (isDeltaTable needs the
            # spark.sql.sources.provider property; classification/table_type
            # alone are not enough for DeltaTable.forName).
            delta_table = DeltaTable.forPath(self._spark, target.location)

            # DMS injects CDC metadata columns (Op, dms_timestamp, ...) into
            # every file. Project only the columns that exist in the target
            # Delta table, keeping the CDC op column (if present) available
            # so the merge can implement DMS delete semantics (Op = 'D').
            op_col = CDC_OP_COLUMN if CDC_OP_COLUMN in df_to_write.columns else "Op"
            target_cols = set(delta_table.toDF().columns)
            write_cols = [c for c in df_to_write.columns if c in target_cols]
            merge_cols = write_cols
            if op_col in df_to_write.columns and op_col not in merge_cols:
                merge_cols = merge_cols + [op_col]
            df_to_write = df_to_write.select(*merge_cols)

            merge_condition = "source.cod_unico = target.cod_unico"

            builder = delta_table.alias("target").merge(df_to_write.alias("source"), merge_condition)

            if op_col in df_to_write.columns:
                builder = (builder
                           .whenNotMatchedInsert(
                               condition=f"COALESCE(source.{op_col}, 'I') <> 'D'",
                               values={c: f"source.{c}" for c in write_cols})
                           .whenMatchedDelete(condition=f"source.{op_col} = 'D'")
                           .whenMatchedUpdateAll())
            else:
                builder = builder.whenNotMatchedInsertAll().whenMatchedUpdateAll()

            builder.execute()

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
    def _map_cdc_columns(df: DataFrame, source: Optional[SourceConfig]) -> DataFrame:
        """
        Rename DMS CDC metadata columns to the physical catalog column names.

        DMS CDC files carry the short names (``Op`` / ``dms_timestamp``)
        while the Glue Catalog tables in the data lakehouse are defined with
        ``cdc_operation`` / ``cdc_timestamp``. Renaming here keeps the Delta
        table schema aligned with the catalog definition.
        """
        if not (source and source.cdc_config):
            return df

        mapping = {}
        if source.cdc_config.op_column and source.cdc_config.op_column != CDC_OP_COLUMN:
            mapping[source.cdc_config.op_column] = CDC_OP_COLUMN
        if source.cdc_config.timestamp_column and source.cdc_config.timestamp_column != CDC_TIMESTAMP_COLUMN:
            mapping[source.cdc_config.timestamp_column] = CDC_TIMESTAMP_COLUMN

        for old, new in mapping.items():
            if old in df.columns:
                df = df.withColumnRenamed(old, new)
        return df

    @staticmethod
    def _bootstrap_table(
        df: DataFrame,
        target: TargetConfig,
        partition_cols: list,
    ) -> None:
        """
        Create the physical Delta table at the target location.

        The Glue Catalog table is registered upfront (EXTERNAL_TABLE), but the
        Delta transaction log (``_delta_log``) only exists after the first
        write. On the first batch we write the full load directly to the
        location, materializing the Delta table, then subsequent runs MERGE.
        """
        writer = df.write.format("delta").mode("append")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(target.location)

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
