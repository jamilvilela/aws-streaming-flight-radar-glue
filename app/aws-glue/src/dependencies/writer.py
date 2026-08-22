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


# Physical (catalog) column names for the CDC metadata. The Glue Catalog
# tables in the data lakehouse are defined with these names, while the raw
# CDC files carry the short names (e.g. Op / dms_timestamp) configured in
# the source's cdc_config. The writer renames the DataFrame columns to
# match the catalog schema before writing.
CDC_OP_COLUMN = "cdc_operation"
CDC_TIMESTAMP_COLUMN = "cdc_timestamp"


class Writer:
    """
    Writes validated DataFrames to the raw S3 bucket (Delta Lake).

    Features:
    - Delta Lake format with MERGE (insert/update) by primary key
    - Automatic generation of cod_unique from PK columns
    - Dynamic partitioning (event_date or per-table partition keys)
    - Rejects writing with metadata (Parquet format)
    - No manual compaction needed — Delta handles via auto-optimize
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark

    def write(self, df: DataFrame, target: TargetConfig, source: Optional[SourceConfig] = None) -> None:
        """
        Write a validated DataFrame to the Delta table resolved through the
        Glue Data Catalog (``target.database.target.table``).

        Generates ``cod_unique`` from the primary key columns (via
        ``cod_unique_expr`` or by concatenating PK columns with "_"), then
        performs a Delta MERGE resolved by catalog name
        (``DeltaTable.forName``):
          - ``WHEN NOT MATCHED AND op <> 'D' THEN INSERT`` — new records
          - ``WHEN MATCHED AND op = 'D' THEN DELETE`` — CDC deletes
          - ``WHEN MATCHED THEN UPDATE SET *`` — existing records updated

        On the first write the physical Delta table (``_delta_log``) does not
        exist yet, so it is bootstrapped via ``insertInto``; subsequent writes
        perform the Delta MERGE. ``insertInto`` preserves the schema already
        registered in the Glue Data Catalog, whereas ``saveAsTable`` can
        replace that metadata with an invalid placeholder schema.

        Args:
            df: Validated DataFrame to write.
            target: TargetConfig with catalog (database, table),
                    partition_keys, primary_key, cod_unique_expr.
            source: Optional SourceConfig for CDC timestamp resolution.
        """
        if df.isEmpty():
            logger.info("Empty DataFrame — nothing to write for %s", target.table)
            return

        df_with_pk = self._generate_cod_unique(df, target)
        df_to_write = self._prepare_with_partitions(df_with_pk, target, source)
        df_to_write = self._map_cdc_columns(df_to_write, source)

        table_name = f"{target.database}.{target.table}"
        partition_cols = [pk.name for pk in target.partition_keys if pk.name in df_to_write.columns]

        try:
            # Bootstrap check — the Glue Catalog table is only metadata until
            # the first physical write creates _delta_log at its location.
            if not self._is_delta_table(table_name):
                self._bootstrap_table(df_to_write, target, partition_cols, table_name)
                logger.info("Created Delta table %s (bootstrap)", table_name)
                return

            # Resolve by catalog name — the table is registered in the Glue
            # Data Catalog and recognized as a Delta table (provider=delta).
            delta_table = DeltaTable.forName(self._spark, table_name)

            # CDC metadata columns are renamed to the catalog names by
            # _map_cdc_columns. Project only the columns that exist in the
            # target Delta table, keeping the CDC op column (if present)
            # available so the merge can implement delete semantics.
            op_col = CDC_OP_COLUMN if CDC_OP_COLUMN in df_to_write.columns else None
            target_cols = set(delta_table.toDF().columns)
            write_cols = [c for c in df_to_write.columns if c in target_cols]
            merge_cols = write_cols
            if op_col and op_col not in merge_cols:
                merge_cols = merge_cols + [op_col]
            df_to_write = df_to_write.select(*merge_cols)

            merge_condition = "source.cod_unique = target.cod_unique"

            builder = delta_table.alias("target").merge(df_to_write.alias("source"), merge_condition)

            if op_col:
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
                f"Failed to merge into Delta table {table_name}: {exc}"
            ) from exc

    def _is_delta_table(self, table_name: str) -> bool:
        """
        Check whether the catalog table is already a physical Delta table.

        The Glue Catalog table is registered upfront (EXTERNAL_TABLE), but
        the Delta transaction log (``_delta_log``) only exists after the
        first write. ``DeltaTable.forName`` fails until the table is
        recognized as Delta, which is used as the bootstrap signal.
        """
        try:
            DeltaTable.forName(self._spark, table_name)
            return True
        except Exception:  # noqa: BLE001
            return False

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


    @staticmethod
    def _map_cdc_columns(df: DataFrame, source: Optional[SourceConfig]) -> DataFrame:
        """
        Rename CDC metadata columns to the physical catalog column names.

        CDC files carry short names (e.g. ``Op`` / ``dms_timestamp``)
        configured via ``source.cdc_config``, while the Glue Catalog tables
        in the data lakehouse are defined with ``cdc_operation`` /
        ``cdc_timestamp``. Renaming here keeps the Delta table schema
        aligned with the catalog definition.
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
        table_name: str,
    ) -> None:
        """
        Create the physical Delta table registered in the Glue Data Catalog.

        The Glue Catalog table is registered upfront (EXTERNAL_TABLE), but the
        Delta transaction log (``_delta_log``) only exists after the first
        write. On the first batch we write the full load directly via
        ``insertInto`` materializes the Delta table without replacing the
        schema registered in the Glue Data Catalog. The catalog definition
        must remain authoritative for Athena and future Glue job runs.
        """
        df.write.mode("overwrite").insertInto(table_name)

    @staticmethod
    def _generate_cod_unique(df: DataFrame, target: TargetConfig) -> DataFrame:
        """
        Generate the ``cod_unique`` column by concatenating primary key columns.

        Uses ``cod_unique_expr`` from TargetConfig if available, otherwise
        concatenates primary key columns with "_" separator.
        """
        if "cod_unique" in df.columns:
            return df

        expr_config: Optional[Dict[str, Any]] = target.cod_unique_expr
        if expr_config:
            pk_cols = expr_config.get("columns", target.primary_key)
            separator = expr_config.get("separator", "_")
        else:
            pk_cols = target.primary_key
            separator = "_"

        pk_cols_in_df = [c for c in pk_cols if c in df.columns]
        if not pk_cols_in_df:
            logger.warning("No PK columns found in DataFrame for cod_unique generation")
            return df

        return df.withColumn("cod_unique", F.concat_ws(separator, *pk_cols_in_df))

    @staticmethod
    def _prepare_with_partitions(
        df: DataFrame,
        target: TargetConfig,
        source: Optional[SourceConfig] = None,
    ) -> DataFrame:
        """
        Add partition columns required by the target table.

        Each partition key can declare a ``source_column`` to derive its
        value from (e.g. ``event_date`` from ``scheduled_departure``).
        Without one, ``event_date`` is derived from the CDC timestamp column.
        """
        needed = [pk for pk in target.partition_keys if pk.name not in df.columns]
        if not needed:
            return df

        ts_col: Optional[str] = None
        if source and source.cdc_config and source.cdc_config.timestamp_column:
            ts_col = source.cdc_config.timestamp_column

        result = df
        for pk in needed:
            if pk.source_column and pk.source_column in df.columns:
                result = result.withColumn(pk.name, F.to_date(F.col(pk.source_column)))
            elif pk.name == "event_date":
                if ts_col is None or ts_col not in df.columns:
                    logger.warning("No timestamp column found for partition extraction")
                    continue
                result = result.withColumn("event_date", F.to_date(F.col(ts_col)))
            else:
                logger.warning("Unknown partition key '%s' — skipping", pk.name)

        return result
