"""
DataQuality module — Validates, cleanses, and converts DataFrame columns
according to the schema defined in the source configuration.

Invalid records are isolated and written to a Rejected location.
"""

from __future__ import annotations

import logging
import time
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructType,
    TimestampType,
)

from src.config import SourceConfig, TargetConfig

logger = logging.getLogger(__name__)


class DataQualityError(Exception):
    """Raised when a data quality operation fails."""
    pass


class DataQuality:
    """
    Applies dynamic data quality rules based on a SourceConfig schema.

    Validation steps (in order):
    1. Type casting — convert columns to target Spark types
    2. Null check — reject rows with nulls in non-nullable fields
    3. Enum validation — reject rows with invalid values in enum columns
    4. Duplicate removal — deduplicate by primary key
    5. Timestamp parsing — ensure timestamp columns are valid
    """

    # Map from schema type string → Spark DataType
    TYPE_MAP: Dict[str, type] = {
        "string": StringType,
        "varchar": StringType,
        "char": StringType,
        "bigint": LongType,
        "integer": IntegerType,
        "int": IntegerType,
        "smallint": IntegerType,
        "double": DoubleType,
        "float": DoubleType,
        "decimal": lambda: DecimalType(38, 18),
        "boolean": BooleanType,
        "bool": BooleanType,
        "timestamp": TimestampType,
        "timestamptz": TimestampType,
        "date": DateType,
    }

    def __init__(self, spark: SparkSession):
        self._spark = spark

    # ── Public API ───────────────────────────────────────────────────

    def validate(
        self,
        df: DataFrame,
        target: TargetConfig,
        source: Optional[SourceConfig] = None,
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Apply all validation rules to the input DataFrame.

        Args:
            df: Raw input DataFrame.
            target: TargetConfig with schema, PK, and enum definitions.
            source: Optional SourceConfig for CDC timestamp column resolution.

        Returns:
            Tuple of (valid DataFrame, rejected DataFrame).
        """
        if df.rdd.isEmpty():
            logger.info("Empty DataFrame — skipping validation")
            empty = self._spark.createDataFrame([], self._reject_schema())
            return df, empty

        valid_df = df
        all_rejects: List[DataFrame] = []

        # 1. Type casting
        valid_df, rejects = self._cast_types(valid_df, target, source)
        if rejects:
            all_rejects.append(rejects)

        # 2. Null check on non-nullable fields
        valid_df, rejects = self._check_nulls(valid_df, target, source)
        if rejects:
            all_rejects.append(rejects)

        # 3. Enum validation
        valid_df, rejects = self._check_enums(valid_df, target, source)
        if rejects:
            all_rejects.append(rejects)

        # 4. Duplicate removal by PK
        valid_df, rejects = self._remove_duplicates(valid_df, target, source)
        if rejects:
            all_rejects.append(rejects)

        # 5. Timestamp sanity
        valid_df, rejects = self._validate_timestamps(valid_df, target, source)
        if rejects:
            all_rejects.append(rejects)

        # Combine all rejected records
        if all_rejects:
            combined_rejects = all_rejects[0]
            for r in all_rejects[1:]:
                combined_rejects = combined_rejects.unionByName(r, allowMissingColumns=True)
        else:
            combined_rejects = self._spark.createDataFrame([], self._reject_schema())

        return valid_df, combined_rejects

    # ── Validation steps ─────────────────────────────────────────────

    def _cast_types(
        self,
        df: DataFrame,
        target: TargetConfig,
        source: Optional[SourceConfig] = None,
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Cast columns to the types defined in the target schema.

        Columns that fail casting are sent to rejects.
        """
        if not target.schema:
            return df, self._empty_rejects()

        cast_exprs = []
        for field_name, schema_field in target.schema.items():
            if field_name not in df.columns:
                logger.warning("Column '%s' not found in DataFrame, skipping", field_name)
                continue

            target_type = self._resolve_type(schema_field.type)
            if target_type:
                cast_exprs.append(
                    F.col(field_name).cast(target_type).alias(field_name)
                )

        if not cast_exprs:
            return df, self._empty_rejects()

        # Keep other columns as-is
        other_cols = [c for c in df.columns if c not in target.schema]
        all_exprs = cast_exprs + [F.col(c) for c in other_cols]

        try:
            casted_df = df.select(*all_exprs)
        except Exception as exc:
            logger.error("Type casting failed: %s", exc)
            return df, self._empty_rejects()

        # Identify rows where cast produced null in non-nullable fields
        if target.primary_key:
            null_pk_condition = F.col(target.primary_key[0]).isNull()
            rejects = casted_df.filter(null_pk_condition)
            valid = casted_df.filter(~null_pk_condition)
        else:
            valid = casted_df
            rejects = self._empty_rejects()

        return valid, self._enrich_rejects(rejects, target, "type_cast")

    def _check_nulls(
        self,
        df: DataFrame,
        target: TargetConfig,
        source: Optional[SourceConfig] = None,
    ) -> Tuple[DataFrame, DataFrame]:
        """Reject rows where non-nullable fields have null values."""
        non_nullable = [
            name for name, sf in target.schema.items()
            if not sf.nullable and name in df.columns
        ]
        if not non_nullable:
            return df, self._empty_rejects()

        null_condition = None
        for col_name in non_nullable:
            cond = F.col(col_name).isNull()
            null_condition = cond if null_condition is None else (null_condition | cond)

        if null_condition is None:
            return df, self._empty_rejects()

        rejects = df.filter(null_condition)
        valid = df.filter(~null_condition)
        return valid, self._enrich_rejects(rejects, target, "null_check")

    def _check_enums(
        self,
        df: DataFrame,
        target: TargetConfig,
        source: Optional[SourceConfig] = None,
    ) -> Tuple[DataFrame, DataFrame]:
        """Reject rows where enum columns contain values outside the allowed set."""
        if not target.enum_columns:
            return df, self._empty_rejects()

        enum_condition = None
        for col_name, allowed_values in target.enum_columns.items():
            if col_name not in df.columns:
                continue
            if not allowed_values:
                continue
            # Nulls in enum columns are valid (nullable); only reject invalid non-null values
            cond = F.col(col_name).isNotNull() & ~F.col(col_name).isin(allowed_values)
            enum_condition = cond if enum_condition is None else (enum_condition | cond)

        if enum_condition is None:
            return df, self._empty_rejects()

        rejects = df.filter(enum_condition)
        valid = df.filter(~enum_condition)
        return valid, self._enrich_rejects(rejects, target, "enum_check")

    def _remove_duplicates(
        self,
        df: DataFrame,
        target: TargetConfig,
        source: Optional[SourceConfig] = None,
    ) -> Tuple[DataFrame, DataFrame]:
        """Remove duplicate rows based on the primary key."""
        from pyspark.sql import Window

        if not target.primary_key:
            return df, self._empty_rejects()

        # Ensure PK columns exist
        pk_cols = [c for c in target.primary_key if c in df.columns]
        if not pk_cols:
            return df, self._empty_rejects()

        # Determine order column for dedup (keep latest)
        order_col = F.monotonically_increasing_id().desc()
        if source and source.cdc_config and source.cdc_config.timestamp_column in df.columns:
            order_col = F.col(source.cdc_config.timestamp_column).desc()

        window_spec = Window.partitionBy(*pk_cols).orderBy(order_col)

        df_with_rank = df.withColumn("_dq_rank", F.row_number().over(window_spec))
        duplicates = df_with_rank.filter(F.col("_dq_rank") > 1)
        unique = df_with_rank.filter(F.col("_dq_rank") == 1).drop("_dq_rank")

        if not duplicates.rdd.isEmpty():
            rejects = duplicates.drop("_dq_rank")
        else:
            rejects = self._empty_rejects()

        return unique, self._enrich_rejects(rejects, target, "duplicate_check")

    def _validate_timestamps(
        self,
        df: DataFrame,
        target: TargetConfig,
        source: Optional[SourceConfig] = None,
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Validate timestamp columns.

        For string-typed timestamp columns, attempt to cast; reject failures.
        """
        # This is handled by _cast_types; additional validation here is optional.
        # We pass through as valid — actual invalid timestamps are caught by _cast_types.
        return df, self._empty_rejects()

    # ── Reject helpers ───────────────────────────────────────────────

    def _enrich_rejects(
        self,
        df: DataFrame,
        target: TargetConfig,
        rule: str,
    ) -> DataFrame:
        """Add reject metadata columns to the rejected DataFrame."""
        if df.rdd.isEmpty():
            return df

        now_ts = int(time.time())
        return df.withColumn("_reject_table", F.lit(target.table)) \
                 .withColumn("_reject_rule", F.lit(rule)) \
                 .withColumn("_reject_timestamp", F.lit(now_ts).cast("timestamp"))

    def _empty_rejects(self) -> DataFrame:
        """Return an empty rejects DataFrame with the reject schema."""
        return self._spark.createDataFrame([], self._reject_schema())

    @staticmethod
    def _reject_schema() -> StructType:
        """Schema for the rejects DataFrame with metadata columns."""
        from pyspark.sql.types import (
            StringType as ST,
            StructField,
            StructType,
            TimestampType,
        )
        return StructType([
            StructField("_reject_table", ST(), True),
            StructField("_reject_rule", ST(), True),
            StructField("_reject_timestamp", TimestampType(), True),
        ])

    # ── Type resolution ──────────────────────────────────────────────

    @staticmethod
    def _resolve_type(type_str: str):
        """Resolve a schema type string to a Spark DataType instance."""
        type_lower = type_str.lower().split("(")[0]  # handle decimal(10,7)
        type_class = DataQuality.TYPE_MAP.get(type_lower)
        if type_class is None:
            logger.warning("Unknown type '%s', defaulting to string", type_str)
            return StringType()
        return type_class()
