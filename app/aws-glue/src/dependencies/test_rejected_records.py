"""
TestRejectedRecordsGenerator module — Generates test rejected records
for all table schemas to validate the centralized rejected records table.

Used for testing/validation during batch and streaming loads.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .config import SourceConfig, TargetConfig

logger = logging.getLogger(__name__)


class TestRejectedRecordsGenerator:
    """
    Generates test rejected records for all configured table schemas.

    Features:
    - Generates realistic rejected records matching each table's schema
    - Covers various rejection types (null_check, type_cast, enum_check, etc.)
    - Works for both batch and streaming modes
    - Inserts directly into the centralized rejected records table
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark

    def generate_and_write_all(
        self,
        sources: List[SourceConfig],
        execution_id: str,
        reject_reason: str = "TEST_RECORD_GENERATED_FOR_VALIDATION",
    ) -> Dict[str, int]:
        """
        Generate and write test rejected records for all source configurations.

        Args:
            sources: List of SourceConfig with target schemas
            execution_id: Unique execution UUID
            reject_reason: Reason to use for test records

        Returns:
            Dict mapping source name to number of test records generated
        """
        results = {}

        for source in sources:
            target = source.target
            count = self._generate_for_source(source, target, execution_id, reject_reason)
            results[source.source] = count
            logger.info("Generated %d test rejected records for %s", count, source.source)

        return results

    def create_test_dataframe(self, source: SourceConfig) -> DataFrame:
        """Create a test DataFrame for a source that can be passed to the processor pipeline.
        
        This generates test records with valid source types but invalid values,
        suitable for passing through the full DataQuality validation pipeline.
        
        Args:
            source: SourceConfig with target schema
            
        Returns:
            DataFrame with test records (StringType schema, valid for validation pipeline)
        """
        target = source.target
        test_records = self._create_test_records(source, target)
        if not test_records:
            return self._spark.createDataFrame([], self._build_schema_from_target(target, for_test=True))
        
        schema = self._build_schema_from_target(target, for_test=True)
        df = self._spark.createDataFrame(test_records, schema)
        
        # Add test metadata
        test_df = df \
            .withColumn("_reject_rule", F.lit("TEST_GENERATED")) \
            .withColumn("_reject_reason", F.lit("Test record generated for validation")) \
            .withColumn("_reject_timestamp", F.lit(datetime.now(timezone.utc))) \
            .withColumn("_reject_table", F.lit(target.table))
        
        return test_df

    def _generate_for_source(
        self,
        source: SourceConfig,
        target: TargetConfig,
        execution_id: str,
        reject_reason: str,
    ) -> int:
        """Generate test rejected records for a single source."""
        test_records = self._create_test_records(source, target)
        if not test_records:
            return 0

        # Create DataFrame with StringType schema for test records (allows any test values)
        # The validation pipeline will do the actual casting and catch errors
        schema = self._build_schema_from_target(target, for_test=True)
        df = self._spark.createDataFrame(test_records, schema)

        # Add test rejection metadata
        test_df = df \
            .withColumn("_reject_rule", F.lit("TEST_GENERATED")) \
            .withColumn("_reject_reason", F.lit("Test record generated for validation")) \
            .withColumn("_reject_timestamp", F.lit(datetime.now(timezone.utc))) \
            .withColumn("_reject_table", F.lit(target.table))

        # Write using RejectedRecords module
        from .rejected_records import RejectedRecords
        rejected_writer = RejectedRecords(self._spark)

        rejected_writer.write(
            rejected_df=test_df,
            source=source,
            target=target,
            execution_id=execution_id,
            reject_rule="TEST_GENERATED",
            reject_reason=reject_reason,
        )

        return len(test_records)

    def _create_test_records(
        self,
        source: SourceConfig,
        target: TargetConfig,
    ) -> List[Dict]:
        """Create test records with valid source types but invalid values for target validation.

        These records simulate what could exist in the source Parquet:
        - Correct source types (matching source Parquet schema)
        - But semantically invalid values that will fail target validation:
          * null_check: None in required fields
          * type_cast: valid source type but value that fails target cast (out of range, wrong format)
          * enum_check: value not in allowed list
          * duplicate_pk: duplicate primary keys
          * date_range: future dates, malformed timestamps
          * range_check: negative for unsigned, out of bounds
        """
        source_name = source.source.lower()
        records = []
        base_record = self._get_base_record_for_source(source_name)

        # 1. null_check: None in primary key (valid type, null value)
        record1 = base_record.copy()
        pk_fields = target.primary_key
        if pk_fields:
            record1[pk_fields[0]] = None
        record1["_expected_reject_rule"] = "null_check"
        records.append(record1)

        # 2. enum_check: valid string type but invalid enum value
        if target.enum_columns:
            record2 = base_record.copy()
            for enum_field, valid_values in target.enum_columns.items():
                if enum_field in record2:
                    record2[enum_field] = f"INVALID_{enum_field.upper()}"
                    break
            record2["_expected_reject_rule"] = "enum_check"
            records.append(record2)

        # 3. type_cast: valid source type but value that fails target cast
        record3 = base_record.copy()
        type_cast_applied = False
        for field_name, field_info in target.schema.items():
            if field_name in record3:
                if field_info.type in ("bigint", "int", "smallint", "tinyint"):
                    record3[field_name] = 999999999999999999999
                    type_cast_applied = True
                    break
                elif field_info.type in ("double", "float", "decimal"):
                    record3[field_name] = float("inf")
                    type_cast_applied = True
                    break
                elif field_info.type in ("timestamp", "date"):
                    record3[field_name] = "not-a-valid-timestamp"
                    type_cast_applied = True
                    break
        if type_cast_applied:
            record3["_expected_reject_rule"] = "type_cast"
            records.append(record3)

        # 4. duplicate_pk: duplicate primary key
        if pk_fields:
            record4 = base_record.copy()
            record4["_expected_reject_rule"] = "duplicate_pk"
            records.append(record4)

        # 5. range_check: negative value for unsigned field
        record5 = base_record.copy()
        range_applied = False
        for field_name, field_info in target.schema.items():
            if field_name in record5 and field_info.type in ("bigint", "int", "smallint", "tinyint", "double", "float"):
                current = record5.get(field_name, 0)
                if isinstance(current, (int, float)) and current >= 0:
                    record5[field_name] = -abs(current) - 1
                    range_applied = True
                    break
        if range_applied:
            record5["_expected_reject_rule"] = "range_check"
            records.append(record5)

        # 6. future_date: timestamp in the future
        record6 = base_record.copy()
        future_applied = False
        for field_name, field_info in target.schema.items():
            if field_name in record6 and field_info.type in ("timestamp", "date"):
                record6[field_name] = "2099-12-31T23:59:59"
                future_applied = True
                break
        if future_applied:
            record6["_expected_reject_rule"] = "future_date"
            records.append(record6)

        return records[:3]

    def _get_base_record_for_source(self, source_name: str) -> Dict:
        """Get a base valid record template for each source type."""
        base_records = {
            "flights": {
                "flight_id": 999999,
                "flight_number": "TEST999",
                "airline_icao": "TEST",
                "aircraft_icao24": "TEST24",
                "origin_airport": "SBGR",
                "destination_airport": "SBSP",
                "scheduled_departure": "2026-01-15T10:00:00",
                "scheduled_arrival": "2026-01-15T12:00:00",
                "actual_departure": "2026-01-15T10:15:00",
                "actual_arrival": "2026-01-15T12:15:00",
                "status": "active",
                "created_at": "2026-01-15T08:00:00",
                "updated_at": "2026-01-15T08:00:00",
                "cod_unique": "999999",
            },
            "aircraft": {
                "icao24": "TEST24",
                "registration": "PP-TST",
                "manufacturer_icao": "TEST",
                "model": "TEST_MODEL",
                "type_code": "TST",
                "serial_number": "TEST123",
                "line_number": 1,
                "icao_aircraft_type": "TST",
                "operator_icao": "TEST",
                "operator": "Test Airline",
                "built": 2020,
                "first_flight_date": "2020-01-01",
                "seat_configuration": "Y180",
                "engines": "2",
                "engine_type": "Turbofan",
                "status": "active",
            },
            "airports": {
                "icao": "SBTT",
                "iata": "TST",
                "name": "Test Airport",
                "city": "Test City",
                "country": "Brazil",
                "country_code": "BR",
                "latitude": -23.5,
                "longitude": -46.6,
                "elevation": 800,
                "timezone": "America/Sao_Paulo",
                "type": "airport",
            },
            "airlines": {
                "icao": "TST",
                "iata": "TT",
                "name": "Test Airlines",
                "callsign": "TEST",
                "country": "Brazil",
                "country_code": "BR",
                "status": "active",
            },
            "aircraft_positions": {
                "icao24": "TEST24",
                "first_seen": "2026-01-15T10:00:00",
                "est_departure_airport": "SBGR",
                "last_seen": "2026-01-15T12:00:00",
                "est_arrival_airport": "SBSP",
                "callsign": "TEST999",
                "est_departure_airport_horiz_distance": 1000,
                "est_arrival_airport_horiz_distance": 1000,
            },
            "countries": {
                "code": "TT",
                "name": "Test Country",
                "continent": "SA",
                "population": 1000000,
            },
            "aircraft_types": {
                "icao": "TST",
                "manufacturer": "Test Mfg",
                "model": "Test Model",
                "type": "Landplane",
                "engine_type": "Turbofan",
                "engine_count": 2,
                "wake_category": "M",
            },
            "routes": {
                "airline_icao": "TST",
                "airline_iata": "TT",
                "source_airport_icao": "SBGR",
                "source_airport_iata": "GRU",
                "dest_airport_icao": "SBSP",
                "dest_airport_iata": "SP",
                "codeshare": "N",
                "stops": 0,
                "equipment": "TST",
            },
        }

        return base_records.get(source_name, {})

    def _build_schema_from_target(self, target: TargetConfig, nullable: bool = False, for_test: bool = False) -> StructType:
        """Build Spark StructType from target schema definition.

        Args:
            target: TargetConfig with schema definition
            nullable: If True, make all fields nullable (for test records with intentional nulls)
            for_test: If True, use StringType for all fields to allow any test values
        """
        fields = []
        for field_name, field_info in target.schema.items():
            if for_test:
                # For test records: use StringType to allow any value (None, "inf", invalid timestamps, etc.)
                # The validation pipeline will do the actual casting and catch errors
                spark_type = StringType()
                field_nullable = True
            else:
                spark_type = self._map_type(field_info.type)
                field_nullable = True if nullable else field_info.nullable
            fields.append(StructField(
                field_name,
                spark_type,
                field_nullable,
            ))
        return StructType(fields)

    @staticmethod
    def _map_type(type_str: str):
        """Map schema type string to Spark DataType."""
        type_map = {
            "bigint": LongType(),
            "int": LongType(),
            "integer": LongType(),
            "smallint": LongType(),
            "tinyint": LongType(),
            "double": DoubleType(),
            "float": DoubleType(),
            "decimal": DoubleType(),
            "string": StringType(),
            "varchar": StringType(),
            "char": StringType(),
            "timestamp": TimestampType(),
            "date": DateType(),
            "boolean": BooleanType(),
        }
        return type_map.get(type_str.lower(), StringType())


def add_test_rejected_records_arg(parser):
    """Add command-line argument for test rejected records generation."""
    parser.add_argument(
        "--generate-test-rejects",
        action="store_true",
        help="Generate test rejected records for all tables to validate the rejected records table",
    )
    parser.add_argument(
        "--test-reject-reason",
        default="TEST_RECORD_GENERATED_FOR_VALIDATION",
        help="Reason to use for generated test rejected records",
    )