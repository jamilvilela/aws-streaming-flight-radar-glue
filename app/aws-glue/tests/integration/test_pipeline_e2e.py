"""
End-to-end integration test — Pipeline execution with mock data.

Runs the Processor in batch mode with a pre-read in-memory DataFrame
and validates the Delta output in the raw layer.

Requires:
- AWS credentials with access to S3 and Glue Data Catalog
- A running Spark session (local or Glue) with the Delta Lake package
- Existing landing/raw buckets and the db_raw.tbl_flights catalog table

All tests are marked @pytest.mark.integration.
"""

from __future__ import annotations

from datetime import datetime

import boto3
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from src.dependencies.config import CdcConfig, PartitionKey, SchemaField, SourceConfig, TargetConfig
from src.dependencies.processor import Processor


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for the E2E test."""
    return SparkSession.builder \
        .master("local[2]") \
        .appName("test-pipeline-e2e") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()


@pytest.fixture(scope="session")
def account_id():
    return boto3.client("sts").get_caller_identity()["Account"]


@pytest.fixture(scope="session")
def raw_bucket(account_id):
    return f"lakehouse-raw-{account_id}"


@pytest.fixture
def flights_source(account_id, raw_bucket):
    """SourceConfig with an embedded Delta TargetConfig (db_raw.tbl_flights)."""
    target = TargetConfig(
        catalog={"database": "db_raw", "table": "tbl_flights"},
        location=f"s3://{raw_bucket}/tables/tbl_flights/",
        rejected_location=f"s3://{raw_bucket}/tables/tbl_flights/Rejected/",
        format="delta",
        compression="snappy",
        partition_keys=[PartitionKey("event_date", "date")],
        schema={
            "flight_id": SchemaField(type="bigint", nullable=False, comment="PK"),
            "airline_icao": SchemaField(type="string", nullable=True, comment="ICAO"),
            "flight_number": SchemaField(type="string", nullable=True, comment="Number"),
            "status": SchemaField(type="string", nullable=True, comment="Status"),
            "dms_timestamp": SchemaField(type="timestamp", nullable=True, comment="CDC ts"),
            "Op": SchemaField(type="string", nullable=True, comment="CDC op"),
        },
        primary_key=["flight_id"],
        cod_unico_expr={"columns": ["flight_id"], "separator": "_"},
        enum_columns={
            "status": ["scheduled", "active", "landed", "cancelled", "diverted", "unknown"],
            "Op": ["I", "U", "D"],
        },
    )
    return SourceConfig(
        source="flights",
        format="parquet",
        cdc_config=CdcConfig(op_column="Op", timestamp_column="dms_timestamp"),
        source_location=f"s3://lakehouse-landing-{account_id}/dms/flightradar/flight_radar/flights/",
        checkpoint_location="",  # batch mode
        target=target,
    )


# ── Helper ───────────────────────────────────────────────────────────────────

def _generate_sample_data(spark, count: int = 10):
    """Generate sample flight rows."""
    rows = []
    base = datetime(2026, 6, 30, 0, 0, 0)
    for i in range(count):
        rows.append((
            i + 1,
            "UAL",
            f"UA{100 + i}",
            "active" if i % 2 == 0 else "landed",
            base,
            "I",
        ))
    schema = StructType([
        StructField("flight_id", LongType(), True),
        StructField("airline_icao", StringType(), True),
        StructField("flight_number", StringType(), True),
        StructField("status", StringType(), True),
        StructField("dms_timestamp", TimestampType(), True),
        StructField("Op", StringType(), True),
    ])
    return spark.createDataFrame(rows, schema)


# ── Tests ────────────────────────────────────────────────────────────────────

@pytest.mark.integration
class TestPipelineE2E:
    """End-to-end pipeline test with mock data."""

    def test_full_pipeline(self, spark, flights_source):
        """Run the processor in batch mode and validate the Delta output."""
        input_df = _generate_sample_data(spark, count=10)
        assert input_df.count() == 10

        processor = Processor(spark)
        processor.run(flights_source, flights_source.target, mode="batch", dataframe=input_df)

        raw_df = spark.read.format("delta").load(flights_source.target.location)
        assert raw_df.count() >= 10, f"Expected >= 10 rows, got {raw_df.count()}"

        assert "event_date" in raw_df.columns
        assert "cod_unico" in raw_df.columns

    def test_pipeline_with_rejected_data(self, spark, flights_source):
        """Invalid rows should be rejected and never reach the Delta table."""
        rows = [
            (1, "UAL", "UA100", "invalid_status", datetime(2026, 6, 30, 10, 0, 0), "I"),
            (2, "DAL", "DL200", "active", datetime(2026, 6, 30, 11, 0, 0), "I"),
        ]
        schema = StructType([
            StructField("flight_id", LongType(), True),
            StructField("airline_icao", StringType(), True),
            StructField("flight_number", StringType(), True),
            StructField("status", StringType(), True),
            StructField("dms_timestamp", TimestampType(), True),
            StructField("Op", StringType(), True),
        ])
        input_df = spark.createDataFrame(rows, schema)

        processor = Processor(spark)
        processor.run(flights_source, flights_source.target, mode="batch", dataframe=input_df)

        raw_df = spark.read.format("delta").load(flights_source.target.location)
        statuses = [r.status for r in raw_df.select("status").distinct().collect()]
        assert "invalid_status" not in statuses, (
            "Invalid status leaked into the Delta table — rejection failed"
        )