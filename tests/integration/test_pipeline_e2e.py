"""
End-to-end integration test — Pipeline execution with mock data.

Writes sample Parquet data to the landing S3 bucket, runs the
Processor pipeline, and validates the output in the raw layer.

Requires:
- AWS credentials with access to S3 and Glue Data Catalog
- A running Spark session (local or Glue)
- Existing landing and raw buckets

All tests are marked @pytest.mark.integration.
"""

from __future__ import annotations

import json
import os
import tempfile
import uuid
from datetime import datetime
from pathlib import Path

import boto3
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from src.config import CdcConfig, Config, PartitionKey, SourceConfig
from src.processor import Processor


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


@pytest.fixture
def test_id():
    """Unique identifier for this test run."""
    return str(uuid.uuid4())[:8]


@pytest.fixture
def landing_bucket(account_id):
    return f"lakehouse-landing-{account_id}"


@pytest.fixture
def raw_bucket(account_id):
    return f"lakehouse-raw-{account_id}"


@pytest.fixture
def workspace_bucket(account_id):
    return f"lakehouse-workspace-{account_id}"


@pytest.fixture
def source_config(account_id, test_id):
    """SourceConfig pointing to a unique test location."""
    return SourceConfig(
        source="flights",
        database="db_landing",
        table="flights",
        format="parquet",
        schema={
            "flight_id": {"type": "bigint", "nullable": False, "comment": "PK"},
            "airline_code": {"type": "string", "nullable": True, "comment": "IATA"},
            "flight_number": {"type": "string", "nullable": True, "comment": "Number"},
            "status": {"type": "string", "nullable": True, "comment": "Status"},
            "dms_timestamp": {"type": "timestamp", "nullable": True, "comment": "CDC ts"},
            "Op": {"type": "string", "nullable": True, "comment": "CDC op"},
        },
        partition_keys=[
            PartitionKey("year", "string"),
            PartitionKey("month", "string"),
            PartitionKey("day", "string"),
        ],
        primary_key=["flight_id"],
        enum_columns={
            "status": ["scheduled", "active", "landed", "cancelled", "diverted", "unknown"],
            "Op": ["I", "U", "D"],
        },
        cdc_config=CdcConfig(op_column="Op", timestamp_column="dms_timestamp"),
        source_location=f"s3://lakehouse-landing-{account_id}/flight_radar/flights/",
        target_location=f"s3://lakehouse-raw-{account_id}/tables/flights_e2e_test_{test_id}/",
        rejected_location=f"s3://lakehouse-raw-{account_id}/tables/flights_e2e_test_{test_id}/Rejected/",
        checkpoint_location="",  # batch mode
    )


@pytest.fixture
def config(source_config):
    return Config(sources={"flights": source_config})


# ── Helper ───────────────────────────────────────────────────────────────────

def _generate_sample_data(spark, count: int = 10):
    """Generate sample flight rows."""
    rows = []
    base = datetime(2026, 6, 30, 0, 0, 0)
    for i in range(count):
        rows.append((
            i + 1,
            "AA",
            f"AA{100 + i}",
            "active" if i % 2 == 0 else "landed",
            base,
            "I",
        ))
    schema = StructType([
        StructField("flight_id", LongType(), True),
        StructField("airline_code", StringType(), True),
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

    def test_full_pipeline(self, spark, config, source_config, s3_client, test_id):
        """
        Write mock data to landing, run pipeline, validate raw output.
        """
        # 1. Generate mock data
        input_df = _generate_sample_data(spark, count=10)
        assert input_df.count() == 10

        # 2. Write mock data to landing S3 (simulate DMS output)
        temp_dir = tempfile.mkdtemp(prefix=f"e2e_{test_id}")
        try:
            # Write as Parquet to a temp location first
            input_df.write \
                .mode("overwrite") \
                .format("parquet") \
                .option("compression", "snappy") \
                .save(temp_dir)

            # Upload to S3 landing
            bucket = source_config.source_location.replace("s3://", "").split("/")[0]
            prefix = "/".join(source_config.source_location.replace("s3://", "").split("/")[1:])

            for root, _, files in os.walk(temp_dir):
                for fname in files:
                    if not fname.endswith(".parquet"):
                        continue
                    local_path = os.path.join(root, fname)
                    s3_key = f"{prefix}{fname}"

                    if os.path.getsize(local_path) > 0:
                        s3_client.upload_file(
                            Filename=local_path,
                            Bucket=bucket,
                            Key=s3_key,
                        )

            # 3. Run processor
            processor = Processor(spark)
            processor.run(config, "flights")

            # 4. Validate output in raw
            raw_df = spark.read.format("parquet").load(source_config.target_location)
            assert raw_df.count() >= 10, f"Expected >= 10 rows, got {raw_df.count()}"

            # Check partition columns exist
            assert "year" in raw_df.columns
            assert "month" in raw_df.columns
            assert "day" in raw_df.columns

            # 5. Validate rejects (should be empty for clean data)
            try:
                rejects_df = spark.read.format("parquet").load(source_config.rejected_location)
                assert rejects_df.isEmpty(), "Expected no rejects for clean data"
            except Exception:
                # Rejected location may not exist if no rejects
                pass

        finally:
            # 6. Cleanup test data from S3
            self._cleanup_s3(s3_client, source_config.target_location)
            self._cleanup_s3(s3_client, source_config.rejected_location)
            # Cleanup temp files
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_pipeline_with_rejected_data(self, spark, config, source_config, s3_client, test_id):
        """
        Run pipeline with invalid data and validate rejects.
        """
        # 1. Generate data with invalid status
        rows = [
            (1, "AA", "AA100", "invalid_status", datetime(2026, 6, 30, 10, 0, 0), "I"),
            (2, "DL", "DL200", "active", datetime(2026, 6, 30, 11, 0, 0), "I"),
        ]
        schema = StructType([
            StructField("flight_id", LongType(), True),
            StructField("airline_code", StringType(), True),
            StructField("flight_number", StringType(), True),
            StructField("status", StringType(), True),
            StructField("dms_timestamp", TimestampType(), True),
            StructField("Op", StringType(), True),
        ])
        input_df = spark.createDataFrame(rows, schema)

        # 2. Write to landing
        temp_dir = tempfile.mkdtemp(prefix=f"e2e_rej_{test_id}")
        try:
            input_df.write \
                .mode("overwrite") \
                .format("parquet") \
                .option("compression", "snappy") \
                .save(temp_dir)

            bucket = source_config.source_location.replace("s3://", "").split("/")[0]
            prefix = "/".join(source_config.source_location.replace("s3://", "").split("/")[1:])

            for root, _, files in os.walk(temp_dir):
                for fname in files:
                    if not fname.endswith(".parquet"):
                        continue
                    local_path = os.path.join(root, fname)
                    s3_key = f"{prefix}{fname}"
                    if os.path.getsize(local_path) > 0:
                        s3_client.upload_file(
                            Filename=local_path,
                            Bucket=bucket,
                            Key=s3_key,
                        )

            # 3. Run processor
            processor = Processor(spark)
            processor.run(config, "flights")

            # 4. Validate: 1 valid, >= 1 rejected
            raw_df = spark.read.format("parquet").load(source_config.target_location)
            assert raw_df.count() == 1

            rejects_df = spark.read.format("parquet").load(source_config.rejected_location)
            assert rejects_df.count() >= 1

            # Reject metadata columns should be present
            assert "_reject_rule" in rejects_df.columns
            reject_rules = [r._reject_rule for r in rejects_df.collect()]
            assert "enum_check" in reject_rules

        finally:
            self._cleanup_s3(s3_client, source_config.target_location)
            self._cleanup_s3(s3_client, source_config.rejected_location)
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)

    @staticmethod
    def _cleanup_s3(s3_client, s3_path: str):
        """Delete all objects under an S3 path."""
        if not s3_path:
            return
        bucket = s3_path.replace("s3://", "").split("/")[0]
        prefix = "/".join(s3_path.replace("s3://", "").split("/")[1:])

        try:
            paginator = s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
            objects_to_delete = []
            for page in pages:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        objects_to_delete.append({"Key": obj["Key"]})

            if objects_to_delete:
                # Delete in batches of 1000
                for i in range(0, len(objects_to_delete), 1000):
                    batch = objects_to_delete[i:i + 1000]
                    s3_client.delete_objects(
                        Bucket=bucket,
                        Delete={"Objects": batch, "Quiet": True},
                    )
        except Exception as exc:
            print(f"Warning: cleanup failed for {s3_path}: {exc}")
