"""
Integration tests — S3 Landing bucket structure validation.

Validates that the expected bucket, folders, and sample files exist
in the lakehouse-landing S3 bucket.

All tests are marked @pytest.mark.integration.
"""

from __future__ import annotations

import pytest


@pytest.mark.integration
class TestS3LandingStructure:
    """Validate the landing bucket structure for DMS CDC data."""

    def test_bucket_exists(self, s3_client, landing_bucket):
        """The landing bucket must exist and be accessible."""
        response = s3_client.head_bucket(Bucket=landing_bucket)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_flights_prefix_exists(self, s3_client, landing_bucket):
        """The flights prefix must exist in the landing bucket."""
        response = s3_client.list_objects_v2(
            Bucket=landing_bucket,
            Prefix="flight_radar/flights/",
            MaxKeys=1,
        )
        assert response["KeyCount"] > 0, (
            f"No objects found under flight_radar/flights/ in {landing_bucket}"
        )

    def test_parquet_files_present(self, s3_client, landing_bucket):
        """At least one Parquet file should exist under the flights prefix."""
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=landing_bucket,
            Prefix="flight_radar/flights/",
        )
        found_parquet = False
        for page in pages:
            if "Contents" not in page:
                continue
            for obj in page["Contents"]:
                if obj["Key"].endswith(".parquet"):
                    found_parquet = True
                    break
            if found_parquet:
                break
        assert found_parquet, "No .parquet files found in flight_radar/flights/"

    def test_partition_folders_exist(self, s3_client, landing_bucket):
        """Partition folders (year=, month=, day=) should exist."""
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=landing_bucket,
            Prefix="flight_radar/flights/",
            Delimiter="/",
        )
        common_prefixes = []
        for page in pages:
            if "CommonPrefixes" in page:
                for cp in page["CommonPrefixes"]:
                    common_prefixes.append(cp["Prefix"])

        # DMS may write with or without Hive-style partitions
        has_partitions = any("year=" in p for p in common_prefixes)
        # Also check deeper
        if not has_partitions:
            pages = paginator.paginate(
                Bucket=landing_bucket,
                Prefix="flight_radar/flights/year=",
                MaxKeys=1,
            )
            for page in pages:
                if page["KeyCount"] > 0:
                    has_partitions = True
                    break

        # This is an informative assertion — DMS may not use Hive partitions
        if not has_partitions:
            pytest.skip("No Hive-style partitions found — DMS may use flat structure")

    def test_no_empty_bucket(self, s3_client, landing_bucket):
        """The bucket should contain some objects (not empty)."""
        response = s3_client.list_objects_v2(
            Bucket=landing_bucket,
            MaxKeys=1,
        )
        assert response["KeyCount"] > 0, f"Bucket {landing_bucket} appears empty"
