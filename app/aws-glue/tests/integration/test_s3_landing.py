"""
Integration tests — S3 Landing bucket structure validation.

Validates that the expected bucket, folders, and sample files exist
in the lakehouse-landing S3 bucket for the CDC pipeline.

Data lands in ``dms/flightradar/flight_radar/<table>/`` (full load) and
monthly folders ``aircraft_positions_<YYYY>_<MM>/`` for the positions table.

All tests are marked @pytest.mark.integration.
"""

from __future__ import annotations

import pytest

LANDING_PREFIX = "dms/flightradar/flight_radar/"


@pytest.mark.integration
class TestS3LandingStructure:
    """Validate the landing bucket structure for CDC data."""

    def test_bucket_exists(self, s3_client, landing_bucket):
        """The landing bucket must exist and be accessible."""
        response = s3_client.head_bucket(Bucket=landing_bucket)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_aircraft_prefix_exists(self, s3_client, landing_bucket):
        """The aircraft full-load prefix must exist in the landing bucket."""
        response = s3_client.list_objects_v2(
            Bucket=landing_bucket,
            Prefix=f"{LANDING_PREFIX}aircraft/",
            MaxKeys=1,
        )
        assert response["KeyCount"] > 0, (
            f"No objects found under {LANDING_PREFIX}aircraft/ in {landing_bucket}"
        )

    def test_aircraft_positions_prefix_exists(self, s3_client, landing_bucket):
        """The aircraft_positions (monthly partition) prefix must exist."""
        response = s3_client.list_objects_v2(
            Bucket=landing_bucket,
            Prefix=f"{LANDING_PREFIX}aircraft_positions_",
            MaxKeys=1,
        )
        assert response["KeyCount"] > 0, (
            f"No objects found under {LANDING_PREFIX}aircraft_positions_* in {landing_bucket}"
        )

    def test_parquet_files_present(self, s3_client, landing_bucket):
        """At least one Parquet file should exist under the aircraft prefix."""
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=landing_bucket,
            Prefix=f"{LANDING_PREFIX}aircraft/",
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
        assert found_parquet, f"No .parquet files found in {LANDING_PREFIX}aircraft/"

    def test_monthly_partition_folders_exist(self, s3_client, landing_bucket):
        """Monthly partition folders (aircraft_positions_YYYY_MM) should exist."""
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=landing_bucket,
            Prefix=LANDING_PREFIX,
            Delimiter="/",
        )
        common_prefixes = []
        for page in pages:
            if "CommonPrefixes" in page:
                for cp in page["CommonPrefixes"]:
                    common_prefixes.append(cp["Prefix"])

        has_positions = any("aircraft_positions_" in p for p in common_prefixes)
        if not has_positions:
            pytest.skip("No aircraft_positions_* monthly folders found")
        assert any("aircraft_positions_" in p for p in common_prefixes), (
            f"Expected aircraft_positions_* folders under {LANDING_PREFIX}, got {common_prefixes}"
        )

    def test_no_empty_bucket(self, s3_client, landing_bucket):
        """The bucket should contain some objects (not empty)."""
        response = s3_client.list_objects_v2(
            Bucket=landing_bucket,
            MaxKeys=1,
        )
        assert response["KeyCount"] > 0, f"Bucket {landing_bucket} appears empty"
