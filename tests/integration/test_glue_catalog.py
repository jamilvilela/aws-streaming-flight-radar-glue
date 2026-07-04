"""
Integration tests — Glue Data Catalog validation.

Validates that the expected databases and tables exist in the Glue Catalog.

All tests are marked @pytest.mark.integration.
"""

from __future__ import annotations

import pytest


@pytest.mark.integration
class TestGlueCatalogDatabases:
    """Validate Glue Catalog databases."""

    def test_db_landing_exists(self, glue_client):
        """The 'db_landing' database must exist."""
        response = glue_client.get_database(Name="db_landing")
        assert response["Database"]["Name"] == "db_landing"

    def test_db_raw_exists(self, glue_client):
        """The 'db_raw' database must exist."""
        response = glue_client.get_database(Name="db_raw")
        assert response["Database"]["Name"] == "db_raw"


@pytest.mark.integration
class TestGlueCatalogTables:
    """Validate Glue Catalog tables in db_landing and db_raw."""

    def test_flights_table_exists_in_landing(self, glue_client):
        """The 'flights' table must exist in db_landing."""
        response = glue_client.get_table(
            DatabaseName="db_landing",
            Name="flights",
        )
        assert response["Table"]["Name"] == "flights"

    def test_flights_has_cdc_params(self, glue_client):
        """The flights table should have CDC-related parameters."""
        response = glue_client.get_table(
            DatabaseName="db_landing",
            Name="flights",
        )
        params = response["Table"].get("Parameters", {})
        cdc_source = params.get("cdc.source", "")
        assert cdc_source == "dms", f"Expected cdc.source=dms, got {cdc_source}"

    def test_flights_has_partitions(self, glue_client):
        """The flights table should be partitioned."""
        response = glue_client.get_table(
            DatabaseName="db_landing",
            Name="flights",
        )
        partition_keys = response["Table"].get("PartitionKeys", [])
        assert len(partition_keys) > 0, "flights table has no partition keys"
        key_names = [pk["Name"] for pk in partition_keys]
        assert "year" in key_names, "year partition not found"
        assert "month" in key_names, "month partition not found"
        assert "day" in key_names, "day partition not found"

    def test_flights_has_columns(self, glue_client):
        """The flights table should have the expected columns."""
        response = glue_client.get_table(
            DatabaseName="db_landing",
            Name="flights",
        )
        sd = response["Table"].get("StorageDescriptor", {})
        columns = sd.get("Columns", [])
        col_names = [c["Name"] for c in columns]

        expected_cols = [
            "flight_id", "airline_code", "flight_number",
            "aircraft_type", "aircraft_registration",
            "origin_airport", "destination_airport",
            "scheduled_departure", "scheduled_arrival",
            "actual_departure", "actual_arrival",
            "status", "created_at", "updated_at",
            "Op", "dms_timestamp",
        ]
        for col in expected_cols:
            assert col in col_names, f"Column '{col}' not found in flights table"

    def test_etl_control_table_exists(self, glue_client):
        """The etl_control table must exist in db_raw."""
        response = glue_client.get_table(
            DatabaseName="db_raw",
            Name="etl_control",
        )
        assert response["Table"]["Name"] == "etl_control"

    def test_data_quality_metrics_table_exists(self, glue_client):
        """The data_quality_metrics table must exist in db_raw."""
        response = glue_client.get_table(
            DatabaseName="db_raw",
            Name="data_quality_metrics",
        )
        assert response["Table"]["Name"] == "data_quality_metrics"
