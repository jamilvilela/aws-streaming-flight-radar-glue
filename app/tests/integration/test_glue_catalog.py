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

    def test_db_raw_exists(self, glue_client):
        """The 'db_raw' database must exist."""
        response = glue_client.get_database(Name="db_raw")
        assert response["Database"]["Name"] == "db_raw"


@pytest.mark.integration
class TestGlueCatalogTables:
    """Validate Glue Catalog tables in db_raw."""

    def test_tbl_flights_exists(self, glue_client):
        """The 'tbl_flights' table must exist in db_raw."""
        response = glue_client.get_table(
            DatabaseName="db_raw",
            Name="tbl_flights",
        )
        assert response["Table"]["Name"] == "tbl_flights"

    def test_tbl_flights_has_columns(self, glue_client):
        """The tbl_flights table should have the expected columns."""
        response = glue_client.get_table(
            DatabaseName="db_raw",
            Name="tbl_flights",
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
