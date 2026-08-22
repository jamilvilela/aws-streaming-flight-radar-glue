"""Shared fixtures for the unit test suite."""

import pytest


@pytest.fixture(scope="session")
def spark():
    """
    Provide a real local SparkSession.

    Creating a SparkSession requires a JVM (Java 17/21) and winutils.exe on
    Windows. When those are unavailable the fixture skips the test cleanly
    instead of erroring, so the suite stays green on machines without a JVM
    while still running the real Spark logic in CI/Glue.
    """
    try:
        from pyspark.sql import SparkSession

        return (
            SparkSession.builder
            .master("local[2]")
            .appName("unit-tests")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
    except Exception as exc:
        pytest.skip(f"SparkSession unavailable (JVM not installed?): {exc}")