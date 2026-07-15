"""
Integration test fixtures — boto3 clients for S3 and Glue.

These tests require real AWS credentials and infrastructure.
They are marked with @pytest.mark.integration and are skipped
by default (use `pytest -m integration` to run).
"""

from __future__ import annotations

import os

import boto3
import pytest


def pytest_configure(config):
    """Register the 'integration' marker."""
    config.addinivalue_line("markers", "integration: marks tests that require real AWS resources")


@pytest.fixture(scope="session")
def account_id():
    """Return the current AWS account ID."""
    return boto3.client("sts").get_caller_identity()["Account"]


@pytest.fixture(scope="session")
def s3_client():
    """Return a boto3 S3 client."""
    return boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))


@pytest.fixture(scope="session")
def glue_client():
    """Return a boto3 Glue client."""
    return boto3.client("glue", region_name=os.getenv("AWS_REGION", "us-east-1"))


@pytest.fixture(scope="session")
def landing_bucket(account_id):
    """Return the landing bucket name."""
    return f"lakehouse-landing-{account_id}"


@pytest.fixture(scope="session")
def raw_bucket(account_id):
    """Return the raw bucket name."""
    return f"lakehouse-raw-{account_id}"


@pytest.fixture(scope="session")
def workspace_bucket(account_id):
    """Return the workspace bucket name."""
    return f"lakehouse-workspace-{account_id}"
