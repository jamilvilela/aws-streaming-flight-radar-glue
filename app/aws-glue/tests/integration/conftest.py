"""
Integration test fixtures — boto3 clients for S3 and Glue.

These tests require real AWS credentials and infrastructure.
They are marked with @pytest.mark.integration and are skipped
by default (use `pytest -m integration` to run).

Before creating any client, the current identity assumes the Lake
Formation admin role (datalake-admins-lf-role), which holds the
LF-TBAC grants needed by the Glue Catalog tests. Override the role
with the AWS_IT_ROLE_ARN environment variable if needed.
"""

from __future__ import annotations

import os

import boto3
import pytest


def pytest_configure(config):
    """Register the 'integration' marker."""
    config.addinivalue_line("markers", "integration: marks tests that require real AWS resources")


@pytest.fixture(scope="session")
def aws_session():
    """Assume the LF admin role and return a boto3 Session with it.

    Temporary credentials are also exported to os.environ so that code
    paths creating their own clients (or Spark reading env vars) reuse
    them instead of the base identity.
    """
    region = os.getenv("AWS_REGION", "us-east-1")
    sts = boto3.client("sts", region_name=region)
    identity = sts.get_caller_identity()
    role_name = os.getenv("AWS_IT_ROLE_NAME", "datalake-admins-lf-role")

    if f":assumed-role/{role_name}/" in identity["Arn"]:
        # Already running as the target role — reuse current credentials.
        return boto3.Session(region_name=region)

    role_arn = os.getenv(
        "AWS_IT_ROLE_ARN",
        f"arn:aws:iam::{identity['Account']}:role/{role_name}",
    )
    response = sts.assume_role(RoleArn=role_arn, RoleSessionName="pytest-integration")
    creds = response["Credentials"]
    os.environ["AWS_ACCESS_KEY_ID"] = creds["AccessKeyId"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = creds["SecretAccessKey"]
    os.environ["AWS_SESSION_TOKEN"] = creds["SessionToken"]
    return boto3.Session(
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
        region_name=region,
    )


@pytest.fixture(scope="session")
def account_id(aws_session):
    """Return the current AWS account ID."""
    return aws_session.client("sts").get_caller_identity()["Account"]


@pytest.fixture(scope="session")
def s3_client(aws_session):
    """Return a boto3 S3 client."""
    return aws_session.client("s3")


@pytest.fixture(scope="session")
def glue_client(aws_session):
    """Return a boto3 Glue client."""
    return aws_session.client("glue")


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
